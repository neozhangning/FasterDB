package fasterDB.store;

import fasterDB.vo.Config;
import fasterDB.vo.InitializingBean;
import fasterDB.vo.PageFaultException;
import fasterDB.util.FileSystemUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zn on 15/4/18.
 */
public class MappedStorage implements InitializingBean {

    /**
     * Metadata
     */
    private static final int METADATA_SIZE = 1;
    private static final int STATE_INDEX = 0;   // the index of state byte in metadata
    private static final byte VALID = 1;        // state of the page
    private static final byte INVALID = 2;      // state of the page
    /**
     * File
     */
    private final String parentPath;
    private final String file;
    private final boolean delIfExist;

    /**
     * Page size
     */
    private final int pageSize;         // include the metadata
    private final int actualPageSize;   // not include the metadata
    private final int pageSizeShift;

    /**
     * Region size
     */
    private static final int regionSizeShift = 30;
    private static final int regionSize = 1 << regionSizeShift;

    /**
     * Page count per region
     */
    private final int pageCountInRegionShift;

    /**
     * Invalid regions
     */
    private List<MappedByteBuffer> regions;

    /**
     * Max region index in regions list
     */
    private volatile long maxRegion;

    /**
     * File storage used for regions
     */
    private FileChannel fileChannel;

    /**
     * Create instance
     * @param config
     * @param delIfExist
     * @throws IOException
     */
    public MappedStorage(Config config, boolean delIfExist) {
        String parentPath = config.getDataPath();
        String file = config.getDataFile();
        int pageSize = config.getPageSize() + METADATA_SIZE;
        int shift = 31 - Integer.numberOfLeadingZeros(pageSize);
        this.pageSizeShift = (1 << shift) == pageSize ? shift : shift + 1;
        this.pageSize = 1 << pageSizeShift;
        this.actualPageSize = this.pageSize - METADATA_SIZE;
        if (this.pageSize > this.regionSize) {
            throw new IllegalArgumentException("param pageSize should less than " + (this.actualPageSize));
        }
        config.setPageSize(actualPageSize);
        this.pageCountInRegionShift = this.regionSizeShift - this.pageSizeShift;
        this.parentPath = parentPath;
        this.file = file;
        this.delIfExist = delIfExist;
    }

    @Override
    public void initialize() throws IOException {
        restoreRegions();
    }

    /**
     * Forces any changes made to this to be written to the storage device
     */
    public synchronized void flush() {
        for (MappedByteBuffer region : regions) {
            region.force();
        }
    }

    /**
     * Set the page to valid
     * @param pageId
     * @throws IOException
     */
    public void valid(int pageId) throws IOException {
        if (pageId < 0) {
            throw new IllegalArgumentException("param pageId should >= 0");
        }
        ByteBuffer page = null;
        try {
            page = getOrCreatePage(pageId, true);
        } catch (PageFaultException e) {}
        FileLock lock = null;
        try {
            lock = lock(pageId);
            page.put(STATE_INDEX, VALID);
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }

    /**
     * Set the page to invalid
     * @param pageId
     * @throws IOException
     */
    public void invalid(int pageId) throws IOException {
        if (pageId < 0) {
            throw new IllegalArgumentException("param pageId should >= 0");
        }
        ByteBuffer page = null;
        try {
            page = getOrCreatePage(pageId, true);
        } catch (PageFaultException e) {}
        FileLock lock = null;
        try {
            lock = lock(pageId);
            page.put(STATE_INDEX, INVALID);
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }

    /**
     * Copy page content to dst
     * @param pageId
     * @param dst
     * @return  true valid false invalid
     * @throws IOException
     */
    public boolean getPage(int pageId, byte[] dst) throws IOException, PageFaultException {
        if (pageId < 0) {
            throw new IllegalArgumentException("param pageId should >= 0");
        }
        if (dst == null || dst.length < actualPageSize) {
            throw new IllegalArgumentException("param dst is null or length < " + actualPageSize);
        }

        ByteBuffer page = getOrCreatePage(pageId, false);
        page.position(METADATA_SIZE);
        FileLock lock = null;
        try {
            lock = lock(pageId);
            page.get(dst, 0, dst.length);
            byte state = page.get(STATE_INDEX);
            if (state == VALID) {
                return true;
            } else if (state == INVALID) {
                return false;
            } else {
                throw new PageFaultException("page not exist");
            }
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }

    /**
     * Copy bytes to page located by pageId
     * @param pageId
     * @param bytes
     * @param setInvalidBefore whether set the page to invalid before setPage or not
     * @throws IOException
     */
    public void setPage(int pageId, byte[] bytes, boolean setInvalidBefore) throws IOException {
        if (pageId < 0) {
            throw new IllegalArgumentException("param pageId should >= 0");
        }
        if (bytes == null || bytes.length == 0) {
            return;
        }
        if (bytes.length > actualPageSize) {
            throw new IllegalArgumentException("bytes.length should <= " + actualPageSize);
        }

        ByteBuffer page = null;
        try {
            page = getOrCreatePage(pageId, true);
        } catch (PageFaultException e) {}
        page.position(METADATA_SIZE);
        FileLock lock = null;
        try {
            lock = lock(pageId);
            if (setInvalidBefore) {
                page.put(STATE_INDEX, INVALID);
            }
            page.put(bytes);
            page.put(STATE_INDEX, VALID);
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }

    private ByteBuffer getOrCreatePage(int pageId, boolean createRegionIfAbsent) throws PageFaultException {
        int regionId = pageId >>> pageCountInRegionShift;
        if (regionId > maxRegion) {
            if (createRegionIfAbsent) {
                synchronized (this) {
                    while (regionId > maxRegion) {
                        long currentRegion = maxRegion + 1;
                        MappedByteBuffer region = buildRegion(currentRegion);
                        regions.add(region);
                        maxRegion = currentRegion;
                    }
                }
            } else {
                throw new PageFaultException("page not exist");
            }
        }
        int offsetInRegion = (pageId - (regionId << pageCountInRegionShift)) << pageSizeShift;
        ByteBuffer region = regions.get(regionId).duplicate();
        region.position(offsetInRegion).limit(offsetInRegion + pageSize);
        return region.slice();
    }

    private void restoreRegions() throws IOException {
        regions = new ArrayList<MappedByteBuffer>();
        fileChannel = FileSystemUtil.prepareChannel(parentPath, file, delIfExist, FileSystemUtil.MODE.READ_WRITE);
        long length = fileChannel.size();
        for (long region = 0; (region << regionSizeShift) < length; region++) {
            regions.add(buildRegion(region));
        }
        maxRegion = regions.size() - 1;
    }

    private MappedByteBuffer buildRegion(long regionId) {
        try {
            return fileChannel.map(FileChannel.MapMode.READ_WRITE, regionId << regionSizeShift, regionSize);
        } catch (IOException e) {
            throw new RuntimeException("map file fail", e);
        }
    }

    private FileLock lock(long pageId) {
        for (;;) {
            try {
                return fileChannel.lock(pageId, 1, false);
            } catch (OverlappingFileLockException e) {
                Thread.yield();
            } catch (Throwable cause) {
                throw new RuntimeException("lock file fail", cause);
            }
        }
    }
}