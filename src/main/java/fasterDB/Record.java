package fasterDB;

import fasterDB.store.MappedStorage;
import fasterDB.util.ByteUtil;
import fasterDB.vo.PageFaultException;
import fasterDB.vo.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Created by zn on 15/5/3.
 *
 * Structure on first page
 *
 * |   isFirstPage    |        nextPageId         |     pageCount     |     keyLength     |    valueLength     |     key    |   value    |
 * | 1 bit (unsigned) | 7 bit + 3 byte (unsigned) | 2 byte (unsigned) | 1 byte (unsigned) |  4 bytes (signed)  |  keyLength | some bytes |
 *
 * Structure on other page
 *
 * |   isFirstPage    |        nextPageId         |    value    |
 * | 1 bit (unsigned) | 7 bit + 3 byte (unsigned) | rest bytes  |
 *
 */
public class Record {
    protected enum OP {
        GET(0), UPDATE(1), DEL(2);

        byte[] code;

        OP(int code) {
            this.code = new byte[] {(byte) code};
        }

        static OP codeOf(int code) {
            switch (code) {
                case 0: return GET;
                case 1: return UPDATE;
                case 2: return DEL;
            }
            return null;
        }
    }

    private static final int IS_FIRST_PAGE_INDEX = 0;
    private static final int NEXT_PAGE_ID_INDEX = 0;
    private static final int PAGE_COUNT_INDEX = 4;
    private static final int KEY_LENGTH_INDEX = 6;
    private static final int VALUE_LENGTH_INDEX = 7;
    private static final int FIRST_PAGE_DATA_INDEX = 11;
    private static final int OTHER_PAGE_DATA_INDEX = 4;

    private static final int MAX_PAGE_COUNT_PER_RECORD = (1 << 16) - 1;
    private static final int MAX_KEY_LENGTH = (1 << 8) - 1;
    private static final int MAX_VALUE_LENGTH = Integer.MAX_VALUE;

    private static final byte IS_FIRST_PAGE_MASK = (byte) 0x80;
    private static final byte IS_NOT_FIRST_PAGE_MASK = (byte) 0x7f;
    private static final int NEXT_PAGE_ID_MASK = 0x7fffffff;

    private static final int DEL_MASK = 0x80000000;
    private static final int UPDATE_MASK = 0x40000000;
    private static final int REF_MASK = 0x3fffffff;

    private int[] pageIds;
    private int flag;

    private Record(int[] pageIds) {
        this.pageIds = pageIds;
    }

    public synchronized void lock(OP op) throws InterruptedException {
        switch (op) {
            case GET:
                for (;;) {
                    if ((flag & UPDATE_MASK) == UPDATE_MASK
                            || (flag & DEL_MASK) == DEL_MASK) {
                        wait();
                    } else {
                        flag++;
                        return;
                    }
                }
            case UPDATE:
                for (;;) {
                    if ((flag & REF_MASK) != 0
                            || (flag & DEL_MASK) == DEL_MASK) {
                        wait();
                    } else {
                        flag |= UPDATE_MASK;
                        return;
                    }
                }
            case DEL:
                for (;;) {
                    if ((flag & REF_MASK) != 0
                            || (flag & UPDATE_MASK) == UPDATE_MASK) {
                        wait();
                    } else {
                        flag |= DEL_MASK;
                        return;
                    }
                }
        }
    }

    public synchronized void unlock(OP op) {
        switch (op) {
            case GET:
                flag--;
                break;
            case UPDATE:
                flag |= UPDATE_MASK;
                break;
            case DEL:
                flag |= DEL_MASK;
                break;
        }
        notifyAll();
    }

    public static final Pair<Object, Record> restoreRecord(Context context, MappedStorage mappedStorage, int pageId) throws PageFaultException, IOException {
        byte[] page = context.borrowPageCache();
        int pageSize = context.getConfig().getPageSize();
        try {
            int pageCount = 1;
            int[] pageIds = null;
            int keyLength = 0;
            byte[] keyBytes = null;
            int leftKeyBytes = 0;
            int currentPageId = pageId;

            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                boolean valid = mappedStorage.getPage(pageId, page);
                if (!valid) {
                    return null;
                }
                boolean isFirstPage = (page[IS_FIRST_PAGE_INDEX] & IS_FIRST_PAGE_MASK) == IS_FIRST_PAGE_MASK;
                if ((pageIndex == 0 && !isFirstPage)
                        || (pageIndex != 0 && isFirstPage)) {
                    return null;
                }
                if (isFirstPage) {
                    pageCount = ByteUtil.getUnsignedShortBigEndian(page, PAGE_COUNT_INDEX);
                    if (pageCount <= 0) {
                        return null;
                    }
                    pageIds = new int[pageCount];
                    keyLength = ByteUtil.getUnsignedByte(page, KEY_LENGTH_INDEX);
                    keyBytes = getKeyBytes(context, keyLength);
                    leftKeyBytes = keyLength;
                }

                pageIds[pageIndex] = currentPageId;

                if (leftKeyBytes > 0) {
                    int dataIndex = pageIndex == 0 ? FIRST_PAGE_DATA_INDEX : OTHER_PAGE_DATA_INDEX;
                    int canStoreKey = Math.min(leftKeyBytes, pageSize - dataIndex);
                    if (canStoreKey > 0) {
                        System.arraycopy(page, dataIndex, keyBytes, keyLength - leftKeyBytes, canStoreKey);
                        leftKeyBytes -= canStoreKey;
                    }
                }

                int nextPageId = (int) (ByteUtil.getUnsignedIntBigEndian(page, NEXT_PAGE_ID_INDEX) & NEXT_PAGE_ID_MASK);
                if (nextPageId == currentPageId) {
                    break;
                }

                pageIndex++;
                currentPageId = nextPageId;
            }
            Pair<Object, Record> pair = new Pair<Object, Record>();
            pair.first = getKey(context, keyBytes, 0, keyLength);
            pair.second = new Record(pageIds);
            return pair;
        } finally {
            if (page != null) {
                context.returnPageCache(page);
            }
        }
    }

    private static byte[] getKeyBytes(Context context, int keyLength) {
        switch (context.getConfig().getKeyType()) {
            case STRING:
                return new byte[keyLength];
            case SHORT:
                return context.getKeyBytesShortCache();
            case INT:
            case FLOAT:
                return context.getKeyBytesIntCache();
            case LONG:
            case DOUBLE:
                return context.getKeyBytesLongCache();
        }
        throw new IllegalArgumentException("key 's type is invalid");
    }

    public static final Record buildRecord(Context context, MappedStorage mappedStorage, byte[] keyBytes, byte[] valueBytes) throws Throwable {
        if (keyBytes == null || keyBytes.length == 0
                || valueBytes == null || valueBytes.length == 0) {
            throw new IllegalArgumentException("key and value should not be empty");
        }
        int pageSize = context.getConfig().getPageSize();
        if (pageSize < FIRST_PAGE_DATA_INDEX) {
            throw new RuntimeException("pageSize should >= " + FIRST_PAGE_DATA_INDEX);
        }
        if (keyBytes.length > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("key 's length should <= " + MAX_KEY_LENGTH);
        }
        if (valueBytes.length > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("key 's length should <= " + MAX_VALUE_LENGTH);
        }
        int pageCount = pageCount(keyBytes.length, valueBytes.length, pageSize);
        if (pageCount > MAX_PAGE_COUNT_PER_RECORD) {
            throw new IllegalArgumentException("key and value is too large");
        }
        int leftKeyBytes = keyBytes.length;
        int leftValueBytes = valueBytes.length;
        int[] pageIds = context.borrowPageIds(pageCount);
        Arrays.sort(pageIds);
        byte[] page = context.borrowPageCache();
        try {
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                int nextPageId = pageIndex == pageCount - 1 ? pageIds[pageIndex] : pageIds[pageIndex + 1];
                ByteUtil.getUnsignedBytesBigEndian(nextPageId, page, NEXT_PAGE_ID_INDEX);
                if (pageIndex == 0) {
                    page[IS_FIRST_PAGE_INDEX] |= IS_FIRST_PAGE_MASK;
                    ByteUtil.getUnsignedBytesBigEndian((short) pageCount, page, PAGE_COUNT_INDEX);
                    ByteUtil.getUnsignedByte((byte) keyBytes.length, page, KEY_LENGTH_INDEX);
                    ByteUtil.getBytesBigEndian(valueBytes.length, page, VALUE_LENGTH_INDEX);

                    int canStoreKeyBytes = Math.min(pageSize - FIRST_PAGE_DATA_INDEX, leftKeyBytes);
                    if (canStoreKeyBytes > 0) {
                        System.arraycopy(keyBytes, 0, page, FIRST_PAGE_DATA_INDEX, canStoreKeyBytes);
                        leftKeyBytes -= canStoreKeyBytes;
                        int canStoreValueBytes = Math.min(pageSize - FIRST_PAGE_DATA_INDEX - canStoreKeyBytes, leftValueBytes);
                        if (canStoreValueBytes > 0) {
                            System.arraycopy(valueBytes, 0, page, FIRST_PAGE_DATA_INDEX + canStoreKeyBytes, canStoreValueBytes);
                            leftValueBytes -= canStoreValueBytes;
                        }
                    }
                } else {
                    page[IS_FIRST_PAGE_INDEX] &= IS_NOT_FIRST_PAGE_MASK;
                    int canStoreKeyBytes = Math.min(pageSize - OTHER_PAGE_DATA_INDEX, leftKeyBytes);
                    if (canStoreKeyBytes > 0) {
                        System.arraycopy(keyBytes, keyBytes.length - leftKeyBytes, page, OTHER_PAGE_DATA_INDEX, canStoreKeyBytes);
                        leftKeyBytes -= canStoreKeyBytes;
                    }
                    int canStoreValueBytes = Math.min(pageSize - OTHER_PAGE_DATA_INDEX - canStoreKeyBytes, leftValueBytes);
                    if (canStoreValueBytes > 0) {
                        System.arraycopy(valueBytes, valueBytes.length - leftValueBytes, page, OTHER_PAGE_DATA_INDEX + canStoreKeyBytes, canStoreValueBytes);
                        leftValueBytes -= canStoreValueBytes;
                    }
                }
                mappedStorage.setPage(pageIds[pageIndex], page, false);
            }
        } catch (Throwable cause) {
            context.returnPageIds(pageIds);
            throw cause;
        } finally {
            context.returnPageCache(page);
        }
        return new Record(pageIds);
    }

    private static int pageCount(int keyBytes, int valueBytes, int pageSize) {
        int totalSize = FIRST_PAGE_DATA_INDEX + keyBytes + valueBytes;
        int pageCount = 1;
        totalSize -= pageSize;
        for (;;) {
            if (totalSize <= 0) {
                break;
            }
            pageCount++;
            totalSize -= pageSize - OTHER_PAGE_DATA_INDEX;
        }
        return pageCount;
    }

    public byte[] getValue(Context context, MappedStorage mappedStorage) throws InterruptedException, IOException {
        int pageCount = pageIds.length;
        byte[][] pageSegments = new byte[pageCount][];
        try {
            try {
                lock(Record.OP.GET);
                for (int i = 0; i < pageCount; i++) {
                    int pageId = pageIds[i];
                    pageSegments[i] = context.borrowPageCache();
                    boolean valid = mappedStorage.getPage(pageId, pageSegments[i]);
                    if (!valid) {
                        return null;
                    }
                }
            } catch (PageFaultException e) {
                return null;
            } finally {
                unlock(Record.OP.GET);
            }
            int keyLength = ByteUtil.getUnsignedByte(pageSegments[0], KEY_LENGTH_INDEX);
            int valueLength = ByteUtil.getIntBigEndian(pageSegments[0], VALUE_LENGTH_INDEX);
            byte[] valueBytes = new byte[valueLength];
            int pageSize = context.getConfig().getPageSize();
            int leftKeyBytes = keyLength;
            int leftValueBytes = valueLength;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                byte[] pageSegment = pageSegments[pageIndex];
                int dataIndex = pageIndex == 0 ? FIRST_PAGE_DATA_INDEX : OTHER_PAGE_DATA_INDEX;
                int canStoreKeyBytes = Math.min(pageSize - dataIndex, leftKeyBytes);
                int canStoreValueBytes = Math.min(pageSize - dataIndex - canStoreKeyBytes, leftValueBytes);
                if (canStoreValueBytes > 0) {
                    System.arraycopy(pageSegment, dataIndex + canStoreKeyBytes, valueBytes, valueLength - leftValueBytes, canStoreValueBytes);
                }
                leftKeyBytes -= canStoreKeyBytes;
                leftValueBytes -= canStoreValueBytes;
            }
            return valueBytes;
        } finally {
            for (byte[] page : pageSegments) {
                if (page != null) {
                    context.returnPageCache(page);
                }
            }
        }
    }

    public int[] getPageIds() {
        return pageIds;
    }

    public int[] setPageIds(int[] pageIds) {
        int[] old = this.pageIds;
        this.pageIds = pageIds;
        flag = 0;
        return old;
    }

    public int[] removePageIds() {
        pageIds = null;
        flag = 0;
        return pageIds;
    }

    public static Object getKey(Context context, byte[] bytes, int from, int to) {
        switch (context.getConfig().getKeyType()) {
            case STRING:
                byte[] bs = from <= 0 ? bytes : Arrays.copyOfRange(bytes, from, to);
                return new String(bs, StandardCharsets.UTF_8);
            case SHORT:
                return ByteUtil.getShortBigEndian(bytes, from);
            case INT:
                return ByteUtil.getIntBigEndian(bytes, from);
            case FLOAT:
                return Float.intBitsToFloat(ByteUtil.getIntBigEndian(bytes, from));
            case LONG:
                return ByteUtil.getLongBigEndian(bytes, from);
            case DOUBLE:
                return Double.longBitsToDouble(ByteUtil.getLongBigEndian(bytes, from));
        }
        throw new IllegalArgumentException("key 's type is invalid");
    }
}