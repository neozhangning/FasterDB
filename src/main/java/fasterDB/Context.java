package fasterDB;

import fasterDB.util.KeyLocker;
import fasterDB.util.PageIdAllocator;
import fasterDB.vo.Config;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zn on 15/5/3.
 */
public class Context {
    private final Config config;

    private final PageIdAllocator allocator;                          // pageId allocator
    private final KeyLocker keyLocker;
    private final LinkedBlockingQueue<byte[]> pageCache;        // used for MappedStorage
    private final ThreadLocal<byte[]> keyBytesShortCache;       // used for keyBytes shortToByte and byteToShort
    private final ThreadLocal<byte[]> keyBytesIntCache;         // used for keyBytes intToByte and byteToInt
    private final ThreadLocal<byte[]> keyBytesLongCache;        // used for keyBytes longToByte and byteToLong
    private final ThreadLocal<byte[]> keyLengthBytesIntCache;   // used for keyLengthBytes intToByte and byteToInt
    private final ThreadLocal<byte[]> oneByteArrayCache;        // used for byte[] bytes = new byte[1]

    public Context(Config config, int pageIdInitialValue) throws IOException {
        this.config = config;
        this.allocator = new PageIdAllocator(pageIdInitialValue);
        this.keyLocker = new KeyLocker(config.getLockPath(), config.getLockFile());
        int pageCacheSize = config.getPageCacheSize();
        if (pageCacheSize > 0) {
            this.pageCache = new LinkedBlockingQueue<byte[]>(pageCacheSize);
        } else {
            pageCache = null;
        }
        this.keyBytesShortCache = new ThreadLocal<byte[]>() {
            protected byte[] initialValue() {
                return new byte[2];
            }
        };
        this.keyBytesIntCache = new ThreadLocal<byte[]>() {
            protected byte[] initialValue() {
                return new byte[4];
            }
        };
        this.keyBytesLongCache = new ThreadLocal<byte[]>() {
            protected byte[] initialValue() {
                return new byte[8];
            }
        };
        this.keyLengthBytesIntCache = new ThreadLocal<byte[]>() {
            protected byte[] initialValue() {
                return new byte[4];
            }
        };
        this.oneByteArrayCache = new ThreadLocal<byte[]>() {
            protected byte[] initialValue() {
                return new byte[1];
            }
        };
    }

    public Config getConfig() {
        return config;
    }

    public byte[] borrowPageCache() {
        byte[] bytes = null;
        if (pageCache != null) {
            bytes = pageCache.poll();
        }
        return bytes == null ? new byte[config.getPageSize()] : bytes;
    }

    public void returnPageCache(byte[] pageCache) {
        if (pageCache == null || this.pageCache == null
                || pageCache.length == 0) {
            return;
        }
        this.pageCache.offer(pageCache);
    }

    public int[] borrowPageIds(int count) {
        return allocator.borrowIds(count);
    }

    public void returnPageIds(int[] pageIds) {
        allocator.returnIds(pageIds);
    }

    public void returnPageId(int pageId) {
        allocator.returnId(pageId);
    }

    public byte[] getKeyBytesShortCache() {
        return keyBytesShortCache.get();
    }

    public byte[] getKeyBytesIntCache() {
        return keyBytesIntCache.get();
    }

    public byte[] getKeyBytesLongCache() {
        return keyBytesLongCache.get();
    }

    public byte[] getKeyLengthBytesIntCache() {
        return keyLengthBytesIntCache.get();
    }

    public byte[] getOneByteArrayCache() {
        return oneByteArrayCache.get();
    }

    public void lock(Object key) throws IOException {
        keyLocker.lock(key);
    }

    public void unlock(Object key) throws IOException {
        keyLocker.unLock(key);
    }
}
