package fasterDB.vo;

import fasterDB.util.StringUtil;

/**
 * Created by zn on 15/5/3.
 */
public class Config<ValueType> {

    private KeyType keyType;
    private Codec<ValueType> valueCodec;
    private int readCacheKV = 1000000;
    private int writeCacheKV = 1000000;
    private String dataPath;
    private String dataFile = "fdb.data";
    private String redoLogPath;
    private String redoLogSuffix = "redo";
    private String lockPath;
    private String lockFile = "fdb.lock";
    private long rollingThreshold = 100l * 1024 * 1024;
    private long writeBlockThreshold = 1000l * 1024 * 1024;
    private boolean logWithFlush = false;
    private int pageSize = 256;
    private int pageCacheSize = 10000;

    private Config() {}

    public Codec<ValueType> getValueCodec() {
        return valueCodec;
    }

    public void setValueCodec(Codec<ValueType> valueCodec) {
        this.valueCodec = valueCodec;
    }

    public int getReadCacheKV() {
        return readCacheKV;
    }

    public void setReadCacheKV(int readCacheKV) {
        this.readCacheKV = readCacheKV;
    }

    public int getWriteCacheKV() {
        return writeCacheKV;
    }

    public void setWriteCacheKV(int writeCacheKV) {
        this.writeCacheKV = writeCacheKV;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getRedoLogPath() {
        return redoLogPath;
    }

    public void setRedoLogPath(String redoLogPath) {
        this.redoLogPath = redoLogPath;
    }

    public String getRedoLogSuffix() {
        return redoLogSuffix;
    }

    public void setRedoLogSuffix(String redoLogSuffix) {
        this.redoLogSuffix = redoLogSuffix;
    }

    public int getPageCacheSize() {
        return pageCacheSize;
    }

    public void setPageCacheSize(int pageCacheSize) {
        this.pageCacheSize = pageCacheSize;
    }

    public long getRollingThreshold() {
        return rollingThreshold;
    }

    public void setRollingThreshold(long rollingThreshold) {
        this.rollingThreshold = rollingThreshold;
    }

    public long getWriteBlockThreshold() {
        return writeBlockThreshold;
    }

    public void setWriteBlockThreshold(long writeBlockThreshold) {
        this.writeBlockThreshold = writeBlockThreshold;
    }

    public KeyType getKeyType() {
        return keyType;
    }

    public void setKeyType(KeyType keyType) {
        this.keyType = keyType;
    }

    public boolean isLogWithFlush() {
        return logWithFlush;
    }

    public void setLogWithFlush(boolean logWithFlush) {
        this.logWithFlush = logWithFlush;
    }

    public String getLockPath() {
        return lockPath;
    }

    public void setLockPath(String lockPath) {
        this.lockPath = lockPath;
    }

    public String getLockFile() {
        return lockFile;
    }

    public void setLockFile(String lockFile) {
        this.lockFile = lockFile;
    }

    public enum KeyType {
        STRING, SHORT, INT, FLOAT, LONG, DOUBLE
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        Config config = new Config();

        public void readCacheKV(int readCacheKV) {
            config.readCacheKV = readCacheKV;
        }

        public void writeCacheKV(int writeCacheKV) {
            config.writeCacheKV = writeCacheKV;
        }

        public void dataPath(String dataPath) {
            config.dataPath = dataPath;
        }

        public void pageSize(int pageSize) {
            config.pageSize = pageSize;
        }

        public void redoLogPath(String redoLogPath) {
            config.redoLogPath = redoLogPath;
        }

        public void lockPath(String lockPath) {
            config.lockPath = lockPath;
        }

        public void pageCacheSize(int pageCacheSize) {
            config.pageCacheSize = pageCacheSize;
        }

        public void rollingThreshold(long rollingThreshold) {
            config.rollingThreshold = rollingThreshold;
        }

        public void writeBlockThreshold(long writeBlockThreshold) {
            config.writeBlockThreshold = writeBlockThreshold;
        }

        public void keyType(KeyType keyType) {
            config.keyType = keyType;
        }

        public void logWithFlush(boolean logWithFlush) {
            config.logWithFlush = logWithFlush;
        }

        public void valueCodec(Codec valueCodec) {
            config.valueCodec = valueCodec;
        }

        public Config build() {
            if (config.readCacheKV <= 0) {
                throw new IllegalArgumentException("readCacheKV should > 0");
            }
            if (config.writeCacheKV <= 0) {
                throw new IllegalArgumentException("writeCacheKV should > 0");
            }
            if (StringUtil.isEmpty(config.dataPath)) {
                throw new IllegalArgumentException("dataPath is empty");
            }
            if (StringUtil.isEmpty(config.dataFile)) {
                throw new IllegalArgumentException("dataFile is empty");
            }
            if (config.pageSize <= 0) {
                throw new IllegalArgumentException("pageSize should > 0");
            }
            if (StringUtil.isEmpty(config.redoLogPath)) {
                throw new IllegalArgumentException("redoLogPath is empty");
            }
            if (StringUtil.isEmpty(config.redoLogSuffix)) {
                throw new IllegalArgumentException("redoLogSuffix is empty");
            }
            if (config.pageCacheSize < 0) {
                throw new IllegalArgumentException("pageCacheSize should >= 0");
            }
            if (config.rollingThreshold <= 0) {
                throw new IllegalArgumentException("rollingThreshold should > 0");
            }
            if (config.writeBlockThreshold <= 0) {
                throw new IllegalArgumentException("writeBlockThreshold should > 0");
            }
            if (config.keyType == null) {
                throw new IllegalArgumentException("keyType should not be null");
            }
            if (config.valueCodec == null) {
                throw new IllegalArgumentException("valueCodec should not be null");
            }
            return config;
        }
    }

    public interface Codec<T> {
        byte[] toBytes(T t);

        T toObject(byte[] bytes);

        boolean equals(T t1, T t2);
    }
}