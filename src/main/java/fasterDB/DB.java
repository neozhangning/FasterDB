package fasterDB;

import com.google.common.cache.*;
import fasterDB.store.LoggerWrapper;
import fasterDB.store.MappedStorage;
import fasterDB.util.AtomicBitSet;
import fasterDB.util.ByteUtil;
import fasterDB.util.NamedThreadFactory;
import fasterDB.vo.Config;
import fasterDB.vo.InitializingBean;
import fasterDB.vo.PageFaultException;
import fasterDB.vo.Pair;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static fasterDB.Record.buildRecord;

/**
 * Created by zn on 15/5/10.
 */
public class DB<K, V> implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(DB.class);

    private final Config<V> config;
    private final long rollingThreshold;
    private final long writeBlockThreshold;
    private final Lock rollingLock;
    private final Condition rollingGreen;
    private final Condition rollingFinished;

    private Context context;
    private LoggerWrapper redoLog;
    private MappedStorage mappedStorage;
    private ConcurrentSkipListMap<K, Record> indexes;
    private Cache<K, V> readCache;
    private Cache<K, V> writeCache;
    private Thread rollingTask;

    private volatile int lastPageId = -1;
    private final AtomicLong logBytes = new AtomicLong(0);
    private volatile boolean initialized = false;

    public DB(Config<V> config) {
        this.config = config;
        rollingThreshold = config.getRollingThreshold();
        writeBlockThreshold = config.getWriteBlockThreshold();
        rollingLock = new ReentrantLock();
        rollingGreen = rollingLock.newCondition();
        rollingFinished = rollingLock.newCondition();
    }

    @Override
    public synchronized void initialize() throws IOException, InterruptedException {
        if (initialized) {
            return;
        }
        readCache = CacheBuilder.newBuilder().maximumSize(config.getReadCacheKV())
                .removalListener(new RemovalListener<K, V>() {
                    @Override
                    public void onRemoval(RemovalNotification<K, V> rn) {
                    }
                }).build();
        writeCache = CacheBuilder.newBuilder().maximumSize(config.getWriteCacheKV())
                .removalListener(new RemovalListener<K, V>() {
                    @Override
                    public void onRemoval(RemovalNotification<K, V> rn) {
                    }
                }).build();

        redoLog = new LoggerWrapper(config);
        mappedStorage = new MappedStorage(config, false);
        indexes = new ConcurrentSkipListMap<K, Record>();
        mappedStorage.initialize();
        rebuildIndexes();
        context = new Context(config, lastPageId + 1);
        rollingTask = new NamedThreadFactory("FDB", "rolling-task").newThread(new RollingTask());
        rollingTask.start();
        initialized = true;
    }

    /**
     * Put
     *
     * always create new mappedPages, there is no putIfAbsent param
     * @param key
     * @param value
     * @throws InterruptedException
     */
    public void put(K key, V value) throws Throwable {
        assertInitialized();
        if (key == null || value == null) {
            throw new NullPointerException("key and value should not be null");
        }
        try {
            context.lock(key);
            V valueInWriteCache = writeCache.getIfPresent(key);
            if (config.getValueCodec().equals(value, valueInWriteCache)) {
                return;
            }
            V valueInReadCache = readCache.getIfPresent(key);
            if (config.getValueCodec().equals(value, valueInReadCache)) {
                return;
            }
            writeCache.put(key, value);
            if (valueInReadCache != null) {
                readCache.put(key, value);
            }
            byte[] valueBytes = config.getValueCodec().toBytes(value);
            put(key, valueBytes);
        } finally {
            context.unlock(key);
        }
    }

    public void remove(K key) throws InterruptedException, IOException {
        assertInitialized();
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        try {
            context.lock(key);
            Record old = indexes.remove(key);
            if (old == null) {
                return;
            }
            readCache.invalidate(key);
            writeCache.invalidate(key);
            byte[] keyBytes = getKeyBytes(context, key);
            byte[] keyLengthBytes = context.getOneByteArrayCache();
            keyLengthBytes[0] = (byte) keyBytes.length;
            log(Record.OP.DEL.code, keyLengthBytes, keyBytes);

            try {
                old.lock(Record.OP.DEL);
                int[] oldPageIds = old.removePageIds();
                if (oldPageIds != null) {
                    for (int oldPageId : oldPageIds) {
                        mappedStorage.invalid(oldPageId);
                    }
                }
                context.returnPageIds(oldPageIds);
            } finally {
                old.unlock(Record.OP.DEL);
            }
        } finally {
            context.unlock(key);
        }
    }

    public V get(K key) throws IOException, InterruptedException, PageFaultException {
        assertInitialized();
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        if (indexes.get(key) == null) {
            return null;
        }
        V valueInReadCache = readCache.getIfPresent(key);
        if (valueInReadCache != null) {
            return valueInReadCache;
        }
        V valueInWriteCache = writeCache.getIfPresent(key);
        if (valueInWriteCache != null) {
            return valueInWriteCache;
        }

        try {
            context.lock(key);
            Record record;
            if ((record = indexes.get(key)) == null) {
                return null;
            }
            valueInReadCache = readCache.getIfPresent(key);
            if (valueInReadCache != null) {
                return valueInReadCache;
            }
            valueInWriteCache = writeCache.getIfPresent(key);
            if (valueInWriteCache != null) {
                readCache.put(key, valueInWriteCache);
                return valueInWriteCache;
            }
            byte[] valueBytes = record.getValue(context, mappedStorage);
            if (valueBytes == null) {
                throw new PageFaultException("key 's page is missed");
            }
            V value = config.getValueCodec().toObject(valueBytes);
            if (value != null) {
                readCache.put(key, value);
            }
            return value;
        } finally {
            context.unlock(key);
        }
    }

    private void put(K key, byte[] valueBytes) throws Throwable {
        byte[] keyBytes = getKeyBytes(context, key);
        byte[] keyLengthBytes = context.getOneByteArrayCache();
        keyLengthBytes[0] = (byte) keyBytes.length;
        log(Record.OP.UPDATE.code, keyLengthBytes, keyBytes, valueBytes);
        Record record = buildRecord(context, mappedStorage, keyBytes, valueBytes);
        Record old = indexes.put(key, record);
        if (old != null) {
            try {
                old.lock(Record.OP.UPDATE);
                int[] oldPageIds = old.setPageIds(record.getPageIds());
                if (oldPageIds != null) {
                    for (int oldPageId : oldPageIds) {
                        mappedStorage.invalid(oldPageId);
                    }
                }
                context.returnPageIds(oldPageIds);
            } finally {
                old.unlock(Record.OP.UPDATE);
            }
        }
    }

    private byte[] getKeyBytes(Context context, Object key) {
        switch (context.getConfig().getKeyType()) {
            case STRING:
                return ((String) key).getBytes(StandardCharsets.UTF_8);
            case SHORT:
                byte[] shortBytes = context.getKeyBytesShortCache();
                ByteUtil.getBytesBigEndian((Short) key, shortBytes, 0);
            case INT:
                byte[] intBytes = context.getKeyBytesIntCache();
                ByteUtil.getBytesBigEndian((Integer) key, intBytes, 0);
            case FLOAT:
                byte[] floatBytes = context.getKeyBytesIntCache();
                ByteUtil.getBytesBigEndian(Float.floatToIntBits((Float) key), floatBytes, 0);
            case LONG:
                byte[] longBytes = context.getKeyBytesLongCache();
                ByteUtil.getBytesBigEndian((Long) key, longBytes, 0);
            case DOUBLE:
                byte[] doubleBytes = context.getKeyBytesLongCache();
                ByteUtil.getBytesBigEndian(Double.doubleToLongBits((Double) key), doubleBytes, 0);
        }
        throw new IllegalArgumentException("key 's type is invalid");
    }

    private void assertInitialized() {
        if (!initialized) {
            throw new RuntimeException("db has not initialized");
        }
    }

    private void rebuildIndexes() throws IOException, InterruptedException {
        final AtomicBitSet pageIds = new AtomicBitSet(1024);
        final Context tmpContext = new Context(config, 0);
        int processors = Runtime.getRuntime().availableProcessors();
        final int[] maxPageIds = new int[processors];
        final CountDownLatch taskWaiter = new CountDownLatch(processors);
        for (int i = 0; i < processors; i++) {
            maxPageIds[i] = -1;
            new Thread(new MappedRecordReader(taskWaiter, tmpContext, pageIds, maxPageIds, i, processors)).start();
        }
        taskWaiter.await();

        for (int maxPageId : maxPageIds) {
            lastPageId = Math.max(lastPageId, maxPageId);
        }
        List<Long> emptyPageIds = pageIds.emptyBitValues();
        for (Long pageId : emptyPageIds) {
            if (pageId < lastPageId) {
                tmpContext.returnPageId(pageId.intValue());
            }
        }
        redoLog.redo(new fasterDB.store.Logger.Reader() {
            @Override
            public void read(byte[] bytes) {
                try {
                    switch (Record.OP.codeOf(bytes[0])) {
                        case UPDATE:
                            int keyLength0 = ByteUtil.getUnsignedByte(bytes, 1);
                            put((K) Record.getKey(tmpContext, bytes, 1, 1 + keyLength0), Arrays.copyOfRange(bytes, 1 + keyLength0, bytes.length));
                            break;
                        case DEL:
                            int keyLength = ByteUtil.getUnsignedByte(bytes, 1);
                            remove((K) Record.getKey(tmpContext, bytes, 1, 1 + keyLength));
                            break;
                    }
                } catch (Throwable cause) {
                    logger.error("redo log", cause);
                }
            }
        });
    }

    private void restoreRecords(Context context, AtomicBitSet pageIds, int[] maxPageIds, int index, int mode) {
        maxPageIds[index] = -1;
        for (int pageId = 0; ; pageId++) {
            if (pageId % mode == index) {
                try {
                    Pair<Object, Record> key2record = Record.restoreRecord(context, mappedStorage, pageId);
                    if (key2record != null) {
                        int[] recordPageIds = key2record.second.getPageIds();
                        for (int recordPageId : recordPageIds) {
                            pageIds.set(recordPageId);
                        }
                        maxPageIds[index] = Math.max(maxPageIds[index], recordPageIds[recordPageIds.length - 1]);
                        indexes.put((K) key2record.first, key2record.second);
                    }
                } catch (PageFaultException e) {
                    logger.error("rebuild index", e);
                    break;
                } catch (IOException e) {
                    logger.error("rebuild index", e);
                    break;
                }
            }
        }
    }

    public void log(byte[]... byteArrays) throws IOException {
        int writeBytes = redoLog.log(config.isLogWithFlush(), byteArrays);
        long logBytes = this.logBytes.addAndGet(writeBytes);
        if (logBytes > rollingThreshold) {
            try {
                rollingLock.lock();
                rollingGreen.signal();
                if (logBytes > writeBlockThreshold) {
                    rollingFinished.await();
                }
            } catch (InterruptedException ignore) {
            } finally {
                rollingLock.unlock();
            }
        }
    }

    private void doRoll() {
        while (true) {
            try {
                rollingLock.lock();
                rollingGreen.await();
                logBytes.set(0);
                redoLog.roll();
                mappedStorage.flush();
                rollingFinished.signalAll();
            } catch (Throwable ignore) {
            } finally {
                rollingLock.unlock();
            }
        }
    }

    class MappedRecordReader extends Thread {
        CountDownLatch taskWaiter;
        Context context;
        AtomicBitSet pageIds;
        int[] maxPageIds;
        int index;
        int mode;

        MappedRecordReader(CountDownLatch taskWaiter,
                           Context context, AtomicBitSet pageIds,
                           int[] maxPageIds, int index, int mode) {
            super(MappedRecordReader.class.getSimpleName());
            this.taskWaiter = taskWaiter;
            this.context = context;
            this.pageIds = pageIds;
            this.maxPageIds = maxPageIds;
            this.index = index;
            this.mode = mode;
        }

        @Override
        public void run() {
            try {
                restoreRecords(context, pageIds, maxPageIds, index, mode);
            } finally {
                taskWaiter.countDown();
            }
        }
    }

    class RollingTask implements Runnable {
        @Override
        public void run() {
            doRoll();
        }
    }
}
