package fasterDB.store;

import fasterDB.vo.Config;
import fasterDB.util.ByteUtil;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LoggerWrapper {

    private final File baseDir;
    private final String suffix;
    private final ReentrantReadWriteLock.ReadLock logLock;
    private final ReentrantReadWriteLock.WriteLock rollingLock;

    private volatile Logger logger;

    public LoggerWrapper(Config config) throws IOException {
        String basePath = config.getRedoLogPath();
        String suffix = config.getRedoLogSuffix();
        if (basePath == null || suffix == null) {
            throw new NullPointerException("basePath and suffix should not be null");
        }
        baseDir = new File(basePath);
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
        if (!baseDir.isDirectory()) {
            throw new IllegalArgumentException("basePath[" + basePath + "] should be an directory file");
        }
        this.suffix = suffix;
        logger = buildLogger();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        logLock = lock.readLock();
        rollingLock = lock.writeLock();
    }

    public void redo(Logger.Reader reader) throws IOException {
        if (reader == null) {
            throw new NullPointerException("reader should not be null");
        }
        File[] historyLogs = baseDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(suffix);
            }
        });
        if (historyLogs.length == 0) {
            return;
        }
        Arrays.sort(historyLogs, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                String f1name = f1.getName();
                String f2name = f2.getName();
                return Long.valueOf(f1name).compareTo(Long.valueOf(f2name));
            }
        });
        int index = 0;
        Logger[] loggers = new Logger[historyLogs.length];
        for (File historyLog : historyLogs) {
            loggers[index++] = new Logger(historyLog);
        }
        for (Logger logger : loggers) {
            logger.redo(reader);
            logger.destroy();
        }
    }

    public int log(boolean flush, byte[]... byteArrays) throws IOException {
        if (byteArrays == null || byteArrays.length == 0) {
            throw new NullPointerException("byteArrays should not be null");
        }
        int totalSize = 0;
        for (byte[] byteArray : byteArrays) {
            totalSize += byteArray.length;
        }
        ByteBuffer buffer = ByteBuffer
                .allocate(totalSize + 4)
                .put(ByteUtil.getBytesBigEndian(totalSize));
        for (byte[] byteArray : byteArrays) {
            buffer.put(byteArray);
        }
        buffer.flip();
        try {
            logLock.lock();
            Logger logger = this.logger;
            logger.log(buffer);
            if (flush) {
                logger.flush();
            }
        } finally {
            logLock.unlock();
        }
        return totalSize + 4;
    }

    public void roll() throws IOException {
        try {
            rollingLock.lock();
            Logger old = logger;
            logger = buildLogger();
            if (old != null) {
                old.destroy();
            }
        } finally {
            rollingLock.unlock();
        }
    }

    private Logger buildLogger() throws IOException {
        return new Logger(baseDir, String.valueOf(System.currentTimeMillis()) + "." + suffix);
    }
}
