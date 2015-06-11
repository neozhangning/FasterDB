package fasterDB.util;

import fasterDB.util.FileSystemUtil;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * Created by zn on 15/5/3.
 */
public class KeyLocker {

    private FileChannel fileChannel;
    private ThreadLocal<FileLock> fileLockThreadLocal = new ThreadLocal<FileLock>();

    public KeyLocker(String lockPath, String lockFile) throws IOException {
        this.fileChannel = FileSystemUtil.prepareChannel(lockPath, lockFile, true, FileSystemUtil.MODE.READ_WRITE);
    }

    public void lock(Object key) throws IOException {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        int hashCode = key.hashCode();
        FileLock fileLock;
        for (;;) {
            try {
                fileLock = fileChannel.lock(hashCode, 1, false);
                break;
            } catch (OverlappingFileLockException e) {
                Thread.yield();
            } catch (Throwable cause) {
                throw new RuntimeException("lock file fail", cause);
            }
        }
        FileLock old = fileLockThreadLocal.get();
        if (old != null) {
            old.release();
        }
        fileLockThreadLocal.set(fileLock);
    }

    public void unLock(Object key) throws IOException {
        if (key == null) {
            throw new NullPointerException("key should not be null");
        }
        FileLock lock = fileLockThreadLocal.get();
        if (lock != null) {
            lock.release();
        }
    }
}
