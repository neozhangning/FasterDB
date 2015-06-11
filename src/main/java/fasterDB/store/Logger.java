package fasterDB.store;

import fasterDB.util.ByteUtil;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Logger {

    private static final int BLOCK_SIZE = 4 * 1024;
    private final FileChannel fileChannel;
    private final RandomAccessFile rac;
    private final File file;

    public Logger(File parentPath, String filename) throws IOException {
        if (parentPath == null || filename == null) {
            throw new NullPointerException("parentPath and filename should not be null");
        }
        file = new File(parentPath, filename);
        if (!file.exists()) {
            file.createNewFile();
        }
        if (file.isDirectory()) {
            throw new IllegalArgumentException("filename[" + file.getName() + "] should be an ordinary file");
        }
        rac = new RandomAccessFile(file, "rw");
        fileChannel = rac.getChannel();
    }

    public Logger(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("file should not be null");
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        if (file.isDirectory()) {
            throw new IllegalArgumentException("filename[" + file.getName() + "] should be an ordinary file");
        }
        this.file = file;
        rac = new RandomAccessFile(file, "rw");
        fileChannel = rac.getChannel();
    }

    public int log(ByteBuffer buffer) throws IOException {
        return fileChannel.write(buffer);
    }

    public void flush() throws IOException {
        fileChannel.force(false);
    }

    public synchronized void redo(Reader reader) throws IOException {
        if (reader == null) {
            throw new NullPointerException("reader should not be null");
        }
        fileChannel.position(0);
        ByteBuffer cache = ByteBuffer.allocate(BLOCK_SIZE);
        cache.limit(0);
        for (int length = readFromCache(cache); length > 0; length = readFromCache(cache)) {
            byte[] body = readFromCache(cache, length);
            if (body == null) {
                break;
            }
            reader.read(body);
        }
    }

    public void destroy() throws IOException {
        fileChannel.close();
        rac.close();
        file.delete();
    }

    private int readFromCache(ByteBuffer cache) throws IOException {
        byte[] intBytes = readFromCache(cache, 4);
        return intBytes == null ? 0 : ByteUtil.getIntBigEndian(intBytes, 0);
    }

    private byte[] readFromCache(ByteBuffer cache, int bytes) throws IOException {
        byte[] result = new byte[bytes];
        int index = 0;
        while (index < bytes) {
            int remaining = cache.remaining();
            if (remaining > 0) {
                int copyBytes = Math.min(remaining, bytes - index);
                cache.get(result, index, copyBytes);
                index += copyBytes;
            } else if (!fillCache(cache)) {
                return null;
            }
        }
        return result;
    }

    private boolean fillCache(ByteBuffer cache) throws IOException {
        cache.clear();
        int i = fileChannel.read(cache);
        if (i <= 0) {
            return false;
        }
        cache.flip();
        return true;
    }

    public interface Reader {
        void read(byte[] bytes);
    }
}