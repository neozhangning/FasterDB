package fasterDB.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Created by zn on 15/4/19.
 */
public class FileSystemUtil {

    public enum MODE {
        READ("r"), WRITE("w"), READ_WRITE("rw");
        MODE(String mode) {
            this.mode = mode;
        }
        String mode;
    }

    public static final FileChannel prepareChannel(String parentPath, String filename, boolean delIfExist, MODE mode) throws IOException {
        File file = prepareFile(parentPath, filename, delIfExist);
        RandomAccessFile rac = new RandomAccessFile(file, mode.mode);
        return rac.getChannel();
    }

    public static final File prepareFile(String parentPath, String filename, boolean delIfExist) throws IOException {
        File dir = new File(parentPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("param parentPath should be a valid directory");
        }
        File file = new File(dir, filename);
        boolean exist = file.exists();
        if (exist && file.isDirectory()) {
            throw new IllegalArgumentException("param filename should be a ordinary file");
        }
        if (exist && delIfExist) {
            file.delete();
        }
        if (!exist) {
            file.createNewFile();
        }
        return file;
    }

    public static final File prepareDirectory(String parentPath, String directory, boolean delIfExist) throws IOException {
        File dir = new File(parentPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("param parentPath should be a valid directory");
        }
        File file = new File(dir, directory);
        boolean exist = file.exists();
        if (exist && !file.isDirectory()) {
            throw new IllegalArgumentException("param directory should be a directory file");
        }
        if (exist && delIfExist) {
            deleteDirectory(file);
        }
        if (!exist) {
            file.mkdir();
        }
        return file;
    }

    private static final void deleteDirectory(File directory) {
        if (directory == null || !directory.exists()) {
            return;
        }
        if (directory.isFile()) {
            directory.delete();
        } else if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            for (File file : files) {
                deleteDirectory(file);
            }
            directory.delete();
        }
    }
}
