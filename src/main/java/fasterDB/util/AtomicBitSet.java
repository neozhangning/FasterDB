package fasterDB.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by zn on 15/5/3.
 */
public class AtomicBitSet {

    private static final Unsafe unsafe;
    private static final int base;
    private static final int byteLShift;
    private static final int wordRShift;
    private static final long WORDS_OFFSET;
    private volatile long[] words;
    private ReentrantReadWriteLock.ReadLock readWordsLock;
    private ReentrantReadWriteLock.WriteLock expandWordsLock;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            base = unsafe.arrayBaseOffset(long[].class);
            int scale = unsafe.arrayIndexScale(long[].class);
            if ((scale & (scale - 1)) != 0) {
                throw new RuntimeException("data type scale not a power of two");
            }
            byteLShift = 31 - Integer.numberOfLeadingZeros(scale);
            wordRShift = 6;
            WORDS_OFFSET = unsafe.objectFieldOffset(AtomicBitSet.class.getDeclaredField("words"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public AtomicBitSet(int initialBits) {
        if (initialBits < 0) {
            throw new NegativeArraySizeException("initialBits < 0: " + initialBits);
        }
        setWords(new long[wordIndex(initialBits - 1) + 1]);
        initialize();
    }

    public AtomicBitSet(long[] baseArray) {
        if (baseArray == null) {
            throw new NullPointerException("baseArray is null");
        }
        setWords(baseArray);
        initialize();
    }

    private void initialize() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readWordsLock = lock.readLock();
        expandWordsLock = lock.writeLock();
    }

    public List<Long> emptyBitValues() {
        List<Long> invalid = new ArrayList<Long>();
        long[] words = getWords();
        for (int i = 0; i < words.length; i++) {
            long word = words[i];
            long bitIndex = ((long) i) << wordRShift;
            int offset = 0;
            for (;;) {
                long v = word & 0x0000000000000001;
                if (v == 0) {
                    invalid.add(bitIndex + offset);
                }
                if (++offset == 64) {
                    break;
                }
                word >>>= 1;
            }
        }
        return invalid;
    }

    public List<Long> bitValues() {
        List<Long> valid = new ArrayList<Long>();
        long[] words = getWords();
        for (int i = 0; i < words.length; i++) {
            long word = words[i];
            if (word != 0) {
                long bitIndex = ((long) i) << wordRShift;
                int offset = 64;
                for (;;) {
                    int numberOfTrailingZeros = Long.numberOfTrailingZeros(word);
                    bitIndex += numberOfTrailingZeros;
                    valid.add(bitIndex);
                    bitIndex ++;
                    word >>= (numberOfTrailingZeros + 1);
                    offset -= numberOfTrailingZeros + 1;
                    if (offset == 0 || word == 0) {
                        break;
                    }
                }
            }
        }
        return valid;
    }

    public void and(long[] bits) {
        long[] words = getWords();
        int wordsInCommon = Math.min(words.length, bits.length);
        for (int i = 0; i < wordsInCommon; i++) {
            bits[i] &= getLong(words, byteOffset(i));
        }
        Arrays.fill(bits, wordsInCommon, bits.length, 0);
    }

    public void or(long[] bits) {
        long[] words = getWords();
        int wordsInCommon = Math.min(words.length, bits.length);
        for (int i = 0; i < wordsInCommon; i++) {
            bits[i] |= getLong(words, byteOffset(i));
        }
    }

    public void notAnd(long[] bits) {
        long[] words = getWords();
        int wordsInCommon = Math.min(words.length, bits.length);
        for (int i = 0; i < wordsInCommon; i++) {
            bits[i] &= ~getLong(words, byteOffset(i));
        }
    }

    public boolean get(int bitIndex) {
        if (bitIndex < 0) {
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        }
        int wordIndex = wordIndex(bitIndex);
        long[] words = getWords();
        if (words.length <= wordIndex) {
            return false;
        }

        long raw = getLong(words, byteOffset(wordIndex));
        return ((raw & (1L << bitIndex)) != 0);
    }

    public void set(int bitIndex) {
        if (bitIndex < 0) {
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        }

        int wordIndex = wordIndex(bitIndex);
        long offset = byteOffset(wordIndex);
        ensureCapacity(wordIndex);
        try {
            readWordsLock.lock();
            long[] words = getWords();
            for (;;) {
                long oV = getLong(words, offset);
                long nV = oV | (1L << bitIndex);
                if (nV == oV || compareAndSetLong(words, offset, oV, nV)) {
                    break;
                }
            }
        } finally {
            readWordsLock.unlock();
        }
    }

    public void unSet(int bitIndex) {
        if (bitIndex < 0) {
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        }
        int wordIndex = wordIndex(bitIndex);
        long offset = byteOffset(wordIndex);
        try {
            readWordsLock.lock();
            long[] words = getWords();
            if (words.length <= wordIndex) {
                return;
            }
            for (;;) {
                long oV = getLong(words, offset);
                long nV = oV & ~(1L << bitIndex);
                if (nV == oV || compareAndSetLong(words, offset, oV, nV)) {
                    break;
                }
            }
        } finally {
            readWordsLock.unlock();
        }
    }

    /**
     *
     * @param wordIndex
     * @return true words changed false words not changed
     */
    private void ensureCapacity(int wordIndex) {
        int wordsRequired = wordIndex + 1;
        long[] words = getWords();
        if (words.length < wordsRequired) {
            try {
                expandWordsLock.lock();
                words = getWords();
                if (words.length < wordsRequired) {
                    int request = Math.max(2 * words.length, wordsRequired);
                    setWords(Arrays.copyOf(words, request));
                }
            } finally {
                expandWordsLock.unlock();
            }
        }
    }

    private long[] getWords() {
        return (long[]) unsafe.getObjectVolatile(this, WORDS_OFFSET);
    }

    private void setWords(long[] words) {
        unsafe.putObjectVolatile(this, WORDS_OFFSET, words);
    }

    private boolean compareAndSetLong(long[] words, long offset, long expect, long update) {
        return unsafe.compareAndSwapLong(words, offset, expect, update);
    }

    private long getLong(long[] words, long offset) {
        return unsafe.getLongVolatile(words, offset);
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >>> wordRShift;
    }

    private static long byteOffset(int wordIndex) {
        return ((long) wordIndex << byteLShift) + base;
    }

    public static void main(String[] args) throws InterruptedException {
//        final AtomicBitSet bitSet = new AtomicBitSet(64);
//        Thread t1= new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (int i = 1; i < 100000; i++) {
//                    if (i % 2 == 0) {
//                        bitSet.set(i);
//                    }
//                }
//            }
//        });
//        Thread t2 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (int i = 1; i < 100000; i++) {
//                    if (i % 3 == 0) {
//                        bitSet.set(i);
//                    }
//                }
//            }
//        });
//        Thread t3 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (int i = 1; i < 100000; i++) {
//                    if (i % 5 == 0) {
//                        bitSet.set(i);
//                    }
//                }
//            }
//        });
//        t1.start();
//        t2.start();
//        t3.start();
//        t1.join();
//        t2.join();
//        t3.join();
//
//        int twoCount = 0;
//        int threeCount = 0;
//        int fiveCount = 0;
//        for (int i = 1; i < 100000; i++) {
//            if (i % 3 == 0) {
//                threeCount++;
//            }
//            if (i % 2 == 0) {
//                twoCount++;
//            }
//            if (i % 5 == 0) {
//                fiveCount++;
//            }
//        }
//
//        int twoAcount = 0;
//        int threeAcount = 0;
//        int fiveAcount = 0;
//        List<Long> values = bitSet.bitValues();
//        for (long value : values) {
//            if (value % 3 == 0) {
//                threeAcount++;
//            }
//            if (value % 2 == 0) {
//                twoAcount++;
//            }
//            if (value % 5 == 0) {
//                fiveAcount++;
//            }
//        }
//        assert twoAcount == twoCount;
//        assert threeAcount == threeCount;
//        assert fiveAcount == fiveAcount;

        final AtomicBitSet bitSet2 = new AtomicBitSet(64);
        bitSet2.set(63);
        bitSet2.set(1);
        System.out.println(bitSet2.bitValues());
        System.out.println(bitSet2.emptyBitValues());
    }
}
