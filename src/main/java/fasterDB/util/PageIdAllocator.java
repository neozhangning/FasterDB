package fasterDB.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zn on 15/5/3.
 */
public class PageIdAllocator {
    private final AtomicInteger newPageIdGenerator;
    private final LinkedBlockingQueue<Integer> recycle;

    public PageIdAllocator(int pageIdInitialValue) {
        this.newPageIdGenerator = new AtomicInteger(pageIdInitialValue);
        this.recycle = new LinkedBlockingQueue<Integer>();
    }

    public void returnIds(int[] ints) {
        if (ints == null) {
            return;
        }
        for (int i : ints) {
            recycle.offer(i);
        }
    }

    public void returnId(int i) {
        recycle.offer(i);
    }

    public int[] borrowIds(int count) {
        if (count <= 0) {
            return null;
        }
        int[] ints = new int[count];
        for (int i = 0; i < count; i--) {
            Integer old = recycle.poll();
            ints[i] = old == null ? newPageIdGenerator.getAndIncrement() : old;
        }
        return ints;
    }
}
