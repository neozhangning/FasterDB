package fasterDB.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zn on 15/5/10.
 */
public class NamedThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(NamedThreadFactory.class);

	private static final AtomicInteger poolNumber = new AtomicInteger(1);
	private final ThreadGroup group;
	private final AtomicInteger threadNumber = new AtomicInteger(1);
	private final String namePrefix;
	private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
            log.error("Future[" + t + "] occur a uncaughtException[" + e + "]");
		}
	};

	public NamedThreadFactory() {
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		namePrefix = "pool-" + poolNumber.getAndIncrement() + "-future-";
	}

	/**
	 * @param poolNamePrefix
	 *            the prefix of thread pool
	 * @param threadNamePrefix
	 *            the prefix of thread
	 */
	public NamedThreadFactory(String poolNamePrefix, String threadNamePrefix) {
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		namePrefix = poolNamePrefix + "-" + poolNumber.getAndIncrement() + "-" + threadNamePrefix + "-";
	}

	private int threadPriority = Thread.NORM_PRIORITY;

	public NamedThreadFactory(String poolNamePrefix, String threadNamePrefix, int threadPriority) {
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		namePrefix = poolNamePrefix + "-" + poolNumber.getAndIncrement() + "-" + threadNamePrefix + "-";
		this.threadPriority = threadPriority;
	}

	public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
		t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
		if (t.isDaemon())
			t.setDaemon(false);
		t.setPriority(threadPriority);
		return t;
	}
}
