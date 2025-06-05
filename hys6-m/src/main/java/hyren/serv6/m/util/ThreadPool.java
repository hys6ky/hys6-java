package hyren.serv6.m.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import hyren.daos.base.exception.SystemBusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class ThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolUtil.class);

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    public static final int CORE_POOL_SIZE = CPU_COUNT + 1;

    public static final int MAXIMUM_POOL_SIZE = CPU_COUNT * 2 + 1;

    private ExecutorService exec;

    private ScheduledExecutorService scheduleExec;

    private ThreadPoolType poolType;

    private int corePoolSize;

    private String poolName;

    private ThreadFactory namedThreadFactory;

    private ScheduledExecutorService monitorScheduleExec;

    @Deprecated
    private ThreadPool() {
        throw new UnsupportedOperationException("u can't instantiate me...");
    }

    public ThreadPool(final ThreadPoolType type, final int corePoolSize, String... name) {
        this.poolType = type;
        this.corePoolSize = corePoolSize;
        if (Objects.nonNull(name) && name.length > 0) {
            this.poolName = name[0];
        } else {
            this.poolName = "Default Pool";
        }
        namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(this.poolName).build();
        switch(type) {
            case SINGLE:
                exec = newSingle();
                break;
            case CACHED:
                exec = newCached();
                break;
            case SCHEDULED:
                scheduleExec = newSchedule();
                break;
            case FIXED:
            default:
                exec = newFixed();
                break;
        }
        checkThreadPoolStatusPeriodically();
    }

    private ScheduledExecutorService newSchedule() {
        return new ScheduledThreadPoolExecutor(corePoolSize, namedThreadFactory);
    }

    private ExecutorService newCached() {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), namedThreadFactory);
    }

    private ExecutorService newSingle() {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), namedThreadFactory);
    }

    private ExecutorService newFixed() {
        return new ThreadPoolExecutor(corePoolSize, MAXIMUM_POOL_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), namedThreadFactory);
    }

    public void execute(final Runnable command) {
        if (null == exec && null != scheduleExec) {
            throw new SystemBusinessException("U should use method : schedule()");
        }
        if (isShutDown()) {
            throw new SystemBusinessException("The old pool is shutdown! Please new one !");
        }
        exec.execute(command);
    }

    public void execute(final List<Runnable> commands) {
        for (Runnable command : commands) {
            execute(command);
        }
    }

    public void shutDown() {
        exec.shutdown();
    }

    public List<Runnable> shutDownNow() {
        return exec.shutdownNow();
    }

    public boolean isShutDown() {
        return exec.isShutdown();
    }

    public boolean isTerminated() {
        return exec.isTerminated();
    }

    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return exec.awaitTermination(timeout, unit);
    }

    public <T> Future<T> submit(final Callable<T> task) {
        return exec.submit(task);
    }

    public <T> Future<T> submit(final Runnable task, final T result) {
        return exec.submit(task, result);
    }

    public Future<?> submit(final Runnable task) {
        return exec.submit(task);
    }

    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return exec.invokeAll(tasks);
    }

    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws InterruptedException {
        return exec.invokeAll(tasks, timeout, unit);
    }

    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return exec.invokeAny(tasks);
    }

    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return exec.invokeAny(tasks, timeout, unit);
    }

    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
        return scheduleExec.schedule(command, delay, unit);
    }

    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
        return scheduleExec.schedule(callable, delay, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedRate(final Runnable command, final long initialDelay, final long period, final TimeUnit unit) {
        return scheduleExec.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay, final TimeUnit unit) {
        return scheduleExec.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    private void checkThreadPoolStatusPeriodically() {
        monitorScheduleExec = Executors.newSingleThreadScheduledExecutor();
        monitorScheduleExec.scheduleAtFixedRate(() -> {
            synchronized (this) {
                this.shutDown();
                if (isTerminated()) {
                    this.shutDown();
                    logger.info("线程池:{} 任务已完成,关闭线程池", poolName);
                    monitorScheduleExec.shutdownNow();
                } else {
                    logger.debug("含有正在执行的任务，无法shutdown");
                }
            }
        }, 20, 20, TimeUnit.SECONDS);
    }
}
