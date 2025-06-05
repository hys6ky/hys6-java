package hyren.serv6.t.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

@SuppressWarnings("unused")
public class ThreadPoolUtil {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolUtil.class);

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static final int CORE_POOL_SIZE = CPU_COUNT + 1;

    private static final int MAXIMUM_POOL_SIZE = CPU_COUNT * 2 + 1;

    private ExecutorService exec;

    private ScheduledExecutorService scheduleExec;

    private ThreadPoolType poolType;

    private int corePoolSize;

    private ThreadFactory namedThreadFactory;

    public ThreadPoolUtil(ThreadPoolType type, Object... obj) {
        this.poolType = type;
        this.corePoolSize = CORE_POOL_SIZE;
        if (Objects.nonNull(obj) && obj.length > 0) {
            namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(obj[0].toString()).build();
        } else {
            namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("Default Pool").build();
        }
        switch(type) {
            case FIXED:
                exec = newFixed();
                break;
            case SINGLE:
                exec = newSingle();
                break;
            case CACHED:
                exec = newCached();
                break;
            case SCHEDULED:
                scheduleExec = newSchedule();
                break;
            default:
                break;
        }
    }

    public ThreadPoolUtil(final ThreadPoolType type, final int corePoolSize, Object... obj) {
        this.poolType = type;
        this.corePoolSize = corePoolSize;
        if (Objects.nonNull(obj) && obj.length > 0) {
            namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(obj[0].toString()).build();
        } else {
            namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("Default Pool").build();
        }
        switch(type) {
            case FIXED:
                exec = newFixed();
                break;
            case SINGLE:
                exec = newSingle();
                break;
            case CACHED:
                exec = newCached();
                break;
            case SCHEDULED:
                scheduleExec = newSchedule();
                break;
            default:
                break;
        }
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
        return new ThreadPoolExecutor(corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), namedThreadFactory);
    }

    public void execute(final Runnable command) {
        if (null == exec && null != scheduleExec) {
            throw new RuntimeException("U should use method : schedule()");
        }
        if (isShutDown()) {
            throw new RuntimeException("The old pool is shutdown! Please new one !");
        }
        exec.execute(command);
    }

    public void execute(final List<Runnable> commands) {
        for (Runnable command : commands) {
            exec.execute(command);
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
}
