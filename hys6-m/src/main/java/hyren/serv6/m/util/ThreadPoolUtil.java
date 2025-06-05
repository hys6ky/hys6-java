package hyren.serv6.m.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import hyren.daos.base.exception.SystemBusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

@SuppressWarnings("unused")
public class ThreadPoolUtil {

    private static ThreadPool commonThreadPool;

    public static void execCommonFixed(final Runnable command) {
        if (null == commonThreadPool) {
            commonThreadPool = new ThreadPool(ThreadPoolType.FIXED, ThreadPool.CORE_POOL_SIZE, "common_fixed_pool");
        }
        if (commonThreadPool.isShutDown()) {
            commonThreadPool = new ThreadPool(ThreadPoolType.FIXED, ThreadPool.CORE_POOL_SIZE, "common_fixed_pool");
        }
        commonThreadPool.execute(command);
    }

    public static boolean checkCommonIsTerminated() {
        return commonThreadPool.isTerminated();
    }

    public static void shutdownNow() {
        commonThreadPool.shutDownNow();
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            int finalI = i;
            ThreadPoolUtil.execCommonFixed(() -> {
                System.out.println(finalI);
            });
        }
        try {
            Thread.sleep(3000);
            ThreadPoolUtil.execCommonFixed(() -> {
                System.out.println(99);
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
