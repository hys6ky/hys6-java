package hyren.serv6.b.realtimecollection.util;

import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BatchExecutorThread implements Runnable {

    private String command;

    private int duration;

    private long ssj_job_id;

    private static ConcurrentMap<Long, Thread> mapJob = new ConcurrentHashMap<>();

    public BatchExecutorThread(String command, int duration, long ssj_job_id) {
        this.command = command;
        this.duration = duration;
        this.ssj_job_id = ssj_job_id;
    }

    @Override
    public void run() {
        Thread thread = mapJob.get(ssj_job_id);
        if (thread != null && !thread.isInterrupted()) {
            log.info("重复发送，中断上一个实时线程");
            thread.interrupt();
            mapJob.remove(ssj_job_id);
        }
        mapJob.put(ssj_job_id, Thread.currentThread());
        while (true) {
            log.info("开始执行spark作业调度：" + command);
            try {
                CommandLine commandLine = CommandLine.parse(command);
                DefaultExecutor executor = new DefaultExecutor();
                ExecuteWatchdog watchdog = new ExecuteWatchdog(Integer.MAX_VALUE);
                executor.setWatchdog(watchdog);
                executor.execute(commandLine);
                log.info("执行spark作业调度完成");
            } catch (Exception e) {
                throw new BusinessException("调度spark作业失败!!!");
            }
            try {
                TimeUnit.SECONDS.sleep(duration);
            } catch (InterruptedException e) {
                log.info("重复发送，线程被中断,结束线程");
                break;
            }
        }
    }
}
