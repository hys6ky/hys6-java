package hyren.serv6.trigger.server;

import hyren.serv6.commons.jobUtil.task.HazelcastHelper;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.trigger.beans.EtlJobParaAnaly;
import hyren.serv6.trigger.constans.TriggerConfigure;
import hyren.serv6.trigger.task.TaskManager;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TriggerManageServer {

    private static final long SLEEP_TIME = 1000;

    private final TaskManager taskManager;

    private final CMServerThread cmThread;

    public TriggerManageServer(String etlSysCode) {
        this.taskManager = new TaskManager(etlSysCode);
        this.cmThread = new CMServerThread();
    }

    public void runCMServer() {
        cmThread.start();
        log.info("调度服务启动成功");
    }

    public void stopCMServer() {
        cmThread.stopThread();
        log.info("调度服务停止成功");
    }

    private class CMServerThread extends Thread {

        private volatile boolean run = true;

        void stopThread() {
            this.run = false;
        }

        @Override
        public void run() {
            try {
                while (run) {
                    if (!taskManager.checkSysGoRun())
                        return;
                    EtlJobParaAnaly etlJobParaAnaly = taskManager.getEtlJob();
                    if (etlJobParaAnaly.isHasEtlJob()) {
                        taskManager.runEtlJob(etlJobParaAnaly.getEtlJobCur(), etlJobParaAnaly.isHasHandle());
                    }
                    TimeUnit.MILLISECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("Exception happened!", ex);
            } finally {
                TaskSqlHelper.closeDbConnector();
                HazelcastHelper.getInstance(TriggerConfigure.getHazelcastConfig()).close();
            }
        }
    }
}
