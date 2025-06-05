package hyren.serv6.control.server;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.HazelcastHelper;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.control.constans.ControlConfigure;
import hyren.serv6.control.task.TaskManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ControlManageServer {

    private static final CMServerThread cmThread = new CMServerThread();

    private static TaskManager taskManager;

    public ControlManageServer(String strSystemCode, String bathDate, String endDate, boolean isResumeRun, boolean isAutoShift) {
        taskManager = new TaskManager(strSystemCode, bathDate, endDate, isResumeRun, isAutoShift);
    }

    public void initCMServer() {
        taskManager.initEtlSystem();
    }

    public void runCMServer() {
        cmThread.start();
        taskManager.startCheckWaitFileThread();
        log.info("调度服务启动成功");
    }

    public void stopCMServer() {
        taskManager.stopCheckWaitFileThread();
        cmThread.stopThread();
        log.info("调度服务停止成功");
    }

    private static class CMServerThread extends Thread {

        private volatile boolean run = true;

        void stopThread() {
            this.run = false;
        }

        @Override
        public void run() {
            try {
                while (run) {
                    taskManager.loadReadyJob();
                    run = taskManager.publishReadyJob();
                    if (!taskManager.needDailyShift()) {
                        log.info("------ 系统无日切信号，系统退出 ------");
                        break;
                    }
                }
            } catch (Exception ex) {
                throw new AppSystemException("加载作业到Control中失败!", ex);
            } finally {
                TaskSqlHelper.closeDbConnector();
                HazelcastHelper.getInstance(ControlConfigure.getHazelcastConfig()).close();
                taskManager.stopCheckWaitFileThread();
            }
        }
    }
}
