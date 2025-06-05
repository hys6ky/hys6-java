package hyren.serv6.trigger.util;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import fd.ng.core.utils.NumberUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.util.YarnUtil;
import hyren.serv6.trigger.task.executor.TaskExecutor;
import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

@Slf4j
public class ProcessUtil {

    private final static long SLEEP_TIME = 3000;

    private final static String WINDOWS_PID_FLAG = "handle";

    private final static String LINUX_PID_FLAG = "pid";

    interface Kernel32 extends Library {

        Kernel32 INSTANCE = (Kernel32) Native.loadLibrary("kernel32", Kernel32.class);

        int GetProcessId(Long hProcess);
    }

    public static int getPid(final Process process) {
        try {
            if (Platform.isWindows()) {
                Field f = process.getClass().getDeclaredField(WINDOWS_PID_FLAG);
                f.setAccessible(true);
                return Kernel32.INSTANCE.GetProcessId((long) f.get(process));
            } else if (Platform.isLinux()) {
                Field f = process.getClass().getDeclaredField(LINUX_PID_FLAG);
                f.setAccessible(true);
                return (int) (Integer) f.get(process);
            } else {
                throw new AppSystemException("不支持的操作系统，目前仅支持Windows、Linux " + Platform.getOSType());
            }
        } catch (Exception ex) {
            throw new AppSystemException("获取进程编号发生异常" + ex);
        }
    }

    public static String getChildPidByPPid(String pid) {
        try {
            if (Platform.isLinux()) {
                Process ps = Runtime.getRuntime().exec((new String[] { "/bin/sh", "-c", "ps -ef | grep -v grep | grep -w '" + pid + "' | awk '{if('" + pid + "'==$3){print $2}}'" }));
                ps.waitFor();
                BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
                StringBuilder cpid = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    if (!NumberUtil.isInteger(line))
                        continue;
                    cpid.append(line).append(",");
                }
                String cpidArry = cpid.toString();
                if (StringUtil.isNotBlank(cpidArry) && !cpidArry.isEmpty()) {
                    cpidArry = cpidArry.substring(0, cpidArry.length() - 1);
                }
                log.info("parent:" + pid + "=========== childPid:" + cpidArry);
                return cpidArry;
            }
        } catch (Exception e) {
            log.error("获取子进程id出差，但是不做任何处理，也不报错", e);
        }
        return "";
    }

    public static String getYarnAppID(final String yarnName) throws InterruptedException {
        while (true) {
            String yarnNameByAppID = YarnUtil.getApplicationIdByJobName(yarnName);
            if (StringUtil.isNotEmpty(yarnNameByAppID)) {
                return yarnNameByAppID;
            }
            Thread.sleep(SLEEP_TIME);
        }
    }

    public static int getStatusOnYarn(final String appId) throws InterruptedException {
        while (true) {
            YarnUtil.YarnApplicationReport yarnApplicationReport = YarnUtil.getApplicationReportByAppId(appId);
            String status = yarnApplicationReport.getStatus();
            if (YarnStatus.FINISHED.getValue().equals(status)) {
                return TaskExecutor.PROGRAM_DONE_FLAG;
            } else if (YarnStatus.FAILED.getValue().equals(status) || YarnStatus.KILLED.getValue().equals(status)) {
                return TaskExecutor.PROGRAM_ERROR_FLAG;
            }
            Thread.sleep(SLEEP_TIME);
        }
    }

    private enum YarnStatus {

        FINISHED("FINISHED"), FAILED("FAILED"), KILLED("KILLED");

        private final String value;

        YarnStatus(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }
}
