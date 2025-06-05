package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class ScriptExecutor {

    private final static String SCRIPTS_DIR = "scripts";

    private final static String SHELL_COMMAND = "sh";

    private final static String PUTHDFS_SCRIPTS = "uploadHDFS.sh";

    @Method(desc = "", logicStep = "")
    @Param(name = "localFile", desc = "", range = "")
    @Param(name = "remoteDir", desc = "", range = "")
    public void executeUpload2Hdfs(String localFile, String remoteDir) throws InterruptedException, IllegalStateException {
        if (StringUtil.isBlank(localFile)) {
            throw new AppSystemException("本地文件地址不能为空");
        }
        if (StringUtil.isBlank(remoteDir)) {
            throw new AppSystemException("hdfs目录地址不能为空");
        }
        service(PUTHDFS_SCRIPTS, localFile, remoteDir);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "localFiles", desc = "", range = "")
    @Param(name = "remoteDir", desc = "", range = "")
    public void executeUpload2Hdfs(String[] localFiles, String remoteDir) throws InterruptedException, IllegalStateException {
        if (localFiles.length == 0) {
            throw new AppSystemException("本地文件地址不能为空");
        }
        if (StringUtil.isBlank(remoteDir)) {
            throw new AppSystemException("hdfs目录地址不能为空");
        }
        service(PUTHDFS_SCRIPTS, localFiles, remoteDir);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "shellName", desc = "", range = "")
    @Param(name = "args", desc = "", range = "")
    private void service(String shellName, String... args) throws InterruptedException, IllegalStateException {
        String shellPath = ProductFileUtil.getProjectPath() + File.separatorChar + StringUtils.join(new String[] { "src", "main", "resources" }, File.separatorChar) + File.separatorChar + SCRIPTS_DIR + File.separatorChar + shellName;
        buildCommandAndExe(shellPath, args);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "shellName", desc = "", range = "")
    @Param(name = "localFiles", desc = "", range = "")
    @Param(name = "remoteDir", desc = "", range = "")
    private void service(String shellName, String[] localFiles, String remoteDir) throws InterruptedException, IllegalStateException {
        String shellPath = ProductFileUtil.getProjectPath() + File.separatorChar + StringUtils.join(new String[] { "src", "main", "resources" }, File.separatorChar) + File.separatorChar + SCRIPTS_DIR + File.separatorChar + shellName;
        buildCommandAndExe(shellPath, localFiles, remoteDir);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "script", desc = "", range = "")
    @Param(name = "args", desc = "", range = "")
    private void buildCommandAndExe(String script, String... args) throws InterruptedException, IllegalStateException {
        String cmd = SHELL_COMMAND + " " + script + " " + StringUtils.join(args, " ");
        callScript(script, cmd);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "script", desc = "", range = "")
    @Param(name = "localFiles", desc = "", range = "")
    @Param(name = "remoteDir", desc = "", range = "")
    private void buildCommandAndExe(String script, String[] localFiles, String remoteDir) throws InterruptedException, IllegalStateException {
        String cmd = SHELL_COMMAND + " " + script + " " + StringUtils.join(localFiles, " ") + " " + remoteDir;
        callScript(script, cmd);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "script", desc = "", range = "")
    @Param(name = "cmd", desc = "", range = "")
    private void callScript(String script, String cmd) throws InterruptedException, IllegalStateException {
        CommandWaitForThread commandThread = new CommandWaitForThread(cmd);
        commandThread.start();
        while (!commandThread.isFinish()) {
            log.info("shell " + script + " 还未执行完毕, 5s后重新探测");
            Thread.sleep(5000);
        }
        if (commandThread.getExitCode() != 0) {
            throw new IllegalStateException(cmd + " 执行失败, exitCode = " + commandThread.getExitCode());
        }
        log.info(cmd + " 执行成功,exitValue = " + commandThread.getExitCode());
    }

    private class CommandWaitForThread extends Thread {

        private String cmd;

        private boolean finish = false;

        private int exitValue = -1;

        CommandWaitForThread(String cmd) {
            this.cmd = cmd;
        }

        public void run() {
            BufferedReader infoInput = null;
            BufferedReader errorInput = null;
            try {
                Process process = Runtime.getRuntime().exec(cmd);
                infoInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
                errorInput = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String line;
                while ((line = infoInput.readLine()) != null) {
                    log.info(line);
                }
                while ((line = errorInput.readLine()) != null) {
                    log.error(line);
                }
                infoInput.close();
                errorInput.close();
                this.exitValue = process.waitFor();
            } catch (Throwable e) {
                log.error("CommandWaitForThread accure exception,shell " + cmd, e);
            } finally {
                if (null != infoInput) {
                    try {
                        infoInput.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (null != errorInput) {
                    try {
                        errorInput.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                setFinish(true);
            }
        }

        public boolean isFinish() {
            return finish;
        }

        public void setFinish(boolean finish) {
            this.finish = finish;
        }

        public int getExitCode() {
            return exitValue;
        }
    }
}
