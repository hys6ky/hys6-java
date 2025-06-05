package hyren.serv6.commons.utils.fileutil.read;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;

@DocClass(desc = "")
@Slf4j
public class ReadLog {

    @Method(desc = "", logicStep = "")
    @Param(name = "logPath", desc = "", range = "")
    @Param(name = "ip", desc = "", range = "")
    @Param(name = "port", desc = "", range = "")
    @Param(name = "userName", desc = "", range = "")
    @Param(name = "password", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String readAgentLog(String logPath, SSHDetails sshDetails, int readNum) {
        String execCommandBySSH;
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            String readShell = "cat " + logPath + " |tail -n " + readNum;
            execCommandBySSH = sshOperate.execCommandBySSH(readShell);
        } catch (JSchException ex) {
            execCommandBySSH = "读取Agent日志失败!";
            log.error("登录验证失败...", ex);
        } catch (IOException ex) {
            execCommandBySSH = "读取Agent日志失败!";
            log.error("读取日志文件-----" + logPath + "-----失败...", ex);
        }
        return execCommandBySSH;
    }
}
