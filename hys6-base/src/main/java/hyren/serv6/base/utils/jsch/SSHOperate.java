package hyren.serv6.base.utils.jsch;

import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Vector;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/11 11:29")
@Slf4j
public class SSHOperate implements Closeable {

    private static final int SSH_TIME_OUT = 6 * 1000;

    public Session session;

    public ChannelSftp channelSftp;

    public SSHOperate(SSHDetails sshDetails) {
        this(sshDetails, SSH_TIME_OUT);
    }

    public SSHOperate(SSHDetails sshDetails, int time_out) {
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(sshDetails.getUser_name(), sshDetails.getHost(), sshDetails.getPort());
            log.debug("Session created.");
            String ftpPassword = sshDetails.getPwd();
            Properties config = new Properties();
            if (ftpPassword != null) {
                session.setPassword(ftpPassword);
            }
            config.put("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");
            session.setConfig(config);
            session.setTimeout(time_out);
            session.connect();
            log.debug("Session connected.");
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
        } catch (JSchException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("建立session连接失败,请检查[IP，端口，用户名，密码]是否正确! " + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "srcDir", desc = "", range = "")
    @Return(desc = "", range = "")
    public Vector<LsEntry> listDir(String srcDir) {
        return listDir(srcDir, "*");
    }

    @SuppressWarnings("unchecked")
    @Method(desc = "", logicStep = "")
    @Param(name = "srcDir", desc = "", range = "")
    @Param(name = "regex", desc = "", range = "")
    @Return(desc = "", range = "")
    public Vector<LsEntry> listDir(String srcDir, String regex) {
        try {
            if (srcDir.endsWith("/")) {
                return channelSftp.ls(srcDir + regex);
            } else {
                return channelSftp.ls(srcDir + "/" + regex);
            }
        } catch (SftpException e) {
            e.printStackTrace();
            log.error("按照正则获取远程目录下的文件对象集合!" + e);
            throw new BusinessException("按照正则获取远程目录下的文件对象集合失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "srcFile", desc = "", range = "")
    @Param(name = "destFile", desc = "", range = "")
    public void transferFile(String srcFile, String destFile) {
        try {
            channelSftp.get(srcFile, destFile);
        } catch (SftpException e) {
            e.printStackTrace();
            log.error("使用sftp拉取远程服务器上的文件到本地! " + e);
            throw new BusinessException("使用sftp拉取远程服务器上的文件到本地!" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "srcFile", desc = "", range = "")
    @Param(name = "destFile", desc = "", range = "")
    public void transferPutFile(String srcFile, String destFile) {
        try {
            channelSftp.put(srcFile, destFile);
        } catch (SftpException e) {
            e.printStackTrace();
            log.error("使用sftp推送本地文件到远程服务器失败! " + e);
            throw new BusinessException("使用sftp推送本地文件到远程服务器失败!" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currentLoadDir", desc = "", range = "")
    public void scpMkdir(String currentLoadDir) {
        String mkdir = "mkdir -p " + currentLoadDir;
        try {
            execCommandBySSHNoRs(mkdir);
        } catch (JSchException | IOException | InterruptedException e) {
            e.printStackTrace();
            throw new BusinessException("使用sftp远程创建目录: " + currentLoadDir + " 失败!");
        }
    }

    public String execCommandBySSH(String command) throws IOException, JSchException {
        String result;
        command = FileNameUtils.normalize(command, true);
        log.info("执行命令为: " + command);
        ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
        InputStream in = channelExec.getInputStream();
        channelExec.setCommand(command);
        channelExec.setErrStream(System.err);
        channelExec.connect();
        result = IOUtils.toString(in, StandardCharsets.UTF_8);
        channelExec.disconnect();
        return result;
    }

    public void execCommandBySSHNoRs(String command) throws JSchException, IOException, InterruptedException {
        log.info("执行命令为: " + command);
        ChannelExec channelExec;
        channelExec = (ChannelExec) session.openChannel("exec");
        channelExec.getInputStream();
        channelExec.setCommand(command);
        channelExec.setErrStream(System.err);
        channelExec.connect();
        Thread.sleep(1000);
        channelExec.disconnect();
    }

    public void executeLocalShell(String executeShell) throws IOException, InterruptedException {
        log.info("执行命令为 ：" + executeShell);
        Process ps = Runtime.getRuntime().exec((new String[] { "sh", "-l", "-c", executeShell }));
        ps.waitFor();
        BufferedReader br = new BufferedReader(new InputStreamReader(ps.getErrorStream()));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line).append(System.lineSeparator());
        }
        if (!StringUtil.isEmpty(sb.toString())) {
            throw new AppSystemException("Linux命令" + executeShell + "执行失败，" + sb.toString());
        }
    }

    public String execCommandBySSHToReadLine(String command) throws JSchException, IOException, InterruptedException {
        log.info("执行命令为 : " + command);
        ChannelExec channelExec;
        StringBuilder result = new StringBuilder();
        channelExec = (ChannelExec) session.openChannel("exec");
        InputStream inputStream = channelExec.getInputStream();
        OutputStream outputStream = channelExec.getOutputStream();
        PrintWriter printWriter = new PrintWriter(outputStream);
        printWriter.println(command);
        Thread.sleep(3000);
        printWriter.println("exit");
        printWriter.flush();
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        String msg;
        while ((msg = in.readLine()) != null) {
            result.append(msg);
        }
        in.close();
        channelExec.disconnect();
        return result.toString();
    }

    public static SSHDetails getSSHDetails(String ftpHost, String ftpUserName, String ftpPassword, int ftpPort) {
        return new SSHDetails(ftpHost, ftpUserName, ftpPassword, ftpPort);
    }

    @Method(desc = "", logicStep = "")
    @Override
    public void close() {
        if (channelSftp != null) {
            channelSftp.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }
}
