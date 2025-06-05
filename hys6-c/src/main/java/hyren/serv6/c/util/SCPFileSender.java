package hyren.serv6.c.util;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.jsch.AgentDeploy;
import hyren.serv6.base.utils.jsch.FileProgressMonitor;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.IOException;

@Slf4j
public class SCPFileSender {

    public static final String SEPARATOR = File.separator;

    public static final String APPINFOCONfNAME = "appinfo.conf";

    public static final String CONTROL_APPINFO = "control_appinfo.conf";

    public static final String TRIGGER_APPINFO = "trigger_appinfo.conf";

    public static final String DBINFOCONFNAME = "dbinfo.conf";

    public static final String CONTROLCONFNAME = "control.conf";

    public static final String TRIGGERCONFNAME = "trigger.conf";

    public static final String LOGINFONAME = "log4j2.xml";

    public static void etlScpToFrom(SSHDetails sshDetails) {
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            String old_deploy_dir = sshDetails.getOld_deploy_dir();
            String controlTarget = sshDetails.getTarget_dir() + "control" + SEPARATOR;
            String triggerTarget = sshDetails.getTarget_dir() + "trigger" + SEPARATOR;
            if (StringUtil.isNotBlank(old_deploy_dir)) {
                sshOperate.execCommandBySSH("rm -rf " + old_deploy_dir);
                log.info("###########是否之前部署过，如果目录存在先删除###########");
            }
            sshOperate.execCommandBySSH("mkdir -p " + sshDetails.getTarget_dir());
            log.info("###########建立etl工程部署存放目录###########");
            mkdirToEtlTarget(sshOperate, sshDetails.getTarget_dir());
            log.info("开始传输control程序的jar包以及启动脚本。。。。。。。");
            File controlFile = new File(PropertyParaValue.getString("controlPath", ""));
            if (!controlFile.exists()) {
                throw new BusinessException("etl工程control的jar包(" + controlFile.getAbsolutePath() + ")未找到!!!");
            }
            sshOperate.channelSftp.put(controlFile.getAbsolutePath(), controlTarget, new FileProgressMonitor(controlFile.length()), ChannelSftp.OVERWRITE);
            File controlShellFile = new File(controlFile.getParentFile(), Constant.CONTROL_OPERATION_SH);
            if (!controlShellFile.exists()) {
                throw new BusinessException("etl工程的脚本(" + Constant.CONTROL_OPERATION_SH + ")未找到!!!");
            }
            sshOperate.channelSftp.put(controlShellFile.getAbsolutePath(), controlTarget, new FileProgressMonitor(controlShellFile.length()), ChannelSftp.OVERWRITE);
            log.info("传输control程序的jar包以及启动脚本结束。。。。。。。");
            log.info("开始传输trigger程序的jar包以及启动脚本。。。。。。。");
            File triggerFile = new File(PropertyParaValue.getString("triggerPath", ""));
            if (!triggerFile.exists()) {
                throw new BusinessException("etl工程trigger的jar包(" + triggerFile.getAbsolutePath() + ")未找到!!!");
            }
            sshOperate.channelSftp.put(triggerFile.getAbsolutePath(), triggerTarget, new FileProgressMonitor(triggerFile.length()), ChannelSftp.OVERWRITE);
            File triggerShellFile = new File(triggerFile.getParentFile(), Constant.TRIGGER_OPERATION_SH);
            if (!triggerShellFile.exists()) {
                throw new BusinessException("etl工程的脚本(" + Constant.TRIGGER_OPERATION_SH + ")未找到!!!");
            }
            sshOperate.channelSftp.put(triggerShellFile.getAbsolutePath(), triggerTarget, new FileProgressMonitor(triggerShellFile.length()), ChannelSftp.OVERWRITE);
            log.info("传输trigger程序的jar包以及启动脚本结束。。。。。。。");
            log.info("开始创建、scp远程机器JRE目录");
            AgentDeploy.createDir(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jre", sshOperate, sshDetails.getTarget_dir() + SEPARATOR + "jre");
            AgentDeploy.sftpFiles(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jre", sshOperate.channelSftp, sshDetails.getTarget_dir());
            log.info("创建 SCP JRE目录文件结束");
            log.info("开始scp远程机器jdbc目录");
            AgentDeploy.createDir(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jdbc", sshOperate, sshDetails.getTarget_dir() + SEPARATOR + "jdbc");
            AgentDeploy.sftpFiles(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jdbc", sshOperate.channelSftp, sshDetails.getTarget_dir());
            log.info("SCP jdbc目录文件结束");
            uploadConfigFiles(controlTarget, triggerTarget, sshOperate, sshDetails.getTempPath());
        } catch (JSchException e) {
            log.error(e.getMessage());
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (SftpException e) {
            log.error(e.getMessage());
            throw new BusinessException("数据传输失败，请检查数据目录是否有权限，请联系管理员" + e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new AppSystemException("部署失败，请重新部署!!!" + e.getMessage());
        }
    }

    private static void uploadConfigFiles(String controlTarget, String triggerTarget, SSHOperate sshOperate, File tempPath) throws SftpException {
        log.info("###########开始上传配置文件###########");
        String controlResourceDir = controlTarget + "resources" + SEPARATOR;
        String triggerResourceDir = triggerTarget + "resources" + SEPARATOR;
        String controlConf = tempPath.getAbsolutePath() + SEPARATOR + ETLConfParam.CONTROL_CONF_NAME;
        String triggerConf = tempPath.getAbsolutePath() + SEPARATOR + ETLConfParam.TRIGGER_CONF_NAME;
        String controlLog4j2 = tempPath.getAbsolutePath() + SEPARATOR + "control" + SEPARATOR + ETLConfParam.CONTROL_LOG4J2_CONF_NAME;
        String triggerLog4j2 = tempPath.getAbsolutePath() + SEPARATOR + "trigger" + SEPARATOR + ETLConfParam.TRIGGER_LOG4J2_CONF_NAME;
        String applicationYml = tempPath.getAbsolutePath() + SEPARATOR + ETLConfParam.APPLICATION_CONF_NAME;
        log.info("=======localPath========" + tempPath.getAbsolutePath());
        sshOperate.channelSftp.put(controlConf, controlResourceDir, ChannelSftp.OVERWRITE);
        sshOperate.channelSftp.put(triggerConf, triggerResourceDir, ChannelSftp.OVERWRITE);
        sshOperate.channelSftp.put(controlLog4j2, controlResourceDir, ChannelSftp.OVERWRITE);
        sshOperate.channelSftp.put(triggerLog4j2, triggerResourceDir, ChannelSftp.OVERWRITE);
        sshOperate.channelSftp.put(applicationYml, controlResourceDir, ChannelSftp.OVERWRITE);
        sshOperate.channelSftp.put(applicationYml, triggerResourceDir, ChannelSftp.OVERWRITE);
        AgentDeploy.updateLog4jXml(sshOperate.channelSftp, controlResourceDir, "Property", controlTarget + "control.log");
        AgentDeploy.updateLog4jXml(sshOperate.channelSftp, triggerResourceDir, "Property", triggerTarget + "trigger.log");
        log.info("###########上传配置文件完成###########");
    }

    private static void sftpConfFile(ChannelSftp chSftp, String tmp_conf_path, String controlFdConfDir, String triggerFdConfDir, String confName) throws SftpException {
        log.info("tmp_conf_path目录" + tmp_conf_path);
        log.info("control/resources目录" + controlFdConfDir);
        log.info("trigger/resources目录" + triggerFdConfDir);
        chSftp.put(tmp_conf_path + confName, controlFdConfDir + "fdconfig", new FileProgressMonitor(new File(tmp_conf_path + confName).length()), ChannelSftp.OVERWRITE);
        chSftp.put(tmp_conf_path + confName, triggerFdConfDir + "fdconfig", new FileProgressMonitor(new File(tmp_conf_path + confName).length()), ChannelSftp.OVERWRITE);
    }

    private static void mkdirToEtlTarget(SSHOperate sshOperate, String targetDir) throws IOException, JSchException {
        String[] targetDir_machine = { "control", "trigger", "lib", "resources", "fdconfig", "i18n", "jdbc" };
        log.info("创建远程目录 jdbc: " + targetDir + SEPARATOR + targetDir_machine[6]);
        sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + targetDir_machine[6]);
        String controlDir = targetDir + SEPARATOR + targetDir_machine[0] + SEPARATOR + targetDir_machine[3];
        log.info("创建远程目录control/resource目录: " + controlDir);
        sshOperate.execCommandBySSH("mkdir -p " + controlDir);
        String triggerDir = targetDir + SEPARATOR + targetDir_machine[1] + SEPARATOR + targetDir_machine[3];
        log.info("创建远程目录trigger/resource目录: " + triggerDir);
        sshOperate.execCommandBySSH("mkdir -p " + triggerDir);
    }
}
