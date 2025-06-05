package hyren.serv6.c.util;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.utils.yaml.Yaml;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "dhw", createdate = "2019/12/19 14:57")
@Slf4j
public class ETLAgentDeployment {

    public static final String SEPARATOR = File.separator;

    public static Resource APPLICATION = new ClassPathResource("application.yml");

    public static Resource LOG4J2 = new ClassPathResource("log4j2.xml");

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_serv_ip", desc = "", range = "")
    @Param(name = "etl_serv_port", desc = "", range = "")
    @Param(name = "redisIP", desc = "", range = "")
    @Param(name = "redisPort", desc = "", range = "")
    @Param(name = "userName", desc = "", range = "")
    @Param(name = "password", desc = "", range = "")
    @Param(name = "targetDir", desc = "", range = "")
    @Param(name = "old_deploy_path", desc = "", range = "")
    public static void scpETLAgent(String etl_sys_cd, String etl_serv_ip, int etl_serv_port, String userName, String password, String targetDir, String old_deploy_path) {
        try {
            File userDirFile = FileUtil.getFile(System.getProperty("user.dir"));
            String tmp_conf_path = userDirFile + SEPARATOR + "etlTempResources" + SEPARATOR + "fdconfig" + SEPARATOR;
            File tempFolder = new File(tmp_conf_path);
            if (!tempFolder.exists()) {
                if (!tempFolder.mkdirs()) {
                    throw new BusinessException("创建文件临时存放目录失败");
                }
            }
            generateConfFile(etl_serv_ip, targetDir, tempFolder);
            String hadoopConf = userDirFile + SEPARATOR + "conf" + SEPARATOR;
            SSHDetails sshDetails = new SSHDetails();
            setSSHDetails(etl_sys_cd, etl_serv_ip, etl_serv_port, userName, password, targetDir, hadoopConf, tempFolder, old_deploy_path, sshDetails);
            SCPFileSender.etlScpToFrom(sshDetails);
            FileUtil.deleteDirectoryFiles(tempFolder.getAbsolutePath());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(e.getMessage());
        }
    }

    private static void generateConfFile(String ip, String dir, File tempFolder) throws IOException {
        log.info("==========配置文件的临时存放路径===========" + tempFolder.getAbsolutePath());
        Yaml.dump(ETLConfParam.getControlConfParam(ip), new File(tempFolder, ETLConfParam.CONTROL_CONF_NAME));
        Yaml.dump(ETLConfParam.getTriggerConfParam(ip), new File(tempFolder, ETLConfParam.TRIGGER_CONF_NAME));
        Yaml.copyYml(APPLICATION, new File(tempFolder, ETLConfParam.APPLICATION_CONF_NAME));
        new File(tempFolder, "control").mkdir();
        Yaml.copyXml(LOG4J2, dir + "/logs/control/", "local_control.log", new File(tempFolder, "control" + SEPARATOR + ETLConfParam.CONTROL_LOG4J2_CONF_NAME));
        new File(tempFolder, "trigger").mkdir();
        Yaml.copyXml(LOG4J2, dir + "/logs/trigger/", "local_trigger.log", new File(tempFolder, "trigger" + SEPARATOR + ETLConfParam.TRIGGER_LOG4J2_CONF_NAME));
    }

    private static void setSSHDetails(String etl_sys_cd, String etl_serv_ip, int etl_serv_port, String userName, String password, String targetDir, String hadoopConf, File tempPath, String old_deploy_path, SSHDetails sshDetails) {
        sshDetails.setHost(etl_serv_ip);
        sshDetails.setUser_name(userName);
        sshDetails.setPwd(password);
        sshDetails.setPort(etl_serv_port);
        sshDetails.setHADOOP_CONF(hadoopConf);
        sshDetails.setTarget_dir(targetDir + SEPARATOR + etl_sys_cd + SEPARATOR);
        sshDetails.setTempPath(tempPath);
        if (old_deploy_path != null) {
            sshDetails.setOld_deploy_dir(old_deploy_path + SEPARATOR + etl_sys_cd);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "batch_date", desc = "", range = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "isResumeRun", desc = "", range = "")
    @Param(name = "isAutoShift", desc = "", range = "")
    @Param(name = "etl_serv_ip", desc = "", range = "")
    @Param(name = "etl_serv_port", desc = "", range = "")
    @Param(name = "userName", desc = "", range = "")
    @Param(name = "userName", desc = "", range = "")
    @Param(name = "deploymentPath", desc = "", range = "")
    public static void startEngineBatchControl(String batch_date, String sys_end_date, String etl_sys_cd, String isResumeRun, String isAutoShift, String etl_serv_ip, String etl_serv_port, String userName, String password, String deploymentPath, Long user_id, String etlJobChildId) {
        SSHDetails sshDetails = new SSHDetails(etl_serv_ip, userName, password, Integer.parseInt(etl_serv_port));
        try (SSHOperate sshOperate = new SSHOperate(sshDetails, 0)) {
            deploymentPath = deploymentPath + "/" + etl_sys_cd + "/control";
            if (sys_end_date.trim().length() == 0 || sys_end_date == null) {
                sys_end_date = Constant._MAX_DATE_8;
            }
            String startAgent = "source /etc/profile;source ~/.bash_profile;source ~/.bashrc; cd " + deploymentPath + "/ ;" + "bash control-operation.sh" + " " + batch_date + " " + etl_sys_cd + " " + isResumeRun + " " + isAutoShift + " " + sys_end_date + " restart" + " " + etlJobChildId;
            log.info("######################" + startAgent);
            String commandResult = sshOperate.execCommandBySSH(startAgent);
            log.info("######执行control脚本返回状态说明######" + commandResult);
            List<String> codeDesc = StringUtil.split(commandResult, "\n");
            codeDesc = codeDesc.stream().filter(item -> !item.isEmpty()).collect(Collectors.toList());
            codeDesc = StringUtil.split(codeDesc.get(codeDesc.size() - 1), ":");
            log.info("######执行control脚本最终返回状态说明######" + codeDesc);
            if (codeDesc.size() >= 2 && (!"0".equals(codeDesc.get(codeDesc.size() - 2)))) {
                throw new BusinessException("启动control失败：" + codeDesc.get(codeDesc.size() - 1));
            }
            TimeUnit.SECONDS.sleep(5);
            log.info("##########启动control成功##############");
        } catch (JSchException e) {
            log.error("连接失败，请确认用户名密码正确", e);
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error("网络异常，请确认网络正常", e);
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (Exception e) {
            throw new AppSystemException("启动CONTROL失败！！！" + e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_serv_ip", desc = "", range = "")
    @Param(name = "etl_serv_port", desc = "", range = "")
    @Param(name = "userName", desc = "", range = "")
    @Param(name = "password", desc = "", range = "")
    @Param(name = "deploymentPath", desc = "", range = "")
    public static void startEngineBatchTrigger(String etl_sys_cd, String etl_serv_ip, String etl_serv_port, String userName, String password, String deploymentPath, String curr_path_date) {
        SSHDetails sshDetails = new SSHDetails(etl_serv_ip, userName, password, Integer.parseInt(etl_serv_port));
        try (SSHOperate sshOperate = new SSHOperate(sshDetails, 0)) {
            deploymentPath = deploymentPath + "/" + etl_sys_cd + "/trigger";
            String startAgent = "source /etc/profile;source ~/.profile;source ~/.bashrc; cd " + deploymentPath + "/ ;" + "bash " + Constant.TRIGGER_OPERATION_SH + " " + etl_sys_cd + Constant.SPACE + curr_path_date + " restart";
            log.info("##############" + startAgent);
            sshOperate.execCommandBySSHNoRs(startAgent);
            log.info("##########启动trigger成功##############");
        } catch (JSchException e) {
            log.error("连接失败，请确认用户名密码正确", e);
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error("网络异常，请确认网络正常", e);
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (Exception e) {
            throw new AppSystemException("启动TRIGGER失败！！！" + e);
        }
    }
}
