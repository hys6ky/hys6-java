package hyren.serv6.commons.utils.jsch;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.jsch.ChineseUtil;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.config.httpconfig.HttpServerConf;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.constant.SysParaYaml;
import hyren.serv6.commons.utils.datastorage.QueryLengthMapping;
import hyren.serv6.commons.utils.scpconf.ScpHadoopConf;
import hyren.serv6.commons.utils.xlstoxml.util.XmlUtil;
import hyren.serv6.commons.utils.yaml.Yaml;
import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.yaml.snakeyaml.DumperOptions;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-01-15 14:20")
@Slf4j
public class AgentDeploy {

    public static final String SEPARATOR = File.separator;

    public static final String LOG4J2_NAME = "log4j2.xml";

    public static final String YML_FILE_NAME = "application.yml";

    private final static File file = new File(PropertyParaValue.getString("agentpath", ""));

    private static final String CONFPATH = System.getProperty("user.dir") + SEPARATOR + "tempresources" + SEPARATOR + "hyrenconf" + SEPARATOR;

    private static final String APPLICATIONCONFPATH = System.getProperty("user.dir") + SEPARATOR + "tempresources" + SEPARATOR;

    static {
        File dir = new File(CONFPATH);
        if (dir.exists()) {
            log.info("创建目录 " + CONFPATH + " 已经存在");
        } else {
            if (dir.mkdirs()) {
                log.info("创建目录" + CONFPATH + "成功！");
            } else {
                log.info("创建目录" + CONFPATH + "失败！");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "down_info", desc = "", range = "", isBean = true)
    @Param(name = "oldAgentPath", desc = "", range = "")
    @Param(name = "oldLogPath", desc = "", range = "")
    public static String agentConfDeploy(AgentDownInfo down_info, String oldAgentPath, String oldLogPath) {
        try {
            Yaml.dump(QueryLengthMapping.getLengthMapping(), new File(CONFPATH + QueryLengthMapping.CONF_FILE_NAME));
            Yaml.dump(new SysParaYaml().yamlDataFormat(down_info.getRemark()), new File(CONFPATH + SysParaYaml.CONF_FILE_NAME));
            writeYmlFile(down_info);
        } catch (FileNotFoundException e) {
            log.error(e.getMessage());
            throw new BusinessException("文件或目录不存在：" + e.getMessage());
        }
        return sftpAgentToTargetMachine(down_info, oldAgentPath, oldLogPath);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "down_info", desc = "", range = "", isBean = true)
    @Param(name = "oldAgentPath", desc = "", range = "")
    @Param(name = "oldLogPath", desc = "", range = "")
    @Return(desc = "", range = "")
    private static String sftpAgentToTargetMachine(AgentDownInfo down_info, String oldAgentPath, String oldLogPath) {
        String agentDirName = ChineseUtil.getPingYin(down_info.getAgent_name()) + "_" + down_info.getAgent_port();
        String agentDir = oldAgentPath + SEPARATOR + agentDirName;
        SSHDetails sshDetails = SSHOperate.getSSHDetails(down_info.getAgent_ip(), down_info.getUser_name(), down_info.getPasswd(), CommonVariables.SFTP_PORT);
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            if (StringUtil.isNotBlank(oldAgentPath)) {
                sshOperate.execCommandBySSH("cd " + agentDir + SEPARATOR + ".bin; bash " + Constant.START_AGENT + " " + file.getName() + " " + down_info.getLog_dir() + " " + down_info.getAgent_port() + " stop");
                sshOperate.execCommandBySSH("rm -rf " + agentDir);
            }
            if (StringUtil.isNotBlank(oldLogPath)) {
                sshOperate.execCommandBySSH("rm -rf " + oldLogPath);
            }
            sshOperate.execCommandBySSH("cd " + agentDir + SEPARATOR + ".bin;bash " + Constant.START_AGENT + " " + file.getName() + " " + down_info.getLog_dir() + " " + down_info.getAgent_port() + " " + " stop");
            sshOperate.execCommandBySSH("rm -rf " + down_info.getSave_dir() + SEPARATOR + agentDirName);
            sshOperate.execCommandBySSH("mkdir -p " + new File(down_info.getLog_dir()).getParent());
            mkdirToTarget(sshOperate, down_info.getSave_dir() + SEPARATOR + agentDirName);
            String targetDir = down_info.getSave_dir() + SEPARATOR + agentDirName + SEPARATOR + ".bin";
            log.info("系统配置Agent-Jar路径" + file.getAbsolutePath());
            sshOperate.channelSftp.put(file.getAbsolutePath(), targetDir);
            String cdcJarFilePath = file.getParent() + SEPARATOR + "hyren-serv6-agent-collect-cdc.jar";
            File cdcJarFile = new File(cdcJarFilePath);
            if (!cdcJarFile.exists()) {
                throw new BusinessException("Agent的实时采集 jar 包(hyren-serv6-agent-collect-cdc.jar)未找到!!!");
            }
            sshOperate.channelSftp.put(cdcJarFilePath, targetDir);
            File shellCommandFile = new File(file.getParent() + SEPARATOR + Constant.COLLECT_JOB_COMMAND);
            if (!shellCommandFile.exists()) {
                throw new BusinessException("Agent的作业脚本(" + Constant.COLLECT_JOB_COMMAND + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.COLLECT_JOB_COMMAND, targetDir);
            File cdcConsumerFile = new File(file.getParent() + SEPARATOR + Constant.COLLECT_CDC_JOB_CONSUMER_COMMAND);
            if (!cdcConsumerFile.exists()) {
                throw new BusinessException("Agent的实时采集作业脚本(" + Constant.COLLECT_CDC_JOB_CONSUMER_COMMAND + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.COLLECT_CDC_JOB_CONSUMER_COMMAND, targetDir);
            File cdcProducerFile = new File(file.getParent() + SEPARATOR + Constant.COLLECT_CDC_JOB_PRODUCER_COMMAND);
            if (!cdcProducerFile.exists()) {
                throw new BusinessException("Agent的实时采集作业脚本(" + Constant.COLLECT_CDC_JOB_PRODUCER_COMMAND + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.COLLECT_CDC_JOB_PRODUCER_COMMAND, targetDir);
            File unstructuredCollectionFile = new File(file.getParent() + SEPARATOR + Constant.UNSTRUCTURED_COLLECTION);
            if (!unstructuredCollectionFile.exists()) {
                throw new BusinessException("Agent的非结构化采集作业脚本(" + Constant.UNSTRUCTURED_COLLECTION + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.UNSTRUCTURED_COLLECTION, targetDir);
            File semiStructuredCollectionFile = new File(file.getParent() + SEPARATOR + Constant.SEMISTRUCTURED_JOB_COMMAND);
            if (!semiStructuredCollectionFile.exists()) {
                throw new BusinessException("Agent的半结构化采集作业脚本(" + Constant.SEMISTRUCTURED_JOB_COMMAND + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.SEMISTRUCTURED_JOB_COMMAND, targetDir);
            File ftpCollectionFile = new File(file.getParent() + SEPARATOR + Constant.FTP_JOB_COMMAND);
            if (!ftpCollectionFile.exists()) {
                throw new BusinessException("Agent的FTP采集作业脚本(" + Constant.FTP_JOB_COMMAND + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.FTP_JOB_COMMAND, targetDir);
            File startShellFile = new File(file.getParent() + SEPARATOR + Constant.START_AGENT);
            if (!startShellFile.exists()) {
                throw new BusinessException("Agent的启动脚本(" + Constant.START_AGENT + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.START_AGENT, targetDir);
            sftpFiles(CONFPATH, sshOperate.channelSftp, targetDir + SEPARATOR + "resources");
            sshOperate.channelSftp.put(APPLICATIONCONFPATH + YML_FILE_NAME, targetDir + SEPARATOR + "resources");
            ScpHadoopConf.scpConfToAgent(targetDir, sshOperate);
            String userDirParent = new File(System.getProperty("user.dir")).getParent();
            sftpFiles(userDirParent + SEPARATOR + "jars", sshOperate.channelSftp, down_info.getSave_dir() + SEPARATOR + agentDirName);
            sftpFiles(userDirParent + SEPARATOR + "jdbc", sshOperate.channelSftp, down_info.getSave_dir() + SEPARATOR + agentDirName);
            sftpFiles(System.getProperty("user.dir") + SEPARATOR + "collect-cdc-job-jars", sshOperate.channelSftp, down_info.getSave_dir() + SEPARATOR + agentDirName + SEPARATOR + ".bin");
            log.info("开始创建远程机器JRE目录");
            createDir(userDirParent + SEPARATOR + "jre", sshOperate, targetDir + SEPARATOR + "jre");
            log.info("开始SCP JRE目录文件");
            sftpFiles(userDirParent + SEPARATOR + "jre", sshOperate.channelSftp, targetDir);
            updateLog4jXml(sshOperate.channelSftp, targetDir + SEPARATOR + "resources", "Property", down_info.getLog_dir());
            if (IsFlag.Shi.getCode().equals(down_info.getDeploy())) {
                sshOperate.execCommandBySSHNoRs("cd " + targetDir + ";bash " + Constant.START_AGENT + " " + file.getName() + " " + down_info.getLog_dir() + " " + down_info.getAgent_port() + " " + " start");
            }
            return targetDir;
        } catch (JSchException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (SftpException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("数据传输失败，请检查数据目录是否有权限，请联系管理员" + e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new AppSystemException("部署失败，请重新部署!!!" + e.getMessage());
        }
    }

    public static void updateLog4jXml(ChannelSftp chSftp, String targetDir, String nodeName, String log_path) {
        Document doc = XmlUtil.readXML(System.getProperty("user.dir") + SEPARATOR + "resources" + SEPARATOR + LOG4J2_NAME);
        NodeList node = doc.getElementsByTagName(nodeName);
        File logFile = new File(log_path);
        for (int i = 0; i < node.getLength(); i++) {
            Node item = node.item(i);
            NodeList childNodes = item.getChildNodes();
            for (int j = 0; j < childNodes.getLength(); j++) {
                Node childItem = childNodes.item(j);
                if (i == 0) {
                    childItem.setNodeValue(logFile.getParent() + SEPARATOR);
                } else {
                    childItem.setNodeValue(logFile.getName());
                }
            }
        }
        XmlUtil.toFile(doc, CONFPATH + SEPARATOR + LOG4J2_NAME, XmlUtil.UTF_8);
        try {
            log.info("开始传输log4j配置文件: " + CONFPATH + SEPARATOR + LOG4J2_NAME);
            chSftp.put(CONFPATH + SEPARATOR + LOG4J2_NAME, targetDir);
        } catch (SftpException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }
    }

    public static void sftpFiles(String sftpDir, ChannelSftp chSftp, String targetDir) {
        File file = new File(sftpDir);
        File[] confFiles = file.listFiles();
        if (null != confFiles && confFiles.length > 0) {
            for (File confFile : confFiles) {
                if (confFile.isDirectory()) {
                    sftpFiles(confFile.getAbsolutePath(), chSftp, targetDir + SEPARATOR + new File(confFile.getParent()).getName());
                } else {
                    try {
                        chSftp.put(confFile.getAbsolutePath(), targetDir + SEPARATOR + new File(confFile.getParent()).getName());
                    } catch (SftpException e) {
                        log.error(e.getMessage(), e);
                        throw new BusinessException(e.getMessage());
                    }
                }
            }
        } else {
            log.info(sftpDir + "目录下没有文件");
        }
    }

    public static void createDir(String sftpDir, SSHOperate sshOperate, String targetDir) {
        try {
            File file = new File(sftpDir);
            File[] confFiles = file.listFiles();
            if (confFiles == null) {
                throw new BusinessException("JRE目录不存在,请给检查");
            }
            for (File confFile : confFiles) {
                if (confFile.isDirectory()) {
                    sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + confFile.getName());
                    createDir(confFile.getAbsolutePath(), sshOperate, targetDir + SEPARATOR + confFile.getName());
                }
            }
        } catch (JSchException | IOException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }
    }

    private static void mkdirToTarget(SSHOperate sshOperate, String targetDir) {
        String bin = ".bin";
        String storeConfigPath = "storeConfigPath";
        String jars = "jars";
        String jdbc = "jdbc";
        String collect_cdc_job_jars = "collect-cdc-job-jars";
        String rootDir = targetDir + SEPARATOR + bin;
        String resources = "resources";
        String hyrenconf = "hyrenconf";
        try {
            log.info("创建远程目录 .bin  : " + rootDir);
            sshOperate.execCommandBySSH("mkdir -p " + rootDir);
            log.info("创建远程目录 lib  : " + targetDir + SEPARATOR + jars);
            sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + jars);
            log.info("创建远程目录 jdbc  : " + targetDir + SEPARATOR + jdbc);
            sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + jdbc);
            log.info("创建远程目录 collect-cdc-job-jars  : " + targetDir + SEPARATOR + collect_cdc_job_jars);
            sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + ".bin" + SEPARATOR + collect_cdc_job_jars);
            log.info("创建远程目录 建立storeConfigPath目录  : " + rootDir + SEPARATOR + storeConfigPath);
            sshOperate.execCommandBySSH("mkdir -p " + rootDir + SEPARATOR + storeConfigPath);
            String resourcesPath = rootDir + SEPARATOR + resources + SEPARATOR;
            log.info("创建远程目录resource目录: {}", resourcesPath);
            sshOperate.execCommandBySSH("mkdir -p " + resourcesPath);
            String hyrenConfPath = rootDir + SEPARATOR + resources + SEPARATOR + hyrenconf + SEPARATOR;
            log.info("创建远程目录hyrenconf目录: {}", hyrenConfPath);
            sshOperate.execCommandBySSH("mkdir -p " + hyrenConfPath);
        } catch (JSchException | IOException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }
    }

    private static void writeYmlFile(AgentDownInfo agentDownInfo) {
        Map<String, Object> ymlConfMap = new LinkedHashMap<>();
        Map<String, Object> serverConfig = new LinkedHashMap<>();
        serverConfig.put("port", Integer.parseInt(agentDownInfo.getAgent_port()));
        Map<String, Object> servletMap = new LinkedHashMap<>();
        servletMap.put("context-path", agentDownInfo.getAgent_context());
        serverConfig.put("servlet", servletMap);
        Map<String, Object> encodingMap = new LinkedHashMap<>();
        encodingMap.put("charset", "UTF-8");
        encodingMap.put("force-response", true);
        servletMap.put("encoding", encodingMap);
        ymlConfMap.put("server", serverConfig);
        Map<String, Object> managementServerConf = new LinkedHashMap<>();
        Map<String, Object> serverConfMap = new LinkedHashMap<>();
        String hyrenHost = PropertyParaValue.getString("hyren_host", "127.0.0.1");
        serverConfMap.put("address", hyrenHost);
        serverConfMap.put("port", PropertyParaValue.getLong("hyren_port", 20001));
        Map<String, Object> mangementServletMap = new LinkedHashMap<>();
        mangementServletMap.put("context-path", Constant.GATEWAY_CONTEXT_PATH + HttpServerConf.HTTP_CONTEXT_PATH);
        serverConfMap.put("servlet", mangementServletMap);
        serverConfMap.put("actionPattern", HttpServerConf.HTTP_ACTION_PATTERN);
        managementServerConf.put("server", serverConfMap);
        ymlConfMap.put("management", managementServerConf);
        Map<String, Object> logConfig = new LinkedHashMap<>();
        Map<String, Object> levelMap = new LinkedHashMap<>();
        levelMap.put("root", "warn");
        levelMap.put("org.springframework", "info");
        levelMap.put("fd.ng", "info");
        levelMap.put("hyren", "info");
        levelMap.put("hrds", "info");
        logConfig.put("level", levelMap);
        Map<String, Object> patternMap = new LinkedHashMap<>();
        patternMap.put("console", "%d{yyyy.MM.dd 'at' HH:mm:ss } %-5level %class{36} %L %M - %msg%xEx%n");
        logConfig.put("pattern", patternMap);
        ymlConfMap.put("logging", logConfig);
        Map<String, Object> springConfMap = new LinkedHashMap<>();
        Map<String, Object> applicationMap = new LinkedHashMap<>();
        applicationMap.put("name", "agent");
        Map<String, Object> mvcMap = new LinkedHashMap<>();
        mvcMap.put("throw-exception-if-no-handler-found", true);
        Map<String, Object> resourcesMap = new LinkedHashMap<>();
        resourcesMap.put("add-mappings", false);
        springConfMap.put("application", applicationMap);
        springConfMap.put("mvc", mvcMap);
        springConfMap.put("resources", resourcesMap);
        ymlConfMap.put("spring", springConfMap);
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml(options);
        String yamlString = yaml.dump(ymlConfMap);
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(APPLICATIONCONFPATH + YML_FILE_NAME)), StandardCharsets.UTF_8))) {
            printWriter.write(yamlString);
        } catch (IOException e) {
            throw new AppSystemException(String.format("写agent %s配置文件失败:%s", YML_FILE_NAME, e));
        }
    }
}
