package hyren.serv6.b.realtimecollection.util;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
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
import hyren.serv6.commons.utils.datastorage.httpserver.HttpServer;
import hyren.serv6.commons.utils.datastorage.httpserver.HttpYaml;
import hyren.serv6.commons.utils.scpconf.ScpHadoopConf;
import hyren.serv6.commons.utils.xlstoxml.util.XmlUtil;
import hyren.serv6.commons.utils.yaml.Yaml;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@Component
@DocClass(desc = "", author = "yec", createdate = "2021/05/12")
@Slf4j
public class SdmAgentDeploy {

    public static final String SEPARATOR = File.separator;

    public static final String DEFAULT_dbBatch_row = "5000";

    public static final String APPLICATION_YML = "application.yml";

    public static final String LOG4J2_NAME = "log4j2.xml";

    private final static File file = new File(PropertyParaValue.getString("kafkaAgentPath", ""));

    private static final String CONFPATH = System.getProperty("user.dir") + SEPARATOR + "tempresources" + SEPARATOR + "fdconfig" + SEPARATOR;

    private static final String APPLICATION_PASS = System.getProperty("user.dir") + SEPARATOR + "tempresources" + SEPARATOR;

    private static void mkdirConfPath() {
        File dir = new File(CONFPATH);
        if (dir.exists()) {
            log.info("创建目录 " + CONFPATH + " 已经存在");
        } else {
            if (dir.mkdirs()) {
                log.info("创建目录" + CONFPATH + "成功！");
            } else {
                throw new BusinessException("创建目录" + CONFPATH + "失败！");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "down_info", desc = "", range = "", isBean = true)
    @Param(name = "oldAgentPath", desc = "", range = "")
    @Param(name = "oldLogPath", desc = "", range = "")
    public static String agentConfDeploy(AgentDownInfo down_info, String oldAgentPath, String oldLogPath) {
        try {
            mkdirConfPath();
            Yaml.dump(new SysParaYaml().yamlDataFormat(DEFAULT_dbBatch_row), new File(CONFPATH + SysParaYaml.CONF_FILE_NAME));
            List<HttpYaml> httpServerConfList = new ArrayList<>();
            httpServerConfList.add(HttpServer.getHttpServerDefaultConf(down_info.getAgent_context(), down_info.getAgent_pattern(), down_info.getAgent_ip(), down_info.getAgent_port()));
            httpServerConfList.add(HttpServer.getHttpserverHyrenMainConf(PropertyParaValue.getString("hyren_host", "127.0.0.1"), Integer.parseInt(HttpServerConf.HTTP_PORT), HttpServerConf.HTTP_CONTEXT_PATH, HttpServerConf.HTTP_ACTION_PATTERN));
            Map<String, List<HttpYaml>> httpServerMap = new HashMap<>();
            httpServerMap.put(HttpServer.HTTP_NAME, httpServerConfList);
            Yaml.dump(httpServerMap, new File(CONFPATH + HttpServer.HTTP_CONF_NAME));
        } catch (FileNotFoundException e) {
            log.error(e.toString());
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
        log.info("==================ssh" + JsonUtil.toJson(sshDetails));
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            if (StringUtil.isNotBlank(oldAgentPath)) {
                sshOperate.execCommandBySSH("cd " + agentDir + SEPARATOR + ".bin; bash " + Constant.STREAM_START_AGENT + " " + file.getName() + " " + down_info.getLog_dir() + " " + down_info.getAgent_port() + " stop");
                sshOperate.execCommandBySSH("rm -rf " + agentDir);
            }
            if (StringUtil.isNotBlank(oldLogPath)) {
                sshOperate.execCommandBySSH("rm -rf " + oldLogPath);
            }
            sshOperate.execCommandBySSH("cd " + agentDir + SEPARATOR + ".bin;bash " + Constant.STREAM_START_AGENT + " " + file.getName() + " " + down_info.getLog_dir() + " " + down_info.getAgent_port() + " " + " stop");
            sshOperate.execCommandBySSH("rm -rf " + down_info.getSave_dir() + SEPARATOR + agentDirName);
            sshOperate.execCommandBySSH("mkdir -p " + new File(down_info.getLog_dir()).getParent());
            mkdirToTarget(sshOperate, down_info.getSave_dir() + SEPARATOR + agentDirName);
            String targetDir = down_info.getSave_dir() + SEPARATOR + agentDirName + SEPARATOR + ".bin";
            log.info("系统配置Agent-Jar路径" + file.getAbsolutePath());
            sshOperate.channelSftp.put(file.getAbsolutePath(), targetDir);
            File shellCommandFile = new File(file.getParent() + SEPARATOR + "commandExecut.sh");
            if (!shellCommandFile.exists()) {
                throw new BusinessException("Agent的作业脚本(" + "commandExecut.sh" + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + "commandExecut.sh", targetDir);
            File shellProducerCommandFile = new File(file.getParent() + SEPARATOR + "stream-producer-command.sh");
            if (!shellProducerCommandFile.exists()) {
                throw new BusinessException("Agent的命令触发脚本(" + "stream-producer-command.sh" + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + "stream-producer-command.sh", targetDir);
            File startShellFile = new File(file.getParent() + SEPARATOR + Constant.STREAM_START_AGENT);
            if (!startShellFile.exists()) {
                throw new BusinessException("Agent的启动脚本(" + Constant.STREAM_START_AGENT + ")未找到!!!");
            }
            sshOperate.channelSftp.put(file.getParent() + SEPARATOR + Constant.STREAM_START_AGENT, targetDir);
            String localConfPath = System.getProperty("user.dir") + SEPARATOR + "resources";
            log.info("本地当前工程下的配置文件路径 : " + localConfPath);
            sftpFiles(localConfPath, sshOperate.channelSftp, targetDir);
            sftpFiles(CONFPATH, sshOperate.channelSftp, targetDir + SEPARATOR + "resources");
            getApplicationYml(down_info, targetDir);
            sshOperate.channelSftp.put(APPLICATION_PASS + APPLICATION_YML, targetDir + SEPARATOR + "resources");
            sshOperate.execCommandBySSH("echo 'hasDatabase=false'>>" + targetDir + SEPARATOR + "resources" + SEPARATOR + "fdconfig" + SEPARATOR + "appinfo.conf");
            ScpHadoopConf.scpConfToAgent(targetDir, sshOperate);
            sftpFiles(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "lib", sshOperate.channelSftp, down_info.getSave_dir() + SEPARATOR + agentDirName);
            sftpFiles(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jdbc", sshOperate.channelSftp, down_info.getSave_dir() + SEPARATOR + agentDirName);
            log.info("开始创建远程机器JRE目录");
            createDir(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jre", sshOperate, targetDir + SEPARATOR + "jre");
            log.info("开始SCP JRE目录文件");
            sftpFiles(new File(System.getProperty("user.dir")).getParent() + SEPARATOR + "jre", sshOperate.channelSftp, targetDir);
            updateLog4jXml(sshOperate.channelSftp, targetDir + SEPARATOR + "resources", "Property", down_info.getLog_dir());
            if (IsFlag.Shi.getCode().equals(down_info.getDeploy())) {
                sshOperate.execCommandBySSHNoRs("cd " + targetDir + ";bash " + Constant.STREAM_START_AGENT + " " + file.getName() + " " + down_info.getLog_dir() + " " + down_info.getAgent_port() + " " + " start");
            }
            return targetDir;
        } catch (JSchException e) {
            log.error(e.toString());
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error(e.toString());
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (SftpException e) {
            log.error(e.toString());
            throw new BusinessException("数据传输失败，请检查数据目录是否有权限，请联系管理员" + e.getMessage());
        } catch (Exception e) {
            log.error(e.toString());
            throw new AppSystemException("部署失败，请重新部署!!!" + e.getMessage());
        }
    }

    private static void getApplicationYml(AgentDownInfo downInfo, String targetDir) {
        try {
            Map<String, Object> map = new HashMap<>();
            Map<String, Object> serverMap = new HashMap<>();
            Map<String, Object> servletMap = new HashMap<>();
            Map<String, Object> encodingMap = new HashMap<>();
            encodingMap.put("charset", "UTF-8");
            encodingMap.put("force-response", true);
            servletMap.put("context-path", "/agent");
            servletMap.put("encoding", encodingMap);
            serverMap.put("port", downInfo.getAgent_port());
            serverMap.put("servlet", servletMap);
            map.put("server", serverMap);
            Map<String, Object> server1Map = new HashMap<>();
            Map<String, Object> managementMap = new HashMap<>();
            Map<String, Object> servlet1Map = new HashMap<>();
            servlet1Map.put("context-path", HttpServerConf.HTTP_CONTEXT_PATH);
            server1Map.put("address", InetAddress.getLocalHost().getHostAddress());
            server1Map.put("port", HttpServerConf.HTTP_PORT);
            server1Map.put("servlet", servlet1Map);
            managementMap.put("server", server1Map);
            map.put("management", managementMap);
            Map<String, Object> loggingMap = new HashMap<>();
            Map<String, Object> levelMap = new HashMap<>();
            Map<String, Object> fileMap = new HashMap<>();
            Map<String, Object> patternMap = new HashMap<>();
            patternMap.put("console", "%d{yyyy.MM.dd ''at'' HH:mm:ss } %-5level %class{36} %L %M - %msg%xEx%n");
            fileMap.put("name", downInfo.getLog_dir());
            levelMap.put("root", "warn");
            levelMap.put("org.springframework", "info");
            levelMap.put("fd.ng", "info");
            levelMap.put("hyren", "info");
            levelMap.put("hrds", "info");
            loggingMap.put("level", levelMap);
            loggingMap.put("file", fileMap);
            loggingMap.put("pattern", patternMap);
            map.put("logging", loggingMap);
            File file = new File(APPLICATION_PASS + APPLICATION_YML);
            if (file.exists()) {
                file.delete();
            }
            Yaml.dump(map, new File(APPLICATION_PASS + APPLICATION_YML));
        } catch (FileNotFoundException | UnknownHostException e) {
            e.printStackTrace();
            throw new BusinessException(" application.yml check failed...");
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
        mkdirConfPath();
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

    static void createDir(String sftpDir, SSHOperate sshOperate, String targetDir) {
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
        String[] targetch_machine = { ".bin", "storeConfigPath", "lib", "resources", "fdconfig", "i18n", "jdbc" };
        String rootDir = targetDir + SEPARATOR + targetch_machine[0];
        log.info("创建远程目录 .bin  : " + rootDir);
        try {
            sshOperate.execCommandBySSH("mkdir -p " + rootDir);
            log.info("创建远程目录 lib  : " + targetDir + SEPARATOR + targetch_machine[2]);
            sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + targetch_machine[2]);
            log.info("创建远程目录 jdbc  : " + targetDir + SEPARATOR + targetch_machine[6]);
            sshOperate.execCommandBySSH("mkdir -p " + targetDir + SEPARATOR + targetch_machine[6]);
            log.info("创建远程目录 建立storeConfigPath目录  : " + rootDir + SEPARATOR + targetch_machine[1]);
            sshOperate.execCommandBySSH("mkdir -p " + rootDir + SEPARATOR + targetch_machine[1]);
            log.info("创建远程目录resource/fdconfig目录  : " + rootDir + SEPARATOR + targetch_machine[3] + SEPARATOR + targetch_machine[4]);
            sshOperate.execCommandBySSH("mkdir -p " + rootDir + SEPARATOR + targetch_machine[3] + SEPARATOR + targetch_machine[4]);
            sshOperate.execCommandBySSH("mkdir -p " + rootDir + SEPARATOR + targetch_machine[3] + SEPARATOR + targetch_machine[5]);
        } catch (JSchException | IOException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException(e.getMessage());
        }
    }
}
