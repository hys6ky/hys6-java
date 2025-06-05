package hyren.serv6.b.batchcollection.agentdepoly;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentStatus;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.base.utils.jsch.ChineseUtil;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.constant.SysParaYaml;
import hyren.serv6.commons.utils.datastorage.QueryLengthMapping;
import hyren.serv6.commons.utils.jsch.AgentDeploy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2019-08-30 10:01")
@Service
@Slf4j
public class AgentDeployService {

    private static final String agentConfName = "agentConf.tar.gz";

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getDataSourceInfo() {
        return Dbo.queryResult("select ds.source_id, ds.datasource_name, " + " sum(case ai.agent_type when ? then 1 else 0 end) as dbflag, " + " sum(case ai.agent_type when ? then 1 else 0 end) as dfflag, " + " sum(case ai.agent_type when ? then 1 else 0 end) as nonstructflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as halfstructflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as ftpflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as fileflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as restflag" + " from " + DataSource.TableName + " ds " + " left join " + AgentInfo.TableName + " ai " + " on ds.source_id = ai.source_id" + " where ai.user_id = ?" + " group by ds.source_id order by datasource_name", AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode(), AgentType.WenJianXiTong.getCode(), AgentType.DuiXiang.getCode(), AgentType.FTP.getCode(), AgentType.WenBenLiu.getCode(), AgentType.XiaoXiLiu.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAgentInfo(long source_id, String agent_type) {
        return Dbo.queryList("SELECT *," + " ( CASE WHEN agent_type = ? THEN ? " + "   WHEN agent_type = ? THEN ?" + "   WHEN agent_type = ? THEN ? " + "   WHEN agent_type = ? THEN ? " + "   WHEN agent_type = ? THEN ? " + "   WHEN agent_type = ? THEN ? " + "   WHEN agent_type = ? THEN ? END) agent_zh_name," + "(CASE WHEN agent_status = ? THEN ? " + "  WHEN agent_status = ? THEN ? END) connection_status" + " FROM agent_info WHERE source_id = ? AND agent_type = ? AND user_id = ?", AgentType.ShuJuKu.getCode(), AgentType.ShuJuKu.getValue(), AgentType.DBWenJian.getCode(), AgentType.DBWenJian.getValue(), AgentType.DuiXiang.getCode(), AgentType.DuiXiang.getValue(), AgentType.WenJianXiTong.getCode(), AgentType.WenJianXiTong.getValue(), AgentType.FTP.getCode(), AgentType.FTP.getValue(), AgentType.WenBenLiu.getCode(), AgentType.WenBenLiu.getValue(), AgentType.XiaoXiLiu.getCode(), AgentType.XiaoXiLiu.getValue(), AgentStatus.WeiLianJie.getCode(), AgentStatus.WeiLianJie.getValue(), AgentStatus.YiLianJie.getCode(), AgentStatus.YiLianJie.getValue(), source_id, agent_type, UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAgentDownInfo(long agent_id) {
        Map<String, Object> queryOneObject = Dbo.queryOneObject("SELECT t2.* FROM " + AgentInfo.TableName + " t1" + " JOIN " + AgentDownInfo.TableName + " t2" + " ON t1.agent_ip = t2.agent_ip AND t1.agent_port = t2.agent_port AND t1.agent_id = t2.agent_id" + " WHERE t1.agent_id = ? AND t1.user_id = ?", agent_id, UserUtil.getUserId());
        queryOneObject.put("agentDeployPath", PropertyParaValue.getString("agentDeployPath", "/home/hyshf/"));
        if (queryOneObject.get("remark") == null) {
            queryOneObject.put("remark", PropertyParaValue.getInt("dbBatch_row", 5000));
        }
        return queryOneObject;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    @Param(name = "customPath", desc = "", range = "", valueIfNull = { "0" })
    @Param(name = "oldAgentDir", desc = "", range = "", nullable = true)
    @Param(name = "oldLogPath", desc = "", range = "", nullable = true)
    public void saveAgentDownInfo(AgentDownInfo agent_down_info, String customPath, String oldAgentDir, String oldLogPath) {
        if (StringUtil.isNotBlank(customPath)) {
            if (IsFlag.Fou.getCode().equals(customPath)) {
                agent_down_info.setSave_dir(PropertyParaValue.getString("agentDeployPath", "/home/hyshf/"));
                String agentDirName = ChineseUtil.getPingYin(agent_down_info.getAgent_name()) + "_" + agent_down_info.getAgent_port();
                agent_down_info.setLog_dir(PropertyParaValue.getString("agentDeployPath", "/home/hyshf/") + File.separator + agentDirName + File.separator + "running" + File.separator + "running.log");
            }
        }
        String deployFinalDir = AgentDeploy.agentConfDeploy(agent_down_info, oldAgentDir, oldLogPath);
        agent_down_info.setAi_desc(deployFinalDir);
        agent_down_info.setAgent_date(DateUtil.getSysDate());
        agent_down_info.setAgent_time(DateUtil.getSysTime());
        if (agent_down_info.getDown_id() == null) {
            agent_down_info.setDown_id(PrimayKeyGener.getNextId());
            if (agent_down_info.add(Dbo.db()) != 1) {
                throw new BusinessException("Agent部署信息保存失败");
            }
        } else {
            if (agent_down_info.update(Dbo.db()) != 1) {
                throw new BusinessException("重新部署Agent (" + agent_down_info.getAgent_name() + ") 失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "down_id", desc = "", range = "")
    public String downloadAgentConf(long down_id) {
        Optional<AgentDownInfo> agent_down_info = Dbo.queryOneObject(AgentDownInfo.class, "select * from " + AgentDownInfo.TableName + " where down_id = ?", down_id);
        if (!agent_down_info.isPresent()) {
            throw new BusinessException(down_id + "对应agent还未部署，请部署后下载");
        }
        AgentDownInfo down_info = agent_down_info.get();
        SSHDetails sshDetails = SSHOperate.getSSHDetails(down_info.getAgent_ip(), down_info.getUser_name(), down_info.getPasswd(), CommonVariables.SFTP_PORT);
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            String confDir = down_info.getSave_dir() + File.separator + ChineseUtil.getPingYin(down_info.getAgent_name()) + "_" + down_info.getAgent_port() + File.separator + ".bin" + File.separator;
            String applicationConf = confDir + "resources" + File.separator + AgentDeploy.YML_FILE_NAME;
            String hyrenConf = confDir + "resources" + File.separator + "hyrenconf" + File.separator;
            String lengthMappingConf = hyrenConf + QueryLengthMapping.CONF_FILE_NAME;
            String sysParaConf = hyrenConf + SysParaYaml.CONF_FILE_NAME;
            String storeConfigPath = confDir + "storeConfigPath";
            sshOperate.execCommandBySSH("tar -zvcf " + confDir + agentConfName + Constant.SPACE + storeConfigPath + Constant.SPACE + lengthMappingConf + Constant.SPACE + sysParaConf + Constant.SPACE + applicationConf);
            String remotePath = confDir + agentConfName;
            String localPath = WebinfoProperties.FileUpload_SavedDirName.endsWith(File.separator) ? WebinfoProperties.FileUpload_SavedDirName + agentConfName : WebinfoProperties.FileUpload_SavedDirName + File.separator + agentConfName;
            ;
            FileDownloadUtil.downloadLogFile(remotePath, localPath, sshDetails);
            FileDownloadUtil.deleteLogFileBySFTP(remotePath, sshDetails);
            return agentConfName;
        } catch (JSchException e) {
            log.error(e.getMessage());
            throw new BusinessException("连接失败，请确认用户名密码正确" + e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new BusinessException("网络异常，请确认网络正常" + e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new AppSystemException("下载失败" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void downloadFile(String fileName) {
        String file_path = WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName;
        FileDownloadUtil.downloadFile(file_path);
    }
}
