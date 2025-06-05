package hyren.serv6.b.realtimecollection.agentdeploy;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.realtimecollection.util.SdmAgentDeploy;
import hyren.serv6.base.codes.AgentStatus;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.jsch.ChineseUtil;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.stream.KafkaMonitorManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2020-04-07")
@Slf4j
@Service
public class SdmAgentDeployService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getSdmDataSourceInfo() {
        return Dbo.queryResult("select ds.source_id, ds.datasource_name, " + " sum(case ai.agent_type when ? then 1 else 0 end) as fileflag, " + " sum(case ai.agent_type when ? then 1 else 0 end) as restflag " + " from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai " + " on ds.source_id = ai.source_id" + " where ai.user_id = ?" + " group by ds.source_id,ds.datasource_name", AgentType.WenBenLiu.getCode(), AgentType.XiaoXiLiu.getCode(), getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Param(name = "sdm_agent_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getSdmAgentInfo(long sdm_source_id, String sdm_agent_type) {
        return Dbo.queryList("SELECT *," + " ( CASE WHEN agent_type = ? THEN ? " + "   WHEN agent_type = ? THEN ? END) agent_zh_name," + "(CASE WHEN agent_status = ? THEN ? " + "  WHEN agent_status = ? THEN ? END) connection_status" + " FROM  " + AgentInfo.TableName + " WHERE source_id = ? AND agent_type = ? AND user_id = ?", AgentType.WenBenLiu.getCode(), AgentType.WenBenLiu.getValue(), AgentType.XiaoXiLiu.getCode(), AgentType.XiaoXiLiu.getValue(), AgentStatus.WeiLianJie.getCode(), AgentStatus.WeiLianJie.getValue(), AgentStatus.YiLianJie.getCode(), AgentStatus.YiLianJie.getValue(), sdm_source_id, sdm_agent_type, getUserId());
    }

    @Autowired
    KafkaMonitorManager manager;

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public String getBrokerServer() {
        String brokerServer = manager.parseBrokerServer();
        if (StringUtil.isEmpty(brokerServer)) {
            throw new BusinessException("获取流服务主机节点信息失败!");
        }
        return brokerServer;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getSdmAgentDownInfo(long agent_id) {
        Map<String, Object> queryOneObject = Dbo.queryOneObject("select * from agent_down_info where agent_id = ? and user_id = ?", agent_id, getUserId());
        queryOneObject.put("agentDeployPath", PropertyParaValue.getString("agentDeployPath", "/home/hyshf/"));
        return queryOneObject;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    @Param(name = "customPath", desc = "", range = "", valueIfNull = { "0" })
    @Param(name = "oldAgentDir", desc = "", range = "", nullable = true)
    @Param(name = "oldLogPath", desc = "", range = "", nullable = true)
    public AgentDownInfo saveSdmAgentDownInfo(AgentDownInfo agent_down_info, String customPath, String oldAgentDir, String oldLogPath) {
        isCustomPath(agent_down_info, customPath);
        String deployFinalDir = SdmAgentDeploy.agentConfDeploy(agent_down_info, oldAgentDir, oldLogPath);
        agent_down_info.setAi_desc(deployFinalDir);
        Optional<AgentDownInfo> dbAgent = Dbo.queryOneObject(AgentDownInfo.class, " select * from " + AgentDownInfo.TableName + " where agent_ip = ? and agent_port = ? and agent_name = ? ", agent_down_info.getAgent_ip(), agent_down_info.getAgent_port(), agent_down_info.getAgent_name());
        if (!dbAgent.isPresent()) {
            agent_down_info.setDown_id(PrimayKeyGener.getNextId());
            if (agent_down_info.add(Dbo.db()) != 1) {
                throw new BusinessException("Agent部署信息保存失败");
            }
        } else {
            agent_down_info.setDown_id(dbAgent.get().getDown_id());
            updateSdmAgentDownInfo(agent_down_info, customPath, oldAgentDir, oldLogPath);
        }
        return agent_down_info;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    @Param(name = "customPath", desc = "", range = "", valueIfNull = { "0" })
    private void isCustomPath(AgentDownInfo agent_down_info, String customPath) {
        if (StringUtil.isNotBlank(customPath)) {
            if (IsFlag.Fou == IsFlag.ofEnumByCode(customPath)) {
                agent_down_info.setSave_dir(PropertyParaValue.getString("agentDeployPath", "/home/hyshf/"));
                String agentDirName = ChineseUtil.getPingYin(agent_down_info.getAgent_name()) + "_" + agent_down_info.getAgent_port();
                agent_down_info.setLog_dir(PropertyParaValue.getString("agentDeployPath", "/home/hyshf/") + File.separator + agentDirName + File.separator + "running" + File.separator + "running.log");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    @Param(name = "customPath", desc = "", range = "", valueIfNull = { "0" })
    @Param(name = "oldAgentDir", desc = "", range = "", nullable = true)
    @Param(name = "oldLogPath", desc = "", range = "", nullable = true)
    public void updateSdmAgentDownInfo(AgentDownInfo agent_down_info, String customPath, String oldAgentDir, String oldLogPath) {
        isCustomPath(agent_down_info, customPath);
        String deployFinalDir = SdmAgentDeploy.agentConfDeploy(agent_down_info, oldAgentDir, oldLogPath);
        agent_down_info.setAi_desc(deployFinalDir);
        if (agent_down_info.update(Dbo.db()) != 1) {
            throw new BusinessException("重新部署Agent (" + agent_down_info.getAgent_name() + ") 失败");
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAgentNumAndSourceNum() {
        return Dbo.queryOneObject("SELECT COUNT(agent_id) agentNum,COUNT(DISTINCT(source_id)) sourceNum FROM " + AgentInfo.TableName + " WHERE user_id = ?", getUserId());
    }
}
