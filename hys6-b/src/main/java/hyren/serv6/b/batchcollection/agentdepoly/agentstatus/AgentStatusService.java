package hyren.serv6.b.batchcollection.agentdepoly.agentstatus;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentStatus;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.jsch.ChineseUtil;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-10-14 14:22")
public class AgentStatusService {

    public static final String SEPARATOR = File.separator;

    public List<Map<String, Object>> agentInfo() {
        return Dbo.queryList("SELECT STRING_AGG(t1.agent_name,',') agent_name, t1.agent_ip, t1.agent_port, t1.agent_status," + " t1.agent_type, t2.down_id, t2.save_dir, t2.log_dir, t2.ai_desc, t2.user_name, t2.passwd," + " t2.agent_date, t2.agent_time, t3.datasource_name," + " (case when t1.agent_status = ? then ? when t1.agent_status = ? then ? else ? end) statuszh" + " FROM " + AgentInfo.TableName + " t1 JOIN " + AgentDownInfo.TableName + " t2" + " ON t1.agent_ip = t2.agent_ip and t1.agent_port = t2.agent_port" + " JOIN " + DataSource.TableName + " t3 ON t1.source_id = t3.source_id" + " WHERE t1.user_id = ? " + " GROUP BY t1.agent_type, t1.agent_ip, t1.agent_port, t1.agent_status," + " t2.save_dir, t2.log_dir, t2.ai_desc, t2.down_id," + " t2.user_name, t2.passwd, t2.agent_date, t2.agent_time, t3.datasource_name", AgentStatus.YiLianJie.getCode(), AgentStatus.YiLianJie.getValue(), AgentStatus.WeiLianJie.getCode(), AgentStatus.WeiLianJie.getValue(), AgentStatus.ZhengZaiYunXing.getValue(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public void startAgent(AgentDownInfo agent_down_info) {
        checkParams(agent_down_info);
        executeShell(agent_down_info, "start");
        updateAgentDateTime(agent_down_info);
    }

    private void updateAgentDateTime(AgentDownInfo agent_down_info) {
        Dbo.execute("UPDATE " + AgentDownInfo.TableName + " set agent_date = ?, agent_time = ? WHERE down_id = ?", DateUtil.getSysDate(), DateUtil.getSysTime(), agent_down_info.getDown_id());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    public void stopAgent(AgentDownInfo agent_down_info) {
        checkParams(agent_down_info);
        executeShell(agent_down_info, "stop");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    public void restartAgent(AgentDownInfo agent_down_info) {
        checkParams(agent_down_info);
        executeShell(agent_down_info, "stop");
        executeShell(agent_down_info, "start");
        updateAgentDateTime(agent_down_info);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_down_info", desc = "", range = "", isBean = true)
    @Param(name = "executeType", desc = "", range = "")
    private void executeShell(AgentDownInfo agent_down_info, String executeType) {
        List<String> agentNameList = StringUtil.split(agent_down_info.getAgent_name(), ",");
        for (String agent_name : agentNameList) {
            String agentDirName = ChineseUtil.getPingYin(agent_name) + "_" + agent_down_info.getAgent_port();
            String targetDir = agent_down_info.getSave_dir() + SEPARATOR + agentDirName + SEPARATOR + ".bin";
            SSHDetails sshDetails = SSHOperate.getSSHDetails(agent_down_info.getAgent_ip(), agent_down_info.getUser_name(), agent_down_info.getPasswd(), CommonVariables.SFTP_PORT);
            try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
                AgentType agentType = AgentType.ofEnumByCode(agent_down_info.getAgent_type());
                IsFlag is_stream_agent_flag = (agentType == AgentType.WenBenLiu || agentType == AgentType.XiaoXiLiu) ? IsFlag.Shi : IsFlag.Fou;
                String start_agent_script = is_stream_agent_flag == IsFlag.Shi ? Constant.STREAM_START_AGENT : Constant.START_AGENT;
                File agent_jar_file = is_stream_agent_flag == IsFlag.Shi ? CommonVariables.STREAM_AGENT_JAR_PATH : CommonVariables.COLLECT_AGENT_JAR_PATH;
                String exec_rs = sshOperate.execCommandBySSH("cd " + targetDir + ";bash " + start_agent_script + " " + agent_jar_file.getName() + " " + agent_down_info.getLog_dir() + " " + agent_down_info.getAgent_port() + " " + executeType);
                log.debug("启动命令执行结果: [ {} ]", exec_rs);
            } catch (JSchException e) {
                log.error("连接失败，请确认用户名密码正确", e);
                throw new BusinessException("连接失败，请确认用户名密码正确" + e);
            } catch (IOException e) {
                log.error("网络异常，请确认网络正常", e);
                throw new BusinessException("网络异常，请确认网络正常" + e);
            } catch (Exception e) {
                log.error("部署失败，请重新部署!!!", e);
                throw new AppSystemException("部署失败，请重新部署!!!" + e);
            }
        }
    }

    private void checkParams(AgentDownInfo agent_down_info) {
        Validator.notNull(agent_down_info.getAgent_ip(), "Agent IP不能为空");
        Validator.notNull(agent_down_info.getAgent_port(), "Agent端口不能为空");
        Validator.notNull(agent_down_info.getLog_dir(), "Agent日志路径不能为空");
        Validator.notNull(agent_down_info.getAgent_name(), "Agent名称不能为空");
    }
}
