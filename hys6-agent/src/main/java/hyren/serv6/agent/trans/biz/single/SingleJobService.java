package hyren.serv6.agent.trans.biz.single;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.Validator;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.commons.utils.agent.constant.PropertyParaUtil;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SingleJobService {

    @Method(desc = "", logicStep = "")
    @Param(name = "database_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "collect_type", desc = "", range = "")
    @Param(name = "etl_date", desc = "", range = "")
    @Param(name = "file_type", desc = "", range = "")
    @Param(name = "agent_down_info", desc = "", range = "")
    @Param(name = "sql_para", desc = "", range = "", nullable = true, valueIfNull = "")
    public void executeSingleJob(String database_id, String table_name, String collect_type, String etl_date, String file_type, String sql_para, String agent_down_info) {
        Validator.notNull(database_id, "任务ID不能为空");
        Validator.notBlank(table_name, "采集表名称不能为空");
        Validator.notBlank(collect_type, "采集类型不能为空");
        Validator.notBlank(etl_date, "跑批日期不能为空");
        Validator.notBlank(file_type, "文件格式类型不能为空");
        AgentDownInfo agentDownInfo = JsonUtil.toObjectSafety(agent_down_info, AgentDownInfo.class).orElse(null);
        if (agentDownInfo != null) {
            String commandShell = String.format("cd %s; bash %s %s %s %s %s %s %s", agentDownInfo.getAi_desc(), Constant.COLLECT_JOB_COMMAND, database_id, table_name, collect_type, etl_date, file_type, sql_para);
            log.info("********agent_down_info: {}*********", agent_down_info);
            SSHDetails sftpDetails = new SSHDetails(agentDownInfo.getAgent_ip(), agentDownInfo.getUser_name(), agentDownInfo.getPasswd(), Integer.parseInt(PropertyParaUtil.getString("sftp_port", "22")));
            try (SSHOperate sftpOperate = new SSHOperate(sftpDetails)) {
                log.info("**************{}**************", "开始执行作业");
                String s = sftpOperate.execCommandBySSH(commandShell);
                log.info("**************{}**************", "执行作业结束");
            } catch (Exception e) {
                log.error(String.valueOf(e));
            }
        } else {
            log.info("**************未获取到Agent信息不执行采集操作**************");
        }
    }
}
