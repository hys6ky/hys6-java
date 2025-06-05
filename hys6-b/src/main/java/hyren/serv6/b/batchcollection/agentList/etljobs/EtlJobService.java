package hyren.serv6.b.batchcollection.agentList.etljobs;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EtlJobService {

    @Method(desc = "", logicStep = "")
    @Param(name = "database_id", desc = "", range = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "sub_sys_cd", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    public void saveEtlJobs(String database_id, Long etl_sys_id, Long sub_sys_id, String agent_type) {
        AgentType agentType = AgentType.ofEnumByCode(agent_type);
        int executeStatus = EtlJobUtil.saveJob(database_id, DataSourceType.DCL, etl_sys_id, sub_sys_id, agentType);
        if (executeStatus == -1) {
            CheckParam.throwErrorMsg("生成作业失败");
        }
    }
}
