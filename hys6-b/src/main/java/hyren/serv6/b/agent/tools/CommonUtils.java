package hyren.serv6.b.agent.tools;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;

public class CommonUtils {

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    public static void isAgentExist(long agent_id, long user_id) {
        if (Dbo.queryNumber("SELECT count(*) FROM " + AgentDownInfo.TableName + " t1 join " + AgentInfo.TableName + " t2 on t1.agent_ip = t2.agent_ip and t1.agent_port=t2.agent_port " + " where  t2.agent_id= ? and t2.user_id = ?", agent_id, user_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("agent未部署或者agent已不存在，agent_id=" + agent_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    public static void isObjectCollectExist(long odc_id) {
        if (Dbo.queryNumber("select count(*) from " + ObjectCollect.TableName + " where odc_id=?", odc_id).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("任务" + odc_id + "已不存在，请检查");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    public static void isObjectCollectTaskExist(long ocs_id) {
        if (Dbo.queryNumber("select count(*) from " + ObjectCollectTask.TableName + " where ocs_id=?", ocs_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("当前对象采集对应信息已不存在，ocs_id=" + ocs_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    public static void isDataStoreLayerExist(long dsl_id) {
        if (Dbo.queryNumber("select count(*) from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("当前存储层配置信息已不存在，dsl_id=" + dsl_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    public static void isObjectCollectStructExist(long ocs_id) {
        if (Dbo.queryNumber("select count(*) from " + ObjectCollectStruct.TableName + " where ocs_id=?", ocs_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("当前表对应的列信息不存在，请检查");
        }
    }
}
