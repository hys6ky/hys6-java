package hyren.serv6.b.agentinfo;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentStatus;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.agentmonitor.AgentMonitorUtil;
import hyren.serv6.base.utils.regular.RegexConstant;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "dhw", createdate = "2019-9-23 10:32:16")
@Service
public class AgentInfoService {

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchDatasourceAndAgentInfo(long source_id) {
        isDatasourceExist(source_id);
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.addSql("select ai.*,su.user_name from " + AgentInfo.TableName + " ai left join " + SysUser.TableName + " su on ai.user_id=su.user_id where ai.source_id = ?");
        asmSql.addParam(source_id);
        AgentType[] values = AgentType.values();
        String[] agentTypeCodes = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            agentTypeCodes[i] = values[i].getCode();
        }
        asmSql.addORParam("agent_type", agentTypeCodes);
        asmSql.addSql(" order by ai.agent_id");
        List<Map<String, Object>> agentList = Dbo.queryList(asmSql.sql(), asmSql.params());
        DataSource data_source = Dbo.queryOneObject(DataSource.class, "select * from " + DataSource.TableName + " where source_id=?", source_id).orElseThrow(() -> new BusinessException("sql查询错误！"));
        Map<Object, List<Map<String, Object>>> agentInfoMap = agentList.stream().collect(Collectors.groupingBy(o -> o.get("agent_type")));
        Map<String, Object> map = new HashMap<>();
        map.put("agentInfoList", agentInfoMap);
        map.put("datasource_name", data_source.getDatasource_name());
        map.put("source_id", source_id);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    private void isDatasourceExist(long source_id) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where source_id = ? " + " and create_user_id=?", source_id, UserUtil.getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("该agent对应的数据源已不存在，source_id=" + source_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentInfo", desc = "", range = "", isBean = true)
    public void saveAgent(AgentInfo agentInfo) {
        fieldLegalityValidation(agentInfo.getAgent_name(), agentInfo.getAgent_type(), agentInfo.getAgent_ip(), agentInfo.getAgent_port());
        boolean flag = AgentMonitorUtil.isPortOccupied(agentInfo.getAgent_ip(), agentInfo.getAgent_port());
        if (flag) {
            throw new BusinessException("端口被占用，agent_port=" + agentInfo.getAgent_port() + "," + "agent_ip =" + agentInfo.getAgent_ip());
        }
        isDatasourceAndAgentExist(agentInfo.getSource_id(), agentInfo.getAgent_type(), agentInfo.getAgent_ip(), agentInfo.getAgent_port(), agentInfo.getAgent_id());
        agentInfo.setAgent_id(PrimayKeyGener.getNextId());
        agentInfo.setAgent_status(AgentStatus.WeiLianJie.getCode());
        agentInfo.setCreate_time(DateUtil.getSysTime());
        agentInfo.setCreate_date(DateUtil.getSysDate());
        agentInfo.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    private void isDatasourceAndAgentExist(long source_id, String agent_type, String agent_ip, String agent_port, Long agent_id) {
        isDatasourceExist(source_id);
        if (null == agent_id) {
            if (Dbo.queryNumber("SELECT count(1) FROM " + AgentInfo.TableName + " WHERE source_id=?" + " AND agent_type=? AND agent_ip=? AND agent_port=?", source_id, agent_type, agent_ip, agent_port).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
                throw new BusinessException("该agent对应的数据源下相同的IP地址中包含相同的端口，" + "source_id=" + source_id);
            }
        } else {
            if (Dbo.queryNumber("SELECT count(1) FROM " + AgentInfo.TableName + " WHERE source_id=?" + " AND agent_type=? AND agent_ip=? AND agent_port=? and agent_id!=?", source_id, agent_type, agent_ip, agent_port, agent_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
                throw new BusinessException("该agent对应的数据源下相同的IP地址中包含相同的端口，" + "source_id=" + source_id);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "agent_name", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    public void updateAgent(Long agent_id, String agent_name, String agent_type, String agent_ip, String agent_port, long source_id, long user_id) {
        fieldLegalityValidation(agent_name, agent_type, agent_ip, agent_port);
        AgentInfo agent_info = Dbo.queryOneObject(AgentInfo.class, "select * from " + AgentInfo.TableName + " where agent_id=?", agent_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        boolean flag = AgentMonitorUtil.isPortOccupied(agent_ip, agent_port);
        if (flag) {
            throw new BusinessException("端口被占用，agent_port=" + agent_port + "," + "agent_ip =" + agent_ip);
        }
        if (AgentStatus.YiLianJie == AgentStatus.ofEnumByCode(agent_info.getAgent_status())) {
            throw new BusinessException("当前agent状态为已连接不能被修改");
        }
        isDatasourceAndAgentExist(source_id, agent_type, agent_ip, agent_port, agent_id);
        if (agent_name.equals(agent_info.getAgent_name())) {
            Dbo.execute("update " + AgentInfo.TableName + " set agent_ip=?,agent_port=?,user_id=?" + " where agent_id=? and agent_type=?", agent_ip, agent_port, user_id, agent_id, agent_type);
        } else {
            Dbo.execute("update " + AgentInfo.TableName + " set agent_ip=?,agent_port=?,user_id=?,agent_name=?" + " where agent_id=? and agent_type=?", agent_ip, agent_port, user_id, agent_name, agent_id, agent_type);
        }
        Dbo.execute("update " + AgentDownInfo.TableName + " set agent_ip=?,agent_port=? where user_id=? " + " and agent_type=? and agent_name=? and agent_id=?", agent_ip, agent_port, user_id, agent_type, agent_name, agent_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_name", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    private void fieldLegalityValidation(String agent_name, String agent_type, String agent_ip, String agent_port) {
        AgentType.ofEnumByCode(agent_type);
        if (StringUtil.isBlank(agent_name)) {
            throw new BusinessException("agent_name不为空且不为空格，agent_name=" + agent_name);
        }
        boolean matcher = RegexConstant.matcher(RegexConstant.IP_VERIFICATION, agent_ip);
        if (!matcher) {
            throw new BusinessException("agent_ip不是一个有效的ip地址,agent_ip=" + agent_ip);
        }
        matcher = RegexConstant.matcher(RegexConstant.PORT_VERIFICATION, agent_port);
        if (!matcher) {
            throw new BusinessException("agent_port端口不是有效的端口,agent_port=" + agent_port);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchAgent(long agent_id, String agent_type) {
        AgentType.ofEnumByCode(agent_type);
        return Dbo.queryOneObject("select ai.user_id,ai.agent_id,ai.source_id," + " ai.agent_name,ai.agent_ip,ai.agent_port,su.user_name from " + AgentInfo.TableName + " ai left join " + SysUser.TableName + " su on ai.user_id=su.user_id where ai.agent_id=?" + " and ai.agent_type=? and ai.user_id=? order by ai.agent_id", agent_id, agent_type, UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "agent_type", desc = "", range = "")
    public void deleteAgent(long source_id, long agent_id, String agent_type) {
        if (Dbo.queryNumber("select count(*) from " + AgentInfo.TableName + " t1 left join " + DataSource.TableName + " t2 on t1.source_id=t2.source_id where t1.agent_id=? " + " and t1.agent_type=? and t2.source_id=?", agent_id, agent_type, source_id).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("数据可访问权限校验失败，数据不可访问");
        }
        if (Dbo.queryNumber("select count(1) from " + AgentDownInfo.TableName + " where agent_id=?", agent_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("此agent已部署不能删除");
        }
        if (Dbo.queryNumber(" SELECT count(1) FROM " + AgentInfo.TableName + " t1 join " + DatabaseSet.TableName + " t2 on t1.agent_id=t2.agent_id WHERE  t1.agent_id=? " + " and t1.agent_type=?", agent_id, agent_type).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("此数据源对应的agent下有任务，不能删除");
        }
        DboExecute.deletesOrThrow("删除表信息失败，agent_id=" + agent_id + ",agent_type=" + agent_type, "delete from " + AgentInfo.TableName + " where agent_id=?", agent_id);
    }
}
