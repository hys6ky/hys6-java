package hyren.serv6.b.realtimecollection.realtimeCollectManagement.agent;

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
import hyren.serv6.base.utils.regular.RegexConstant;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.agentmonitor.AgentMonitorUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2021-3-26")
@Service
@Slf4j
public class SdmAgentInfoService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchSdmDatasourceAndSdmAgentInfo(long sdm_source_id) {
        isDatasourceExist(sdm_source_id);
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.addSql("select ai.*,su.user_name from " + AgentInfo.TableName + " ai join " + SysUser.TableName + " su on ai.user_id = su.user_id WHERE ai.source_id = ? AND ai.agent_type in ( ? , ? ) ORDER BY ai.agent_id");
        asmSql.addParam(sdm_source_id);
        asmSql.addParam(AgentType.WenBenLiu.getCode());
        asmSql.addParam(AgentType.XiaoXiLiu.getCode());
        List<Map<String, Object>> mapList = Dbo.queryList(asmSql.sql(), asmSql.params());
        asmSql.cleanParams();
        DataSource sdm_data_source = Dbo.queryOneObject(DataSource.class, "select * from " + DataSource.TableName + " where source_id = ?", sdm_source_id).orElseThrow(() -> new BusinessException("sql查询错误！"));
        Map<String, Object> map = new HashMap<>();
        List<Object> fileList = new ArrayList<>();
        List<Object> restList = new ArrayList<>();
        for (Map<String, Object> agentList : mapList) {
            if (AgentType.WenBenLiu == AgentType.ofEnumByCode(agentList.get("agent_type").toString())) {
                fileList.add(agentList);
                map.put("fileSystemAgent", fileList);
            } else {
                restList.add(agentList);
                map.put("restAgentList", restList);
            }
        }
        map.put("sdm_source_name", sdm_data_source.getDatasource_name());
        map.put("sdm_source_id", sdm_source_id);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Param(name = "sdm_agent_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchSdmAgent(long sdm_agent_id, String sdm_agent_type) {
        return Dbo.queryOneObject("select ai.user_id,ai.agent_id,ai.source_id,ai.agent_name," + "ai.agent_ip,ai.agent_port,su.user_name from " + AgentInfo.TableName + " ai join " + SysUser.TableName + " su on ai.user_id = su.user_id " + "WHERE ai.agent_id = ? AND ai.agent_type = ? AND ai.user_id = ?" + "ORDER BY ai.agent_id", sdm_agent_id, sdm_agent_type, getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmAgentInfo", desc = "", range = "", isBean = true)
    public void saveSdmAgentInfo(AgentInfo sdmAgentInfo) {
        fieldLegalityValidation(sdmAgentInfo.getAgent_name(), sdmAgentInfo.getAgent_ip(), sdmAgentInfo.getAgent_port());
        boolean flag = AgentMonitorUtil.isPortOccupied(sdmAgentInfo.getAgent_ip(), sdmAgentInfo.getAgent_port());
        if (flag) {
            throw new BusinessException("端口被占用，agent_port=" + sdmAgentInfo.getAgent_port() + "," + "agent_ip =" + sdmAgentInfo.getAgent_ip());
        }
        isDatasourceAndAgentExist(sdmAgentInfo.getSource_id(), sdmAgentInfo.getAgent_type(), sdmAgentInfo.getAgent_ip(), sdmAgentInfo.getAgent_port(), sdmAgentInfo.getAgent_id());
        sdmAgentInfo.setAgent_id(PrimayKeyGener.getNextId());
        sdmAgentInfo.setAgent_status(AgentStatus.WeiLianJie.getCode());
        sdmAgentInfo.setCreate_time(DateUtil.getSysTime());
        sdmAgentInfo.setCreate_date(DateUtil.getSysDate());
        sdmAgentInfo.setUser_id(getUserId());
        sdmAgentInfo.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Param(name = "sdm_agent_type", desc = "", range = "")
    public void deleteSdmAgent(long sdm_source_id, long sdm_agent_id, String sdm_agent_type) {
        if (Dbo.queryNumber("select count(1) from " + AgentInfo.TableName + " t1 join " + DataSource.TableName + " t2 on t1.source_id=t2.source_id where t1.agent_id=? " + " and t1.agent_type=? and t2.source_id=?", sdm_agent_id, sdm_agent_type, sdm_source_id).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("数据可访问权限校验失败，数据不可访问");
        }
        if (Dbo.queryNumber("select count(1) from " + AgentDownInfo.TableName + " where agent_id=?", sdm_agent_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("此agent已部署不能删除");
        }
        DboExecute.deletesOrThrow("删除表信息失败，agent_id=" + sdm_agent_id + ",agent_type=" + sdm_agent_type, "delete from " + AgentInfo.TableName + " where agent_id=?", sdm_agent_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Param(name = "sdm_agent_name", desc = "", range = "")
    @Param(name = "sdm_agent_type", desc = "", range = "")
    @Param(name = "sdm_agent_ip", desc = "", range = "")
    @Param(name = "sdm_agent_port", desc = "", range = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    public void updateSdmAgent(long sdm_agent_id, String sdm_agent_name, String sdm_agent_type, String sdm_agent_ip, String sdm_agent_port, long sdm_source_id, long user_id) {
        fieldLegalityValidation(sdm_agent_name, sdm_agent_ip, sdm_agent_port);
        AgentInfo agent_info = Dbo.queryOneObject(AgentInfo.class, "select * from " + AgentInfo.TableName + " where agent_id=?", sdm_agent_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        boolean flag = AgentMonitorUtil.isPortOccupied(sdm_agent_ip, sdm_agent_port);
        if (flag) {
            throw new BusinessException("端口被占用，agent_port=" + sdm_agent_port + "," + "agent_ip =" + sdm_agent_ip);
        }
        if (AgentStatus.YiLianJie == AgentStatus.ofEnumByCode(agent_info.getAgent_status())) {
            throw new BusinessException("当前agent状态为已连接不能被修改");
        }
        isDatasourceAndAgentExist(sdm_source_id, sdm_agent_type, sdm_agent_ip, sdm_agent_port, sdm_agent_id);
        Dbo.execute("update " + AgentInfo.TableName + " set agent_ip=?,agent_port=?,user_id=?,agent_name=?" + " where agent_id = ? and agent_type = ?", sdm_agent_ip, sdm_agent_port, user_id, sdm_agent_name, sdm_agent_id, sdm_agent_type);
        Dbo.execute("update " + AgentDownInfo.TableName + " set agent_ip=?,agent_port=? where user_id=? " + " and agent_type=? and agent_name=? and agent_id=?", sdm_agent_ip, sdm_agent_port, user_id, sdm_agent_type, sdm_agent_name, sdm_agent_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_name", desc = "", range = "")
    @Param(name = "sdm_agent_ip", desc = "", range = "", example = "")
    @Param(name = "sdm_agent_port", desc = "", range = "")
    private void fieldLegalityValidation(String sdm_agent_name, String sdm_agent_ip, String sdm_agent_port) {
        if (StringUtil.isBlank(sdm_agent_name)) {
            throw new BusinessException("sdm_agent_name不为空且不为空格，sdm_agent_name=" + sdm_agent_name);
        }
        Pattern pattern = Pattern.compile(RegexConstant.IP_VERIFICATION);
        Matcher matcher = pattern.matcher(sdm_agent_ip);
        if (!matcher.matches()) {
            throw new BusinessException("sdm_agent_ip不是一个有效的ip地址,sdm_agent_ip=" + sdm_agent_ip);
        }
        pattern = Pattern.compile(RegexConstant.PORT_VERIFICATION);
        matcher = pattern.matcher(sdm_agent_port);
        if (!matcher.matches()) {
            throw new BusinessException("sdm_agent_port端口不是有效的端口,sdm_agent_port=" + sdm_agent_port);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    private void isDatasourceExist(long sdm_source_id) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where source_id = ? " + " and user_id=?", sdm_source_id, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("该agent对应的数据源已不存在，sdm_source_id=" + sdm_source_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Param(name = "sdm_agent_type", desc = "", range = "")
    @Param(name = "sdm_agent_ip", desc = "", range = "", example = "")
    @Param(name = "sdm_agent_port", desc = "", range = "")
    @Param(name = "sdm_agent_id", desc = "", range = "", nullable = true)
    private void isDatasourceAndAgentExist(long sdm_source_id, String sdm_agent_type, String sdm_agent_ip, String sdm_agent_port, Long sdm_agent_id) {
        isDatasourceExist(sdm_source_id);
        if (null == sdm_agent_id) {
            if (Dbo.queryNumber("SELECT count(1) FROM " + AgentInfo.TableName + " WHERE source_id=?" + " AND agent_type = ? AND agent_ip = ? AND agent_port = ?", sdm_source_id, sdm_agent_type, sdm_agent_ip, sdm_agent_port).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
                throw new BusinessException("该agent对应的数据源下相同的IP地址中包含相同的端口，" + "sdm_source_id=" + sdm_source_id);
            }
        } else {
            if (Dbo.queryNumber("SELECT count(1) FROM " + AgentInfo.TableName + " WHERE source_id=?" + " AND agent_type=? AND agent_ip=? AND agent_port=? and agent_id !=?", sdm_source_id, sdm_agent_type, sdm_agent_ip, sdm_agent_port, sdm_agent_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
                throw new BusinessException("该agent对应的数据源下相同的IP地址中包含相同的端口，" + "source_id=" + sdm_source_id);
            }
        }
    }
}
