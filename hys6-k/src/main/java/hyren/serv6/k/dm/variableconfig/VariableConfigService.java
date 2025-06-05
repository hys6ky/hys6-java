package hyren.serv6.k.dm.variableconfig;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.k.entity.DqSysCfg;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class VariableConfigService {

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_sys_cfg", desc = "", range = "", isBean = true)
    public void addVariableConfigDat(DqSysCfg dq_sys_cfg) {
        Validator.notBlank(dq_sys_cfg.getVar_name(), "变量名为空!");
        Validator.notBlank(dq_sys_cfg.getVar_value(), "变量值为空!");
        if (checkVarNameIsRepeat(dq_sys_cfg.getVar_name())) {
            throw new BusinessException("变量名重复!");
        }
        dq_sys_cfg.setSys_var_id(PrimayKeyGener.getNextId());
        dq_sys_cfg.setApp_updt_dt(DateUtil.getSysDate());
        dq_sys_cfg.setApp_updt_ti(DateUtil.getSysTime());
        dq_sys_cfg.setUser_id(UserUtil.getUserId());
        dq_sys_cfg.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sys_var_id_s", desc = "", range = "")
    public void deleteVariableConfigData(Long[] sys_var_id_s) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("delete from " + DqSysCfg.TableName + " where user_id=?");
        asmSql.addParam(UserUtil.getUserId());
        asmSql.addORParam("sys_var_id ", sys_var_id_s);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_sys_cfg", desc = "", range = "", isBean = true)
    public void updateVariableConfigData(DqSysCfg dq_sys_cfg) {
        Validator.notBlank(dq_sys_cfg.getSys_var_id().toString(), "变量id为空!");
        Validator.notBlank(dq_sys_cfg.getVar_name(), "变量名为空!");
        Validator.notBlank(dq_sys_cfg.getVar_value(), "变量值为空!");
        dq_sys_cfg.setApp_updt_dt(DateUtil.getSysDate());
        dq_sys_cfg.setApp_updt_ti(DateUtil.getSysTime());
        dq_sys_cfg.setUser_id(UserUtil.getUserId());
        dq_sys_cfg.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<DqSysCfg> getVariableConfigDataInfos() {
        return Dbo.queryList(DqSysCfg.class, "SELECT * FROM " + DqSysCfg.TableName + " where user_id=?", UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sys_var_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public DqSysCfg getVariableConfigDataInfo(long sys_var_id) {
        return Dbo.queryOneObject(DqSysCfg.class, "SELECT * FROM " + DqSysCfg.TableName + " where sys_var_id=?", sys_var_id).orElseThrow(() -> new BusinessException("检查变量名称否重复的SQL编写错误"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "var_name", desc = "", range = "", nullable = true)
    @Param(name = "var_value", desc = "", range = "", nullable = true)
    @Param(name = "start_date", desc = "", range = "", nullable = true)
    @Param(name = "end_date", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public List<DqSysCfg> searchVariableConfigData(String var_name, String var_value, String start_date, String end_date) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DqSysCfg.TableName + " where user_id = ?");
        asmSql.addParam(UserUtil.getUserId());
        if (StringUtil.isNotBlank(var_name)) {
            asmSql.addLikeParam(" var_name", '%' + var_name + '%');
        }
        if (StringUtil.isNotBlank(var_value)) {
            asmSql.addLikeParam("var_value", '%' + var_value + '%');
        }
        if (StringUtil.isNotBlank(start_date)) {
            asmSql.addSql(" and app_updt_dt >= ?").addParam(start_date);
        }
        if (StringUtil.isNotBlank(end_date)) {
            asmSql.addSql(" and app_updt_dt <= ?").addParam(end_date);
        }
        return Dbo.queryList(DqSysCfg.class, asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "var_name", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkVarNameIsRepeat(String var_name) {
        return Dbo.queryNumber("select count(var_name) count from " + DqSysCfg.TableName + " WHERE var_name =?", var_name).orElseThrow(() -> new BusinessException("检查变量名称否重复的SQL编写错误")) != 0;
    }
}
