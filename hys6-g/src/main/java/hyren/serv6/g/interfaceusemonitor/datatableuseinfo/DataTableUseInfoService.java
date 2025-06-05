package hyren.serv6.g.interfaceusemonitor.datatableuseinfo;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.SysUser;
import hyren.serv6.base.entity.SysregParameterInfo;
import hyren.serv6.base.entity.TableUseInfo;
import hyren.serv6.g.init.InterfaceManager;
import org.springframework.stereotype.Service;

@Service
public class DataTableUseInfoService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchTableData() {
        return searchTableDataInfo(null);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result searchTableDataById(Long user_id) {
        return searchTableDataInfo(user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "use_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result searchFieldInfoById(Long use_id) {
        return Dbo.queryResult("SELECT parameter_id,table_ch_column,table_en_column FROM " + SysregParameterInfo.TableName + " WHERE use_id = ?", use_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "use_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public void deleteDataTableUseInfo(Long use_id) {
        Dbo.execute("delete from " + SysregParameterInfo.TableName + " where use_id = ?", use_id);
        Dbo.execute("delete from " + TableUseInfo.TableName + " where use_id = ?", use_id);
        InterfaceManager.initTable(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    private Result searchTableDataInfo(Long user_id) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("SELECT distinct t1.use_id,t1.original_name,t1.sysreg_name," + "t3.user_name FROM " + TableUseInfo.TableName + " t1," + SysregParameterInfo.TableName + " t2," + SysUser.TableName + " t3 WHERE t1.use_id = t2.use_id AND t1.user_id = t3.user_id");
        if (user_id != null) {
            assembler.addSql(" AND t1.user_id = ?").addParam(user_id);
        }
        assembler.addSql(" order by t1.use_id");
        return Dbo.queryResult(assembler.sql(), assembler.params());
    }
}
