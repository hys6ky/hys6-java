package hyren.serv6.k.dm.metadatamanage.transctrl;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.entity.DqRuleDef;
import java.util.*;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/5/22 0022 下午 01:19")
public class IOnWayCtrl {

    private static String ERROR_MUTES = "该任务下的数据表有其他任务在使用，请前往先进行删除!";

    private static void checkDQCTask(List<String> tables, DatabaseWrapper db) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        StringBuilder sb = new StringBuilder();
        sb.append(" select  reg_name enname,target_tab souname,case_type_desc cnname ,'");
        sb.append(DataSourceType.DQC.getCode()).append("' as typename from ( ");
        sb.append(" select reg_name,target_tab,case_type from " + DqDefinition.TableName + " where target_tab in ( ");
        for (String table : tables) {
            sb.append("?,");
            asmSql.addParam(table);
        }
        sb.deleteCharAt(sb.length() - 1).append(") ");
        sb.append(" union all ");
        sb.append(" select reg_name,opposite_tab,case_type from " + DqDefinition.TableName + " where opposite_tab in(");
        for (String s : tables) {
            sb.append("?,");
            asmSql.addParam(s);
        }
        sb.deleteCharAt(sb.length() - 1).append(") ");
        sb.append(" ) a join " + DqRuleDef.TableName + " b on a.case_type = b.case_type ");
        asmSql.addSql(sb.toString());
        Result rsMutex = Dbo.queryResult(db, asmSql.sql(), asmSql.params());
        if (!rsMutex.isEmpty()) {
            throw new BusinessException(ERROR_MUTES + rsMutex);
        }
    }

    private static void checkRESTTask(List<String> tables, DatabaseWrapper db) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        StringBuilder sb = new StringBuilder();
        sb.append("select sysreg_name souname,a.user_id enname,b.user_name cnname,'接口' as typename ");
        sb.append(" from table_use_info  ");
        sb.append(" a join sys_user b on a.user_id = b.user_id where sysreg_name in ( ");
        for (String table : tables) {
            sb.append("?,");
            asmSql.addParam(table);
        }
        String sql = sb.deleteCharAt(sb.length() - 1).append(") group by sysreg_name,a.user_id,b.user_name").toString();
        asmSql.addSql(sql);
        Result rsMutex = Dbo.queryResult(db, asmSql.sql(), asmSql.params());
        if (!rsMutex.isEmpty()) {
            throw new BusinessException(ERROR_MUTES + rsMutex);
        }
    }
}
