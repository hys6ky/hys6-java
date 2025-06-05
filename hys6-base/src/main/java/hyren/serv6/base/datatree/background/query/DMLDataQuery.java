package hyren.serv6.base.datatree.background.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/7 0007 上午 11:17")
public class DMLDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDMLDataInfos(User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT distinct t1.* from " + DmInfo.TableName + " t1" + " left join " + SysUser.TableName + " t2 on t1.create_id = t2.user_id where t2.dep_id = ?", user.getDepId());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDMLCategoryInfos(long data_mart_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "select * from " + DmCategory.TableName + " where data_mart_id=?", data_mart_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "category_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDMLTableInfos(long category_id) {
        return getDMLTableInfos(category_id, JobExecuteState.WanCheng);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "category_id", desc = "", range = "")
    @Param(name = "jobExecuteState", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDMLTableInfos(long category_id, JobExecuteState jobExecuteState) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            StringBuilder asmSql = new StringBuilder();
            asmSql.append("select * from " + DmModuleTable.TableName + " dm" + " join " + DtabRelationStore.TableName + " dtab_rs on dm.module_table_id=dtab_rs.tab_id" + " where dm.category_id = ").append(category_id);
            if (null != jobExecuteState) {
                asmSql.append(" and dtab_rs.is_successful = '").append(jobExecuteState.getCode()).append("'");
            }
            return SqlOperator.queryList(db, asmSql.toString());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "module_table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DmModuleTable getDMLTableInfo(String module_table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DmModuleTable dmModuleTable = new DmModuleTable();
            dmModuleTable.setModule_table_id(Long.parseLong(module_table_id));
            return SqlOperator.queryOneObject(db, DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id=?", dmModuleTable.getModule_table_id()).orElseThrow(() -> new BusinessException("获取加工数据表信息的sql失败!"));
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "module_table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDMLTableColumns(String module_table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DmModuleTable dmModuleTable = new DmModuleTable();
            dmModuleTable.setModule_table_id(Long.parseLong(module_table_id));
            return SqlOperator.queryList(db, "SELECT module_field_id AS column_id, jobtab_field_en_name AS COLUMN_NAME, jobtab_field_cn_name AS column_ch_name, " + " concat ( jobtab_field_type, '(', jobtab_field_length, ')' ) AS column_type, '0' AS is_primary_key, " + " jobtab_field_process as field_process, jobtab_group_mapping as group_mapping FROM  " + DmJobTableFieldInfo.TableName + " dtf WHERE jobtab_id in" + " ( SELECT jobtab_id FROM " + DmJobTableInfo.TableName + " WHERE module_table_id = ? )", dmModuleTable.getModule_table_id());
        }
    }
}
