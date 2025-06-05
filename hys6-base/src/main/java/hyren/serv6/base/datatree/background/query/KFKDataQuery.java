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

@DocClass(desc = "", author = "yec", createdate = "2021/6/3")
public class KFKDataQuery {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<SdmConsumeConf> getConsumeManageNameList(User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, SdmConsumeConf.class, "SELECT sdm_consum_id,sdm_cons_name FROM " + SdmConsumeConf.TableName + " where user_id = ?", user.getUserId());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<SdmSpJobinfo> getConsumeReceNameList(User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, SdmSpJobinfo.class, "SELECT ssj_job_id,ssj_job_name FROM " + SdmSpJobinfo.TableName + " where user_id = ?", user.getUserId());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDBLayerList() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT t1.sdm_con_db_id,t1.sdm_consum_id,t1.sdm_tb_name_en,t1.dsl_id,t1.tab_id,t3.dsl_name FROM " + SdmConToDb.TableName + " t1 join " + DtabRelationStore.TableName + " t2 on t1.tab_id = t2.tab_id " + " JOIN " + DataStoreLayer.TableName + " t3 ON t2.dsl_id=t3.dsl_id where t2.is_successful = ?", JobExecuteState.WanCheng.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getHBLayerList() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT t1.sdm_consum_id,t3.hbase_id,t3.hbase_name,t4.dsl_id,t4.tab_id,t5.dsl_name FROM " + SdmConsumeConf.TableName + " t1 join " + SdmConsumeDes.TableName + " t2 on t1.sdm_consum_id = t2.sdm_consum_id " + " JOIN " + SdmConHbase.TableName + " t3 ON t2.sdm_des_id = t3.sdm_des_id" + " JOIN " + DtabRelationStore.TableName + " t4 ON t3.tab_id = t4.tab_id" + " JOIN " + DataStoreLayer.TableName + " t5 ON t4.dsl_id = t5.dsl_id" + " where t4.is_successful = ?", JobExecuteState.WanCheng.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getAnalyseLayerList() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT t1.ssj_job_id, t2.sdm_info_id,t3.ssd_table_name, t4.dsl_id,t4.tab_id,t5.dsl_name FROM " + " sdm_sp_jobinfo t1 JOIN sdm_sp_output t2 on t1.ssj_job_id = t2.ssj_job_id JOIN sdm_sp_database t3 " + " ON t2.sdm_info_id = t3.sdm_info_id JOIN dtab_relation_store t4 ON t4.tab_id = t3.tab_id " + " JOIN data_store_layer t5 on t4.dsl_id = t5.dsl_id WHERE t4.is_successful = ?", JobExecuteState.WanCheng.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getKFKInvalidAnalyseLayerList() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT t1.ssj_job_id, t2.sdm_info_id, t3.ssd_table_name,t3.dsl_id,t3.ssd_table_name," + " t3.cn_table_name,ds.dsl_name FROM sdm_sp_jobinfo " + " t1 JOIN sdm_sp_output t2 ON t1.ssj_job_id = t2.ssj_job_id " + " JOIN sdm_sp_database t3 ON t2.sdm_info_id = t3.sdm_info_id " + " JOIN dq_table_info t4 ON t4.table_id = t3.tab_id " + " JOIN data_store_layer ds ON ds.dsl_id = t3.dsl_id");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqTableInfo getKFKTableInfo(String table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DqTableInfo dq_table_info = new DqTableInfo();
            dq_table_info.setTable_id(Long.parseLong(table_id));
            return SqlOperator.queryOneObject(db, DqTableInfo.class, "select * from " + DqTableInfo.TableName + " where table_id" + "=?", dq_table_info.getTable_id()).orElseThrow(() -> new BusinessException("获取UDL数据表信息的sql失败!"));
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqTableInfo getTableMsgInfo(String table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DqTableInfo dq_table_info = new DqTableInfo();
            dq_table_info.setTable_id(Long.parseLong(table_id));
            return SqlOperator.queryOneObject(db, DqTableInfo.class, "select * from " + DqTableInfo.TableName + " where table_id" + "=?", dq_table_info.getTable_id()).orElseThrow(() -> new BusinessException("获取UDL数据表信息的sql失败!"));
        }
    }
}
