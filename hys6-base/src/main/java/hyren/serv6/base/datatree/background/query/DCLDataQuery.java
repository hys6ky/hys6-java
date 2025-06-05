package hyren.serv6.base.datatree.background.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.util.TreeConstant;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/7 0007 上午 11:10")
public class DCLDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "treeConf", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLDataInfos(TreeConf treeConf) {
        List<Map<String, Object>> dclDataInfos = new ArrayList<>();
        Map<String, Object> map;
        map = new HashMap<>();
        map.put("id", TreeConstant.DCL_BATCH);
        map.put("label", "批量数据");
        map.put("parent_id", DataSourceType.DCL.getCode());
        map.put("description", "批量数据查询");
        map.put("data_layer", DataSourceType.DCL.getCode());
        dclDataInfos.add(map);
        if (treeConf.getShowDCLRealtime()) {
            map = new HashMap<>();
            map.put("id", TreeConstant.DCL_REALTIME);
            map.put("label", "实时数据");
            map.put("parent_id", DataSourceType.DCL.getCode());
            map.put("description", "实时数据查询");
            map.put("data_layer", DataSourceType.DCL.getCode());
            dclDataInfos.add(map);
        }
        return dclDataInfos;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataSourceName", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchDataInfos(User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
            asmSql.clean();
            asmSql.addSql("SELECT distinct ds.source_id, ds.datasource_name from source_relation_dep srd JOIN " + "data_source ds on srd.SOURCE_ID = ds.SOURCE_ID");
            asmSql.addSql("where srd.dep_id = ?");
            asmSql.addParam(user.getDepId());
            return SqlOperator.queryList(db, asmSql.sql(), asmSql.params());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "showFileCollection", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchClassifyInfos(String source_id, Boolean showFileCollection, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            StringBuilder asmSql = new StringBuilder();
            asmSql.append("SELECT ai.*, ds.*, srd.*," + " cjc.classify_id,cjc.classify_name,cjc.classify_num,cjc.remark,cjc.user_id" + " FROM agent_info ai join data_source ds on ai.source_id = ds.source_id JOIN" + " source_relation_dep srd ON ds.source_id = srd.source_id JOIN collect_job_classify cjc" + " ON ai.agent_id = cjc.agent_id where srd.dep_id = ").append(user.getDepId());
            if (StringUtil.isNotBlank(source_id)) {
                asmSql.append(" AND ds.source_id = ").append(Long.parseLong(source_id));
            }
            if (!showFileCollection) {
                asmSql.append(" AND agent_type not in ('").append(AgentType.WenJianXiTong.getCode()).append("','").append(AgentType.FTP.getCode()).append("')");
            }
            List<Map<String, Object>> list = SqlOperator.queryList(db, asmSql.toString());
            List<Map<String, Object>> tieyuanClassifyList = SqlOperator.queryList(db, "SELECT DISTINCT cjc.* FROM " + CollectJobClassify.TableName + " cjc" + " JOIN " + DatabaseSet.TableName + " db_s ON db_s.source_id = cjc.source_id" + " WHERE cjc.source_id = ? AND db_s.collect_type = ?", Long.parseLong(source_id), CollectType.TieYuanDengJi.getCode());
            list.addAll(tieyuanClassifyList);
            return list;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchObjectCollectInfos(String source_id, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            AgentInfo agent_info = new AgentInfo();
            agent_info.setSource_id(source_id);
            StringBuilder asmSql = new StringBuilder();
            asmSql.append("SELECT oc.obj_collect_name as classify_name,oc.odc_id as classify_id," + "oc.obj_number as classify_num,oc.agent_id,ds.source_id,oc.remark" + " FROM " + ObjectCollect.TableName + " oc" + " JOIN " + AgentInfo.TableName + " ai ON oc.agent_id=ai.agent_id" + " JOIN " + DataSource.TableName + " ds" + " ON ai.source_id = ds.source_id" + " JOIN " + SourceRelationDep.TableName + " srd ON ds.source_id = srd.source_id" + " where srd.dep_id = ").append(user.getDepId());
            if (StringUtil.isNotBlank(source_id)) {
                agent_info.setSource_id(Long.parseLong(source_id));
                asmSql.append(" AND ds.source_id = ").append(agent_info.getSource_id());
            }
            return SqlOperator.queryList(db, asmSql.toString());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify_id", desc = "", range = "")
    @Param(name = "classify_name", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchTableInfos(String classify_id, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DatabaseSet database_set = new DatabaseSet();
            database_set.setClassify_id(Long.parseLong(classify_id));
            return SqlOperator.queryList(db, "SELECT t2.task_name,t1.*,t3.classify_id,t3.*" + " FROM data_store_reg t1" + " JOIN database_set t2 ON t1.database_id = t2.database_id" + " JOIN collect_job_classify t3 ON t3.classify_id = t2.classify_id" + " JOIN source_relation_dep t4 ON t1.source_id = t4.source_id " + " WHERE t2.classify_id = ? AND t4.dep_id = ? AND t2.collect_type in (?,?) ", database_set.getClassify_id(), user.getDepId(), CollectType.TieYuanDengJi.getCode(), CollectType.ShuJuKuCaiJi.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify_id", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchObjectCollectTableInfos(String odc_id, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
            asmSql.clean();
            ObjectCollect object_collect = new ObjectCollect();
            object_collect.setOdc_id(Long.parseLong(odc_id));
            asmSql.addSql("SELECT t1.*,t2.obj_collect_name as classify_name,t2.odc_id as classify_id,t2" + ".obj_collect_name as task_name,t2.obj_number as classify_num,t2.remark" + " FROM " + DataStoreReg.TableName + " t1" + " JOIN " + ObjectCollect.TableName + " t2 ON t1.database_id = t2.odc_id" + " JOIN " + AgentInfo.TableName + " t3 ON t3.agent_id = t2.agent_id" + " JOIN " + DataSource.TableName + " t4 ON t4.source_id = t3.source_id" + " JOIN " + SourceRelationDep.TableName + " t5 ON t5.source_id = t4.source_id" + " WHERE t2.odc_id = ? AND t5.dep_id = ?").addParam(object_collect.getOdc_id()).addParam(user.getDepId());
            return SqlOperator.queryList(db, asmSql);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getDCLBatchTableInfo(String file_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryOneObject(db, "SELECT dsr.*,ti.table_ch_name FROM " + DataStoreReg.TableName + " dsr" + " JOIN " + TableInfo.TableName + " ti" + " ON dsr.database_id = ti.database_id AND dsr.table_name = ti.table_name" + " WHERE dsr.file_id =?" + " UNION" + " SELECT dsr.*,oct.zh_name as table_ch_name FROM " + DataStoreReg.TableName + " dsr" + " JOIN " + ObjectCollectTask.TableName + " oct" + " ON dsr.database_id = oct.odc_id AND dsr.table_name = oct.en_name" + " WHERE dsr.file_id =?", file_id, file_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchTableColumns(String file_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DataStoreReg dsr = new DataStoreReg();
            dsr.setFile_id(file_id);
            List<Map<String, Object>> column_list = SqlOperator.queryList(db, "SELECT tc.column_id as column_id, tc.column_name as column_name," + " tc.column_ch_name as column_ch_name," + " tc.tc_remark as remark, tc.column_type as column_type, tc.is_primary_key as is_primary_key" + " FROM " + DataStoreReg.TableName + " dsr" + " JOIN " + TableInfo.TableName + " ti ON dsr.database_id=ti.database_id" + " AND dsr.table_name=ti.table_name" + " JOIN " + TableColumn.TableName + " tc ON ti.table_id=tc.table_id" + " WHERE dsr.file_id = ?" + " UNION" + " SELECT ocs.struct_id as column_id, ocs.column_name as column_name," + "ocs.data_desc as column_ch_name,ocs.remark,ocs.column_type as column_type," + "'" + IsFlag.Fou.getCode() + "'" + " as is_primary_key" + " FROM " + DataStoreReg.TableName + " dsr" + " JOIN " + ObjectCollectTask.TableName + " oct" + " ON dsr.database_id=oct.odc_id AND dsr.table_name=oct.en_name" + " JOIN " + ObjectCollectStruct.TableName + " ocs ON oct.ocs_id=ocs.ocs_id" + " WHERE dsr.file_id = ?", dsr.getFile_id(), dsr.getFile_id());
            if (column_list.isEmpty()) {
                throw new BusinessException("表的Mate信息查询结果为空!");
            }
            return column_list;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collect_set_id", desc = "", range = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLBatchTableColumns(long table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DataStoreReg dsr = new DataStoreReg();
            dsr.setTable_id(table_id);
            List<Map<String, Object>> column_list = SqlOperator.queryList(db, "SELECT tc.column_id as column_id, tc.column_name as column_name,tc.column_ch_name as column_ch_name," + " tc.tc_remark as remark,tc.column_type as column_type,tc.is_primary_key as is_primary_key" + " FROM " + TableInfo.TableName + " ti" + " JOIN " + TableColumn.TableName + " tc ON ti.table_id=tc.table_id WHERE ti.table_id = ?", dsr.getTable_id());
            if (column_list.isEmpty()) {
                throw new BusinessException("表的Mate信息查询结果为空!");
            }
            return column_list;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLRealtimeClassifyInfos(String source_id, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT DISTINCT ds.*, cjc.classify_id , cjc.classify_num , cjc.classify_name " + " FROM " + DatabaseSet.TableName + " db_s" + " JOIN " + CollectJobClassify.TableName + " cjc ON cjc.classify_id = db_s.classify_id" + " JOIN " + AgentInfo.TableName + " ai ON ai.agent_id = cjc.agent_id" + " JOIN " + DataSource.TableName + " ds ON ds.source_id = ai.source_id" + " JOIN " + SourceRelationDep.TableName + " srd ON srd.source_id = ds.source_id" + " WHERE srd.dep_id = ? AND ds.source_id = ? AND db_s.collect_type in (?)", user.getDepId(), Long.parseLong(source_id), CollectType.ShiShiCaiJi.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify_id", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLRealtimeTableInfos(String classify_id, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DatabaseSet database_set = new DatabaseSet();
            database_set.setClassify_id(Long.parseLong(classify_id));
            return SqlOperator.queryList(db, "SELECT t1.*,t2.task_name,t1.*,t3.classify_id,t3.classify_name,t3.classify_num,t3.remark,t3.user_id" + " FROM data_store_reg t1 JOIN database_set t2 ON" + " t1.database_id = t2.database_id JOIN collect_job_classify t3 ON" + " t3.classify_id = t2.classify_id JOIN source_relation_dep t4 ON t1.source_id = t4.source_id " + " WHERE t2.classify_id = ? AND t4.dep_id = ? AND t2.collect_type in (?) ", database_set.getClassify_id(), user.getDepId(), CollectType.ShiShiCaiJi.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify_id", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    public static List<Map<String, Object>> getDCLRealtimeTopicInfos(String classify_id, User user) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DatabaseSet database_set = new DatabaseSet();
            database_set.setClassify_id(Long.parseLong(classify_id));
            return SqlOperator.queryList(db, "SELECT DISTINCT sti.*, cjc.classify_id, ti.table_name, tsi.hyren_name " + " FROM " + SdmTopicInfo.TableName + " sti" + " JOIN table_storage_info tsi ON tsi.hyren_name = sti.sdm_top_name" + " JOIN table_info ti ON ti.table_id = tsi.table_id" + " JOIN database_set db_s ON db_s.database_id = ti.database_id" + " JOIN collect_job_classify cjc ON cjc.classify_id = db_s.classify_id" + " WHERE db_s.classify_id = ? ", database_set.getClassify_id());
        }
    }
}
