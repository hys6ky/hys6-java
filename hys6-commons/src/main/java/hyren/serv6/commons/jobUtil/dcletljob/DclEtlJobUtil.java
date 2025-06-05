package hyren.serv6.commons.jobUtil.dcletljob;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.jsch.ChineseUtil;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.commons.jobUtil.beans.EtlJobInfo;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-05-29 16:26")
public class DclEtlJobUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getPreviewJob(long colSetId, DatabaseWrapper db) {
        Map<String, Object> databaseMap = getDatabaseData(colSetId, db);
        long countNum = SqlOperator.queryNumber(db, "SELECT COUNT(1) FROM " + TableInfo.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum < 1) {
            throw new BusinessException("当前任务(" + colSetId + ")下不存在表信息");
        }
        List<Map<String, Object>> tableList = SqlOperator.queryList(db, "select t1.table_id,t1.table_name,t1.table_ch_name,t2.dbfile_format,ai.agent_type,t2.ded_id from " + TableInfo.TableName + " t1 left join " + DataExtractionDef.TableName + " t2 on t1.table_id = t2.table_id join " + DatabaseSet.TableName + " ds on t1.database_id = ds.database_id " + "join " + AgentInfo.TableName + " ai on ds.agent_id = ai.agent_id  where t1.database_id = ? ORDER BY t1.table_name", colSetId);
        tableList.forEach(itemMap -> setCollectDataBaseParam(colSetId, itemMap, databaseMap));
        return tableList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableItemMap", desc = "", range = "")
    @Param(name = "databaseMap", desc = "", range = "")
    private static void setCollectDataBaseParam(long colSetId, Map<String, Object> tableItemMap, Map<String, Object> databaseMap) {
        String file_format = ChineseUtil.getPingYin(FileFormat.ofValueByCode(((String) tableItemMap.get("dbfile_format"))));
        String pro_name = databaseMap.get("datasource_number") + Constant.SPLITTER + databaseMap.get("classify_num") + Constant.SPLITTER + tableItemMap.get("table_name") + Constant.SPLITTER + file_format;
        tableItemMap.put("etl_job", pro_name);
        String etl_job_desc = databaseMap.get("datasource_name") + Constant.SPLITTER + databaseMap.get("agent_name") + Constant.SPLITTER + databaseMap.get("classify_name") + Constant.SPLITTER + tableItemMap.get("table_ch_name") + Constant.SPLITTER + file_format;
        tableItemMap.put("etl_job_desc", etl_job_desc);
        String pro_para = colSetId + Constant.ETLPARASEPARATOR + tableItemMap.get("table_name") + Constant.ETLPARASEPARATOR + tableItemMap.get("agent_type") + Constant.ETLPARASEPARATOR + Constant.BATCH_DATE + Constant.ETLPARASEPARATOR + tableItemMap.get("dbfile_format");
        tableItemMap.put("pro_para", pro_para);
        tableItemMap.put("disp_freq", Dispatch_Frequency.DAILY.getCode());
        tableItemMap.put("job_priority", "0");
        tableItemMap.put("disp_offset", "0");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private static Map<String, Object> getDatabaseData(long colSetId, DatabaseWrapper db) {
        long countNum = SqlOperator.queryNumber(db, "SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            throw new BusinessException("当前任务(" + colSetId + ")不存在");
        }
        return SqlOperator.queryOneObject(db, "select t1.database_id,t4.datasource_number,t4.datasource_name,t3.agent_id," + "t3.agent_name,t2.classify_num,t3.agent_type,t2.classify_name,t1.task_name from " + DatabaseSet.TableName + " t1 JOIN " + CollectJobClassify.TableName + " t2 ON t1.classify_id = t2.classify_id JOIN " + AgentInfo.TableName + " t3 ON t1.agent_id = t3.agent_id JOIN " + DataSource.TableName + " t4 ON t3.source_id = t4.source_id " + " WHERE t1.database_id = ?", colSetId);
    }

    public static List<EtlJobDef> setDefaultEtlJob(long etl_sys_id, long sub_sys_id, List<Map<String, Object>> previewJob, String jobDataSource) {
        List<EtlJobDef> jobDefList = new ArrayList<>();
        previewJob.forEach(itemMap -> {
            EtlJobDef etl_job_def = JsonUtil.toObjectSafety(JsonUtil.toJson(itemMap), EtlJobDef.class).orElseThrow(() -> new BusinessException("解析作业信息失败"));
            etl_job_def.setEtl_sys_id(etl_sys_id);
            etl_job_def.setSub_sys_id(sub_sys_id);
            etl_job_def.setPro_type(Pro_Type.SHELL.getCode());
            etl_job_def.setPro_name(Constant.COLLECT_JOB_COMMAND);
            etl_job_def.setDisp_type(Dispatch_Type.TPLUS0.getCode());
            etl_job_def.setJob_datasource(jobDataSource);
            etl_job_def.setDisp_time(DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()).toString());
            jobDefList.add(etl_job_def);
        });
        return jobDefList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getAgentPath(long colSetId, DatabaseWrapper db) {
        long countNum = SqlOperator.queryNumber(db, "SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException("当前任务(" + colSetId + ")不再存在");
        }
        Map<String, Object> map = SqlOperator.queryOneObject(db, "SELECT t3.ai_desc pro_dic,t3.log_dir log_dic,t2.source_id FROM " + DatabaseSet.TableName + " t1 JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id JOIN " + AgentDownInfo.TableName + " t3 ON t2.agent_ip = t3.agent_ip AND t2.agent_port = t3.agent_port " + " WHERE t1.database_id = ? LIMIT 1", colSetId);
        map.put("pro_type", Pro_Type.SHELL.getCode());
        map.put("pro_name", Constant.COLLECT_JOB_COMMAND);
        Map<String, Object> relationEtlMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + TakeRelationEtl.TableName + " WHERE take_id = ? LIMIT 1", colSetId);
        map.putAll(relationEtlMap);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "sub_sys_cd", desc = "", range = "")
    @Param(name = "pro_dic", desc = "", range = "")
    @Param(name = "log_dic", desc = "", range = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "jobRelations", desc = "", range = "", example = "", nullable = true)
    @Param(name = "etlJobs", range = "", desc = "", isBean = true)
    @Param(name = "ded_arr", desc = "", range = "")
    public static void saveJobDataToDatabase(EtlJobInfo etlJobInfo, DatabaseWrapper db) {
        List<Long> dedList = etlJobInfo.getDedIds();
        if (etlJobInfo.getEtlJobs().size() != dedList.size()) {
            throw new BusinessException("卸数文件的数量与作业的数量不一致!!!");
        }
        EtlJobUtil.setDefaultEtlParaConf(db, etlJobInfo.getEtl_sys_id(), Constant.PARA_HYRENBIN, etlJobInfo.getPro_dic() + File.separator);
        EtlJobUtil.setDefaultEtlParaConf(db, etlJobInfo.getEtl_sys_id(), Constant.PARA_HYRENLOG, etlJobInfo.getLog_dic());
        EtlJobUtil.setDefaultEtlResource(db, etlJobInfo.getEtl_sys_id());
        List<Long> jobResource = EtlJobUtil.getJobResource(db, etlJobInfo.getEtl_sys_id());
        List<Long> relationEtl = getRelationEtl(etlJobInfo.getSource_id(), db);
        List<Long> etlJobList = EtlJobUtil.getEtlJob(db, etlJobInfo.getEtl_sys_id(), etlJobInfo.getSub_sys_id());
        int index = 0;
        for (EtlJobDef etl_job_def : etlJobInfo.getEtlJobs()) {
            if (StringUtil.isBlank(etl_job_def.getPro_dic())) {
                etl_job_def.setPro_dic(etlJobInfo.getPro_dic());
            }
            if (StringUtil.isBlank(etl_job_def.getLog_dic())) {
                etl_job_def.setLog_dic(etlJobInfo.getLog_dic());
            }
            Validator.notBlank(etl_job_def.getEtl_job(), "作业名称不能为空!!!");
            Validator.notNull(etl_job_def.getEtl_sys_id(), "调度工程主键不能为空!!!");
            Validator.notNull(etl_job_def.getSub_sys_id(), "调度子系统主键不能为空!!!");
            Validator.notBlank(etl_job_def.getPro_type(), "作业程序类型不能为空!!!");
            Validator.notBlank(etl_job_def.getPro_dic(), "作业程序目录不能为空!!!");
            Validator.notBlank(etl_job_def.getLog_dic(), "作业日志目录不能为空!!!");
            etl_job_def.setPro_dic(etlJobInfo.getPro_dic() + File.separator);
            etl_job_def.setLog_dic(Constant.HYRENLOG);
            etl_job_def.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
            etl_job_def.setToday_disp(Today_Dispatch_Flag.YES.getCode());
            etl_job_def.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
            String jobDataSource = EtlJobUtil.getJobDataSource(db, etlJobInfo.getColSetId());
            etl_job_def.setJob_datasource(jobDataSource);
            boolean etlJobExist = EtlJobUtil.isEtlJobDefExist(etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job(), db);
            log.info("=======作业:{}是否存在：{}=======", etl_job_def.getEtl_job(), etlJobExist);
            if (etlJobExist) {
                try {
                    if (etl_job_def.getEtl_job_id() == null) {
                        EtlJobDef etlJobDef = EtlJobUtil.getEtlJobByJob(db, etl_job_def.getEtl_sys_id(), etl_job_def.getEtl_job());
                        etl_job_def.setEtl_job_id(etlJobDef.getEtl_job_id());
                    }
                    etl_job_def.update(db);
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            } else {
                etl_job_def.setEtl_job_id(PrimayKeyGener.getNextId());
                etl_job_def.add(db);
            }
            Map<String, Object> jobRelationMap = null;
            if (StringUtil.isNotBlank(etlJobInfo.getJobRelations())) {
                jobRelationMap = JsonUtil.toObject(etlJobInfo.getJobRelations(), new TypeReference<Map<String, Object>>() {
                });
            }
            if (jobRelationMap != null) {
                Object pre_job = jobRelationMap.get(etl_job_def.getEtl_job());
                if (pre_job != null) {
                    saveEtlDependencies(etlJobInfo.getEtl_sys_id(), etl_job_def.getEtl_job_id(), pre_job.toString(), db);
                }
            }
            EtlJobUtil.setEtl_job_resource_rela(db, etlJobInfo.getEtl_sys_id(), etl_job_def, jobResource);
            setTake_relation_etl(etl_job_def, relationEtl, etlJobInfo.getDedIds().get(index), db);
            index++;
        }
        DboExecute.updatesOrThrow("此次采集任务配置完成,更新状态失败", "UPDATE " + DatabaseSet.TableName + " SET is_sendok = ? WHERE database_id = ?", IsFlag.Shi.getCode(), etlJobInfo.getColSetId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<Long> getRelationEtl(long source_id, DatabaseWrapper db) {
        return SqlOperator.queryOneColumnList(db, "SELECT t1.etl_job_id FROM " + TakeRelationEtl.TableName + " t1" + " JOIN " + DataExtractionDef.TableName + " ded ON ded.ded_id = t1.take_id " + " JOIN " + TableInfo.TableName + " ti ON ded.table_id = ti.table_id " + " JOIN " + DatabaseSet.TableName + " t2 ON ti.database_id = t2.database_id " + " JOIN " + CollectJobClassify.TableName + " t3 ON t2.classify_id = t3.classify_id" + " JOIN " + AgentInfo.TableName + " t4 ON t2.agent_id = t4.agent_id" + " WHERE t4.source_id = ?", source_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "jobRelation", desc = "", range = "")
    private static void saveEtlDependencies(long etl_sys_id, long etl_job_id, String jobRelation, DatabaseWrapper db) {
        SqlOperator.execute(db, "DELETE FROM " + EtlDependency.TableName + " WHERE (etl_job_id = ? OR pre_etl_job_id = ?) AND etl_sys_id = ? ", etl_job_id, etl_job_id, etl_sys_id);
        if (StringUtil.isNotBlank(jobRelation)) {
            StringUtil.split(jobRelation, "^").forEach(item -> {
                EtlDependency etl_dependency = new EtlDependency();
                etl_dependency.setEtl_sys_id(etl_sys_id);
                etl_dependency.setEtl_job_id(etl_job_id);
                etl_dependency.setPre_etl_sys_id(etl_sys_id);
                etl_dependency.setPre_etl_job_id(item);
                etl_dependency.setStatus(Status.TRUE.getCode());
                etl_dependency.add(db);
            });
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "job_datasource", desc = "", range = "")
    @Param(name = "table_id", desc = "", range = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    @Param(name = "relationEtl", desc = "", range = "")
    public static void setTake_relation_etl(EtlJobDef etl_job_def, List<Long> relationEtl, long take_id, DatabaseWrapper db) {
        String jobDatasource = etl_job_def.getJob_datasource();
        String taskSourceTable = getTaskSourceTable(jobDatasource);
        Long etlJobId = etl_job_def.getEtl_job_id();
        if (relationEtl.contains(etlJobId)) {
            TakeRelationEtl takeRelationEtl = SqlOperator.queryOneObject(db, TakeRelationEtl.class, "select * from " + TakeRelationEtl.TableName + " where take_id = ? and etl_job_id = ?", take_id, etlJobId).orElseThrow(() -> new BusinessException("作业调度中已存在作业 : " + etl_job_def.getEtl_job()));
            long etl_sys_id = takeRelationEtl.getEtl_sys_id();
            long sub_sys_cd = takeRelationEtl.getSub_sys_id();
            if (etl_sys_id != etl_job_def.getEtl_sys_id()) {
                takeRelationEtl.setEtl_sys_id(etl_sys_id);
                takeRelationEtl.setSub_sys_id(sub_sys_cd);
                takeRelationEtl.update(db);
                SqlOperator.execute(db, "delete from " + EtlJobDef.TableName + " where etl_sys_id = ? and sub_sys_id = ? and etl_job_id = ?", etl_sys_id, sub_sys_cd, etlJobId);
            }
        } else {
            TakeRelationEtl take_relation_etl = new TakeRelationEtl();
            take_relation_etl.setTre_id(PrimayKeyGener.getNextId());
            take_relation_etl.setTake_id(take_id);
            take_relation_etl.setEtl_job_id(etlJobId);
            take_relation_etl.setEtl_sys_id(etl_job_def.getEtl_sys_id());
            take_relation_etl.setSub_sys_id(etl_job_def.getSub_sys_id());
            take_relation_etl.setJob_datasource(jobDatasource);
            take_relation_etl.setTake_source_table(taskSourceTable);
            take_relation_etl.add(db);
        }
    }

    private static String getTaskSourceTable(String job_datasource) {
        String takeSourceTable;
        if (ETLDataSource.ShuJuKuCaiJi == ETLDataSource.ofEnumByCode(job_datasource) || ETLDataSource.DBWenJianCaiJi == ETLDataSource.ofEnumByCode(job_datasource) || ETLDataSource.ShuJuKuChouShu == ETLDataSource.ofEnumByCode(job_datasource)) {
            takeSourceTable = TableInfo.TableName;
        } else if (ETLDataSource.BanJieGouHuaCaiJi == ETLDataSource.ofEnumByCode(job_datasource)) {
            takeSourceTable = ObjectCollectTask.TableName;
        } else if (ETLDataSource.ShuJuJiaGong == ETLDataSource.ofEnumByCode(job_datasource)) {
            takeSourceTable = DmModuleTable.TableName;
        } else if (ETLDataSource.ShuJuFenFa == ETLDataSource.ofEnumByCode(job_datasource)) {
            takeSourceTable = DataDistribute.TableName;
        } else if (ETLDataSource.ShuJuGuanKong == ETLDataSource.ofEnumByCode(job_datasource)) {
            takeSourceTable = DqTableInfo.TableName;
        } else {
            takeSourceTable = null;
        }
        return takeSourceTable;
    }
}
