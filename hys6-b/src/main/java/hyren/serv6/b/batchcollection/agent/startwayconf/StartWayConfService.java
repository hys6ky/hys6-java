package hyren.serv6.b.batchcollection.agent.startwayconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.Dispatch_Frequency;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.Pro_Type;
import hyren.serv6.base.codes.Status;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.jsch.ChineseUtil;
import hyren.serv6.commons.jobUtil.beans.EtlJobInfo;
import hyren.serv6.commons.jobUtil.dcletljob.DclEtlJobUtil;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Slf4j
@DocClass(desc = "", author = "Lee-Qiang")
@Api("定义启动方式配置")
@Service
public class StartWayConfService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<EtlSys> getEtlSysData() {
        return Dbo.queryList(EtlSys.class, "SELECT * FROM " + EtlSys.TableName + " WHERE user_id = ?", getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", range = "", desc = "")
    @Return(desc = "", range = "")
    public List<EtlSubSysList> getEtlSubSysData(Long etl_sys_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + EtlSys.TableName + " WHERE etl_sys_id = ? AND user_id = ?", etl_sys_id, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException("当前工程ID :" + etl_sys_id + " 对应工程不存在");
        }
        return Dbo.queryList(EtlSubSysList.class, "SELECT * FROM " + EtlSubSysList.TableName + " WHERE etl_sys_id = ?", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getPreviewJob(long colSetId) {
        Map<String, Object> databaseMap = getDatabaseData(colSetId);
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TableInfo.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum < 1) {
            throw new BusinessException("当前任务(" + colSetId + ")下不存在表信息");
        }
        List<Map<String, Object>> tableList = Dbo.queryList("select t1.table_id,t1.table_name,t1.table_ch_name,t2.dbfile_format,ai.agent_type,t2.ded_id" + " FROM " + TableInfo.TableName + " t1" + " LEFT JOIN " + DataExtractionDef.TableName + " t2 ON t1.table_id = t2.table_id" + " JOIN " + DatabaseSet.TableName + " ds ON t1.database_id = ds.database_id " + " JOIN " + AgentInfo.TableName + " ai ON ds.agent_id = ai.agent_id" + " WHERE t1.database_id = ? ORDER BY t1.table_name", colSetId);
        tableList.forEach(itemMap -> setCollectDataBaseParam(colSetId, itemMap, databaseMap));
        return tableList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableItemMap", desc = "", range = "")
    @Param(name = "databaseMap", desc = "", range = "")
    private void setCollectDataBaseParam(long colSetId, Map<String, Object> tableItemMap, Map<String, Object> databaseMap) {
        String dbfile_format = ChineseUtil.getPingYin(FileFormat.ofValueByCode(((String) tableItemMap.get("dbfile_format"))));
        String pro_name = databaseMap.get("datasource_number") + Constant.SPLITTER + databaseMap.get("classify_num") + Constant.SPLITTER + tableItemMap.get("table_name") + Constant.SPLITTER + dbfile_format;
        tableItemMap.put("etl_job", pro_name);
        String etl_job_desc = databaseMap.get("datasource_name") + Constant.SPLITTER + databaseMap.get("agent_name") + Constant.SPLITTER + databaseMap.get("classify_name") + Constant.SPLITTER + tableItemMap.get("table_ch_name") + Constant.SPLITTER + dbfile_format;
        tableItemMap.put("etl_job_desc", etl_job_desc);
        String pro_para = colSetId + Constant.ETLPARASEPARATOR + tableItemMap.get("table_name") + Constant.ETLPARASEPARATOR + tableItemMap.get("agent_type") + Constant.ETLPARASEPARATOR + Constant.BATCH_DATE + Constant.ETLPARASEPARATOR + tableItemMap.get("dbfile_format");
        tableItemMap.put("pro_para", pro_para);
        tableItemMap.put("disp_freq", Dispatch_Frequency.DAILY.getCode());
        tableItemMap.put("job_priority", "0");
        tableItemMap.put("disp_offset", "0");
        tableItemMap.put("etlparaseparator", Constant.ETLPARASEPARATOR);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getDatabaseData(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            throw new BusinessException("当前任务(" + colSetId + ")不存在");
        }
        return Dbo.queryOneObject("select t1.database_id,t4.datasource_number,t4.datasource_name,t3.agent_id," + "t3.agent_name,t2.classify_num,t3.agent_type,t2.classify_name,t1.task_name" + " FROM " + DatabaseSet.TableName + " t1" + " JOIN " + CollectJobClassify.TableName + " t2 ON t1.classify_id = t2.classify_id" + " JOIN " + AgentInfo.TableName + " t3 ON t1.agent_id = t3.agent_id" + " JOIN " + DataSource.TableName + " t4 ON t3.source_id = t4.source_id " + " WHERE t1.database_id = ?", colSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getEtlJobData(long colSetId) {
        List<Map<String, Object>> etlJobList = Dbo.queryList("SELECT t4.database_id, t4.table_id,t3.ded_id,t2.* " + " FROM " + TakeRelationEtl.TableName + " t1" + " JOIN " + EtlJobDef.TableName + " t2 ON t1.etl_job_id = t2.etl_job_id" + " JOIN " + DataExtractionDef.TableName + " t3 ON t3.ded_id = t1.take_id" + " JOIN " + TableInfo.TableName + " t4 ON t4.table_id = t3.table_id" + " WHERE t1.etl_sys_id = t2.etl_sys_id AND t1.sub_sys_id = t2.sub_sys_id AND t4.database_id = ? ", colSetId);
        etlJobList.forEach(itemMap -> {
            List<Object> preJobList = Dbo.queryOneColumnList("SELECT pre_etl_job_id FROM " + EtlDependency.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ?", itemMap.get("etl_sys_id"), itemMap.get("etl_job_id"));
            itemMap.put("pre_etl_job_ids", preJobList);
        });
        List<Map<String, Object>> previewJob = getPreviewJob(colSetId);
        for (Map<String, Object> map : etlJobList) {
            String etl_job = map.get("etl_job").toString();
            for (Map<String, Object> previewMap : previewJob) {
                if (previewMap.get("etl_job").toString().equals(etl_job)) {
                    map.put("agent_type", previewMap.get("agent_type"));
                    map.put("dbfile_format", previewMap.get("dbfile_format"));
                    map.put("table_name", previewMap.get("table_name"));
                    map.put("etlparaseparator", previewMap.get("etlparaseparator"));
                }
            }
        }
        List<Object> databaseDefaultEtlJob = previewJob.stream().map(item -> item.get("etl_job")).collect(Collectors.toList());
        List<Object> etlJobData = etlJobList.stream().map(item -> item.get("etl_job")).distinct().collect(Collectors.toList());
        Map<String, List<Object>> differenceInfo = getDifferenceInfo(databaseDefaultEtlJob, etlJobData);
        List<Object> addEtlJob = differenceInfo.get("add");
        previewJob.removeIf(item -> !addEtlJob.contains(item.get("etl_job")));
        List<Object> delete = differenceInfo.get("delete");
        etlJobList.removeIf(itemMap -> {
            Object etlJob = itemMap.get("etl_job");
            Object etlSysId = itemMap.get("etl_sys_id");
            Object subSysId = itemMap.get("sub_sys_id");
            if (delete.contains(etlJob)) {
                Optional<EtlJobDef> etlJobDef = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_job = ? AND etl_sys_id = ? AND sub_sys_id = ?", etlJob, etlSysId, subSysId);
                etlJobDef.ifPresent(jobDef -> Dbo.execute("DELETE FROM " + TakeRelationEtl.TableName + " WHERE etl_job_id = ?", jobDef.getEtl_job_id()));
                Dbo.execute("DELETE FROM " + EtlJobDef.TableName + " WHERE etl_job = ? AND etl_sys_id = ? AND sub_sys_id = ?", etlJob, etlSysId, subSysId);
                return true;
            } else {
                return false;
            }
        });
        etlJobList.addAll(previewJob);
        return etlJobList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAgentPath(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException("当前任务(" + colSetId + ")不再存在");
        }
        Map<String, Object> map = Dbo.queryOneObject("SELECT t3.ai_desc pro_dic,t3.log_dir log_dic" + " FROM " + DatabaseSet.TableName + " t1" + " JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id" + " JOIN " + AgentDownInfo.TableName + " t3 ON t2.agent_ip = t3.agent_ip AND t2.agent_port = t3.agent_port " + " WHERE t1.database_id = ? LIMIT 1", colSetId);
        map.put("pro_type", Pro_Type.SHELL.getCode());
        map.put("pro_name", Constant.COLLECT_JOB_COMMAND);
        Map<String, Object> relationEtlMap = Dbo.queryOneObject("SELECT es.etl_sys_cd,ess.sub_sys_cd,tre.*" + " FROM " + TakeRelationEtl.TableName + " tre" + " JOIN " + DataExtractionDef.TableName + " ded ON ded.ded_id = tre.take_id" + " JOIN " + TableInfo.TableName + " ti ON ti.table_id = ded.table_id" + " JOIN " + DatabaseSet.TableName + " dbs ON dbs.database_id = ti.database_id" + " JOIN " + EtlSys.TableName + " es ON tre.etl_sys_id = es.etl_sys_id" + " JOIN " + EtlSubSysList.TableName + " ess ON ess.sub_sys_id = tre.sub_sys_id" + " JOIN " + EtlJobDef.TableName + " esd ON esd.etl_job_id = tre.etl_job_id" + " WHERE dbs.database_id = ? LIMIT 1", colSetId);
        map.putAll(relationEtlMap);
        return map;
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public void saveJobDataToDatabase(EtlJobInfo etlJobInfo) {
        DclEtlJobUtil.saveJobDataToDatabase(etlJobInfo, Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Long> getRelationEtl(long source_id) {
        return Dbo.queryOneColumnList("SELECT t1.etl_job_id FROM " + TakeRelationEtl.TableName + " t1" + " JOIN " + DatabaseSet.TableName + " t2 ON t1.database_id = t2.database_id " + " JOIN " + CollectJobClassify.TableName + " t3 ON t2.classify_id = t3.classify_id " + " JOIN " + AgentInfo.TableName + " t4 ON " + "t2.agent_id = t4.agent_id" + " WHERE t4.source_id = ?", source_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "status", desc = "", range = "")
    @Param(name = "jobRelation", desc = "", range = "")
    private void saveEtlDependencies(long etl_sys_id, long etl_job_id, String jobRelation) {
        Dbo.execute("DELETE FROM " + EtlDependency.TableName + " WHERE (etl_job_id = ? OR pre_etl_job_id = ?) AND etl_sys_id = ? ", etl_job_id, etl_job_id, etl_sys_id);
        if (StringUtil.isNotBlank(jobRelation)) {
            StringUtil.split(jobRelation, "^").forEach(item -> {
                EtlDependency etl_dependency = new EtlDependency();
                etl_dependency.setEtl_sys_id(etl_sys_id);
                etl_dependency.setEtl_job_id(etl_job_id);
                etl_dependency.setPre_etl_sys_id(etl_sys_id);
                etl_dependency.setPre_etl_job_id(item);
                etl_dependency.setStatus(Status.TRUE.getCode());
                etl_dependency.add(Dbo.db());
            });
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dicTableList", desc = "", range = "")
    @Param(name = "databaseTableNames", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, List<Object>> getDifferenceInfo(List<Object> dicTableList, List<Object> databaseTableNames) {
        log.info("数据字典的 " + dicTableList);
        log.info("数据库的 " + databaseTableNames);
        List<Object> exists = new ArrayList<>();
        List<Object> delete = new ArrayList<>();
        Map<String, List<Object>> differenceMap = new HashMap<>();
        for (Object databaseTableName : databaseTableNames) {
            if (dicTableList.contains(databaseTableName)) {
                exists.add(databaseTableName);
                dicTableList.remove(databaseTableName);
            } else {
                delete.add(databaseTableName);
            }
        }
        log.info("数据字典存在的===>" + exists);
        differenceMap.put("exists", exists);
        log.info("数据字典删除的===>" + delete);
        differenceMap.put("delete", delete);
        log.info("数据字典新增的===>" + dicTableList);
        differenceMap.put("add", dicTableList);
        return differenceMap;
    }
}
