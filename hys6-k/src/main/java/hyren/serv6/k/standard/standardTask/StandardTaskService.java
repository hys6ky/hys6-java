package hyren.serv6.k.standard.standardTask;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.entity.SysPara;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.k.entity.StandardImpInfo;
import hyren.serv6.k.entity.StandardTask;
import hyren.serv6.k.entity.StandardTaskTab;
import hyren.serv6.k.standard.standardImp.StandardImpService;
import hyren.serv6.k.standard.standardTask.bean.DbmNormbasicBean;
import hyren.serv6.k.standard.standardTask.bean.StandardCheckResult;
import hyren.serv6.k.standard.standardTask.bean.StandardInfoVo;
import hyren.serv6.k.standard.standardTask.entityVo.TaskVo;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.xm.Similarity;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class StandardTaskService {

    private static final String BATCH_NAME = "batch";

    public List<Map<String, Object>> getMetaTable(Long source_id, String table_name, String isAll, Page page) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("SELECT moi.* FROM meta_obj_info moi WHERE  moi.SOURCE_ID = ?  AND moi.TYPE = '0' ").addParam(source_id);
        if (IsFlag.Fou.getCode().toString().equals(isAll)) {
            sql.addSql(" and  moi.obj_id NOT IN (SELECT obj_id FROM " + StandardTaskTab.TableName + "   )");
        }
        if (!StringUtil.isEmpty(table_name)) {
            sql.addSql(" AND  ( moi.EN_NAME like ?  OR moi.CH_NAME like ? )").addParam("%" + table_name + "%").addParam("%" + table_name + "%");
        }
        return Dbo.queryPagedList(page, sql.sql(), sql.params());
    }

    public List<Map<String, Object>> queryTask(String source_name, String start_date, String end_date, Long task_id, Page page) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("select * from " + StandardTask.TableName + " where 1=1 ");
        if (!StringUtil.isEmpty(source_name)) {
            sql.addLikeParam("source_name", "%" + source_name + "%");
        }
        if (!StringUtil.isEmpty(start_date) && !StringUtil.isEmpty(end_date)) {
            sql.addSql(" AND to_date(upcheck_data,'yyyyMMDD') <= to_date(?,'yyyyMMDD') AND to_date(upcheck_data,'yyyyMMDD') >= to_date(?,'yyyyMMDD')").addParam(end_date).addParam(start_date);
        }
        if (task_id != null) {
            sql.addSql("AND task_id = ? ").addParam(task_id);
        }
        sql.addSql(" order by upcheck_data,upcheck_time,CREATED_DATE");
        return Dbo.queryPagedList(page, sql);
    }

    public Map<String, Object> getVime(Long task_id) {
        Map<String, Object> map = Dbo.queryOneObject("select * from " + StandardTask.TableName + " where task_ID = ? ", task_id);
        if (!map.isEmpty()) {
            List<StandardTaskTab> standardTaskTabs = Dbo.queryList(StandardTaskTab.class, " select obj_id from  " + StandardTaskTab.TableName + " where task_id = ? ", task_id);
            ArrayList<Long> objIds = new ArrayList<>();
            standardTaskTabs.forEach(table -> {
                objIds.add(table.getObj_id());
            });
            map.put("objList", objIds);
        }
        return map;
    }

    public void saveTask(TaskVo taskVo) {
        try {
            Dbo.beginTransaction();
            StandardTask standardTask = new StandardTask();
            BeanUtils.copyProperties(taskVo, standardTask);
            standardTask.setTask_id(PrimaryKeyUtils.nextId());
            standardTask.setCreated_date(DateUtil.getSysDate());
            standardTask.setCreated_time(DateUtil.getSysTime());
            standardTask.add(Dbo.db());
            if (standardTask.getIs_all_test().equals(IsFlag.Shi.getCode().toString())) {
                List<Map<String, Object>> allMetaTable = getAllMetaTable(standardTask.getSource_id(), Dbo.db());
                if (allMetaTable.size() != 0) {
                    List<Long> objId = allMetaTable.stream().map(map -> (Long) map.get("obj_id")).collect(Collectors.toList());
                    saveStanTaskTab(standardTask.getTask_id(), objId, Dbo.db());
                } else {
                    throw new BusinessException("元系统中的所有表已经生成过任务不能在进行生成操作");
                }
            } else {
                saveStanTaskTab(standardTask.getTask_id(), taskVo.getObjIds(), Dbo.db());
            }
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            log.error("错误信息:", e);
            throw new BusinessException(e.getMessage());
        }
    }

    public List<Map<String, Object>> getAllMetaTable(String source_id, DatabaseWrapper db) {
        return SqlOperator.queryList(db, "SELECT moi.* FROM meta_obj_info moi WHERE  moi.SOURCE_ID = ?  " + " AND moi.TYPE = '0' and  moi.obj_id NOT IN (SELECT obj_id FROM " + StandardTaskTab.TableName + "  )", Long.valueOf(source_id));
    }

    public void saveStanTaskTab(Long task_id, List<Long> objIds, DatabaseWrapper db) {
        ArrayList<Object[]> objects = new ArrayList<>();
        objIds.forEach(id -> {
            Object[] object = new Object[3];
            object[0] = PrimaryKeyUtils.nextId();
            object[1] = task_id;
            object[2] = id;
            objects.add(object);
        });
        if (!CollectionUtils.isEmpty(objects)) {
            SqlOperator.executeBatch(db, "insert  into " + StandardTaskTab.TableName + " (id,task_id,obj_id ) values (?,?,?)", objects);
        }
    }

    public void updateTask(TaskVo taskVo) {
        try {
            Dbo.beginTransaction();
            StandardTask standardTask = new StandardTask();
            BeanUtils.copyProperties(taskVo, standardTask);
            standardTask.setUpdated_date(DateUtil.getSysDate());
            standardTask.setUpdated_time(DateUtil.getSysTime());
            standardTask.update(Dbo.db());
            delStanTaskTab(standardTask.getTask_id(), Dbo.db());
            if (standardTask.getIs_all_test().equals(IsFlag.Shi.getCode())) {
                List<Map<String, Object>> allMetaTable = getAllMetaTable(standardTask.getSource_id(), Dbo.db());
                List<Long> objId = allMetaTable.stream().map(map -> (Long) map.get("obj_id")).collect(Collectors.toList());
                saveStanTaskTab(standardTask.getTask_id(), objId, Dbo.db());
            } else {
                saveStanTaskTab(standardTask.getTask_id(), taskVo.getObjIds(), Dbo.db());
            }
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            throw new BusinessException(e.getMessage());
        }
    }

    public void delStanTaskTab(Long task_id, DatabaseWrapper db) {
        SqlOperator.execute(db, "Delete from  " + StandardTaskTab.TableName + " where task_id = ?", task_id);
    }

    public void delTask(List<Long> ids) {
        try {
            Dbo.beginTransaction();
            StringBuilder sbTask = new StringBuilder();
            StringBuilder sbTaskTab = new StringBuilder();
            ids.forEach(id -> {
                List<StandardTask> standardTasks = Dbo.queryList(StandardTask.class, " select * from " + StandardTask.TableName + " where task_id = ?", id);
                EtlJobDef jobData = getJobData(id, standardTasks.get(0).getTask_name());
                if (jobData != null) {
                    throw new BusinessException(standardTasks.get(0).getTask_name() + " 中已生成作业不能进行删除操作！");
                }
            });
            sbTask.append("delete from " + StandardTask.TableName + " where task_id in ( ");
            sbTaskTab.append("delete from " + StandardTaskTab.TableName + " where task_id in ( ");
            ids.forEach(id -> {
                sbTask.append("?,");
                sbTaskTab.append("?,");
            });
            sbTask.deleteCharAt(sbTask.length() - 1);
            sbTask.append(")");
            sbTaskTab.deleteCharAt(sbTaskTab.length() - 1);
            sbTaskTab.append(")");
            Dbo.execute(sbTask.toString(), ids.toArray());
            Dbo.execute(sbTaskTab.toString(), ids.toArray());
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            throw new BusinessException(e.getMessage());
        }
    }

    public void standardBatch(long taskId, DatabaseWrapper db) {
        try {
            long startTime = System.currentTimeMillis();
            log.info("批量对标开始，开始时间为：{}", startTime);
            List<StandardImpInfo> standardImpInfoList = SqlOperator.queryList(db, StandardImpInfo.class, "SELECT T5.source_name AS source_ename, T2.obj_id, T2.en_name AS table_ename, " + "T2.ch_name AS table_cname, T1.dtl_id, T1.col_en_name AS src_col_ename, " + "T1.col_ch_name AS src_col_cname, T1.col_type AS src_col_type, " + "T1.col_len AS src_col_len, T1.col_prec AS src_col_preci " + "FROM meta_obj_tbl_col T1 " + "JOIN meta_obj_info T2 ON T1.obj_id = T2.obj_id " + "JOIN " + StandardTaskTab.TableName + " T3 ON T2.obj_id = T3.obj_id " + "JOIN " + StandardTask.TableName + " T4 ON T3.task_id = T4.task_id " + "JOIN meta_data_source T5 ON T2.source_id = T5.source_id " + "WHERE T4.task_id = ?", taskId);
            if (ObjectUtils.isEmpty(standardImpInfoList)) {
                log.warn("当前日期：{}无落标数据", taskId);
                return;
            }
            List<DbmNormbasicBean> dbmNormbasicList = SqlOperator.queryList(db, DbmNormbasicBean.class, "SELECT * FROM " + DbmNormbasicBean.TableName + " WHERE norm_status = ?", IsFlag.Shi.getCode());
            if (ObjectUtils.isEmpty(dbmNormbasicList)) {
                throw new SystemBusinessException("无标准记录");
            }
            SqlOperator.beginTransaction(db);
            for (StandardImpInfo standardImpInfo : standardImpInfoList) {
                if (ObjectUtils.isNotEmpty(standardImpInfo.getBasic_id()) && IsFlag.Shi.getCode().equals(standardImpInfo.getImp_result())) {
                    continue;
                }
                List<StandardImpInfo> standardImpInfoListCheck = SqlOperator.queryList(db, StandardImpInfo.class, "SELECT * FROM " + StandardImpInfo.TableName + " WHERE obj_id = ? and upper(src_col_ename) = ?", standardImpInfo.getObj_id(), standardImpInfo.getSrc_col_ename().toUpperCase());
                if (ObjectUtils.isNotEmpty(standardImpInfoListCheck) && IsFlag.Fou.getCode().equals(standardImpInfoListCheck.get(0).getImp_result())) {
                    SqlOperator.execute(db, "DELETE FROM " + StandardImpInfo.TableName + " WHERE imp_id = ?", standardImpInfoListCheck.get(0).getImp_id());
                    standardImpInfo = standardImpInfoListCheck.get(0);
                    standardImpInfo.setUpdated_by(BATCH_NAME);
                    standardImpInfo.setUpdated_date(DateUtil.getSysDate());
                    standardImpInfo.setUpdated_time(DateUtil.getSysTime());
                } else if (ObjectUtils.isNotEmpty(standardImpInfoListCheck) && IsFlag.Shi.getCode().equals(standardImpInfoListCheck.get(0).getImp_result())) {
                    continue;
                } else {
                    standardImpInfo.setImp_id(PrimaryKeyUtils.nextId());
                    standardImpInfo.setCreated_by(BATCH_NAME);
                    standardImpInfo.setCreated_date(DateUtil.getSysDate());
                    standardImpInfo.setCreated_time(DateUtil.getSysTime());
                }
                String srcColCname = standardImpInfo.getSrc_col_cname();
                dbmNormbasicList.forEach(e -> {
                    if (ObjectUtils.isEmpty(e.getDecimal_point())) {
                        e.setDecimal_point(0L);
                    }
                    if (ObjectUtils.isEmpty(e.getCol_len())) {
                        e.setCol_len(0L);
                    }
                    e.setPoint(Similarity.charBasedSimilarity(srcColCname, e.getNorm_cname()));
                });
                Collections.sort(dbmNormbasicList, Comparator.comparing(DbmNormbasicBean::getPoint).reversed());
                DbmNormbasicBean dbmNormbasicBean = dbmNormbasicList.get(0);
                standardImpInfo.setBasic_id(dbmNormbasicBean.getBasic_id());
                standardImpInfo.setNorm_cname(dbmNormbasicBean.getNorm_cname());
                standardImpInfo.setNorm_ename(dbmNormbasicBean.getNorm_ename());
                standardImpInfo.setNorm_col_type(dbmNormbasicBean.getData_type());
                standardImpInfo.setNorm_col_len(dbmNormbasicBean.getCol_len().intValue());
                standardImpInfo.setNorm_col_preci(dbmNormbasicBean.getDecimal_point().intValue());
                StandardInfoVo standardInfoVo = new StandardInfoVo();
                standardInfoVo.setBasic_id(standardImpInfo.getBasic_id());
                standardInfoVo.setSrc_col_cname(standardImpInfo.getSrc_col_cname());
                standardInfoVo.setSrc_col_ename(standardImpInfo.getSrc_col_ename());
                standardInfoVo.setSrc_col_type(standardImpInfo.getSrc_col_type());
                standardInfoVo.setSrc_col_len(standardImpInfo.getSrc_col_len());
                standardInfoVo.setSrc_col_preci(standardImpInfo.getSrc_col_preci());
                standardInfoVo.setCode_type_id(standardImpInfo.getCode_type_id());
                StandardCheckResult standardCheckResult = StandardImpService.standardCheck(dbmNormbasicBean, standardInfoVo);
                standardImpInfo.setImp_result(standardCheckResult.getImp_result());
                standardImpInfo.setImp_detail(standardCheckResult.getImp_detail());
                standardImpInfo.add(db);
            }
            SqlOperator.commitTransaction(db);
            log.info("批量对标结束，结束时间为：{}，耗时：{}", System.currentTimeMillis(), System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            SqlOperator.rollbackTransaction(db);
            throw new BusinessException(e.getMessage());
        }
    }

    public void updateStandardInfoByBatch(StandardImpInfo standardImpInfo, DatabaseWrapper db) {
        SqlOperator.execute(db, "UPDATE " + StandardImpInfo.TableName + " SET basic_id = ?, norm_cname = ?, norm_ename = ?, " + "norm_col_type = ?, norm_col_len = ?, norm_col_preci = ?, " + "imp_result = ?, imp_detail = ?, updated_by = ?, updated_date = ?, " + "updated_time = ? WHERE imp_id = ?", standardImpInfo.getBasic_id(), standardImpInfo.getNorm_cname(), standardImpInfo.getNorm_ename(), standardImpInfo.getNorm_col_type(), standardImpInfo.getNorm_col_len(), standardImpInfo.getNorm_col_preci(), standardImpInfo.getImp_result(), standardImpInfo.getImp_detail(), BATCH_NAME, DateUtil.getSysDate(), DateUtil.getSysTime(), standardImpInfo.getImp_id());
    }

    public EtlJobDef getJobData(Long task_id, String task_name) {
        Optional<EtlJobDef> etlJobDef = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_job = ? order By last_exe_time limit 1", (task_name + "_" + task_id));
        if (etlJobDef.isPresent()) {
            return etlJobDef.get();
        } else {
            return null;
        }
    }

    public Map<String, String> getSysPara(String paraType) {
        List<SysPara> sysParas = Dbo.queryList(SysPara.class, "select * from " + SysPara.TableName + " where para_type = ?", paraType);
        Map<String, String> map = new HashMap<>();
        sysParas.forEach(para -> {
            map.put(para.getPara_name(), para.getPara_value());
        });
        return map;
    }
}
