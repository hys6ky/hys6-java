package hyren.serv6.c.joblevelintervention;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.c.entity.JobHandBean;
import hyren.serv6.c.joblevelintervention.dto.BatchJobLevelInterventionOperateDTO;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.codes.Main_Server_Sync;
import hyren.serv6.base.codes.Meddle_status;
import hyren.serv6.base.codes.Meddle_type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class JobLevelInterventionService {

    @Method(desc = "", logicStep = "")
    @Param(name = "jobHandBeans", desc = "", range = "")
    @Param(name = "job_priority", desc = "", range = "", nullable = true)
    public void batchJobLevelInterventionOperate(BatchJobLevelInterventionOperateDTO dto, Long userId) {
        Validator.notEmpty(dto.getJobHandBeans(), "干预作业为空，请检查");
        List<JobHandBean> jobHandBeanList = dto.getJobHandBeans();
        for (JobHandBean jobHandBean : jobHandBeanList) {
            Validator.notNull(jobHandBean.getEtl_job_id(), "作业不能为空");
            Validator.notBlank(jobHandBean.getEtl_hand_type(), "干预类型不能为空");
            Validator.notNull(jobHandBean.getEtl_sys_id(), "工程不能为空");
            Validator.notBlank(jobHandBean.getCurr_bath_date(), "批量日期不能为空");
            jobLevelInterventionOperate(jobHandBean.getEtl_sys_id(), jobHandBean.getEtl_job_id(), jobHandBean.getEtl_hand_type(), jobHandBean.getCurr_bath_date(), dto.getJob_priority(), userId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "etl_hand_type", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Param(name = "job_priority", desc = "", range = "", nullable = true)
    public void jobLevelInterventionOperate(Long etl_sys_id, Long etl_job_id, String etl_hand_type, String curr_bath_date, Integer job_priority, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        EtlSys etlSys = EtlJobUtil.getEtlSysById(etl_sys_id, userId, Dbo.db());
        EtlJobDef etlJob = SqlOperator.queryOneObject(Dbo.db(), EtlJobDef.class, "SELECT * from " + EtlJobDef.TableName + " where etl_job_id=? AND etl_sys_id=?", etl_job_id, etl_sys_id).orElseThrow(() -> (new BusinessException("未找到对应的作业")));
        EtlJobHand etl_job_hand = new EtlJobHand();
        etl_job_hand.setEtl_sys_id(etl_sys_id);
        etl_job_hand.setEtl_hand_type(etl_hand_type);
        etl_job_hand.setEtl_job_id(etl_job_id);
        etl_job_hand.setEvent_id(DateUtil.getSysDate() + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
        etl_job_hand.setSt_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
        etl_job_hand.setHand_status(Meddle_status.TRUE.getCode());
        etl_job_hand.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        if (curr_bath_date.length() == 10 && curr_bath_date.contains("-")) {
            curr_bath_date = StringUtil.replace(curr_bath_date, "-", "");
        }
        etl_job_hand.setPro_para(etlSys.getEtl_sys_name() + "," + etlJob.getEtl_job() + "," + curr_bath_date);
        if (EtlJobUtil.isEtlJobHandExistByJob(etl_sys_id, etl_job_id, Dbo.db())) {
            throw new BusinessException("工程下有作业" + etlJob.getEtl_job() + "正在干预！");
        }
        if (Meddle_type.JOB_STOP == (Meddle_type.ofEnumByCode(etl_hand_type))) {
            String[] jobStatus = { Job_Status.DONE.getCode(), Job_Status.ERROR.getCode(), Job_Status.STOP.getCode() };
            long count = getEtlJobStatus(etl_sys_id, etl_job_id, jobStatus);
            if (count > 0) {
                Dbo.rollbackTransaction();
                throw new BusinessException("作业状态为完成或错误或停止的不可以停止，etl_job=" + etlJob.getEtl_job());
            } else {
                etl_job_hand.add(Dbo.db());
            }
        } else if (Meddle_type.JOB_JUMP == Meddle_type.ofEnumByCode(etl_hand_type)) {
            String[] jobStatus = { Job_Status.DONE.getCode(), Job_Status.RUNNING.getCode() };
            long count = getEtlJobStatus(etl_sys_id, etl_job_id, jobStatus);
            if (count > 0) {
                Dbo.rollbackTransaction();
                throw new BusinessException("作业状态为运行或完成的不可以跳过，etl_job=" + etlJob.getEtl_job_id());
            } else {
                etl_job_hand.add(Dbo.db());
            }
        } else if (Meddle_type.JOB_RERUN == Meddle_type.ofEnumByCode(etl_hand_type)) {
            String[] jobStatus = { Job_Status.PENDING.getCode(), Job_Status.RUNNING.getCode(), Job_Status.WAITING.getCode() };
            long count = getEtlJobStatus(etl_sys_id, etl_job_id, jobStatus);
            if (count > 0) {
                Dbo.rollbackTransaction();
                throw new BusinessException("作业状态为挂起或运行或等待的不可以重跑，etl_job=" + etlJob.getEtl_job());
            } else {
                etl_job_hand.add(Dbo.db());
            }
        } else if (Meddle_type.JOB_TRIGGER == Meddle_type.ofEnumByCode(etl_hand_type)) {
            String[] jobStatus = { Job_Status.DONE.getCode(), Job_Status.ERROR.getCode(), Job_Status.RUNNING.getCode(), Job_Status.STOP.getCode() };
            long count = getEtlJobStatus(etl_sys_id, etl_job_id, jobStatus);
            if (count > 0) {
                Dbo.rollbackTransaction();
                throw new BusinessException("作业状态为完成、错误、运行、停止不可以强制执行，etl_job=" + etlJob.getEtl_job());
            } else {
                etl_job_hand.add(Dbo.db());
            }
        } else if (Meddle_type.JOB_PRIORITY == Meddle_type.ofEnumByCode(etl_hand_type)) {
            Validator.notNull(job_priority, "干预类型为调整优先级时，作业优先级不能为空");
            String[] jobStatus = { Job_Status.RUNNING.getCode() };
            long count = getEtlJobStatus(etl_sys_id, etl_job_id, jobStatus);
            if (count > 0) {
                Dbo.rollbackTransaction();
                throw new BusinessException("作业状态为运行不可以临时调整优先级，etl_job=" + etlJob.getEtl_job());
            } else {
                if (StringUtil.isNotBlank(String.valueOf(job_priority))) {
                    etl_job_hand.setPro_para(etlSys.getEtl_sys_name() + "," + etlJob.getEtl_job() + "," + curr_bath_date + "," + job_priority);
                }
                etl_job_hand.add(Dbo.db());
            }
        } else {
            throw new AppSystemException("暂时不支持该干预类型！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "5")
    @Return(desc = "", range = "")
    public Map<String, Object> searchJobLevelCurrInterventionByPage(Long etl_sys_id, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> currInterventionList = Dbo.queryPagedList(page, "SELECT t1.event_id,t4.etl_sys_id,t4.etl_sys_cd,t2.etl_job_id,t2.etl_job,t1.etl_hand_type,t1.pro_para,hand_status,st_time,warning,CONCAT(t3.sub_sys_desc,'(',t3.sub_sys_id,')') AS subSysName " + " FROM " + EtlJobHand.TableName + " t1 " + " left join " + EtlJobDef.TableName + " t2 on t1.etl_job_id=t2.etl_job_id and t1.etl_sys_id=t2.etl_sys_id " + " left join " + EtlSubSysList.TableName + " t3 on t2.sub_sys_id=t3.sub_sys_id and t2.etl_sys_id=t3.etl_sys_id " + " left join " + EtlSys.TableName + " t4 on t1.etl_sys_id = t4.etl_sys_id " + " WHERE t1.etl_sys_id=? AND t1.etl_job_id<>?", etl_sys_id, 1000000000000000000L);
        Map<String, Object> currInterventionMap = new HashMap<>();
        currInterventionMap.put("totalSize", page.getTotalSize());
        currInterventionMap.put("currInterventionList", currInterventionList);
        return currInterventionMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "", nullable = true)
    @Param(name = "sub_sys_desc", desc = "", range = "", nullable = true)
    @Param(name = "job_status", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "5")
    @Return(desc = "", range = "")
    public Map<String, Object> searchJobLevelIntervention(Long etl_sys_id, String etl_job, String sub_sys_desc, String job_status, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT t1.etl_sys_id,t1.etl_job_id,t1.etl_job,");
        asmSql.addSql("CONCAT(t2.sub_sys_desc,'(',t2.sub_sys_cd,')') AS subSysName,");
        asmSql.addSql("t1.curr_bath_date,t1.job_disp_status FROM " + EtlJobCur.TableName + " t1 LEFT JOIN ");
        asmSql.addSql(EtlSubSysList.TableName + " t2 ON t1.etl_sys_id=t2.etl_sys_id");
        asmSql.addSql(" AND t1.sub_sys_id=t2.sub_sys_id");
        asmSql.addSql(" WHERE t1.etl_sys_id=?").addParam(etl_sys_id);
        if (StringUtil.isNotBlank(etl_job)) {
            asmSql.addLikeParam(" lower(t1.etl_job)", "%" + etl_job.toLowerCase() + "%");
        }
        if (StringUtil.isNotBlank(sub_sys_desc)) {
            asmSql.addLikeParam(" lower(t2.sub_sys_desc)", "%" + sub_sys_desc.toLowerCase() + "%");
        }
        if (StringUtil.isNotBlank(job_status)) {
            asmSql.addSql(" AND t1.job_disp_status=? ").addParam(job_status);
        }
        asmSql.addSql(" order by etl_job");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> etlJobInfoList = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> etlJobInfoMap = new HashMap<>();
        etlJobInfoMap.put("totalSize", page.getTotalSize());
        etlJobInfoMap.put("etlJobInfoList", etlJobInfoList);
        return etlJobInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchJobLeverHisInterventionByPage(Long etl_sys_id, int currPage, int pageSize, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> handHisList = Dbo.queryPagedList(page, "SELECT t1.event_id,t4.etl_sys_id,t4.etl_sys_cd,t2.etl_job_id,t2.etl_job,t1.etl_hand_type,t1.pro_para,hand_status,st_time,warning,CONCAT(t3.sub_sys_desc,'(',t3.sub_sys_id,')') AS subSysName " + " FROM " + EtlJobHandHis.TableName + " t1 " + " left join " + EtlJobDef.TableName + " t2 on t1.etl_job_id=t2.etl_job_id and t1.etl_sys_id=t2.etl_sys_id " + " left join " + EtlSubSysList.TableName + " t3 on t2.sub_sys_id=t3.sub_sys_id and t2.etl_sys_id=t3.etl_sys_id " + " left join " + EtlSys.TableName + " t4 on t1.etl_sys_id = t4.etl_sys_id " + " WHERE t1.etl_sys_id=? " + " AND t1.etl_job_id<>?", etl_sys_id, 1000000000000000000L);
        Map<String, Object> handHisMap = new HashMap<>();
        handHisMap.put("handHisList", handHisList);
        handHisMap.put("totalSize", page.getTotalSize());
        return handHisMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "jobStatus", desc = "", range = "")
    @Return(desc = "", range = "")
    private long getEtlJobStatus(Long etl_sys_id, Long etl_job_id, String[] jobStatus) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT count(1) from " + EtlJobCur.TableName + " where etl_job_id=? AND etl_sys_id=?");
        asmSql.addParam(etl_job_id);
        asmSql.addParam(etl_sys_id);
        if (jobStatus != null && jobStatus.length != 0) {
            asmSql.addORParam("job_disp_status", jobStatus);
        }
        return Dbo.queryNumber(asmSql.sql(), asmSql.params()).orElseThrow(() -> new BusinessException("sql查询错误！"));
    }
}
