package hyren.serv6.commons.jobUtil.task;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.beans.EtlJobBean;
import hyren.serv6.commons.jobUtil.beans.EtlJobDefBean;
import lombok.extern.slf4j.Slf4j;
import java.util.*;

@Slf4j
public class TaskSqlHelper {

    private static final ThreadLocal<DatabaseWrapper> _dbBox = new ThreadLocal<>();

    private TaskSqlHelper() {
    }

    private static DatabaseWrapper getDbConnector() {
        DatabaseWrapper db = _dbBox.get();
        if (db == null || !db.isConnected()) {
            db = new DatabaseWrapper();
            _dbBox.set(db);
        }
        try {
            SqlOperator.commitTransaction(db);
        } catch (Exception e) {
            db = new DatabaseWrapper();
            _dbBox.set(db);
            SqlOperator.commitTransaction(db);
        }
        return db;
    }

    public static void closeDbConnector() {
        try {
            DatabaseWrapper db = TaskSqlHelper.getDbConnector();
            SqlOperator.rollbackTransaction(db);
            db.close();
            _dbBox.remove();
            log.info("-------------- 调度服务DB连接已经关闭 --------------");
        } catch (Exception e) {
            log.warn("关闭连接异常，" + e.getMessage());
        }
    }

    public static EtlSys getEltSysBySysCode(String etlSysCd) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlSys.class, "SELECT * FROM " + EtlSys.TableName + " WHERE etl_sys_cd = ?", etlSysCd).orElseThrow(() -> new AppSystemException("无法根据调度系统编号获取系统信息 " + etlSysCd));
    }

    public static EtlSys getEltSysBySysId(long etlSysId) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlSys.class, "SELECT * FROM " + EtlSys.TableName + " WHERE etl_sys_id = ?", etlSysId).orElseThrow(() -> new AppSystemException("无法根据调度系统编号获取系统信息 " + etlSysId));
    }

    public static EtlJobCur getEtlJob(final long etlSysId, final long etlJobId, final String currBathDate) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ? AND curr_bath_date = ?", etlSysId, etlJobId, currBathDate).orElseThrow(() -> new AppSystemException("根据调度系统编号、调度作业标识、当前跑批日期获取调度作业信息失败"));
    }

    public static EtlJobCur getEtlJobByCd(final long etlSysId, final String etlJobId, final String currBathDate) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? AND etl_job = ? AND curr_bath_date = ?", etlSysId, etlJobId, currBathDate).orElseThrow(() -> new AppSystemException("根据调度系统编号、调度作业标识、当前跑批日期获取调度作业信息失败"));
    }

    public static EtlJobCur getEtlJob(long etlSysid, long etlJobid) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE etl_sys_id= ? AND etl_job_id = ?", etlSysid, etlJobid).orElseThrow(() -> new AppSystemException("根据调度系统编号、调度作业标识获取调度作业信息失败" + etlJobid));
    }

    public static Optional<EtlJobHand> getEtlJobHandle(final long etlSysId, final long etlJobId, final String handType) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlJobHand.class, "SELECT * FROM " + EtlJobHand.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ? AND etl_hand_type = ?", etlSysId, etlJobId, handType);
    }

    public static List<EtlJobResourceRela> getJobNeedResources(long etlSysId, long etlJobId) {
        List<EtlJobResourceRela> jobNeedResources = SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobResourceRela.class, "SELECT * FROM " + EtlJobResourceRela.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ?", etlSysId, etlJobId);
        if (null == jobNeedResources || jobNeedResources.size() == 0) {
            throw new AppSystemException("根据调度系统编号、调度作业名获取不到该作业需要的资源 " + etlJobId);
        }
        return jobNeedResources;
    }

    public static List<EtlJobDefBean> getAllDefJob(long etlSysId) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobDefBean.class, "SELECT * FROM " + EtlJobDef.TableName + " WHERE etl_sys_id = ? AND job_eff_flag != ?", etlSysId, Job_Effective_Flag.NO.getCode());
    }

    public static void updateFrequencyDefJob(long etlSysId) {
        SqlOperator.execute(TaskSqlHelper.getDbConnector(), "UPDATE " + EtlJobDef.TableName + " set com_exe_num = ?" + " WHERE etl_sys_id = ? AND job_eff_flag != ? AND disp_freq = ?", 0, etlSysId, Job_Effective_Flag.NO.getCode(), Dispatch_Frequency.PinLv.getCode());
    }

    private static Integer getSystemHistoryCertainDate(long etlSysId, String strBathDate) {
        Long lc = SqlOperator.queryNumber(TaskSqlHelper.getDbConnector(), " select 1 from etl_sys_his where etl_sys_id = ? and curr_bath_date = ? ", etlSysId, strBathDate).orElse(0);
        return Integer.parseInt(lc.toString());
    }

    public static List<Map<String, Object>> getJobDependencyBySysCode(long etlSysId) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), "SELECT ed.*, ejd.etl_job, prejd.etl_job as pre_etl_job" + " FROM  " + EtlDependency.TableName + " ed" + " JOIN " + EtlJobDef.TableName + " ejd on ed.etl_sys_id = ejd.etl_sys_id and ejd.etl_job_id = ed.etl_job_id" + " JOIN " + EtlJobDef.TableName + " prejd on ed.etl_sys_id = prejd.etl_sys_id and prejd.etl_job_id = ed.pre_etl_job_id" + " WHERE ed.etl_sys_id = ? AND ed.status = ? ", etlSysId, Status.TRUE.getCode());
    }

    public static List<EtlResource> getEtlSystemResources(long etlSysId, String etlSysCd) {
        List<EtlResource> etlResourceList = SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlResource.class, "SELECT *  FROM " + EtlResource.TableName + " WHERE etl_sys_id = ?", etlSysId);
        if (etlResourceList.isEmpty()) {
            throw new AppSystemException("根据调度系统编号获取系统资源信息失败" + etlSysCd + "_" + etlSysId);
        }
        return etlResourceList;
    }

    public static List<EtlSysDependency> getSystemDependencies(long etlSysId) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlSysDependency.class, "select * from etl_sys_dependency where etl_sys_id = ? and status = ?", etlSysId, Status.TRUE.getCode());
    }

    public static EtlErrorResource querySystemErrorResource(long etlSysId) {
        return SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlErrorResource.class, "select * from etl_error_resource where etl_sys_id = ?", etlSysId).orElse(null);
    }

    public static List<EtlJobHand> getEtlJobHands(long etlSysId) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobHand.class, "SELECT * FROM " + EtlJobHand.TableName + " WHERE hand_status = ? AND etl_sys_id = ?", Meddle_status.TRUE.getCode(), etlSysId);
    }

    public static List<EtlJobCur> getReadyEtlJobs(long etlSysId) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE (job_disp_status = ? OR job_disp_status = ?) AND etl_sys_id = ?", Job_Status.PENDING.getCode(), Job_Status.WAITING.getCode(), etlSysId);
    }

    public static List<EtlJobCur> getEtlJobsByJobStatus(long etlSysId, String jobStatus) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? AND job_disp_status = ?", etlSysId, jobStatus);
    }

    public static void updateEtlJob2Running(final long etlSysId, final long etlJobId, final String currStTime) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET curr_st_time = ?," + " main_serv_sync = ? WHERE etl_sys_id = ? AND etl_job_id = ?", currStTime, Main_Server_Sync.YES.getCode(), etlSysId, etlJobId);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJob2Complete(final String jobDispStatus, final String currEndTime, final Integer jobReturnVal, final String lastExeTime, final long etlJobId, final String currBathDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?," + " main_serv_sync = ?, curr_end_time = ?, job_return_val = ?," + " last_exe_time = ?, com_exe_num = com_exe_num + 1" + " WHERE etl_job_id = ? AND curr_bath_date = ?", jobDispStatus, Main_Server_Sync.YES.getCode(), currEndTime, jobReturnVal, lastExeTime, etlJobId, currBathDate);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobDefLastExeTime(final String lastExeTime, final long etlJobId) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobDef.TableName + " SET last_exe_time = ?," + " com_exe_num = com_exe_num + 1 WHERE etl_job_id = ?", lastExeTime, etlJobId);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobProcessId(final String processId, final long etlJobId) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_process_id = ?" + " WHERE etl_job_id = ?", processId, etlJobId);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobChildPId(String processId, long etlSysId, long etlJobId, String childPidByPPid) {
        if (!StringUtil.isEmpty(childPidByPPid)) {
            DatabaseWrapper db = TaskSqlHelper.getDbConnector();
            Result result = SqlOperator.queryResult(db, "select * from Etl_job_cpid WHERE etl_sys_id = ? AND etl_job_id = ? ", etlSysId, etlJobId);
            EtlJobCpid ejc = new EtlJobCpid();
            ejc.setEtl_job_id(etlJobId);
            ejc.setEtl_sys_id(etlSysId);
            ejc.setJob_child_id(childPidByPPid);
            ejc.setJob_process_id(processId);
            if (result.isEmpty())
                ejc.add(db);
            else
                ejc.update(db);
            SqlOperator.commitTransaction(db);
        }
    }

    public static void delEtlJobChildPId(long etlSysId) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "DELETE FROM  Etl_job_cpid WHERE etl_sys_id = ?", etlSysId);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobHandle(final EtlJobHand etlJobHand) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobHand.TableName + " SET hand_status = ?, main_serv_sync = ?, end_time = ?, warning = ?" + " WHERE etl_sys_id = ? AND etl_job_id = ? AND etl_hand_type = ?", etlJobHand.getHand_status(), etlJobHand.getMain_serv_sync(), etlJobHand.getEnd_time(), etlJobHand.getWarning(), etlJobHand.getEtl_sys_id(), etlJobHand.getEtl_job_id(), etlJobHand.getEtl_hand_type());
        if (num < 1) {
            throw new AppSystemException("修改调度作业干预表（etl_job_hand）失败 " + etlJobHand.getEtl_job_id());
        }
        SqlOperator.commitTransaction(db);
    }

    public static void insertIntoEtlJobHandleHistory(final EtlJobHandHis etlJobHandHis) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        etlJobHandHis.add(db);
        SqlOperator.commitTransaction(db);
    }

    public static boolean checkSystemDependency(List<EtlSysDependency> systemDependencies, String strBathDate) {
        for (EtlSysDependency systemDependency : systemDependencies) {
            long preEtlSysId = systemDependency.getPre_etl_sys_id();
            Integer systemHistoryCertainDate = getSystemHistoryCertainDate(preEtlSysId, strBathDate);
            if (systemHistoryCertainDate == 0) {
                log.info(String.format("等待上游系统 %s 在批量日期 %s 下完成 ...", preEtlSysId, strBathDate));
                return false;
            }
        }
        return true;
    }

    public static void updateReadyEtlJobStatus(long etlSysId, String runStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?, " + "main_serv_sync = ? WHERE (job_disp_status = ? or job_disp_status = ?) " + "AND etl_sys_id = ?", runStatus, Main_Server_Sync.YES.getCode(), Job_Status.PENDING.getCode(), Job_Status.WAITING.getCode(), etlSysId);
        SqlOperator.commitTransaction(db);
    }

    public static void updateReadyEtlJobsDispStatus(long etlSysId, String dispStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ? " + " WHERE etl_sys_id = ? AND (job_disp_status = ? OR job_disp_status = ?)", dispStatus, etlSysId, Job_Status.PENDING.getCode(), Job_Status.WAITING.getCode());
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlSysRunStatusAndBathDate(long etlSysId, String strBathDate, String sysEndDate, String runStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlSys.TableName + " SET main_serv_sync = ?, curr_bath_date = ?,sys_end_date = ?, sys_run_status = ?" + " WHERE etl_sys_id = ?", Main_Server_Sync.YES.getCode(), strBathDate, sysEndDate, runStatus, etlSysId);
        if (num != 1) {
            throw new AppSystemException("根据调度系统编号修改调度系统运行状态及当前跑批日期失败 " + etlSysId);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlSysBathDate(long etlSysId, String currBathDate, String sysEndDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlSys.TableName + " SET main_serv_sync = ?, curr_bath_date = ?,sys_end_date = ? WHERE etl_sys_id = ?", Main_Server_Sync.YES.getCode(), currBathDate, sysEndDate, etlSysId);
        if (num != 1) {
            SqlOperator.rollbackTransaction(db);
            throw new AppSystemException("根据调度系统编号修改调度系统跑批日期失败" + etlSysId);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlSysRunStatus(long etlSysId, String runStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlSys.TableName + " SET main_serv_sync = ?, sys_run_status = ? WHERE etl_sys_id = ?", Main_Server_Sync.YES.getCode(), runStatus, etlSysId);
        if (num != 1) {
            throw new AppSystemException("根据调度系统编号修改调度系统跑批日期失败" + etlSysId);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void deleteEtlJobByBathDate(long etlSysId, String currBathDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "DELETE FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? " + " AND curr_bath_date = ? AND disp_type != ? AND disp_freq != ?", etlSysId, currBathDate, Dispatch_Type.TPLUS0.getCode(), Dispatch_Frequency.PinLv.getCode());
        SqlOperator.commitTransaction(db);
    }

    public static void deleteEtlJobBySysCode(long etlSysId) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "DELETE FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? ", etlSysId);
        SqlOperator.commitTransaction(db);
    }

    public static void deleteEtlJobWithoutFrequency(long etlSysId, String currBathDate, String jobStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "DELETE FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? " + " AND curr_bath_date = ? AND job_disp_status = ? AND disp_freq != ?", etlSysId, currBathDate, jobStatus, Dispatch_Frequency.PinLv.getCode());
        SqlOperator.commitTransaction(db);
    }

    public static void deleteEtlJobByJobStatus(long etlSysId, String currBathDate, String jobStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "DELETE FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ? " + " AND curr_bath_date = ? AND job_disp_status = ?", etlSysId, currBathDate, jobStatus);
        SqlOperator.commitTransaction(db);
    }

    public static String getEtlParameterVal(long eltSysId, String paraCd) {
        Object[] row = SqlOperator.queryArray(TaskSqlHelper.getDbConnector(), "SELECT para_val FROM " + EtlPara.TableName + " WHERE etl_sys_id = ? AND para_cd = ?", eltSysId, paraCd);
        if (row.length == 0) {
            throw new AppSystemException(String.format("找不到对应的变量[%s]", paraCd));
        }
        return (String) row[0];
    }

    public static void updateSystemInfo(long etlSysId, String sysRunStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, " update etl_sys set sys_run_status=? where etl_sys_id=?", sysRunStatus, etlSysId);
        if (num < 1) {
            throw new AppSystemException("根据调度系统编号、当前批量日期修改作业信息失败" + etlSysId);
        }
        SqlOperator.commitTransaction(db);
    }

    public static EtlSysHis getAllJobTimesCertainDate(long etlSysId, String strBathDate) {
        final List<String> timeSortList = new ArrayList<>();
        List<EtlJobCur> etlJobs = SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ?" + " AND curr_bath_date = ?", etlSysId, strBathDate);
        if (etlJobs.isEmpty()) {
            timeSortList.add(strBathDate + " 000000");
            timeSortList.add(strBathDate + " 000000");
        } else {
            for (EtlJobCur etlJob : etlJobs) {
                String curr_st_time = etlJob.getCurr_st_time();
                String curr_end_time = etlJob.getCurr_end_time();
                if (curr_st_time != null) {
                    timeSortList.add(curr_st_time);
                }
                if (curr_end_time != null) {
                    timeSortList.add(curr_end_time);
                }
            }
        }
        Collections.sort(timeSortList);
        EtlSysHis esh = new EtlSysHis();
        esh.setRun_start_time(timeSortList.get(0));
        esh.setRun_end_time(timeSortList.get(timeSortList.size() - 1));
        return esh;
    }

    public static void archiveToHis(long etlSysId, EtlSysHis etlsh) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, " update etl_sys set main_serv_sync = ?," + "run_start_time = ?,run_end_time = ? ,sys_run_status = ? where etl_sys_id = ?", Main_Server_Sync.YES.getCode(), etlsh.getRun_start_time(), etlsh.getRun_end_time(), etlsh.getSys_run_status(), etlSysId);
        SqlOperator.execute(db, "delete from etl_sys_his where etl_sys_id = ? and curr_bath_date = ?", etlSysId, etlsh.getCurr_bath_date());
        EtlSysHis Etl_sys_his = SqlOperator.queryOneObject(TaskSqlHelper.getDbConnector(), EtlSysHis.class, "SELECT * from etl_sys where etl_sys_id = ? and curr_bath_date = ?", etlSysId, etlsh.getCurr_bath_date()).orElseThrow(() -> new AppSystemException("根据调度系统编号、调度作业标识获取调度作业信息失败" + etlSysId));
        Etl_sys_his.add(db);
    }

    public static Object[] insertIntoJobTable(EtlJobCur job) {
        return new Object[] { job.getEtl_job_id(), job.getCom_exe_num(), job.getComments(), job.getCurr_bath_date(), job.getCurr_end_time(), job.getCurr_st_time(), job.getDisp_freq(), job.getDisp_offset(), job.getDisp_time(), job.getDisp_type(), job.getEnd_time(), job.getEtl_job(), job.getEtl_job_desc(), job.getEtl_sys_id(), job.getExe_frequency(), job.getExe_num(), job.getJob_disp_status(), job.getJob_eff_flag(), job.getJob_priority(), job.getJob_priority_curr(), job.getJob_process_id(), job.getJob_return_val(), job.getLast_exe_time(), job.getLog_dic(), job.getMain_serv_sync(), job.getOverlength_val(), job.getOvertime_val(), job.getPro_dic(), job.getPro_name(), job.getPro_para(), job.getPro_type(), job.getStar_time(), job.getSub_sys_id(), job.getToday_disp(), job.getSuccess_job(), job.getFail_job(), job.getJob_datasource() };
    }

    public static void insertIntoJobTable(List<Object[]> paramsList) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        db.execBatch("insert into " + EtlJobCur.TableName + "(etl_job_id,com_exe_num,comments,curr_bath_date,curr_end_time,curr_st_time,disp_freq," + "disp_offset,disp_time,disp_type,end_time,etl_job,etl_job_desc,etl_sys_id," + "exe_frequency,exe_num,job_disp_status,job_eff_flag,job_priority,job_priority_curr," + "job_process_id,job_return_val,last_exe_time,log_dic,main_serv_sync,overlength_val," + "overtime_val,pro_dic,pro_name,pro_para,pro_type,star_time,sub_sys_id,today_disp," + "success_job,fail_job,job_datasource) " + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", paramsList);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtjJobWithDispStatus(String dispStatus, long etlSysId, String currBathDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?, main_serv_sync = ? WHERE etl_sys_id = ?" + " AND curr_bath_date = ?", dispStatus, Main_Server_Sync.YES.getCode(), etlSysId, currBathDate);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobDispStatus(String dispStatus, long etlSysId, String etlJob, String currBathDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ? WHERE etl_sys_id = ? AND etl_job = ?" + " AND curr_bath_date = ?", dispStatus, etlSysId, etlJob, currBathDate);
        if (num != 1) {
            throw new AppSystemException("根据调度系统编号、调度作业标识、" + "当前批量日期修改作业信息失败" + etlJob);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobDispStatus(String dispStatus, long etlSysId, List<EtlJobBean> etlJobCurs) {
        List<Object[]> params = new ArrayList<>();
        for (EtlJobBean etlJobCur : etlJobCurs) {
            List<String> items = new ArrayList<>();
            items.add(dispStatus);
            items.add(String.valueOf(etlSysId));
            items.add(etlJobCur.getEtl_job());
            items.add(etlJobCur.getCurr_bath_date());
            params.add(items.toArray());
        }
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int[] nums = SqlOperator.executeBatch(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ? WHERE etl_sys_id = ? AND etl_job = ?" + " AND curr_bath_date = ?", params);
        for (int i = 0; i < nums.length; i++) {
            if (nums[i] != 1) {
                throw new AppSystemException("根据调度系统编号、调度作业标识、" + "当前批量日期修改作业信息失败" + params.get(i)[2]);
            }
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobDispStatus(String dispStatus, long etlSysId, long etlJob_id) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?, main_serv_sync = ? WHERE etl_sys_id = ?" + " AND etl_job_id = ?", dispStatus, Main_Server_Sync.YES.getCode(), etlSysId, etlJob_id);
        if (num != 1) {
            throw new AppSystemException("根据调度系统编号、调度作业标识修改作业信息失败" + etlJob_id);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobRunTime(String currStTime, long etlSysId, String etlJob) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET curr_st_time = ?, main_serv_sync = ? WHERE etl_sys_id = ?" + " AND etl_job = ?", currStTime, Main_Server_Sync.YES.getCode(), etlSysId, etlJob);
        if (num != 1) {
            throw new AppSystemException("根据调度系统编号、调度作业标识，进行修改当前执行时间失败" + etlJob);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateVirtualJob(long etlSysId, String eltJob, String currBathDate, String currStTime, String currEndTime) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET main_serv_sync = ?," + " job_disp_status = ?, curr_st_time = ?, curr_end_time = ?" + " WHERE etl_sys_id = ? AND etl_job = ? AND curr_bath_date = ?", Main_Server_Sync.YES.getCode(), Job_Status.DONE.getCode(), currStTime, currEndTime, etlSysId, eltJob, currBathDate);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtjJobByResumeRun(long etlSysId, String currBathDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?," + " main_serv_sync = ? WHERE etl_sys_id = ? AND curr_bath_date = ?" + " AND job_disp_status NOT IN (?, ?) AND today_disp = ?", Job_Status.PENDING.getCode(), Main_Server_Sync.YES.getCode(), etlSysId, currBathDate, Job_Status.PENDING.getCode(), Job_Status.DONE.getCode(), Today_Dispatch_Flag.YES.getCode());
        SqlOperator.commitTransaction(db);
    }

    public static List<EtlJobCur> getEtlJobs(long etlSysId, String currBathDate) {
        return SqlOperator.queryList(TaskSqlHelper.getDbConnector(), EtlJobCur.class, "SELECT * FROM " + EtlJobCur.TableName + " WHERE etl_sys_id = ?" + " AND curr_bath_date <= ?", etlSysId, currBathDate);
    }

    public static void updateEtlResourceUsed(long etlSysId, int used) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlResource.TableName + " SET " + " resource_used = ? WHERE etl_sys_id = ?", used, etlSysId);
        if (num < 1) {
            throw new AppSystemException("根据调度系统编号来更新[资源使用数]失败" + etlSysId);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlResourceUsedByResourceType(long etlSysId, List<EtlResource> etlResources) {
        List<Object[]> params = new ArrayList<>();
        for (EtlResource etlResource : etlResources) {
            List<Object> items = new ArrayList<>();
            items.add(etlResource.getResource_used());
            items.add(etlSysId);
            items.add(etlResource.getResource_type());
            params.add(items.toArray());
        }
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int[] nums = SqlOperator.executeBatch(db, "UPDATE " + EtlResource.TableName + " SET resource_used = ? WHERE etl_sys_id = ? AND resource_type = ?", params);
        for (int i = 0; i < nums.length; i++) {
            if (nums[i] != 1) {
                throw new AppSystemException(String.format("据调度系统编号%s、" + "资源类型%s修改[已使用资源]失败", etlSysId, params.get(i)[2]));
            }
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlResourceUsedByResourceType(long etlSysId, String resourceType, int used) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlResource.TableName + " SET resource_used = ? WHERE etl_sys_id = ? AND resource_type = ?", used, etlSysId, resourceType);
        if (num != 1) {
            throw new AppSystemException(String.format("据调度系统编号%s、" + "资源类型%s修改[已使用资源]失败", etlSysId, resourceType));
        }
        SqlOperator.commitTransaction(db);
    }

    public static void deleteEtlJobHand(long etlSysId, long etlJobId, String etlHandType) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "DELETE FROM " + EtlJobHand.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ? AND etl_hand_type = ?", etlSysId, etlJobId, etlHandType);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobToPending(long etlSysId) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?, main_serv_sync = ?," + " job_priority_curr = job_priority WHERE etl_sys_id= ? AND today_disp = ?", Job_Status.PENDING.getCode(), Main_Server_Sync.YES.getCode(), etlSysId, Today_Dispatch_Flag.YES.getCode());
        if (num < 1) {
            throw new AppSystemException("根据调度系统编号，将该系统下的作业置为挂起状态失败" + etlSysId);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobToPendingInResume(long etlSysId, String jobStatus) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_disp_status = ?," + " main_serv_sync = ?, job_priority_curr = job_priority WHERE" + " (job_disp_status = ? OR job_disp_status = ?) AND etl_sys_id = ?", jobStatus, Main_Server_Sync.YES.getCode(), Job_Status.STOP.getCode(), Job_Status.ERROR.getCode(), etlSysId);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobCurrPriority(int jobPriorityCurr, long etlSysId, String etlJob, String currBathDate) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET job_priority_curr = ? WHERE etl_sys_id = ? AND etl_job = ?" + " AND curr_bath_date = ?", jobPriorityCurr, etlSysId, etlJob, currBathDate);
        if (num != 1) {
            throw new AppSystemException("根据调度系统编号、调度作业编号、" + "当前跑批日期修改当前作业优先级失败" + etlJob);
        }
        SqlOperator.commitTransaction(db);
    }

    public static void insertIntoEtlJobHand(EtlJobHand jobHand) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        jobHand.add(db);
        SqlOperator.commitTransaction(db);
    }

    public static void insertIntoEtlJobDispHis(EtlJobDispHis etlJobDispHis) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        etlJobDispHis.add(db);
        SqlOperator.commitTransaction(db);
    }

    public static void updateEtlJobFinished(EtlJobCur etlJobCur) {
        DatabaseWrapper db = TaskSqlHelper.getDbConnector();
        int num = SqlOperator.execute(db, "UPDATE " + EtlJobCur.TableName + " SET main_serv_sync = ?, job_disp_status = ?, curr_end_time = ?," + " job_return_val = ? WHERE etl_sys_id = ? AND etl_job_id = ?" + " AND curr_bath_date = ?", Main_Server_Sync.YES.getCode(), etlJobCur.getJob_disp_status(), etlJobCur.getCurr_end_time(), etlJobCur.getJob_return_val(), etlJobCur.getEtl_sys_id(), etlJobCur.getEtl_job_id(), etlJobCur.getCurr_bath_date());
        if (num != 1) {
            throw new AppSystemException("根据系统编号、作业标识、当前作业跑批日期更新作业信息失败" + etlJobCur.getEtl_job());
        }
        SqlOperator.commitTransaction(db);
    }

    public static String getParaByPara(String para) {
        Object[] row = SqlOperator.queryArray(TaskSqlHelper.getDbConnector(), "SELECT para_cd FROM " + EtlPara.TableName + " WHERE para_cd = ?", para);
        if (row.length == 0) {
            throw new AppSystemException("所使用的参数标识不存在" + para);
        }
        return (String) row[0];
    }
}
