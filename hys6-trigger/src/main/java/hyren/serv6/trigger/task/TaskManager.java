package hyren.serv6.trigger.task;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.Dispatch_Frequency;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.codes.Main_Server_Sync;
import hyren.serv6.base.codes.Meddle_status;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.jobUtil.task.HazelcastHelper;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.trigger.beans.EtlJobParaAnaly;
import hyren.serv6.trigger.constans.TriggerConfigure;
import hyren.serv6.trigger.task.executor.TaskExecutor;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class TaskManager {

    private final String etlSysCode;

    private final long etlSysId;

    private final String strRunningJob;

    private final String strFinishedJob;

    private final static String RUNNINGJOBFLAG = "RunningJob";

    private final static String FINISHEDJOBFLAG = "FinishedJob";

    private final static String REDISCONTENTSEPARATOR = "@";

    private final static String REDISHANDLE = "Handle";

    private final static String ERRORJOBMSG = "作业执行失败";

    private final static String JT = "JT";

    private static final HazelcastHelper REDIS = HazelcastHelper.getInstance(TriggerConfigure.getHazelcastConfig());

    private static final ExecutorService executeThread = Executors.newCachedThreadPool();

    public TaskManager(String etlSysCode) {
        this.etlSysCode = etlSysCode;
        this.etlSysId = TaskSqlHelper.getEltSysBySysCode(etlSysCode).getEtl_sys_id();
        this.strRunningJob = etlSysCode + RUNNINGJOBFLAG;
        this.strFinishedJob = etlSysCode + FINISHEDJOBFLAG;
    }

    public boolean checkSysGoRun() {
        try {
            EtlSys etlSys = TaskSqlHelper.getEltSysBySysCode(etlSysCode);
            if (Job_Status.STOP.getCode().equals(etlSys.getSys_run_status())) {
                log.warn("----- 调度系统编号为{}的系统已是停止状态，系统停止 -----", etlSysCode);
                return false;
            }
            return true;
        } catch (AppSystemException e) {
            log.error("没有对应的调度系统，调度系统编号为{}", etlSysCode);
            return false;
        }
    }

    public EtlJobParaAnaly getEtlJob() {
        EtlJobParaAnaly etlJobParaAnaly = new EtlJobParaAnaly();
        etlJobParaAnaly.setHasEtlJob(false);
        long runningListSize = REDIS.llen(strRunningJob);
        log.info("REDIS.llen(strRunningJob):{}", runningListSize);
        if (runningListSize < 1) {
            log.info("------ 没有被登记的可运行作业 ------");
            return etlJobParaAnaly;
        }
        String runningJobStr = REDIS.lpop(strRunningJob);
        if (StringUtil.isEmpty(runningJobStr)) {
            log.info("------ 未被分配到可运行作业 ------");
            return etlJobParaAnaly;
        }
        String[] jobKey = runningJobStr.split(REDISCONTENTSEPARATOR);
        if (jobKey.length < 2 || jobKey.length > 3) {
            log.warn("------ 错误参数的可运行作业：{} ------", Arrays.toString(jobKey));
            return etlJobParaAnaly;
        }
        String etlJob = jobKey[0];
        String currBathDate = jobKey[1];
        try {
            EtlJobCur etlJobCur = TaskSqlHelper.getEtlJobByCd(etlSysId, etlJob, currBathDate);
            if (jobKey.length == 3)
                etlJobParaAnaly.setHasHandle(REDISHANDLE.equals(jobKey[2]));
            etlJobParaAnaly.setEtlJobCur(etlJobCur);
            etlJobParaAnaly.setHasEtlJob(true);
            return etlJobParaAnaly;
        } catch (AppSystemException e) {
            log.warn("{} 作业不存在", etlJob);
            return etlJobParaAnaly;
        }
    }

    public void runEtlJob(final EtlJobCur etlJobCur, final boolean hasHandle) {
        try {
            executeThread.execute(() -> {
                try {
                    String etlJob = etlJobCur.getEtl_job();
                    String jobOnly = etlJob + "_" + etlJobCur.getCurr_bath_date();
                    try {
                        String currDateTime = DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT);
                        etlJobCur.setCurr_st_time(currDateTime);
                        TaskSqlHelper.updateEtlJob2Running(etlSysId, etlJobCur.getEtl_job_id(), currDateTime);
                        log.info("{} 作业开始执行，开始执行时间为 {}", jobOnly, currDateTime);
                        EtlJobCur etlJobCurResult = TaskExecutor.executeEtlJob(etlJobCur, etlSysCode);
                        if (TaskExecutor.PROGRAM_DONE_FLAG == etlJobCurResult.getJob_return_val()) {
                            etlJobCurResult.setJob_disp_status(Job_Status.DONE.getCode());
                        } else {
                            log.warn("{} 作业异常结束", jobOnly);
                            etlJobCurResult.setJob_disp_status(Job_Status.ERROR.getCode());
                        }
                        currDateTime = DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT);
                        etlJobCurResult.setCurr_end_time(currDateTime);
                        etlJobCurResult.setLast_exe_time(currDateTime);
                        freedEtlJob(etlJobCurResult);
                    } catch (IOException | InterruptedException e) {
                        log.warn("{} 作业异常结束并修改作业状态", jobOnly, e);
                        etlJobCur.setJob_disp_status(Job_Status.ERROR.getCode());
                        String currDateTime = DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT);
                        etlJobCur.setCurr_end_time(currDateTime);
                        etlJobCur.setLast_exe_time(currDateTime);
                        freedEtlJob(etlJobCur);
                    } catch (Exception e) {
                        log.warn("{} errorHappens", jobOnly, e);
                    }
                    try {
                        if (hasHandle) {
                            Optional<EtlJobHand> etlJobHandOptional = TaskSqlHelper.getEtlJobHandle(etlSysId, etlJobCur.getEtl_job_id(), JT);
                            if (!etlJobHandOptional.isPresent()) {
                                log.warn("{} 该作业的干预无法查询到，干预处理将会忽略", jobOnly);
                                return;
                            }
                            EtlJobHand etlJobHand = etlJobHandOptional.get();
                            if (Job_Status.DONE.getCode().equals(etlJobCur.getJob_disp_status())) {
                                etlJobHand.setHand_status(Meddle_status.DONE.getCode());
                                etlJobHand.setMain_serv_sync(Main_Server_Sync.YES.getCode());
                            } else if (Job_Status.ERROR.getCode().equals(etlJobCur.getJob_disp_status())) {
                                etlJobHand.setHand_status(Meddle_status.ERROR.getCode());
                                etlJobHand.setMain_serv_sync(Main_Server_Sync.NO.getCode());
                                etlJobHand.setWarning(ERRORJOBMSG);
                            }
                            TaskManager.updateHandle(etlJobHand);
                        }
                    } catch (Exception e) {
                        log.warn("{} 干预失败", jobOnly, e);
                    }
                } finally {
                    TaskSqlHelper.closeDbConnector();
                }
            });
        } catch (Exception ex) {
            log.error("Exception happened!", ex);
        }
    }

    private void freedEtlJob(EtlJobCur etlJobCur) {
        TaskSqlHelper.insertIntoEtlJobDispHis(TaskManager.etlJobCur2EtlJobDispHis(etlJobCur));
        TaskSqlHelper.updateEtlJob2Complete(etlJobCur.getJob_disp_status(), etlJobCur.getCurr_end_time(), etlJobCur.getJob_return_val(), etlJobCur.getLast_exe_time(), etlJobCur.getEtl_job_id(), etlJobCur.getCurr_bath_date());
        TaskSqlHelper.updateEtlJobDefLastExeTime(etlJobCur.getLast_exe_time(), etlJobCur.getEtl_job_id());
        String finishedJob = etlJobCur.getEtl_job() + REDISCONTENTSEPARATOR + etlJobCur.getCurr_bath_date();
        if (Dispatch_Frequency.ofEnumByCode(etlJobCur.getDisp_freq()) == Dispatch_Frequency.PinLv) {
            int exeNum = etlJobCur.getExe_num();
            int com_exe_num = etlJobCur.getCom_exe_num();
            LocalDateTime endDateTime = LocalDateTime.parse(etlJobCur.getEnd_time(), DateUtil.DATETIME_DEFAULT);
            LocalDateTime currDateTime = LocalDateTime.now();
            log.info("频率作业{}第{}次执行，执行开始时间:{}", etlJobCur.getEtl_job(), (com_exe_num + 1), currDateTime);
            if (com_exe_num + 1 >= exeNum || currDateTime.isAfter(endDateTime)) {
                REDIS.rpush(strFinishedJob, finishedJob);
            } else {
                try {
                    Thread.sleep(etlJobCur.getExe_frequency() * 60 * 1000);
                    REDIS.rpush(strRunningJob, finishedJob);
                } catch (InterruptedException e) {
                    throw new AppSystemException(e);
                }
            }
        } else {
            REDIS.rpush(strFinishedJob, finishedJob);
        }
    }

    private static void updateHandle(final EtlJobHand etlJobHand) {
        etlJobHand.setEnd_time(DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT));
        TaskSqlHelper.updateEtlJobHandle(etlJobHand);
        EtlJobHandHis etlJobHandHis = new EtlJobHandHis();
        etlJobHandHis.setEvent_id(String.valueOf(PrimayKeyGener.getNextId()));
        etlJobHandHis.setEtl_sys_id(etlJobHand.getEtl_sys_id());
        etlJobHandHis.setEtl_job_id(etlJobHand.getEtl_job_id());
        etlJobHandHis.setEtl_hand_type(etlJobHand.getEtl_hand_type());
        etlJobHandHis.setPro_para(etlJobHand.getPro_para());
        etlJobHandHis.setHand_status(etlJobHand.getHand_status());
        etlJobHandHis.setSt_time(etlJobHand.getSt_time());
        etlJobHandHis.setEnd_time(etlJobHand.getEnd_time());
        etlJobHandHis.setWarning(etlJobHand.getWarning());
        etlJobHandHis.setMain_serv_sync(etlJobHand.getMain_serv_sync());
        TaskSqlHelper.insertIntoEtlJobHandleHistory(etlJobHandHis);
        TaskSqlHelper.deleteEtlJobHand(etlJobHand.getEtl_sys_id(), etlJobHand.getEtl_job_id(), etlJobHand.getEtl_hand_type());
    }

    private static EtlJobDispHis etlJobCur2EtlJobDispHis(final EtlJobCur etlJobCur) {
        EtlJobDispHis etlJobDispHis = new EtlJobDispHis();
        etlJobDispHis.setEtl_sys_id(etlJobCur.getEtl_sys_id());
        etlJobDispHis.setEtl_job(etlJobCur.getEtl_job());
        etlJobDispHis.setCurr_bath_date(etlJobCur.getCurr_bath_date());
        etlJobDispHis.setSub_sys_id(etlJobCur.getSub_sys_id());
        etlJobDispHis.setEtl_job_desc(etlJobCur.getEtl_job_desc());
        etlJobDispHis.setPro_type(etlJobCur.getPro_type());
        etlJobDispHis.setPro_dic(etlJobCur.getPro_dic());
        etlJobDispHis.setPro_name(etlJobCur.getPro_name());
        etlJobDispHis.setPro_para(etlJobCur.getPro_para());
        etlJobDispHis.setLog_dic(etlJobCur.getLog_dic());
        etlJobDispHis.setDisp_freq(etlJobCur.getDisp_freq());
        etlJobDispHis.setDisp_offset(etlJobCur.getDisp_offset());
        etlJobDispHis.setDisp_type(etlJobCur.getDisp_type());
        etlJobDispHis.setDisp_time(etlJobCur.getDisp_time());
        etlJobDispHis.setJob_eff_flag(etlJobCur.getJob_eff_flag());
        etlJobDispHis.setJob_priority(etlJobCur.getJob_priority());
        etlJobDispHis.setJob_disp_status(etlJobCur.getJob_disp_status());
        etlJobDispHis.setCurr_st_time(etlJobCur.getCurr_st_time());
        etlJobDispHis.setCurr_end_time(etlJobCur.getCurr_end_time());
        etlJobDispHis.setOverlength_val(etlJobCur.getOverlength_val());
        etlJobDispHis.setOvertime_val(etlJobCur.getOvertime_val());
        etlJobDispHis.setComments(etlJobCur.getComments());
        etlJobDispHis.setToday_disp(etlJobCur.getToday_disp());
        etlJobDispHis.setExe_frequency(etlJobCur.getExe_frequency());
        etlJobDispHis.setExe_num(etlJobCur.getExe_num());
        etlJobDispHis.setCom_exe_num(etlJobCur.getCom_exe_num());
        etlJobDispHis.setLast_exe_time(etlJobCur.getLast_exe_time());
        etlJobDispHis.setStar_time(etlJobCur.getStar_time());
        etlJobDispHis.setEnd_time(etlJobCur.getEnd_time());
        etlJobDispHis.setSuccess_job(etlJobCur.getSuccess_job());
        etlJobDispHis.setFail_job(etlJobCur.getFail_job());
        etlJobDispHis.setJob_datasource(etlJobCur.getJob_datasource());
        return etlJobDispHis;
    }
}
