package hyren.serv6.control.task.helper;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.codes.Main_Server_Sync;
import hyren.serv6.base.codes.Meddle_status;
import hyren.serv6.base.codes.Pro_Type;
import hyren.serv6.base.entity.EtlJobCur;
import hyren.serv6.base.entity.EtlJobHand;
import hyren.serv6.base.entity.EtlJobHandHis;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.util.YarnUtil;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.control.task.TaskManager;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class TaskJobHandleHelper {

    private static final String JT = "JT";

    private static final String SO = "SO";

    private static final String SP = "SP";

    private static final String SR = "SR";

    private static final String JS = "JS";

    private static final String JR = "JR";

    private static final String JP = "JP";

    private static final String JJ = "JJ";

    private static final String SS = "SS";

    private static final String SB = "SB";

    private static final String SF = "SF";

    private static final String PARAERROR = "干预参数错误";

    private static final String NOEXITSERROR = "作业不存在";

    private static final String NOSUPPORT = "不支持的干预类型";

    private static final String STATEERROR = "当前状态不允许执行此操作";

    private static final String JOBSTOPERROR = "作业停止失败";

    private static final String PRIORITYERROR = "作业优先级设置超过范围";

    private static final String KILL9COMMANDLINE = "kill -9";

    private static final int DEFAULT_MILLISECONDS = 5000;

    private final TaskManager taskManager;

    public TaskJobHandleHelper(TaskManager taskManager) {
        this.taskManager = taskManager;
    }

    public void doHandle(List<EtlJobHand> handles) {
        for (EtlJobHand handle : handles) {
            log.info("检测到作业干预，作业名为 {}，干预类型为 {}", handle.getEtl_job_id(), handle.getEtl_hand_type());
            switch(handle.getEtl_hand_type()) {
                case JT:
                    handleRunning(handle);
                    break;
                case SO:
                    handleSysRerun(handle);
                    break;
                case SP:
                    handleSysPause(handle);
                    break;
                case SR:
                    handleSysResume(handle);
                    break;
                case SB:
                case SS:
                    handleSysStopAll(handle);
                    break;
                case JS:
                    handleJobStop(handle);
                    break;
                case JR:
                    handleJobRerun(handle);
                    break;
                case JP:
                    handleJobPriority(handle);
                    break;
                case JJ:
                    handleJobskip(handle);
                    break;
                case SF:
                    handleSysShift(handle);
                    break;
                default:
                    {
                        log.warn("{}  {}，{}", handle.getEtl_job_id(), handle.getEtl_hand_type(), NOSUPPORT);
                        handle.setWarning(NOSUPPORT);
                        updateErrorHandle(handle);
                    }
                    break;
            }
        }
    }

    private void handleRunning(EtlJobHand handle) {
        Optional<EtlJobCur> etlJobOptional = analyzeParameter(handle.getEtl_hand_type(), handle.getPro_para());
        if (!etlJobOptional.isPresent()) {
            log.warn("{}任务分析参数异常，{}", handle.getEtl_job_id(), PARAERROR);
            handle.setWarning(PARAERROR);
            updateErrorHandle(handle);
            return;
        }
        long etlSysId = handle.getEtl_sys_id();
        long etlJobId = handle.getEtl_job_id();
        String currBathDate = etlJobOptional.get().getCurr_bath_date();
        EtlJobCur etlJobCur;
        try {
            etlJobCur = TaskSqlHelper.getEtlJob(etlSysId, etlJobId, currBathDate);
        } catch (AppSystemException e) {
            handle.setWarning(NOEXITSERROR);
            updateErrorHandle(handle);
            return;
        }
        if (Job_Status.PENDING.getCode().equals(etlJobCur.getJob_disp_status()) || Job_Status.WAITING.getCode().equals(etlJobCur.getJob_disp_status())) {
            handle.setHand_status(Meddle_status.RUNNING.getCode());
            handle.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            handle.setEnd_time(DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT));
            TaskSqlHelper.updateEtlJobHandle(handle);
            taskManager.handleJob2Run(currBathDate, etlJobCur.getEtl_job());
        } else {
            log.warn("{}  {}，{}", etlJobCur.getEtl_job(), currBathDate, NOEXITSERROR);
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
        }
    }

    private void handleSysRerun(EtlJobHand handle) {
        long etlSysId = handle.getEtl_sys_id();
        if (!taskManager.isSysPause()) {
            log.warn("在进行重跑干预时，系统[{}]不是暂停状态，{}", etlSysId, STATEERROR);
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
            return;
        }
        List<EtlJobCur> etlJobs = TaskSqlHelper.getReadyEtlJobs(etlSysId);
        if (0 != etlJobs.size()) {
            log.warn("在进行重跑干预时，[{}]有未完成或未停止的作业，{}", handle.getEtl_job_id(), STATEERROR);
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
            return;
        }
        TaskSqlHelper.updateEtlJobToPending(etlSysId);
        taskManager.handleSys2Rerun();
        taskManager.closeSysPause();
        updateDoneHandle(handle);
    }

    private void handleSysPause(EtlJobHand handle) {
        if (taskManager.isSysPause()) {
            log.warn("在进行系统暂停干预时，[{}]已经是暂停状态，{}", handle.getEtl_job_id(), STATEERROR);
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
            return;
        }
        taskManager.handleSys2Pause();
        TaskSqlHelper.updateReadyEtlJobStatus(handle.getEtl_sys_id(), Job_Status.STOP.getCode());
        List<EtlJobCur> etlJobs = TaskSqlHelper.getEtlJobsByJobStatus(handle.getEtl_sys_id(), Job_Status.RUNNING.getCode());
        if (etlJobs.size() != 0) {
            stopRunningJobs(etlJobs);
            try {
                do {
                    Thread.sleep(DEFAULT_MILLISECONDS);
                } while (checkJobsNotStop(etlJobs));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        taskManager.openSysPause();
        updateDoneHandle(handle);
    }

    private void handleSysResume(EtlJobHand handle) {
        if (!taskManager.isSysPause()) {
            log.warn("在进行续跑干预时，[{}]不是暂停状态，{}", handle.getEtl_job_id(), STATEERROR);
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
            return;
        }
        long etlSysId = handle.getEtl_sys_id();
        List<EtlJobCur> etlJobs = TaskSqlHelper.getReadyEtlJobs(etlSysId);
        if (0 != etlJobs.size()) {
            log.warn("在进行续跑干预时，[{}]有未完成或未停止的作业，{}", handle.getEtl_job_id(), STATEERROR);
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
            return;
        }
        TaskSqlHelper.updateEtlJobToPendingInResume(etlSysId, Job_Status.PENDING.getCode());
        taskManager.handleSys2Resume();
        taskManager.closeSysPause();
        updateDoneHandle(handle);
    }

    private void handleSysStopAll(EtlJobHand handle) {
        TaskSqlHelper.updateReadyEtlJobsDispStatus(handle.getEtl_sys_id(), Job_Status.STOP.getCode());
        List<EtlJobCur> etlJobs = TaskSqlHelper.getEtlJobsByJobStatus(handle.getEtl_sys_id(), Job_Status.RUNNING.getCode());
        if (0 != etlJobs.size()) {
            stopRunningJobs(etlJobs);
            try {
                do {
                    Thread.sleep(DEFAULT_MILLISECONDS);
                } while (checkJobsNotStop(etlJobs));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        TaskSqlHelper.updateEtlSysRunStatus(handle.getEtl_sys_id(), Job_Status.STOP.getCode());
        updateDoneHandle(handle);
    }

    private void handleJobStop(EtlJobHand handle) {
        Optional<EtlJobCur> etlJobOptional = analyzeParameter(handle.getEtl_hand_type(), handle.getPro_para());
        if (!etlJobOptional.isPresent()) {
            log.warn("{}任务分析参数异常，{}", handle.getEtl_job_id(), PARAERROR);
            handle.setWarning(PARAERROR);
            updateErrorHandle(handle);
            return;
        }
        long etlSysId = handle.getEtl_sys_id();
        long etlJobId = handle.getEtl_job_id();
        String currBathDate = etlJobOptional.get().getCurr_bath_date();
        EtlJobCur etlJob;
        try {
            etlJob = TaskSqlHelper.getEtlJob(etlSysId, etlJobId, currBathDate);
        } catch (AppSystemException e) {
            handle.setWarning(NOEXITSERROR);
            updateErrorHandle(handle);
            return;
        }
        if (Job_Status.RUNNING.getCode().equals(etlJob.getJob_disp_status())) {
            updateRunningHandle(handle);
            if (closeProcessById(etlJob.getJob_process_id(), etlJob.getPro_type())) {
                updateDoneHandle(handle);
            } else {
                handle.setWarning(JOBSTOPERROR);
                updateErrorHandle(handle);
            }
            try {
                do {
                    Thread.sleep(DEFAULT_MILLISECONDS);
                } while (checkJobsNotStop(Collections.singletonList(etlJob)));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            TaskSqlHelper.updateEtlJobDispStatus(Job_Status.STOP.getCode(), etlJob.getEtl_sys_id(), etlJob.getEtl_job_id());
        } else if (Job_Status.PENDING.getCode().equals(etlJob.getJob_disp_status()) || Job_Status.WAITING.getCode().equals(etlJob.getJob_disp_status())) {
            taskManager.handleJob2Stop(etlJob.getCurr_bath_date(), etlJob.getEtl_job());
            updateDoneHandle(handle);
        } else {
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
        }
    }

    private void handleJobRerun(EtlJobHand handle) {
        Optional<EtlJobCur> etlJobOptional = analyzeParameter(handle.getEtl_hand_type(), handle.getPro_para());
        if (!etlJobOptional.isPresent()) {
            log.warn("{} 任务分析参数异常，{}", handle.getEtl_job_id(), PARAERROR);
            handle.setWarning(PARAERROR);
            updateErrorHandle(handle);
            return;
        }
        long etlSysId = handle.getEtl_sys_id();
        long etlJobId = handle.getEtl_job_id();
        String currBathDate = etlJobOptional.get().getCurr_bath_date();
        EtlJobCur etlJob;
        try {
            etlJob = TaskSqlHelper.getEtlJob(etlSysId, etlJobId, currBathDate);
        } catch (AppSystemException e) {
            handle.setWarning(NOEXITSERROR);
            updateErrorHandle(handle);
            return;
        }
        if (Job_Status.STOP.getCode().equals(etlJob.getJob_disp_status()) || Job_Status.ERROR.getCode().equals(etlJob.getJob_disp_status()) || Job_Status.DONE.getCode().equals(etlJob.getJob_disp_status())) {
            taskManager.handleJob2Rerun(currBathDate, etlJob.getEtl_job());
            updateDoneHandle(handle);
        } else {
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
        }
    }

    private void handleJobPriority(EtlJobHand handle) {
        Optional<EtlJobCur> etlJobOptional = analyzeParameter(handle.getEtl_hand_type(), handle.getPro_para());
        if (!etlJobOptional.isPresent()) {
            log.warn("{} 任务分析参数异常，{}", handle.getEtl_job_id(), PARAERROR);
            handle.setWarning(PARAERROR);
            updateErrorHandle(handle);
            return;
        }
        long etlSysId = handle.getEtl_sys_id();
        long etlJobId = handle.getEtl_job_id();
        String currBathDate = etlJobOptional.get().getCurr_bath_date();
        EtlJobCur etlJob;
        try {
            etlJob = TaskSqlHelper.getEtlJob(etlSysId, etlJobId, currBathDate);
        } catch (AppSystemException e) {
            handle.setWarning(NOEXITSERROR);
            updateErrorHandle(handle);
            return;
        }
        int priority = etlJobOptional.get().getJob_priority();
        if (priority < TaskManager.MINPRIORITY || priority > TaskManager.MAXPRIORITY) {
            handle.setWarning(PRIORITYERROR);
            updateErrorHandle(handle);
            return;
        }
        if (Job_Status.RUNNING.getCode().equals(etlJob.getJob_disp_status())) {
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
        } else {
            taskManager.handleJob2ChangePriority(currBathDate, etlJob.getEtl_job(), priority);
            updateDoneHandle(handle);
        }
    }

    private void handleJobskip(EtlJobHand handle) {
        Optional<EtlJobCur> etlJobOptional = analyzeParameter(handle.getEtl_hand_type(), handle.getPro_para());
        if (!etlJobOptional.isPresent()) {
            log.warn("{} 任务分析参数异常，{}", handle.getEtl_job_id(), PARAERROR);
            handle.setWarning(PARAERROR);
            updateErrorHandle(handle);
            return;
        }
        long etlSysId = handle.getEtl_sys_id();
        long etlJobId = handle.getEtl_job_id();
        String currBathDate = etlJobOptional.get().getCurr_bath_date();
        EtlJobCur etlJob;
        try {
            etlJob = TaskSqlHelper.getEtlJob(etlSysId, etlJobId, currBathDate);
        } catch (AppSystemException e) {
            handle.setWarning(NOEXITSERROR);
            updateErrorHandle(handle);
            return;
        }
        if (Job_Status.RUNNING.getCode().equals(etlJob.getJob_disp_status()) || Job_Status.DONE.getCode().equals(etlJob.getJob_disp_status())) {
            handle.setWarning(STATEERROR);
            updateErrorHandle(handle);
        } else {
            taskManager.handleJob2Skip(currBathDate, etlJob.getEtl_job());
            updateDoneHandle(handle);
        }
    }

    private void handleSysShift(EtlJobHand handle) {
        taskManager.handleSysDayShift();
        updateDoneHandle(handle);
    }

    private boolean checkJobsNotStop(List<EtlJobCur> etlJobs) {
        for (EtlJobCur job : etlJobs) {
            try {
                job = TaskSqlHelper.getEtlJob(job.getEtl_sys_id(), job.getEtl_job_id(), job.getCurr_bath_date());
            } catch (AppSystemException e) {
                throw new AppSystemException("在检查作业是否为停止状态时发生异常，该作业不存在：" + job.getEtl_job());
            }
            if (Job_Status.RUNNING.getCode().equals(job.getJob_disp_status())) {
                return true;
            }
        }
        return false;
    }

    private void stopRunningJobs(List<EtlJobCur> etlJobs) {
        for (EtlJobCur etlJob : etlJobs) {
            closeProcessById(etlJob.getJob_process_id(), etlJob.getPro_type());
            TaskSqlHelper.updateEtlJobDispStatus(Job_Status.STOP.getCode(), etlJob.getEtl_sys_id(), etlJob.getEtl_job_id());
        }
    }

    private boolean closeProcessById(String processId, String proType) {
        if (Pro_Type.Yarn.getCode().equals(proType)) {
            try {
                YarnUtil.killApplicationByid(processId);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else {
            if (StringUtil.isEmpty(processId))
                return true;
            String cmd = KILL9COMMANDLINE + " " + processId;
            try {
                Runtime.getRuntime().exec(cmd);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        log.info("作业关闭成功，进程号为 {}", processId);
        return true;
    }

    private Optional<EtlJobCur> analyzeParameter(String handleType, String paraStr) {
        if (StringUtil.isEmpty(paraStr)) {
            return Optional.empty();
        }
        String[] paraArray = paraStr.split(TaskManager.PARASEPARATOR);
        EtlJobCur etlJob = new EtlJobCur();
        switch(handleType) {
            case JT:
            case JS:
            case JR:
            case JJ:
                if (3 == paraArray.length) {
                    etlJob.setCurr_bath_date(paraArray[2]);
                } else {
                    return Optional.empty();
                }
                break;
            case JP:
                if (4 == paraArray.length) {
                    etlJob.setCurr_bath_date(paraArray[2]);
                    etlJob.setJob_priority(paraArray[3]);
                } else {
                    return Optional.empty();
                }
                break;
            case SS:
            case SP:
            case SO:
            case SR:
                if (2 == paraArray.length) {
                    etlJob.setCurr_bath_date(paraArray[1]);
                } else {
                    return Optional.empty();
                }
                break;
            default:
                return Optional.empty();
        }
        return Optional.of(etlJob);
    }

    private void updateErrorHandle(EtlJobHand etlJobHand) {
        etlJobHand.setHand_status(Meddle_status.ERROR.getCode());
        etlJobHand.setMain_serv_sync(Main_Server_Sync.NO.getCode());
        updateHandle(etlJobHand);
    }

    private void updateDoneHandle(EtlJobHand etlJobHand) {
        etlJobHand.setHand_status(Meddle_status.DONE.getCode());
        etlJobHand.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        updateHandle(etlJobHand);
    }

    private void updateRunningHandle(EtlJobHand etlJobHand) {
        etlJobHand.setHand_status(Meddle_status.RUNNING.getCode());
        etlJobHand.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        TaskSqlHelper.updateEtlJobHandle(etlJobHand);
    }

    private void updateHandle(EtlJobHand etlJobHand) {
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
}
