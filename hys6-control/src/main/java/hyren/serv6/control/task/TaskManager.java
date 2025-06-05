package hyren.serv6.control.task;

import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.beans.EtlJobBean;
import hyren.serv6.commons.jobUtil.beans.EtlJobDefBean;
import hyren.serv6.commons.jobUtil.task.HazelcastHelper;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.control.beans.WaitFileJobInfo;
import hyren.serv6.control.constans.ControlConfigure;
import hyren.serv6.control.task.helper.NotifyMessageHelper;
import hyren.serv6.control.task.helper.TaskJobHandleHelper;
import hyren.serv6.control.task.helper.TaskJobHelper;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class TaskManager {

    public static final DateTimeFormatter DATETIME_DEFAULT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    private static final String JOB_DEFAULTSTART_DATETIME_STR = "20001231 235959";

    private static final LocalDateTime JOB_DEFAULTSTART_DATETIME = LocalDateTime.parse(JOB_DEFAULTSTART_DATETIME_STR, DateUtil.DATETIME_DEFAULT);

    private static final long zclong = 999999999999999999L;

    public static final int MAXPRIORITY = 99;

    public static final int MINPRIORITY = 1;

    public static final int DEFAULT_PRIORITY = 5;

    private static final Map<String, EtlJobDefBean> jobDefineMap = new HashMap<>();

    private static final Map<String, EtlJobDefBean> jobFrequencyMap = new HashMap<>();

    private static final Map<String, String> jobTimeDependencyMap = new HashMap<>();

    private static final Map<String, List<String>> jobDependencyMap = new HashMap<>();

    private static final Map<String, Map<String, EtlJobBean>> jobExecuteMap = new HashMap<>();

    private static final List<WaitFileJobInfo> waitFileJobList = new ArrayList<>();

    private static final List<EtlJobBean> jobWaitingList = new ArrayList<>();

    private static final List<EtlSysDependency> systemDependencies = new ArrayList<>();

    private static final Map<String, EtlResource> sysResourceMap = new HashMap<>();

    private static final HazelcastHelper REDIS = HazelcastHelper.getInstance(ControlConfigure.getHazelcastConfig());

    private static final NotifyMessageHelper NOTIFY = NotifyMessageHelper.getInstance();

    private final TaskJobHandleHelper handleHelper;

    private String bathDateStr;

    private String endDateStr = "99991231";

    private final String etlSysCd;

    private final long etlSysId;

    private final boolean isResumeRun;

    private final boolean isAutoShift;

    private final boolean isNeedSendSMS;

    private static boolean isSysPause = false;

    private static boolean isSysJobShift = false;

    private boolean sysRunning = false;

    private CheckWaitFileThread checkWaitFileThread;

    private final static String RUNNINGJOBFLAG = "RunningJob";

    private final static String FINISHEDJOBFLAG = "FinishedJob";

    private final String strRunningJob;

    private final String strFinishedJob;

    private final static String REDISCONTENTSEPARATOR = "@";

    private final static String REDISHANDLE = "Handle";

    public final static String PARASEPARATOR = ",";

    private final static long LOCKMILLISECONDS = 1000;

    private volatile boolean isLock = false;

    private static final int SLEEPMILLIS = 3000;

    private boolean frequencyFlag = false;

    private boolean sysDateShiftFlag = false;

    private boolean hasDependencyProject = true;

    public TaskManager(String etlSysCd, String bathDate, String endDate, boolean isResumeRun, boolean isAutoShift) {
        this.etlSysCd = etlSysCd;
        this.etlSysId = TaskSqlHelper.getEltSysBySysCode(etlSysCd).getEtl_sys_id();
        this.bathDateStr = bathDate;
        if (isAutoShift) {
            this.endDateStr = endDate;
        }
        this.isResumeRun = isResumeRun;
        this.isAutoShift = isAutoShift;
        this.strRunningJob = etlSysCd + RUNNINGJOBFLAG;
        this.strFinishedJob = etlSysCd + FINISHEDJOBFLAG;
        this.isNeedSendSMS = ControlConfigure.NotifyConfig.isNeedSendSMS;
        this.handleHelper = new TaskJobHandleHelper(this);
    }

    public void initEtlSystem() {
        REDIS.deleteByKey(strRunningJob, strFinishedJob);
        List<EtlResource> resources = TaskSqlHelper.getEtlSystemResources(etlSysId, etlSysCd);
        for (EtlResource resource : resources) {
            String resourceType = resource.getResource_type();
            int maxCount = resource.getResource_max();
            int usedCount = 0;
            EtlResource newResource = new EtlResource();
            newResource.setResource_type(resourceType);
            newResource.setResource_max(maxCount);
            newResource.setResource_used(usedCount);
            sysResourceMap.put(resourceType, newResource);
            log.info("{}'s maxCount {}", resourceType, maxCount);
            log.info("{}'s usedCount {}", resourceType, usedCount);
        }
        List<EtlSysDependency> systemD = TaskSqlHelper.getSystemDependencies(etlSysId);
        if (systemD.isEmpty()) {
            hasDependencyProject = false;
            log.info("工程{}无依赖工程", etlSysCd);
        } else {
            for (EtlSysDependency etl_sys_dependency : systemD) {
                systemDependencies.add(etl_sys_dependency);
            }
        }
    }

    private static Map<String, Integer> querySystemErrorResource(long etlSysId) {
        EtlErrorResource eer = TaskSqlHelper.querySystemErrorResource(etlSysId);
        int start_number = 3;
        int start_interval = 2;
        if (eer != null) {
            start_number = eer.getStart_number();
            start_interval = eer.getStart_interval();
        }
        Map<String, Integer> map = new HashMap<>();
        map.put("start_number", start_number);
        map.put("start_interval", start_interval);
        return map;
    }

    public void startCheckWaitFileThread() {
        if (null == checkWaitFileThread) {
            checkWaitFileThread = new CheckWaitFileThread();
        }
        checkWaitFileThread.start();
    }

    public void stopCheckWaitFileThread() {
        if (null != checkWaitFileThread) {
            checkWaitFileThread.stopThread();
        }
    }

    public void loadReadyJob() {
        jobDefineMap.clear();
        jobTimeDependencyMap.clear();
        jobDependencyMap.clear();
        jobFrequencyMap.clear();
        if (!isAutoShift) {
            TaskSqlHelper.updateFrequencyDefJob(etlSysId);
        }
        List<EtlJobDefBean> jobs = TaskSqlHelper.getAllDefJob(etlSysId);
        loadJobDefine(jobs);
        loadJobDependency();
        loadExecuteJob(jobs);
    }

    public boolean publishReadyJob() {
        int etlhandNumber = 0;
        long etlhandInerval = 0;
        Map<String, Integer> stringLongMap = querySystemErrorResource(etlSysId);
        int start_number = stringLongMap.get("start_number");
        long start_interval = stringLongMap.get("start_interval") * 1000;
        sysDateShiftFlag = false;
        int checkCount = 0;
        while (true) {
            if (checkCount == 200) {
                checkReadyJob();
                checkCount = 0;
            }
            checkCount++;
            checkFinishedJob();
            checkExecutedJob();
            EtlSys etlSys = TaskSqlHelper.getEltSysBySysId(etlSysId);
            if (Job_Status.STOP.getCode().equals(etlSys.getSys_run_status())) {
                sysRunning = false;
                stopCheckWaitFileThread();
                log.warn("---------- 系统干预，{} 调度停止 ------------", etlSysCd);
                sysDateShiftFlag = false;
                return false;
            }
            if (hasDependencyProject) {
                if (!TaskSqlHelper.checkSystemDependency(systemDependencies, bathDateStr)) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        log.warn("检测上游工程是否完成异常，异常可以跳过", e.getMessage());
                    }
                    continue;
                }
            }
            List<EtlJobHand> handles = TaskSqlHelper.getEtlJobHands(etlSysId);
            if (handles.size() != 0) {
                handleHelper.doHandle(handles);
                if (isSysJobShift) {
                    bathDateStr = TaskJobHelper.getNextBathDate(bathDateStr);
                    log.info("{}系统日切干预完成，下一批次为{}", etlSysCd, bathDateStr);
                    isSysJobShift = false;
                    sysDateShiftFlag = true;
                    return true;
                }
            }
            updateSysUsedResource();
            while (isLock) {
                log.info("Lock is true, Please wait.");
                try {
                    Thread.sleep(LOCKMILLISECONDS);
                } catch (InterruptedException ignored) {
                }
            }
            if (!isLock)
                isLock = true;
            if (jobWaitingList.size() > 1) {
                log.info("调度系统[{}]的作业等待队列中有待执行的作业，将进行排序", etlSysCd);
                Collections.sort(jobWaitingList);
            }
            List<EtlJobBean> removeList = new ArrayList<>();
            for (EtlJobBean waitingJob : jobWaitingList) {
                String etlJob = waitingJob.getEtl_job();
                if (!Job_Status.WAITING.getCode().equals(waitingJob.getJob_disp_status())) {
                    removeList.add(waitingJob);
                }
                if (checkJobResource(etlJob)) {
                    decreaseResource(etlJob);
                    waitingJob.setJob_disp_status(Job_Status.RUNNING.getCode());
                    waitingJob.setJobStartTime(System.currentTimeMillis());
                    TaskSqlHelper.updateEtlJobDispStatus(waitingJob.getJob_disp_status(), etlSysId, etlJob, waitingJob.getCurr_bath_date());
                    String runningJob = etlJob + REDISCONTENTSEPARATOR + waitingJob.getCurr_bath_date();
                    REDIS.rpush(strRunningJob, runningJob);
                    removeList.add(waitingJob);
                } else {
                    EtlJobDefBean etlJobDef = jobDefineMap.get(etlJob);
                    if (etlJobDef != null && etlJobDef.getJob_priority() + DEFAULT_PRIORITY > waitingJob.getJob_priority_curr()) {
                        waitingJob.setJob_priority_curr(waitingJob.getJob_priority_curr() + MINPRIORITY);
                        TaskSqlHelper.updateEtlJobCurrPriority(waitingJob.getJob_priority_curr(), etlSysId, waitingJob.getEtl_job(), waitingJob.getCurr_bath_date());
                    }
                }
            }
            jobWaitingList.removeIf(removeList::contains);
            isLock = false;
            if (jobWaitingList.isEmpty()) {
                boolean isDoneError = checkAllJobFinishedORError(bathDateStr);
                long currDate = System.currentTimeMillis();
                long diff = currDate - etlhandInerval;
                if ((etlhandNumber < start_number && diff >= start_interval) || etlhandNumber == 0) {
                    log.info("检查是不是需要在发生错误时进行干预，{},总干预次数{},间隔时间{},已经干预次数{}", isDoneError, start_number, start_interval, etlhandNumber);
                    if (isDoneError) {
                        insertErrorJob2Handle(bathDateStr);
                        etlhandNumber = etlhandNumber + 1;
                        etlhandInerval = System.currentTimeMillis();
                    }
                }
                if (isDoneError) {
                    TaskSqlHelper.updateSystemInfo(etlSysId, Job_Status.ERROR.getCode());
                }
                if (checkAllJobFinished(bathDateStr)) {
                    EtlSysHis etlSysHis = TaskSqlHelper.getAllJobTimesCertainDate(etlSysId, bathDateStr);
                    if (etlSysHis != null) {
                        etlSysHis.setSys_run_status(Job_Status.DONE.getCode());
                        etlSysHis.setCurr_bath_date(bathDateStr);
                        TaskSqlHelper.archiveToHis(etlSysId, etlSysHis);
                    }
                    removeExecuteJobs(bathDateStr);
                    if (!isAutoShift && !frequencyFlag) {
                        TaskSqlHelper.updateEtlSysRunStatus(etlSysId, Job_Status.STOP.getCode());
                        log.info("不需要做自动日切，退出！");
                        stopCheckWaitFileThread();
                        sysDateShiftFlag = false;
                        return false;
                    } else if (isAutoShift) {
                        if (DateUtil.dateMargin(bathDateStr, etlSys.getSys_end_date()) > 1) {
                            TaskSqlHelper.deleteEtlJobByJobStatus(etlSysId, bathDateStr, Job_Status.DONE.getCode());
                        }
                        bathDateStr = TaskJobHelper.getNextBathDate(bathDateStr);
                        if (DateUtil.dateMargin(bathDateStr, etlSys.getSys_end_date()) <= 0) {
                            TaskSqlHelper.updateEtlSysRunStatus(etlSysId, Job_Status.STOP.getCode());
                            log.info("==不日切，因为下个跑批日期已经或等于结束日期了==！");
                            stopCheckWaitFileThread();
                            sysDateShiftFlag = false;
                            return false;
                        }
                        log.info("所有要执行的任务都已结束，下一批量日期 {}", bathDateStr);
                        sysDateShiftFlag = true;
                        return true;
                    }
                }
            }
            try {
                Thread.sleep(SLEEPMILLIS);
                log.info("当前时间为 {}，还有任务未执行完", LocalDateTime.now().format(DateUtil.DATETIME_ZHCN));
            } catch (InterruptedException e) {
                log.warn("系统出现异常：{}，但是继续执行", e.getMessage());
            }
        }
    }

    private void loadJobDefine(List<EtlJobDefBean> jobs) {
        for (EtlJobDefBean job : jobs) {
            String etlJobId = job.getEtl_job();
            String etlDispType = job.getDisp_type();
            if (Dispatch_Frequency.PinLv.getCode().equals(job.getDisp_freq())) {
                if (checkEtlDefJob(job)) {
                    jobFrequencyMap.put(etlJobId, job);
                }
            }
            if (Dispatch_Type.TPLUS0.getCode().equals(etlDispType) || Dispatch_Type.TPLUS1.getCode().equals(etlDispType)) {
                jobTimeDependencyMap.put(etlJobId, job.getDisp_time());
            } else if (Dispatch_Type.DEPENDENCE.getCode().equals(etlDispType)) {
                if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(job.getDisp_freq())) {
                    continue;
                }
            }
            List<EtlJobResourceRela> jobNeedResources = TaskSqlHelper.getJobNeedResources(etlSysId, job.getEtl_job_id());
            job.setJobResources(jobNeedResources);
            jobDefineMap.put(etlJobId, job);
        }
    }

    private void loadJobDependency() {
        List<Map<String, Object>> jobDependencyBySysCode = TaskSqlHelper.getJobDependencyBySysCode(etlSysId);
        for (Map<String, Object> etlDependencies : jobDependencyBySysCode) {
            String etlJob = String.valueOf(etlDependencies.get("etl_job"));
            if (!jobDefineMap.containsKey(etlJob)) {
                continue;
            }
            String preEtlJob = String.valueOf(etlDependencies.get("pre_etl_job"));
            if (StringUtil.isEmpty(preEtlJob)) {
                continue;
            }
            preEtlJob = preEtlJob.trim();
            jobDependencyMap.computeIfAbsent(etlJob, k -> new ArrayList<>()).add(preEtlJob);
        }
    }

    private void loadExecuteJob(List<EtlJobDefBean> jobs) {
        if (sysRunning) {
            TaskSqlHelper.updateEtlSysBathDate(etlSysId, bathDateStr, endDateStr);
            TaskSqlHelper.deleteEtlJobByBathDate(etlSysId, bathDateStr);
            loadCanDoJobWithNoResume(jobs);
            TaskSqlHelper.updateEtjJobWithDispStatus(Job_Status.PENDING.getCode(), etlSysId, bathDateStr);
            checkJobDependency(bathDateStr);
            initWaitingJob(bathDateStr);
        } else {
            if (isResumeRun) {
                TaskSqlHelper.updateEtlSysRunStatus(etlSysId, Job_Status.RUNNING.getCode());
                TaskSqlHelper.updateEtjJobByResumeRun(etlSysId, bathDateStr);
                loadExecuteJobWithRunning(etlSysId, bathDateStr);
                TaskSqlHelper.updateEtlResourceUsed(etlSysId, 0);
                TaskSqlHelper.delEtlJobChildPId(etlSysId);
            } else {
                TaskSqlHelper.updateEtlSysRunStatusAndBathDate(etlSysId, bathDateStr, endDateStr, Job_Status.RUNNING.getCode());
                TaskSqlHelper.deleteEtlJobBySysCode(etlSysId);
                loadCanDoJobWithNoResume(jobs);
                TaskSqlHelper.updateEtlResourceUsed(etlSysId, 0);
                TaskSqlHelper.delEtlJobChildPId(etlSysId);
                TaskSqlHelper.updateEtjJobWithDispStatus(Job_Status.PENDING.getCode(), etlSysId, bathDateStr);
            }
            checkJobDependency("");
            initWaitingJob("");
            sysRunning = true;
        }
    }

    private void loadCanDoJobWithNoResume(List<EtlJobDefBean> jobs) {
        Map<String, EtlJobBean> executeJobMap = new HashMap<>();
        List<Object[]> paramsList = new ArrayList<>();
        for (EtlJobDefBean job : jobs) {
            String curr_st_time = job.getCurr_st_time();
            if (StringUtil.isEmpty(curr_st_time)) {
                job.setCurr_st_time(JOB_DEFAULTSTART_DATETIME_STR);
            }
            EtlJobBean executeJob = new EtlJobBean();
            executeJob.setEtl_job_id(job.getEtl_job_id());
            executeJob.setEtl_sys_id(job.getEtl_sys_id());
            executeJob.setSub_sys_id(job.getSub_sys_id());
            executeJob.setEtl_job(job.getEtl_job());
            executeJob.setJob_disp_status(Job_Status.PENDING.getCode());
            executeJob.setCurr_bath_date(bathDateStr);
            executeJob.setJob_priority_curr(job.getJob_priority());
            executeJob.setPro_type(job.getPro_type());
            executeJob.setExe_num(job.getExe_num());
            executeJob.setCom_exe_num(job.getCom_exe_num());
            executeJob.setEnd_time(job.getEnd_time());
            String strDispFreq = job.getDisp_freq();
            log.info("=================job:{}", JsonUtil.toJson(job));
            if (checkDispFrequency(strDispFreq, job.getDisp_offset(), bathDateStr, job.getExe_num(), job.getCom_exe_num(), job.getStar_time(), job.getEnd_time())) {
                job.setCurr_bath_date(bathDateStr);
                job.setMain_serv_sync(Main_Server_Sync.NO.getCode());
                String currBathDate = job.getCurr_bath_date();
                job.setPro_dic(TaskJobHelper.transformDirOrName(currBathDate, job.getEtl_sys_id(), job.getPro_dic()));
                job.setLog_dic(TaskJobHelper.transformDirOrName(currBathDate, job.getEtl_sys_id(), job.getLog_dic()));
                job.setPro_name(TaskJobHelper.transformDirOrName(currBathDate, job.getEtl_sys_id(), job.getPro_name()));
                job.setPro_para(TaskJobHelper.transformProgramPara(currBathDate, job.getEtl_sys_id(), job.getPro_para()));
                job.setToday_disp(Today_Dispatch_Flag.YES.getCode());
                if (!(frequencyFlag && Dispatch_Frequency.PinLv.getCode().equals(strDispFreq))) {
                    EtlJobCur etlJob = new EtlJobCur();
                    BeanUtil.copyProperties(job, etlJob);
                    Object[] objects = TaskSqlHelper.insertIntoJobTable(etlJob);
                    paramsList.add(objects);
                }
                executeJob.setStrNextDate(TaskJobHelper.getNextExecuteDate(bathDateStr, strDispFreq));
                executeJobMap.put(executeJob.getEtl_job(), executeJob);
            } else {
                job.setToday_disp(Today_Dispatch_Flag.NO.getCode());
            }
        }
        if (paramsList.size() > 0) {
            TaskSqlHelper.insertIntoJobTable(paramsList);
        }
        jobExecuteMap.put(bathDateStr, executeJobMap);
    }

    private void loadExecuteJobWithRunning(long etlSysId, String strBathDate) {
        List<EtlJobCur> currentJobs = TaskSqlHelper.getEtlJobs(etlSysId, strBathDate);
        for (EtlJobCur job : currentJobs) {
            EtlJobBean executeJob = new EtlJobBean();
            executeJob.setEtl_job_id(job.getEtl_job_id());
            executeJob.setEtl_sys_id(job.getEtl_sys_id());
            executeJob.setSub_sys_id(job.getSub_sys_id());
            executeJob.setEtl_job(job.getEtl_job());
            executeJob.setJob_disp_status(job.getJob_disp_status());
            executeJob.setCurr_bath_date(job.getCurr_bath_date());
            executeJob.setJob_priority_curr(job.getJob_priority_curr());
            executeJob.setPro_type(job.getPro_type());
            executeJob.setExe_num(job.getExe_num());
            executeJob.setCom_exe_num(job.getCom_exe_num());
            executeJob.setEnd_time(job.getEnd_time());
            executeJob.setStrNextDate(TaskJobHelper.getNextExecuteDate(strBathDate, job.getDisp_freq()));
            jobExecuteMap.computeIfAbsent(job.getCurr_bath_date(), k -> new HashMap<>()).put(executeJob.getEtl_job(), executeJob);
        }
    }

    private void checkJobDependency(String strCurrBathDate) {
        for (String strBathDate : jobExecuteMap.keySet()) {
            if (!strCurrBathDate.isEmpty() && !strCurrBathDate.equals(strBathDate)) {
                continue;
            }
            LocalDate currBathDate = LocalDate.parse(strBathDate, DateUtil.DATE_DEFAULT);
            Map<String, EtlJobBean> jobMap = jobExecuteMap.get(strBathDate);
            for (String strJobName : jobMap.keySet()) {
                EtlJobDef jobDefine = jobDefineMap.get(strJobName);
                if (null == jobDefine) {
                    throw new AppSystemException("该作业无法在作业定义中取得：" + strJobName);
                }
                EtlJobBean job = jobMap.get(strJobName);
                String strPreDate = TaskJobHelper.getPreExecuteDate(strBathDate, jobDefine.getDisp_freq());
                job.setPreDateFlag(checkJobFinished(strPreDate, strJobName));
                String dispType = jobDefine.getDisp_type();
                if (Dispatch_Type.DEPENDENCE.getCode().equals(dispType)) {
                    List<String> dependencyJobList = jobDependencyMap.get(strJobName);
                    if (dependencyJobList == null) {
                        job.setDependencyFlag(true);
                    } else {
                        int finishedDepJobCount = 0;
                        for (String s : dependencyJobList) {
                            if (checkJobFinished(strBathDate, s)) {
                                ++finishedDepJobCount;
                            }
                        }
                        job.setDoneDependencyJobCount(finishedDepJobCount);
                        job.setDependencyFlag(finishedDepJobCount == dependencyJobList.size());
                    }
                    job.setExecuteTime(0L);
                } else if (Dispatch_Type.TPLUS1.getCode().equals(dispType)) {
                    String strDispTime = jobTimeDependencyMap.get(strJobName);
                    if (StringUtil.isEmpty(strDispTime)) {
                        job.setExecuteTime(currBathDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
                    } else {
                        job.setExecuteTime(TaskJobHelper.getExecuteTimeByTPlus1(strBathDate + " " + strDispTime).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                    }
                    job.setDependencyFlag(false);
                } else if (Dispatch_Type.TPLUS0.getCode().equals(dispType)) {
                    String strDispTime = jobTimeDependencyMap.get(strJobName);
                    if (StringUtil.isBlank(strDispTime)) {
                        job.setExecuteTime(currBathDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
                    } else {
                        job.setExecuteTime(LocalDateTime.parse(strBathDate + " " + strDispTime, TaskManager.DATETIME_DEFAULT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                    }
                    job.setDependencyFlag(false);
                } else if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(jobDefine.getDisp_freq())) {
                    job.setExecuteTime(LocalDateTime.parse(jobDefine.getStar_time(), DateUtil.DATETIME_DEFAULT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                    job.setDependencyFlag(false);
                } else {
                    throw new AppSystemException("目前仅支持的调度类型：" + Dispatch_Type.DEPENDENCE.getValue() + " " + Dispatch_Type.TPLUS1.getValue() + " " + Dispatch_Type.TPLUS0.getValue());
                }
                log.info("{}'s executeTime={}", strJobName, job.getExecuteTime());
            }
        }
    }

    private void initWaitingJob(String strCurrBathDate) {
        for (String strBathDate : jobExecuteMap.keySet()) {
            if (!strCurrBathDate.isEmpty() && !strCurrBathDate.equals(strBathDate)) {
                continue;
            }
            Map<String, EtlJobBean> jobMap = jobExecuteMap.get(strBathDate);
            for (String strJobName : jobMap.keySet()) {
                EtlJobBean exeJob = jobMap.get(strJobName);
                String etlJob = exeJob.getEtl_job();
                String currBathDate = exeJob.getCurr_bath_date();
                EtlJobDef jobDefine = jobDefineMap.get(strJobName);
                if (null == jobDefine) {
                    continue;
                }
                if (Job_Status.PENDING.getCode().equals(exeJob.getJob_disp_status())) {
                    if (!exeJob.isPreDateFlag()) {
                        continue;
                    }
                    String dispType = jobDefine.getDisp_type();
                    if (Dispatch_Type.DEPENDENCE.getCode().equals(dispType)) {
                        if (!exeJob.isDependencyFlag()) {
                            continue;
                        }
                    } else if (Dispatch_Type.TPLUS1.getCode().equals(dispType) || Dispatch_Type.TPLUS0.getCode().equals(dispType)) {
                        if (LocalDateTime.now().isBefore(LocalDateTime.ofInstant(Instant.ofEpochMilli(exeJob.getExecuteTime()), ZoneId.systemDefault()))) {
                            continue;
                        }
                    } else if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(jobDefine.getDisp_freq())) {
                        if (!checkEtlJob(exeJob)) {
                            continue;
                        }
                    } else {
                        throw new AppSystemException("目前仅支持的调度类型：" + Dispatch_Type.DEPENDENCE.getValue() + " " + Dispatch_Type.TPLUS1.getValue() + " " + Dispatch_Type.TPLUS0.getValue());
                    }
                    if (Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
                        exeJob.setJob_disp_status(Job_Status.RUNNING.getCode());
                        handleVirtualJob(etlJob, currBathDate);
                        continue;
                    } else {
                        exeJob.setJob_disp_status(Job_Status.WAITING.getCode());
                        TaskSqlHelper.updateEtlJobDispStatus(Job_Status.WAITING.getCode(), etlSysId, etlJob, currBathDate);
                    }
                } else if (!Job_Status.WAITING.getCode().equals(exeJob.getJob_disp_status())) {
                    continue;
                }
                if (Pro_Type.WF.getCode().equals(exeJob.getPro_type())) {
                    addWaitFileJobToList(exeJob);
                } else {
                    jobWaitingList.add(exeJob);
                }
            }
        }
    }

    private void addWaitFileJobToList(EtlJobBean exeJob) {
        EtlJobCur etlJob = TaskSqlHelper.getEtlJob(etlSysId, exeJob.getEtl_job_id());
        String strEtlJob = exeJob.getEtl_job();
        String currBathDate = exeJob.getCurr_bath_date();
        TaskSqlHelper.updateEtlJobDispStatus(Job_Status.RUNNING.getCode(), etlSysId, strEtlJob, currBathDate);
        etlJob.setCurr_st_time(DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT));
        TaskSqlHelper.updateEtlJobRunTime(etlJob.getCurr_st_time(), etlSysId, strEtlJob);
        WaitFileJobInfo waitFileJob = new WaitFileJobInfo();
        waitFileJob.setStrJobName(strEtlJob);
        waitFileJob.setStrBathDate(currBathDate);
        String waitFilePath = exeJob.getPro_dic() + exeJob.getPro_name();
        waitFileJob.setWaitFilePath(waitFilePath);
        waitFileJobList.add(waitFileJob);
        log.info("WaitFilePath=[{}]", waitFilePath);
    }

    private boolean checkEtlDefJob(EtlJobDefBean job) {
        int exeNum = job.getExe_num();
        int com_exe_num = job.getCom_exe_num();
        if (com_exe_num >= exeNum) {
            return false;
        }
        String endTime = job.getEnd_time();
        LocalDateTime endDateTime = LocalDateTime.parse(endTime, DateUtil.DATETIME_DEFAULT);
        return LocalDateTime.now().isBefore(endDateTime);
    }

    private boolean checkEtlJob(EtlJobBean exeJob) {
        EtlJobCur etlJob = TaskSqlHelper.getEtlJob(exeJob.getEtl_sys_id(), exeJob.getEtl_job_id());
        EtlJobDefBean job = new EtlJobDefBean();
        BeanUtil.copyProperties(etlJob, job);
        if (!checkEtlDefJob(job)) {
            return false;
        }
        LocalDateTime currDateTime = LocalDateTime.now();
        LocalDateTime dateTime = LocalDateTime.parse(job.getLast_exe_time(), DateUtil.DATETIME_DEFAULT);
        long last_exe_time = dateTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
        long exe_frequency_time = last_exe_time + (job.getExe_frequency() * 60 * 1000);
        LocalDateTime nextExeDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(exe_frequency_time), ZoneId.systemDefault());
        return currDateTime.isAfter(nextExeDateTime);
    }

    private boolean checkDispFrequency(String frequancy, int nDispOffset, String currDateStr, int exe_num, int com_exe_num, String star_time, String end_time) {
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdt = LocalDate.parse(currDateStr, DateUtil.DATE_DEFAULT).atStartOfDay(zoneId);
        Calendar cal = Calendar.getInstance();
        cal.setTime(Date.from(zdt.toInstant()));
        if (frequancy.endsWith(Dispatch_Frequency.DAILY.getCode())) {
            return true;
        } else if (frequancy.equals(Dispatch_Frequency.MONTHLY.getCode())) {
            int x = cal.get(Calendar.DAY_OF_MONTH);
            if (nDispOffset < 0) {
                cal.add(Calendar.MONTH, 1);
            }
            cal.set(Calendar.DAY_OF_MONTH, nDispOffset + 1);
            int y = cal.get(Calendar.DAY_OF_MONTH);
            return x == y;
        } else if (frequancy.equals(Dispatch_Frequency.WEEKLY.getCode())) {
            int x = cal.get(Calendar.DAY_OF_WEEK);
            if (nDispOffset <= 0) {
                cal.add(Calendar.WEEK_OF_MONTH, 0);
            }
            cal.set(Calendar.DAY_OF_WEEK, nDispOffset + 1);
            int y = cal.get(Calendar.DAY_OF_WEEK);
            return x == y;
        } else if (frequancy.equals(Dispatch_Frequency.YEARLY.getCode())) {
            int x = cal.get(Calendar.DAY_OF_YEAR);
            if (nDispOffset < 0) {
                cal.add(Calendar.YEAR, 1);
            }
            cal.set(Calendar.DAY_OF_YEAR, nDispOffset + 1);
            int y = cal.get(Calendar.DAY_OF_YEAR);
            return x == y;
        } else if (frequancy.equals(Dispatch_Frequency.PinLv.getCode())) {
            if (com_exe_num < exe_num) {
                LocalDateTime startDateTime = LocalDateTime.parse(star_time, DateUtil.DATETIME_DEFAULT);
                LocalDateTime endDateTime = LocalDateTime.parse(end_time, DateUtil.DATETIME_DEFAULT);
                long currMilli = System.currentTimeMillis();
                long startCurrMilli = startDateTime.atZone(zoneId).toInstant().toEpochMilli() - currMilli;
                long endCurrMilli = endDateTime.atZone(zoneId).toInstant().toEpochMilli() - currMilli;
                return startCurrMilli <= 0 && endCurrMilli >= 0;
            }
        }
        return false;
    }

    private boolean checkJobFinished(String currBathDate, String jobName) {
        if (!jobExecuteMap.containsKey(currBathDate)) {
            return true;
        }
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(currBathDate);
        if (!jobMap.containsKey(jobName)) {
            return false;
        }
        return Job_Status.DONE.getCode().equals(jobMap.get(jobName).getJob_disp_status());
    }

    private void waitFileJobFinished(WaitFileJobInfo waitJobInfo) {
        EtlJobCur etlJobCur = TaskSqlHelper.getEtlJobByCd(etlSysId, waitJobInfo.getStrJobName(), waitJobInfo.getStrBathDate());
        etlJobCur.setJob_disp_status(Job_Status.DONE.getCode());
        etlJobCur.setJob_return_val(0);
        etlJobCur.setCurr_end_time(String.valueOf(System.currentTimeMillis()));
        EtlJobDispHis etlJobDispHis = etlJobCur2EtlJobDispHis(etlJobCur);
        TaskSqlHelper.insertIntoEtlJobDispHis(etlJobDispHis);
        TaskSqlHelper.updateEtlJobFinished(etlJobCur);
        updateFinishedJob(waitJobInfo.getStrJobName(), waitJobInfo.getStrBathDate());
        log.info("{} 作业正常结束", etlJobCur.getEtl_job());
    }

    private EtlJobDispHis etlJobCur2EtlJobDispHis(EtlJobCur etlJobCur) {
        EtlJobDispHis etlJobDispHis = new EtlJobDispHis();
        etlJobDispHis.setEtl_sys_id(etlJobCur.getEtl_sys_id());
        etlJobDispHis.setEtl_job(etlJobCur.getEtl_job());
        etlJobDispHis.setCom_exe_num(etlJobCur.getCom_exe_num());
        etlJobDispHis.setDisp_offset(etlJobCur.getDisp_offset());
        etlJobDispHis.setExe_frequency(etlJobCur.getExe_frequency());
        etlJobDispHis.setExe_num(etlJobCur.getExe_num());
        etlJobDispHis.setJob_priority(etlJobCur.getJob_priority());
        etlJobDispHis.setJob_priority_curr(etlJobCur.getJob_priority_curr());
        etlJobDispHis.setJob_return_val(etlJobCur.getJob_return_val());
        etlJobDispHis.setOverlength_val(etlJobCur.getOverlength_val());
        etlJobDispHis.setComments(etlJobCur.getComments());
        etlJobDispHis.setCurr_bath_date(etlJobCur.getCurr_bath_date());
        etlJobDispHis.setCurr_end_time(etlJobCur.getCurr_end_time());
        etlJobDispHis.setCurr_st_time(etlJobCur.getCurr_st_time());
        etlJobDispHis.setOvertime_val(etlJobCur.getOvertime_val());
        etlJobDispHis.setDisp_freq(etlJobCur.getDisp_freq());
        etlJobDispHis.setDisp_time(etlJobCur.getDisp_time());
        etlJobDispHis.setDisp_type(etlJobCur.getDisp_type());
        etlJobDispHis.setEnd_time(etlJobCur.getEnd_time());
        etlJobDispHis.setEtl_job_desc(etlJobCur.getEtl_job_desc());
        etlJobDispHis.setJob_disp_status(etlJobCur.getJob_disp_status());
        etlJobDispHis.setJob_eff_flag(etlJobCur.getJob_eff_flag());
        etlJobDispHis.setJob_process_id(etlJobCur.getJob_process_id());
        etlJobDispHis.setLast_exe_time(etlJobCur.getLast_exe_time());
        etlJobDispHis.setLog_dic(etlJobCur.getLog_dic());
        etlJobDispHis.setMain_serv_sync(etlJobCur.getMain_serv_sync());
        etlJobDispHis.setPro_dic(etlJobCur.getPro_dic());
        etlJobDispHis.setPro_name(etlJobCur.getPro_name());
        etlJobDispHis.setPro_para(etlJobCur.getPro_para());
        etlJobDispHis.setPro_type(etlJobCur.getPro_type());
        etlJobDispHis.setStar_time(etlJobCur.getStar_time());
        etlJobDispHis.setSub_sys_id(etlJobCur.getSub_sys_id());
        etlJobDispHis.setToday_disp(etlJobCur.getToday_disp());
        etlJobDispHis.setSuccess_job(etlJobCur.getSuccess_job());
        etlJobDispHis.setFail_job(etlJobCur.getFail_job());
        etlJobDispHis.setJob_datasource(etlJobCur.getJob_datasource());
        return etlJobDispHis;
    }

    private void checkReadyJob() {
        for (String strBathDate : jobExecuteMap.keySet()) {
            Map<String, EtlJobBean> jobMap = jobExecuteMap.get(strBathDate);
            for (String strJobName : jobMap.keySet()) {
                EtlJobBean job = jobMap.get(strJobName);
                if (Pro_Type.WF.getCode().equals(job.getPro_type())) {
                    continue;
                }
                if (Job_Status.RUNNING.getCode().equals(job.getJob_disp_status())) {
                    String etlJob = job.getEtl_job();
                    String currBathDate = job.getCurr_bath_date();
                    EtlJobCur jobInfo = TaskSqlHelper.getEtlJob(etlSysId, job.getEtl_job_id(), job.getCurr_bath_date());
                    String jobStatus = jobInfo.getJob_disp_status();
                    if (Job_Status.DONE.getCode().equals(jobStatus) || Job_Status.ERROR.getCode().equals(jobStatus)) {
                        log.warn("{} 检测到执行完成", etlJob);
                        String finishedJob = etlJob + REDISCONTENTSEPARATOR + currBathDate;
                        REDIS.rpush(strFinishedJob, finishedJob);
                        continue;
                    }
                    LocalDateTime currStTime = LocalDateTime.parse(jobInfo.getCurr_st_time(), DateUtil.DATETIME_DEFAULT);
                    if (currStTime.equals(JOB_DEFAULTSTART_DATETIME)) {
                        if (System.currentTimeMillis() - job.getJobStartTime() > 600000) {
                            log.warn("{} 被再次执行", etlJob);
                            String runningJob = etlJob + REDISCONTENTSEPARATOR + currBathDate;
                            REDIS.rpush(strRunningJob, runningJob);
                        }
                    }
                }
            }
        }
    }

    private void checkFinishedJob() {
        long finishedListSize = REDIS.llen(strFinishedJob);
        for (int i = 0; i < finishedListSize; ++i) {
            String finishJobString = REDIS.lpop(strFinishedJob);
            String[] jobKey = finishJobString.split(REDISCONTENTSEPARATOR);
            if (jobKey.length != 2) {
                continue;
            }
            updateFinishedJob(jobKey[0], jobKey[1]);
        }
    }

    private void updateFinishedJob(String jobName, String currBathDate) {
        if (!jobExecuteMap.containsKey(currBathDate)) {
            throw new AppSystemException("无法在数据库中找到作业" + jobName);
        }
        EtlJobBean exeJobInfo = jobExecuteMap.get(currBathDate).get(jobName);
        if (null == exeJobInfo) {
            throw new AppSystemException("无法在内存表jobExecuteMap中找到作业" + jobName);
        }
        if (Job_Status.DONE.getCode().equals(exeJobInfo.getJob_disp_status())) {
            return;
        }
        EtlJobDef finishedJobDefine = jobDefineMap.get(jobName);
        if (!Pro_Type.WF.getCode().equals(exeJobInfo.getPro_type()) && Job_Status.RUNNING.getCode().equals(exeJobInfo.getJob_disp_status()) && !Job_Effective_Flag.VIRTUAL.getCode().equals(finishedJobDefine.getJob_eff_flag())) {
            increaseResource(jobName);
        }
        EtlJobCur jobInfo = TaskSqlHelper.getEtlJobByCd(etlSysId, jobName, currBathDate);
        String jobStatus = jobInfo.getJob_disp_status();
        exeJobInfo.setJob_disp_status(jobStatus);
        log.info("{} 作业结束，跑批日期为 {} 作业状态为 {}", jobName, currBathDate, jobStatus);
        if (Job_Status.DONE.getCode().equals(jobStatus)) {
            for (String strJobName : jobDependencyMap.keySet()) {
                List<String> depJobList = jobDependencyMap.get(strJobName);
                if (depJobList.contains(exeJobInfo.getEtl_job())) {
                    EtlJobBean nextJobInfo = jobExecuteMap.get(currBathDate).get(strJobName);
                    if (null == nextJobInfo) {
                        continue;
                    }
                    nextJobInfo.setDoneDependencyJobCount(nextJobInfo.getDoneDependencyJobCount() + 1);
                    String nextEtlJob = nextJobInfo.getEtl_job();
                    log.info("{}总依赖数{},目前达成{}", nextEtlJob, depJobList.size(), nextJobInfo.getDoneDependencyJobCount());
                    if (nextJobInfo.getDoneDependencyJobCount() != depJobList.size()) {
                        continue;
                    }
                    nextJobInfo.setDependencyFlag(true);
                    String nextEtlJobCurrBathDate = nextJobInfo.getCurr_bath_date();
                    if (nextJobInfo.isPreDateFlag()) {
                        EtlJobDefBean jobDefine = jobDefineMap.get(nextEtlJob);
                        if (jobDefine == null) {
                            continue;
                        }
                        if (Job_Status.PENDING.getCode().equals(nextJobInfo.getJob_disp_status())) {
                            if (Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
                                nextJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
                                handleVirtualJob(nextEtlJob, nextEtlJobCurrBathDate);
                            } else {
                                nextJobInfo.setJob_disp_status(Job_Status.WAITING.getCode());
                                TaskSqlHelper.updateEtlJobDispStatus(nextJobInfo.getJob_disp_status(), etlSysId, nextEtlJob, nextEtlJobCurrBathDate);
                                if (Pro_Type.WF.getCode().equals(nextJobInfo.getPro_type())) {
                                    addWaitFileJobToList(nextJobInfo);
                                } else {
                                    jobWaitingList.add(nextJobInfo);
                                }
                            }
                        }
                    }
                }
            }
            String strNextDate = exeJobInfo.getStrNextDate();
            if (jobExecuteMap.containsKey(strNextDate)) {
                EtlJobBean nextJobInfo = jobExecuteMap.get(strNextDate).get(exeJobInfo.getEtl_job());
                if (null != nextJobInfo) {
                    nextJobInfo.setPreDateFlag(true);
                    String nextEtlJob = nextJobInfo.getEtl_job();
                    String nextEtlJobCurrBathDate = nextJobInfo.getCurr_bath_date();
                    if (nextJobInfo.isDependencyFlag()) {
                        EtlJobDefBean jobDefine = jobDefineMap.get(nextEtlJob);
                        if (jobDefine == null) {
                            throw new AppSystemException("无法在内存表jobDefineMap中找到作业" + jobName);
                        }
                        String nextEtlJobStatus = nextJobInfo.getJob_disp_status();
                        if (Job_Status.PENDING.getCode().equals(nextEtlJobStatus)) {
                            if (Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
                                nextJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
                                handleVirtualJob(nextEtlJob, nextEtlJobCurrBathDate);
                            } else {
                                nextJobInfo.setJob_disp_status(Job_Status.WAITING.getCode());
                                TaskSqlHelper.updateEtlJobDispStatus(Job_Status.WAITING.getCode(), etlSysId, nextEtlJob, nextEtlJobCurrBathDate);
                                if (Pro_Type.WF.getCode().equals(nextJobInfo.getPro_type())) {
                                    addWaitFileJobToList(nextJobInfo);
                                } else {
                                    jobWaitingList.add(nextJobInfo);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            String message = String.format("调度系统：%s，跑批日期：%s，作业名：%s 调度失败", etlSysCd, currBathDate, jobName);
            if (isNeedSendSMS) {
                NOTIFY.sendMsg(message);
            }
            log.warn(message);
        }
    }

    private void increaseResource(String etlJobKey) {
        EtlJobDefBean jobDefine = jobDefineMap.get(etlJobKey);
        if (null == jobDefine) {
            throw new AppSystemException("无法在内存表jobDefineMap中找到作业" + etlJobKey);
        }
        List<EtlJobResourceRela> resources = jobDefine.getJobResources();
        for (EtlJobResourceRela resource : resources) {
            String resourceType = resource.getResource_type();
            int needCount = resource.getResource_req();
            log.info("Resource {} need {} {}", etlJobKey, resourceType, needCount);
            EtlResource etlResource = sysResourceMap.get(resourceType);
            log.info("Before increase, {} used {}", resourceType, etlResource.getResource_used());
            etlResource.setResource_used(etlResource.getResource_used() - needCount);
            log.info("After increase, {} used {}", resourceType, etlResource.getResource_used());
            TaskSqlHelper.updateEtlResourceUsedByResourceType(etlSysId, resourceType, etlResource.getResource_used());
        }
    }

    private void checkExecutedJob() {
        for (Map<String, EtlJobBean> jobMap : jobExecuteMap.values()) {
            for (EtlJobBean exeJob : jobMap.values()) {
                if (Job_Status.PENDING.getCode().equals(exeJob.getJob_disp_status())) {
                    if (!exeJob.isPreDateFlag()) {
                        continue;
                    }
                    if (exeJob.getExecuteTime() == zclong) {
                        if (!checkEtlJob(exeJob)) {
                            continue;
                        }
                    } else if (exeJob.getExecuteTime() != 0L) {
                        if (exeJob.getExecuteTime() > System.currentTimeMillis()) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                    log.info("{}'s executeTime={}, can run!", exeJob.getEtl_job(), exeJob.getExecuteTime());
                    exeJob.setJob_disp_status(Job_Status.WAITING.getCode());
                    TaskSqlHelper.updateEtlJobDispStatus(Job_Status.WAITING.getCode(), etlSysId, exeJob.getEtl_job(), exeJob.getCurr_bath_date());
                } else if (exeJob.getExecuteTime() == zclong) {
                    if (!checkEtlJob(exeJob)) {
                        continue;
                    }
                } else {
                    continue;
                }
                if (Pro_Type.WF.getCode().equals(exeJob.getPro_type())) {
                    addWaitFileJobToList(exeJob);
                } else {
                    jobWaitingList.add(exeJob);
                }
            }
        }
    }

    public void handleJob2Run(String currbathDate, String etlJob) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(currbathDate);
        if (null == jobMap) {
            log.warn("在进行直接调度干预时，根据{} {}无法找到作业信息", etlJob, currbathDate);
            return;
        }
        EtlJobBean exeJobInfo = jobMap.get(etlJob);
        if (null == exeJobInfo) {
            log.warn("在进行直接调度干预时，{} {}作业不存在调度列表中", etlJob, currbathDate);
            return;
        }
        EtlJobDef jobDefine = jobDefineMap.get(etlJob);
        if (null == jobDefine) {
            log.warn("在进行直接调度干预时，{} {}作业不存在定义列表中", etlJob, currbathDate);
            return;
        }
        if (Job_Status.PENDING.getCode().equals(exeJobInfo.getJob_disp_status()) || Job_Status.WAITING.getCode().equals(exeJobInfo.getJob_disp_status())) {
            etlJob = exeJobInfo.getEtl_job();
            String currBathDate = exeJobInfo.getCurr_bath_date();
            if (Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
                exeJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
                handleVirtualJob(etlJob, currBathDate);
            } else {
                exeJobInfo.setJob_disp_status(Job_Status.WAITING.getCode());
                TaskSqlHelper.updateEtlJobDispStatus(Job_Status.WAITING.getCode(), etlSysId, etlJob, currBathDate);
                if (Pro_Type.WF.getCode().equals(exeJobInfo.getPro_type())) {
                    addWaitFileJobToList(exeJobInfo);
                } else {
                    decreaseResource(etlJob);
                    exeJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
                    exeJobInfo.setJobStartTime(System.currentTimeMillis());
                    TaskSqlHelper.updateEtlJobDispStatus(Job_Status.RUNNING.getCode(), etlSysId, etlJob, currBathDate);
                    String runningJob = etlJob + REDISCONTENTSEPARATOR + currBathDate + REDISCONTENTSEPARATOR + REDISHANDLE;
                    REDIS.rpush(strRunningJob, runningJob);
                }
            }
        }
    }

    public void handleSys2Rerun() {
        for (String strBathDate : jobExecuteMap.keySet()) {
            Map<String, EtlJobBean> jobMap = jobExecuteMap.get(strBathDate);
            for (String strJobName : jobMap.keySet()) {
                EtlJobBean exeJobInfo = jobMap.get(strJobName);
                exeJobInfo.setPreDateFlag(false);
                exeJobInfo.setJob_disp_status(Job_Status.PENDING.getCode());
                exeJobInfo.setDependencyFlag(false);
                exeJobInfo.setDoneDependencyJobCount(0);
                EtlJobDefBean jobDefine = jobDefineMap.get(strJobName);
                if (jobDefine != null) {
                    exeJobInfo.setJob_priority_curr(jobDefine.getJob_priority());
                }
            }
        }
        checkJobDependency("");
        initWaitingJob("");
    }

    public void handleSys2Pause() {
        for (String strBathDate : jobExecuteMap.keySet()) {
            Map<String, EtlJobBean> jobMap = jobExecuteMap.get(strBathDate);
            for (String strJobName : jobMap.keySet()) {
                EtlJobBean exeJobInfo = jobMap.get(strJobName);
                if (Job_Status.PENDING.getCode().equals(exeJobInfo.getJob_disp_status()) || Job_Status.WAITING.getCode().equals(exeJobInfo.getJob_disp_status())) {
                    exeJobInfo.setJob_disp_status(Job_Status.STOP.getCode());
                }
                if (Pro_Type.WF.getCode().equals(exeJobInfo.getPro_type()) && Job_Status.RUNNING.getCode().equals(exeJobInfo.getJob_disp_status())) {
                    exeJobInfo.setJob_disp_status(Job_Status.STOP.getCode());
                }
            }
        }
        waitFileJobList.clear();
    }

    public void handleSys2Resume() {
        for (Map<String, EtlJobBean> jobMap : jobExecuteMap.values()) {
            for (String strJobName : jobMap.keySet()) {
                EtlJobBean exeJobInfo = jobMap.get(strJobName);
                if (Job_Status.STOP.getCode().equals(exeJobInfo.getJob_disp_status()) || Job_Status.ERROR.getCode().equals(exeJobInfo.getJob_disp_status())) {
                    exeJobInfo.setJob_disp_status(Job_Status.PENDING.getCode());
                    EtlJobDefBean jobDefine = jobDefineMap.get(strJobName);
                    if (jobDefine != null) {
                        exeJobInfo.setJob_priority_curr(jobDefine.getJob_priority());
                    }
                }
            }
        }
        initWaitingJob("");
    }

    public void handleJob2Stop(String currbathDate, String etlJob) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(currbathDate);
        if (null == jobMap) {
            return;
        }
        EtlJobBean exeJobInfo = jobMap.get(etlJob);
        if (null == exeJobInfo) {
            return;
        }
        if (Job_Status.PENDING.getCode().equals(exeJobInfo.getJob_disp_status()) || Job_Status.WAITING.getCode().equals(exeJobInfo.getJob_disp_status())) {
            exeJobInfo.setJob_disp_status(Job_Status.STOP.getCode());
            TaskSqlHelper.updateEtlJobDispStatus(Job_Status.STOP.getCode(), etlSysId, etlJob, currbathDate);
        }
    }

    public void handleJob2Rerun(String currbathDate, String etlJob) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(currbathDate);
        if (null == jobMap) {
            log.warn("在进行作业重跑干预时，根据{} {}无法找到作业信息", etlJob, currbathDate);
            return;
        }
        EtlJobBean exeJobInfo = jobMap.get(etlJob);
        if (null == exeJobInfo) {
            log.warn("在进行作业重跑干预时，{} {}作业不存在调度列表中", etlJob, currbathDate);
            return;
        }
        EtlJobDef jobDefine = jobDefineMap.get(etlJob);
        if (null == jobDefine) {
            log.warn("在进行作业重跑干预时，{} {}作业不存在定义列表中", etlJob, currbathDate);
            return;
        }
        if (Job_Status.STOP.getCode().equals(exeJobInfo.getJob_disp_status()) || Job_Status.ERROR.getCode().equals(exeJobInfo.getJob_disp_status()) || Job_Status.DONE.getCode().equals(exeJobInfo.getJob_disp_status())) {
            exeJobInfo.setJob_priority_curr(MAXPRIORITY);
            TaskSqlHelper.updateEtlJobCurrPriority(exeJobInfo.getJob_priority_curr(), etlSysId, etlJob, currbathDate);
            if (!exeJobInfo.isPreDateFlag()) {
                exeJobInfo.setJob_disp_status(Job_Status.PENDING.getCode());
                TaskSqlHelper.updateEtlJobDispStatus(exeJobInfo.getJob_disp_status(), etlSysId, etlJob, currbathDate);
                return;
            }
            if (0 != exeJobInfo.getExecuteTime()) {
                if (System.currentTimeMillis() < exeJobInfo.getExecuteTime()) {
                    exeJobInfo.setJob_disp_status(Job_Status.PENDING.getCode());
                    TaskSqlHelper.updateEtlJobDispStatus(exeJobInfo.getJob_disp_status(), etlSysId, etlJob, currbathDate);
                } else {
                    if (Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
                        exeJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
                        handleVirtualJob(etlJob, currbathDate);
                    } else {
                        exeJobInfo.setJob_disp_status(Job_Status.WAITING.getCode());
                        TaskSqlHelper.updateEtlJobDispStatus(exeJobInfo.getJob_disp_status(), etlSysId, etlJob, currbathDate);
                        if (Pro_Type.WF.getCode().equals(exeJobInfo.getPro_type())) {
                            addWaitFileJobToList(exeJobInfo);
                        } else {
                            jobWaitingList.add(exeJobInfo);
                        }
                    }
                }
            } else {
                if ((!exeJobInfo.isDependencyFlag())) {
                    exeJobInfo.setJob_disp_status(Job_Status.PENDING.getCode());
                    TaskSqlHelper.updateEtlJobDispStatus(exeJobInfo.getJob_disp_status(), etlSysId, etlJob, currbathDate);
                } else {
                    if (Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
                        exeJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
                        handleVirtualJob(etlJob, currbathDate);
                    } else {
                        exeJobInfo.setJob_disp_status(Job_Status.WAITING.getCode());
                        TaskSqlHelper.updateEtlJobDispStatus(exeJobInfo.getJob_disp_status(), etlSysId, etlJob, currbathDate);
                        if (Pro_Type.WF.getCode().equals(exeJobInfo.getPro_type())) {
                            addWaitFileJobToList(exeJobInfo);
                        } else {
                            jobWaitingList.add(exeJobInfo);
                        }
                    }
                }
            }
        }
    }

    public void handleJob2ChangePriority(String currBathDate, String etlJob, int priority) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(currBathDate);
        if (null == jobMap) {
            return;
        }
        EtlJobBean exeJobInfo = jobMap.get(etlJob);
        if (null == exeJobInfo) {
            return;
        }
        exeJobInfo.setJob_priority_curr(priority);
        TaskSqlHelper.updateEtlJobCurrPriority(priority, etlSysId, etlJob, currBathDate);
    }

    public void handleJob2Skip(String currbathDate, String etlJob) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(currbathDate);
        if (null == jobMap) {
            return;
        }
        EtlJobBean exeJobInfo = jobMap.get(etlJob);
        if (null == exeJobInfo) {
            return;
        }
        EtlJobDefBean jobDefine = jobDefineMap.get(etlJob);
        if (null == jobDefine) {
            return;
        }
        exeJobInfo.setJob_disp_status(Job_Status.RUNNING.getCode());
        if (!Pro_Type.WF.getCode().equals(exeJobInfo.getPro_type()) && !Job_Effective_Flag.VIRTUAL.getCode().equals(jobDefine.getJob_eff_flag())) {
            decreaseResource(etlJob);
        }
        handleVirtualJob(exeJobInfo.getEtl_job(), exeJobInfo.getCurr_bath_date());
    }

    public void handleSysDayShift() {
        isSysJobShift = true;
    }

    public boolean isSysPause() {
        return isSysPause;
    }

    public void closeSysPause() {
        isSysPause = false;
    }

    public void openSysPause() {
        isSysPause = true;
    }

    private void decreaseResource(String etlJob) {
        EtlJobDefBean jobDefine = jobDefineMap.get(etlJob);
        if (null == jobDefine) {
            throw new AppSystemException("无法在内存表jobDefineMap中找到作业" + etlJob);
        }
        List<EtlJobResourceRela> resources = jobDefine.getJobResources();
        for (EtlJobResourceRela resource : resources) {
            String resourceType = resource.getResource_type();
            int needCount = resource.getResource_req();
            log.info("{} need {} {}", etlJob, resourceType, needCount);
            EtlResource etlResource = sysResourceMap.get(resourceType);
            log.info("Before decrease, {} used {}", resourceType, etlResource.getResource_used());
            etlResource.setResource_used(etlResource.getResource_used() + needCount);
            log.info("After decrease, {} used {}", resourceType, etlResource.getResource_used());
            TaskSqlHelper.updateEtlResourceUsedByResourceType(etlSysId, resourceType, etlResource.getResource_used());
        }
    }

    private boolean checkJobResource(String etlJob) {
        EtlJobDefBean jobDefine = jobDefineMap.get(etlJob);
        log.info("检测资源：{}", etlJob);
        if (null == jobDefine) {
            return true;
        }
        List<EtlJobResourceRela> resources = jobDefine.getJobResources();
        for (EtlJobResourceRela resource : resources) {
            String resourceType = resource.getResource_type();
            int needCount = resource.getResource_req();
            log.info("{} need {} {}", etlJob, resourceType, needCount);
            EtlResource etlResource = sysResourceMap.get(resourceType);
            log.info("{} maxCount is {}", resourceType, etlResource.getResource_max());
            log.info("{} usedCount is {}", resourceType, etlResource.getResource_used());
            if (etlResource.getResource_max() < etlResource.getResource_used() + needCount) {
                log.info("{}'s resource is not enough", etlJob);
                return false;
            }
        }
        return true;
    }

    private boolean checkAllJobFinished(String bathDateStr) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(bathDateStr);
        if (jobMap == null) {
            return false;
        }
        for (EtlJobBean exeJobInfo : jobMap.values()) {
            if (exeJobInfo.getExecuteTime() == zclong) {
                continue;
            }
            if (!Job_Status.DONE.getCode().equals(exeJobInfo.getJob_disp_status())) {
                return false;
            }
        }
        return true;
    }

    private boolean checkAllJobFinishedORError(String bathDateStr) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(bathDateStr);
        if (null == jobMap) {
            return false;
        }
        Iterator<String> jobIter = jobMap.keySet().iterator();
        Set<String> status = new HashSet<>();
        while (jobIter.hasNext()) {
            EtlJobBean exeJobInfo = jobMap.get(jobIter.next());
            status.add(exeJobInfo.getJob_disp_status());
        }
        log.info("内存表中存在 {} 个作业，它们的调度状态种类有 {}", jobMap.size(), status);
        return !status.contains(Job_Status.RUNNING.getCode()) && status.contains(Job_Status.ERROR.getCode());
    }

    private void insertErrorJob2Handle(String bathDateStr) {
        String localDateTime = DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT);
        EtlJobHand etlJobHand = new EtlJobHand();
        etlJobHand.setEtl_sys_id(etlSysId);
        etlJobHand.setHand_status(Meddle_status.TRUE.getCode());
        etlJobHand.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        etlJobHand.setEtl_hand_type(Meddle_type.JOB_RERUN.getCode());
        etlJobHand.setSt_time(localDateTime);
        etlJobHand.setEnd_time(localDateTime);
        etlJobHand.setEvent_id(localDateTime);
        List<EtlJobCur> etlJobCurs = TaskSqlHelper.getEtlJobsByJobStatus(etlSysId, Job_Status.ERROR.getCode());
        for (EtlJobCur etlJobCur : etlJobCurs) {
            etlJobHand.setEtl_job_id(etlJobCur.getEtl_job_id());
            etlJobHand.setPro_para(etlSysCd + PARASEPARATOR + etlJobCur.getEtl_job() + PARASEPARATOR + bathDateStr);
            TaskSqlHelper.insertIntoEtlJobHand(etlJobHand);
            log.info("该作业发生了错误，需要重跑 {}", etlJobCur.getEtl_job());
        }
    }

    private void removeExecuteJobs(String bathDateStr) {
        Map<String, EtlJobBean> jobMap = jobExecuteMap.get(bathDateStr);
        if (null != jobMap && !jobMap.isEmpty()) {
            Iterator<String> jobIter = jobMap.keySet().iterator();
            while (jobIter.hasNext()) {
                String strJobName = jobIter.next();
                EtlJobDefBean jobDefine = jobDefineMap.get(strJobName);
                if (null == jobDefine) {
                    jobIter.remove();
                    continue;
                }
                jobIter.remove();
                if (Dispatch_Frequency.PinLv == Dispatch_Frequency.ofEnumByCode(jobDefine.getDisp_freq())) {
                    jobFrequencyMap.remove(jobDefine.getEtl_job());
                    frequencyFlag = false;
                }
            }
            if (jobMap.isEmpty()) {
                jobExecuteMap.remove(bathDateStr);
            }
        }
    }

    private void updateSysUsedResource() {
        List<EtlResource> resources = TaskSqlHelper.getEtlSystemResources(etlSysId, etlSysCd);
        for (EtlResource etlResource : resources) {
            String resourceType = etlResource.getResource_type();
            int resourceMax = etlResource.getResource_max();
            EtlResource etlResourceMap = sysResourceMap.get(resourceType);
            if (null != etlResourceMap && etlResourceMap.getResource_max() != resourceMax) {
                etlResourceMap.setResource_max(resourceMax);
                log.info("{}'s maxCount change to {}", resourceType, etlResourceMap.getResource_max());
            }
        }
    }

    public boolean needDailyShift() {
        return sysDateShiftFlag;
    }

    private void handleVirtualJob(String etlJob, String currBathDate) {
        String localDateTime = DateUtil.getDateTime(DateUtil.DATETIME_DEFAULT);
        TaskSqlHelper.updateVirtualJob(etlSysId, etlJob, currBathDate, localDateTime, localDateTime);
        String finishedJob = etlJob + REDISCONTENTSEPARATOR + currBathDate;
        REDIS.rpush(strFinishedJob, finishedJob);
    }

    private class CheckWaitFileThread extends Thread {

        private static final long CHECKMILLISECONDS = 60000;

        private volatile boolean run = true;

        public void run() {
            try {
                while (run) {
                    while (isLock) {
                        log.info("Wait file lock is true.Please wait");
                        try {
                            Thread.sleep(LOCKMILLISECONDS);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    isLock = true;
                    List<WaitFileJobInfo> jobList = new ArrayList<>(waitFileJobList);
                    List<WaitFileJobInfo> checkList = new ArrayList<>(jobList);
                    List<WaitFileJobInfo> finishedJobList = new ArrayList<>();
                    for (WaitFileJobInfo jobInfo : checkList) {
                        File file = new File(jobInfo.getWaitFilePath());
                        if (file.exists()) {
                            waitFileJobFinished(jobInfo);
                            finishedJobList.add(jobInfo);
                            log.info("{} 文件已经等到。", jobInfo.getStrJobName());
                        }
                    }
                    isLock = false;
                    jobList.removeIf(finishedJobList::contains);
                    try {
                        Thread.sleep(CHECKMILLISECONDS);
                    } catch (InterruptedException ignored) {
                    }
                }
            } catch (Exception ex) {
                log.error("CheckWaitFileThread exception happened! {}", ex.getMessage());
            }
            log.info("CheckWaitFileThread Stop!");
        }

        void stopThread() {
            log.info("CheckWaitFileThread stop!");
            run = false;
        }
    }
}
