package hyren.serv6.agent.job.biz.core;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.utils.CommunicationUtil;
import hyren.serv6.agent.job.biz.utils.EnumUtil;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.ExecuteState;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.CollectCase;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.base.exception.AppSystemException;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

@DocClass(desc = "", author = "WangZhengcheng")
public class JobStageController {

    private JobStageInterface head;

    private JobStageInterface last;

    @Method(desc = "", logicStep = "")
    @Param(name = "stages", desc = "", range = "")
    public void registerJobStage(JobStageInterface... stages) {
        for (JobStageInterface stage : stages) {
            registerJobStage(stage);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "stage", desc = "", range = "")
    private void registerJobStage(JobStageInterface stage) {
        if (head == null) {
            last = head = stage;
        } else {
            last.setNextStage(stage);
            last = stage;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "statusFilePath", desc = "", range = "")
    @Return(desc = "", range = "")
    public JobStatusInfo handleStageByOrder(String statusFilePath, JobStatusInfo jobStatusInfo) throws Exception {
        if (StringUtil.isBlank(statusFilePath)) {
            throw new AppSystemException("状态文件路径不能为空");
        }
        if (jobStatusInfo == null) {
            throw new AppSystemException("作业状态对象不能为空");
        }
        String job_id = jobStatusInfo.getJobId();
        File file = new File(statusFilePath);
        boolean fileFlag = file.exists();
        boolean redoFlag = false;
        if (fileFlag) {
            StageStatusInfo unloadStatus = jobStatusInfo.getUnloadDataStatus();
            StageStatusInfo uploadStatus = jobStatusInfo.getUploadStatus();
            StageStatusInfo loadingStatus = jobStatusInfo.getDataLodingStatus();
            StageStatusInfo calIncrementStatus = jobStatusInfo.getCalIncrementStatus();
            StageStatusInfo registStatus = jobStatusInfo.getDataRegistrationStatus();
            if (unloadStatus != null && uploadStatus != null && loadingStatus != null && calIncrementStatus != null && registStatus != null) {
                int unloadStatusCode = unloadStatus.getStatusCode();
                int uploadStatusCode = uploadStatus.getStatusCode();
                int loadingStatusCode = loadingStatus.getStatusCode();
                int incrementStatusCode = calIncrementStatus.getStatusCode();
                int registStatusCode = registStatus.getStatusCode();
                int succeedCode = RunStatusConstant.SUCCEED.getCode();
                if (unloadStatusCode == succeedCode && uploadStatusCode == succeedCode && loadingStatusCode == succeedCode && incrementStatusCode == succeedCode && registStatusCode == succeedCode) {
                    redoFlag = true;
                    jobStatusInfo = new JobStatusInfo();
                    jobStatusInfo.setRunStatus(RunStatusConstant.RUNNING.getCode());
                    jobStatusInfo.setStartDate(DateUtil.getSysDate());
                    jobStatusInfo.setStartTime(DateUtil.getSysTime());
                    jobStatusInfo.setJobId(job_id);
                }
            }
        }
        int unloadStatusCode = RunStatusConstant.SUCCEED.getCode();
        int unloadStageRedoNum = 0;
        if (fileFlag && jobStatusInfo.getUnloadDataStatus() != null) {
            unloadStatusCode = jobStatusInfo.getUnloadDataStatus().getStatusCode();
            unloadStageRedoNum = jobStatusInfo.getUnloadDataStatus().getAgainNum();
        }
        if (!fileFlag || redoFlag || (unloadStatusCode == RunStatusConstant.FAILED.getCode())) {
            StageParamInfo stageParamInfo = jobStatusInfo.getStageParamInfo();
            if (stageParamInfo == null) {
                stageParamInfo = new StageParamInfo();
            }
            StageParamInfo firstStageParamInfo = head.handleStage(stageParamInfo);
            StageStatusInfo firstStageStatus = firstStageParamInfo.getStatusInfo();
            if (unloadStatusCode == RunStatusConstant.FAILED.getCode()) {
                firstStageStatus.setIsAgain(IsFlag.Shi.getCode());
                firstStageStatus.setAgainNum(unloadStageRedoNum + 1);
                firstStageParamInfo.setStatusInfo(firstStageStatus);
            }
            jobStatusInfo = setStageStatus(firstStageStatus, jobStatusInfo);
            if (firstStageParamInfo.getStatusInfo().getStatusCode() == RunStatusConstant.SUCCEED.getCode()) {
                dealSucceedStage(file, jobStatusInfo, firstStageParamInfo);
            } else if (firstStageParamInfo.getStatusInfo().getStatusCode() == RunStatusConstant.FAILED.getCode()) {
                dealFailedStage(file, jobStatusInfo, firstStageParamInfo);
                return jobStatusInfo;
            } else {
                throw new AppSystemException("除了成功和失败，其他状态目前暂时未做处理");
            }
        }
        JobStageInterface stage = head;
        while ((stage = stage.getNextStage()) != null) {
            jobStatusInfo = JsonUtil.toObject(FileUtil.readFile2String(file), new TypeReference<JobStatusInfo>() {
            });
            StageStatusInfo currentStageStatus = getStageStatusByCode(stage.getStageCode(), jobStatusInfo);
            if (currentStageStatus != null) {
                Integer currentStageAgainNum = currentStageStatus.getAgainNum();
                if (currentStageStatus.getStatusCode() == RunStatusConstant.SUCCEED.getCode()) {
                    continue;
                } else if (currentStageStatus.getStatusCode() == RunStatusConstant.FAILED.getCode()) {
                    StageParamInfo otherParamInfo = stage.handleStage(jobStatusInfo.getStageParamInfo());
                    otherParamInfo.getStatusInfo().setIsAgain(IsFlag.Shi.getCode());
                    otherParamInfo.getStatusInfo().setAgainNum(currentStageAgainNum + 1);
                    jobStatusInfo = setStageStatus(otherParamInfo.getStatusInfo(), jobStatusInfo);
                    if (otherParamInfo.getStatusInfo().getStatusCode() == RunStatusConstant.SUCCEED.getCode()) {
                        dealSucceedStage(file, jobStatusInfo, otherParamInfo);
                    } else if (otherParamInfo.getStatusInfo().getStatusCode() == RunStatusConstant.FAILED.getCode()) {
                        dealFailedStage(file, jobStatusInfo, otherParamInfo);
                        return jobStatusInfo;
                    } else {
                        throw new AppSystemException("除了成功和失败，其他状态目前暂时未做处理");
                    }
                } else {
                    throw new AppSystemException("除了成功和失败，其他状态目前暂时未做处理");
                }
            } else {
                StageParamInfo otherParamInfo = stage.handleStage(jobStatusInfo.getStageParamInfo());
                jobStatusInfo = setStageStatus(otherParamInfo.getStatusInfo(), jobStatusInfo);
                if (otherParamInfo.getStatusInfo().getStatusCode() == RunStatusConstant.SUCCEED.getCode()) {
                    dealSucceedStage(file, jobStatusInfo, otherParamInfo);
                } else if (otherParamInfo.getStatusInfo().getStatusCode() == RunStatusConstant.FAILED.getCode()) {
                    dealFailedStage(file, jobStatusInfo, otherParamInfo);
                    return jobStatusInfo;
                } else {
                    throw new AppSystemException("除了成功和失败，其他状态目前暂时未做处理");
                }
            }
        }
        return jobStatusInfo;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "statusFile", desc = "", range = "")
    @Param(name = "jobStatusInfo", desc = "", range = "")
    @Param(name = "stageParamInfo", desc = "", range = "")
    private void dealFailedStage(File statusFile, JobStatusInfo jobStatusInfo, StageParamInfo stageParamInfo) throws IOException {
        CollectCase collectCaseForFailed = getCollectCaseForFailed(stageParamInfo);
        CommunicationUtil.saveCollectCase(collectCaseForFailed, stageParamInfo.getStatusInfo().getMessage());
        FileUtil.writeString2File(statusFile, JsonUtil.toJson(jobStatusInfo), DataBaseCode.UTF_8.getValue());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "statusFile", desc = "", range = "")
    @Param(name = "jobStatusInfo", desc = "", range = "")
    @Param(name = "stageParamInfo", desc = "", range = "")
    private void dealSucceedStage(File statusFile, JobStatusInfo jobStatusInfo, StageParamInfo stageParamInfo) throws IOException {
        CollectCase collectCaseForSuccess = getCollectCaseForSuccess(stageParamInfo);
        CommunicationUtil.saveCollectCase(collectCaseForSuccess, stageParamInfo.getStatusInfo().getMessage());
        jobStatusInfo.setStageParamInfo(stageParamInfo);
        FileUtil.writeString2File(statusFile, JsonUtil.toJson(jobStatusInfo), DataBaseCode.UTF_8.getValue());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "stageStatus", desc = "", range = "")
    @Param(name = "jobStatus", desc = "", range = "")
    @Return(desc = "", range = "")
    private JobStatusInfo setStageStatus(StageStatusInfo stageStatus, JobStatusInfo jobStatus) {
        StageConstant stage = EnumUtil.getEnumByCode(StageConstant.class, stageStatus.getStageNameCode());
        if (stage == null) {
            throw new AppSystemException("获取阶段信息失败");
        }
        if (stage == StageConstant.UNLOADDATA) {
            jobStatus.setUnloadDataStatus(stageStatus);
        } else if (stage == StageConstant.UPLOAD) {
            jobStatus.setUploadStatus(stageStatus);
        } else if (stage == StageConstant.DATALOADING) {
            jobStatus.setDataLodingStatus(stageStatus);
        } else if (stage == StageConstant.CALINCREMENT) {
            jobStatus.setCalIncrementStatus(stageStatus);
        } else if (stage == StageConstant.DATAREGISTRATION) {
            jobStatus.setDataRegistrationStatus(stageStatus);
        } else {
            throw new AppSystemException("系统不支持的采集阶段");
        }
        return jobStatus;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "stageCode", desc = "", range = "")
    @Param(name = "jobStatus", desc = "", range = "")
    @Return(desc = "", range = "")
    private StageStatusInfo getStageStatusByCode(int stageCode, JobStatusInfo jobStatus) {
        StageConstant stage = EnumUtil.getEnumByCode(StageConstant.class, stageCode);
        if (stage == null) {
            throw new AppSystemException("获取阶段信息失败");
        }
        StageStatusInfo stageStatus;
        if (stage == StageConstant.UNLOADDATA) {
            stageStatus = jobStatus.getUnloadDataStatus();
        } else if (stage == StageConstant.UPLOAD) {
            stageStatus = jobStatus.getUploadStatus();
        } else if (stage == StageConstant.DATALOADING) {
            stageStatus = jobStatus.getDataLodingStatus();
        } else if (stage == StageConstant.CALINCREMENT) {
            stageStatus = jobStatus.getCalIncrementStatus();
        } else if (stage == StageConstant.DATAREGISTRATION) {
            stageStatus = jobStatus.getDataRegistrationStatus();
        } else {
            throw new AppSystemException("系统不支持的采集阶段");
        }
        return stageStatus;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "stageParamInfo", desc = "", range = "")
    @Return(desc = "", range = "")
    private CollectCase getCollectCaseForSuccess(StageParamInfo stageParamInfo) {
        CollectCase collectCase = new CollectCase();
        collectCase.setJob_rs_id(UUID.randomUUID().toString().replaceAll("-", ""));
        collectCase.setCollect_type(stageParamInfo.getCollectType());
        collectCase.setJob_type(String.valueOf(stageParamInfo.getStatusInfo().getStageNameCode()));
        collectCase.setCollect_total(stageParamInfo.getFileArr() != null ? (long) stageParamInfo.getFileArr().length : 0);
        collectCase.setColect_record(stageParamInfo.getRowCount());
        collectCase.setCollet_database_size(String.valueOf(stageParamInfo.getFileSize()));
        collectCase.setCollect_s_date(stageParamInfo.getStatusInfo().getStartDate());
        collectCase.setCollect_e_date(stageParamInfo.getStatusInfo().getEndDate());
        collectCase.setCollect_s_time(stageParamInfo.getStatusInfo().getStartTime());
        collectCase.setCollect_e_time(stageParamInfo.getStatusInfo().getEndTime());
        collectCase.setExecute_state(ExecuteState.YunXingWanCheng.getCode());
        collectCase.setIs_again(stageParamInfo.getStatusInfo().getIsAgain());
        collectCase.setAgain_num(Long.valueOf(stageParamInfo.getStatusInfo().getAgainNum()));
        collectCase.setJob_group(stageParamInfo.getTaskClassify());
        collectCase.setTask_classify(stageParamInfo.getTaskClassify());
        collectCase.setEtl_date(stageParamInfo.getEtlDate());
        collectCase.setAgent_id(stageParamInfo.getAgentId());
        collectCase.setCollect_set_id(stageParamInfo.getCollectSetId());
        collectCase.setSource_id(stageParamInfo.getSourceId());
        return collectCase;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "stageParamInfo", desc = "", range = "")
    @Return(desc = "", range = "")
    private CollectCase getCollectCaseForFailed(StageParamInfo stageParamInfo) {
        CollectCase collectCase = new CollectCase();
        collectCase.setJob_rs_id(UUID.randomUUID().toString().replaceAll("-", ""));
        collectCase.setCollect_type(stageParamInfo.getCollectType());
        collectCase.setJob_type(String.valueOf(stageParamInfo.getStatusInfo().getStageNameCode()));
        collectCase.setCollect_total(stageParamInfo.getFileArr() != null ? (long) stageParamInfo.getFileArr().length : 0);
        collectCase.setColect_record(stageParamInfo.getRowCount());
        collectCase.setCollet_database_size(String.valueOf(stageParamInfo.getFileSize()));
        collectCase.setCollect_s_date(stageParamInfo.getStatusInfo().getStartDate());
        collectCase.setCollect_e_date(stageParamInfo.getStatusInfo().getEndDate());
        collectCase.setCollect_s_time(stageParamInfo.getStatusInfo().getStartTime());
        collectCase.setCollect_e_time(stageParamInfo.getStatusInfo().getEndTime());
        collectCase.setExecute_state(ExecuteState.YunXingShiBai.getCode());
        collectCase.setIs_again(stageParamInfo.getStatusInfo().getIsAgain());
        collectCase.setAgain_num(Long.valueOf(stageParamInfo.getStatusInfo().getAgainNum()));
        collectCase.setJob_group(stageParamInfo.getTaskClassify());
        collectCase.setTask_classify(stageParamInfo.getTaskClassify());
        collectCase.setEtl_date(stageParamInfo.getEtlDate());
        collectCase.setAgent_id(stageParamInfo.getAgentId());
        collectCase.setCollect_set_id(stageParamInfo.getCollectSetId());
        collectCase.setSource_id(stageParamInfo.getSourceId());
        return collectCase;
    }

    public JobStageInterface getHead() {
        return head;
    }

    public void setHead(JobStageInterface head) {
        this.head = head;
    }

    public JobStageInterface getLast() {
        return last;
    }

    public void setLast(JobStageInterface last) {
        this.last = last;
    }
}
