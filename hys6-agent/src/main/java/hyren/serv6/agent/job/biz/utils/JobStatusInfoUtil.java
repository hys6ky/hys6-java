package hyren.serv6.agent.job.biz.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;
import java.util.concurrent.Future;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/12/2 14:19")
public class JobStatusInfoUtil {

    @Method(desc = "", logicStep = "")
    @Param(desc = "", name = "statusFilePath", range = "")
    @Return(desc = "", range = "")
    public static JobStatusInfo getStartJobStatusInfo(String statusFilePath, String job_id, String table_name) {
        File file = new File(statusFilePath);
        JobStatusInfo jobStatus;
        if (file.exists()) {
            jobStatus = JsonUtil.toObject(FileUtil.readFile2String(file), new TypeReference<JobStatusInfo>() {
            });
        } else {
            String parent = file.getParent();
            File parentPath = new File(parent);
            if (!parentPath.exists()) {
                if (!parentPath.mkdirs()) {
                    throw new AppSystemException("创建文件夹" + parentPath + "失败！");
                }
            }
            jobStatus = new JobStatusInfo();
            jobStatus.setJobId(table_name + "_" + job_id);
            jobStatus.setRunStatus(RunStatusConstant.RUNNING.getCode());
            jobStatus.setStartDate(DateUtil.getSysDate());
            jobStatus.setStartTime(DateUtil.getSysTime());
        }
        return jobStatus;
    }

    @Method(desc = "", logicStep = "")
    public static void printJobStatusInfo(List<Future<JobStatusInfo>> statusInfoFutureList) throws Exception {
        for (Future<JobStatusInfo> statusInfoFuture : statusInfoFutureList) {
            JobStatusInfo jobStatusInfo = statusInfoFuture.get();
            if (jobStatusInfo.getUnloadDataStatus() == null || RunStatusConstant.FAILED.getCode() == jobStatusInfo.getUnloadDataStatus().getStatusCode()) {
                if (jobStatusInfo.getUnloadDataStatus() == null) {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：卸数执行失败: 请查看日志获取异常信息");
                } else {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：卸数执行失败:" + jobStatusInfo.getUnloadDataStatus().getMessage());
                }
            } else {
                log.info(jobStatusInfo.getJobId() + "：卸数执行成功");
            }
            if (jobStatusInfo.getUploadStatus() == null || RunStatusConstant.FAILED.getCode() == jobStatusInfo.getUploadStatus().getStatusCode()) {
                if (jobStatusInfo.getUploadStatus() == null) {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：上传执行失败: 请查看日志获取异常信息");
                } else {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：上传执行失败:" + jobStatusInfo.getUploadStatus().getMessage());
                }
            } else {
                log.info(jobStatusInfo.getJobId() + "：上传执行成功");
            }
            if (jobStatusInfo.getDataLodingStatus() == null || RunStatusConstant.FAILED.getCode() == jobStatusInfo.getDataLodingStatus().getStatusCode()) {
                if (jobStatusInfo.getDataLodingStatus() == null) {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：加载执行失败: 请查看日志获取异常信息");
                } else {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：加载执行失败:" + jobStatusInfo.getDataLodingStatus().getMessage());
                }
            } else {
                log.info(jobStatusInfo.getJobId() + "：加载执行成功");
            }
            if (jobStatusInfo.getCalIncrementStatus() == null || RunStatusConstant.FAILED.getCode() == jobStatusInfo.getCalIncrementStatus().getStatusCode()) {
                if (jobStatusInfo.getCalIncrementStatus() == null) {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：增量执行失败: 请查看日志获取异常信息");
                } else {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：增量执行失败:" + jobStatusInfo.getCalIncrementStatus().getMessage());
                }
            } else {
                log.info(jobStatusInfo.getJobId() + "：增量执行成功");
            }
            if (jobStatusInfo.getDataRegistrationStatus() == null || RunStatusConstant.FAILED.getCode() == jobStatusInfo.getDataRegistrationStatus().getStatusCode()) {
                if (jobStatusInfo.getDataRegistrationStatus() == null) {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：登记执行失败: 请查看日志获取异常信息");
                } else {
                    throw new AppSystemException(jobStatusInfo.getJobId() + "：登记执行失败:" + jobStatusInfo.getDataRegistrationStatus().getMessage());
                }
            } else {
                log.info(jobStatusInfo.getJobId() + "：登记执行成功");
            }
        }
    }

    public static void startStageStatusInfo(StageStatusInfo stageStatusInfo, String job_id, int stageNameCode) {
        stageStatusInfo.setStageNameCode(stageNameCode);
        stageStatusInfo.setJobId(job_id);
        stageStatusInfo.setStartDate(DateUtil.getSysDate());
        stageStatusInfo.setStartTime(DateUtil.getSysTime());
    }

    public static void endStageStatusInfo(StageStatusInfo stageStatusInfo, int statusCode, String message) {
        stageStatusInfo.setMessage(message);
        stageStatusInfo.setStatusCode(statusCode);
        stageStatusInfo.setEndDate(DateUtil.getSysDate());
        stageStatusInfo.setEndTime(DateUtil.getSysTime());
    }

    public static void endStageParamInfo(StageParamInfo stageParamInfo, StageStatusInfo stageStatusInfo, CollectTableBean collectTableBean, String collectType) {
        stageParamInfo.setStatusInfo(stageStatusInfo);
        stageParamInfo.setAgentId(collectTableBean.getAgent_id());
        stageParamInfo.setSourceId(collectTableBean.getSource_id());
        stageParamInfo.setCollectSetId(Long.parseLong(collectTableBean.getDatabase_id()));
        stageParamInfo.setTaskClassify(collectTableBean.getTable_name());
        stageParamInfo.setCollectType(collectType);
        stageParamInfo.setEtlDate(collectTableBean.getEtlDate());
    }

    public static void endStageParamInfo(StageParamInfo stageParamInfo, StageStatusInfo stageStatusInfo, ObjectTableBean objectTableBean, String collectType) {
        stageParamInfo.setStatusInfo(stageStatusInfo);
        stageParamInfo.setAgentId(objectTableBean.getAgent_id());
        stageParamInfo.setSourceId(objectTableBean.getSource_id());
        stageParamInfo.setCollectSetId(Long.parseLong(objectTableBean.getOdc_id()));
        stageParamInfo.setTaskClassify(objectTableBean.getEn_name());
        stageParamInfo.setCollectType(collectType);
        stageParamInfo.setEtlDate(objectTableBean.getEtlDate());
    }
}
