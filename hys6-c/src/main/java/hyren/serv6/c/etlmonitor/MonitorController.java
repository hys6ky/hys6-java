package hyren.serv6.c.etlmonitor;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/etlMage/etlmonitor")
@Slf4j
public class MonitorController {

    @Autowired
    private MonitorService service;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class) })
    @PostMapping("downHistoryJobLog")
    public String downHistoryJobLog(Long etl_sys_id, String etl_job, String curr_bath_date) {
        return service.downHistoryJobLog(etl_sys_id, etl_job, curr_bath_date, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "projectData", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "taskData", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "jobData", value = "", dataTypeClass = String.class) })
    @PostMapping("generateExcel")
    public void generateExcel(@RequestBody Map<String, Object> req) {
        String projectData = null;
        String taskData = null;
        String jobData = null;
        try {
            projectData = req.get("projectData").toString();
            taskData = req.get("taskData").toString();
            jobData = req.get("jobData").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req data is not format ... ");
        }
        service.generateExcel(projectData, taskData, jobData);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class) })
    @PostMapping("downloadFile")
    public void downloadFile(String fileName) {
        FileDownloadUtil.downloadFile(WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName);
        try {
            FileUtil.forceDelete(new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName));
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new BusinessException("删除文件失败！" + e.getMessage());
        }
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_job", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "etl_job_desc", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currJobPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageJobSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("getJobConsumeTimeSum")
    public Map<String, Object> getJobConsumeTimeSum(Long etl_sys_id, String curr_bath_date, String etl_job, String etl_job_desc, @RequestParam(name = "currJobPage", defaultValue = "1") Integer currJobPage, @RequestParam(name = "pageJobSize", defaultValue = "10") Integer pageJobSize) {
        return service.getJobConsumeTimeSum(etl_sys_id, curr_bath_date, etl_job, etl_job_desc, currJobPage, pageJobSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("getProjectConsumeTimeSum")
    public Result getProjectConsumeTimeSum(Long etl_sys_id, String curr_bath_date) {
        return service.getProjectConsumeTimeSum(etl_sys_id, curr_bath_date);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sub_sys_cd_or_name", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("getTaskConsumeTimeSum")
    public Map<String, Object> getTaskConsumeTimeSum(Long etl_sys_id, String curr_bath_date, String sub_sys_cd_or_name, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.getTaskConsumeTimeSum(etl_sys_id, curr_bath_date, sub_sys_cd_or_name, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorAllProjectChartsData")
    public Result monitorAllProjectChartsData() {
        return service.monitorAllProjectChartsData(UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorBatchEtlJobDependencyInfo")
    public String monitorBatchEtlJobDependencyInfo(Long etl_sys_id) {
        return service.monitorBatchEtlJobDependencyInfo(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorCurrentBatchInfo")
    public Map<String, Object> monitorCurrentBatchInfo(Long etl_sys_id) {
        return service.monitorCurrentBatchInfo(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorCurrentBatchInfoByTask")
    public List<Map<String, Object>> monitorCurrentBatchInfoByTask(Long etl_sys_id) {
        return service.monitorCurrentBatchInfoByTask(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", example = ""), @ApiImplicitParam(name = "sub_sys_id", value = "", example = ""), @ApiImplicitParam(name = "curr_bath_date", value = "", example = "") })
    @PostMapping("/searchMonitorJobStateBySubCd")
    public List<Map<String, Object>> searchMonitorJobStateBySubCd(Long etl_sys_id, Long sub_sys_id, String curr_bath_date) {
        return service.searchMonitorJobStateBySubCd(etl_sys_id, sub_sys_id, curr_bath_date);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", required = true, dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorCurrJobInfo")
    public Map<String, Object> monitorCurrJobInfo(Long etl_sys_id, Long etl_job_id) {
        return service.monitorCurrJobInfo(etl_sys_id, etl_job_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "curr_bath_date", value = "", required = true, dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorHistoryBatchInfo")
    public List<Map<String, Object>> monitorHistoryBatchInfo(Long etl_sys_id, String curr_bath_date) {
        return service.monitorHistoryBatchInfo(etl_sys_id, curr_bath_date, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "start_date", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "end_date", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "isHistoryBatch", value = "", required = true, dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorHistoryJobInfo")
    public List<Map<String, Object>> monitorHistoryJobInfo(Long etl_sys_id, String etl_job, String start_date, String end_date, String isHistoryBatch) {
        if (StringUtils.isBlank(etl_job)) {
            throw new BusinessException("please check job ...");
        }
        return service.monitorHistoryJobInfo(etl_sys_id, etl_job, start_date, end_date, isHistoryBatch, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorJobDependencyInfo")
    public Map<String, Object> monitorJobDependencyInfo(Long etl_sys_id, Long etl_job_id) {
        return service.monitorJobDependencyInfo(etl_sys_id, etl_job_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("monitorSystemResourceInfo")
    public Map<String, Object> monitorSystemResourceInfo(Long etl_sys_id) {
        return service.monitorSystemResourceInfo(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "readNum", value = "", defaultValue = "100", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("readHistoryJobLogInfo")
    public String readHistoryJobLogInfo(Long etl_sys_id, String etl_job, @RequestParam(name = "readNum", defaultValue = "100") Integer readNum) {
        return service.readHistoryJobLogInfo(etl_sys_id, etl_job, readNum, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("findJobByEtlSysId")
    public List<EtlJobDef> findJobByEtlSysId(Long etl_sys_id) {
        return service.findJobByEtlSysId(etl_sys_id, UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "sub_sys_cd", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    @Return(desc = "", range = "")
    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sub_sys_cd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchMonitorHisBatchJobBySubCd")
    public List<Map<String, Object>> searchMonitorHisBatchJobBySubCd(Long etl_sys_id, Long sub_sys_id, String sub_sys_cd, String curr_bath_date) {
        return service.searchMonitorHisBatchJobBySubCd(etl_sys_id, sub_sys_id, sub_sys_cd, curr_bath_date, UserUtil.getUserId());
    }
}
