package hyren.serv6.b.batchcollection.agent.startwayconf;

import hyren.serv6.base.entity.EtlSubSysList;
import hyren.serv6.base.entity.EtlSys;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.beans.EtlJobInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RequestMapping("/dataCollectionO/agent/startwayconf")
@RestController
@Slf4j
@Api("定义启动方式配置")
@Validated
public class StartWayController {

    @Autowired
    StartWayConfService startWayService;

    @RequestMapping("/getEtlSysData")
    @ApiOperation(value = "", tags = "")
    public List<EtlSys> getEtlSysData() {
        return startWayService.getEtlSysData();
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "etl_sys_id", value = "", type = "作业工程主键ID", dataTypeClass = String.class)
    @RequestMapping("/getEtlSubSysData")
    public List<EtlSubSysList> getEtlSubSysData(Long etl_sys_id) {
        if (Objects.isNull(etl_sys_id)) {
            throw new BusinessException("请选择工程编号...");
        }
        return startWayService.getEtlSubSysData(etl_sys_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", type = "不可为空的整数", dataTypeClass = Long.class)
    @RequestMapping("/getPreviewJob")
    public List<Map<String, Object>> getPreviewJob(@NotNull Long colSetId) {
        return startWayService.getPreviewJob(colSetId);
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "colSetId", value = "", type = "不可为空", dataTypeClass = Long.class)
    @RequestMapping("/getEtlJobData")
    public List<Map<String, Object>> getEtlJobData(@NotNull Long colSetId) {
        return startWayService.getEtlJobData(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", type = "不可为空的整数", dataTypeClass = Long.class)
    @RequestMapping("/getAgentPath")
    public Map<String, Object> getAgentPath(@NotNull Long colSetId) {
        return startWayService.getAgentPath(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveJobDataToDatabase")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public void saveJobDataToDatabase(@RequestBody EtlJobInfo etlJobInfo) {
        startWayService.saveJobDataToDatabase(etlJobInfo);
    }
}
