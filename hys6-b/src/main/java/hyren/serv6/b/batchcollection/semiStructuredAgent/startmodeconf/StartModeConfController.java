package hyren.serv6.b.batchcollection.semiStructuredAgent.startmodeconf;

import hyren.serv6.commons.jobUtil.beans.JobStartConf;
import hyren.serv6.commons.jobUtil.beans.ObjJobBean;
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

@RestController
@Slf4j
@RequestMapping("/dataCollectionO/semiStructuredAgent/startmodeconf")
@Api(value = "", tags = "")
@Validated
public class StartModeConfController {

    @Autowired
    public StartModeConfService modeConfService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getEtlJobConfInfoFromObj")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public List<Map<String, Object>> getEtlJobConfInfoFromObj(@NotNull Long odc_id) {
        return modeConfService.getEtlJobConfInfoFromObj(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getPreviewJob")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public List<Map<String, Object>> getPreviewJob(@NotNull Long odc_id) {
        return modeConfService.getPreviewJob(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAgentPath")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> getAgentPath(@NotNull Long odc_id) {
        return modeConfService.getAgentPath(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveStartModeConfData")
    @ApiImplicitParam(name = "objJobBean", value = "", dataTypeClass = JobStartConf[].class)
    public void saveStartModeConfData(@RequestBody ObjJobBean objJobBean) {
        modeConfService.saveStartModeConfData(objJobBean);
    }
}
