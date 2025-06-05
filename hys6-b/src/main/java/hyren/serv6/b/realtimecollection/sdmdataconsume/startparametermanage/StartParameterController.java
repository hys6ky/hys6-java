package hyren.serv6.b.realtimecollection.sdmdataconsume.startparametermanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.SdmSpParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RequestMapping("/dataCollectionO/sdmdataconsume/startparametermanage")
@RestController
@Slf4j
@Validated
@Api("流数据分析,启动参数配置")
public class StartParameterController {

    @Autowired
    StartParameterService startParameterService;

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveStartParameters")
    public void saveStartParameters(@RequestBody Map<String, Object> req) {
        long ssj_job_id = ReqDataUtils.getLongData(req, "ssj_job_id");
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        SdmSpParam[] sdm_sp_param = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdm_sp_param"), new TypeReference<SdmSpParam[]>() {
        });
        startParameterService.saveStartParameters(sdm_sp_param, ssj_job_id, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getJobParamsList")
    public Map<String, Object> getJobParamsList(long ssj_job_id) {
        return startParameterService.getJobParamsList(ssj_job_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/startTask")
    public void startTask(long ssj_job_id) {
        startParameterService.startTask(ssj_job_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/addInputConfig")
    public void addInputConfig(@RequestBody Map<String, Object> req) {
        long ssj_job_id = ReqDataUtils.getLongData(req, "ssj_job_id");
        String engine = ReqDataUtils.getStringData(req, "engine");
        List<Map<String, Object>> array = JsonUtil.toObject(ReqDataUtils.getStringData(req, "array"), new TypeReference<List<Map<String, Object>>>() {
        });
        startParameterService.addInputConfig(ssj_job_id, engine, array);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/addAnalysisConfig")
    public void addAnalysisConfig(@RequestBody Map<String, Object> req) {
        long ssj_job_id = ReqDataUtils.getLongData(req, "ssj_job_id");
        String engine = ReqDataUtils.getStringData(req, "engine");
        List<Map<String, Object>> array = JsonUtil.toObject(ReqDataUtils.getStringData(req, "array"), new TypeReference<List<Map<String, Object>>>() {
        });
        startParameterService.addAnalysisConfig(ssj_job_id, engine, array);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "path", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "filename", value = "", dataTypeClass = String.class) })
    @RequestMapping("/startup")
    public void startup(long ssj_job_id, String path, String filename) {
        startParameterService.startup(ssj_job_id, path, filename);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/addOutputConfig")
    public void addOutputConfig(@RequestBody Map<String, Object> req) {
        long ssj_job_id = ReqDataUtils.getLongData(req, "ssj_job_id");
        String engine = ReqDataUtils.getStringData(req, "engine");
        List<Map<String, Object>> array = JsonUtil.toObject(ReqDataUtils.getStringData(req, "array"), new TypeReference<List<Map<String, Object>>>() {
        });
        startParameterService.addOutputConfig(ssj_job_id, engine, array);
    }
}
