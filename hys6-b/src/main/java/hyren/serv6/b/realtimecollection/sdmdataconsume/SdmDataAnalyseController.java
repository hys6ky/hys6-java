package hyren.serv6.b.realtimecollection.sdmdataconsume;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.entity.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("/dataCollectionO/sdmdataconsume")
@Api("流数据分析,添加任务,保存数据源")
public class SdmDataAnalyseController {

    @Autowired
    SdmDataAnalyseService sdmDataAnalyseService;

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdmSpInfo", value = "", dataTypeClass = SdmSpJobinfo.class)
    @RequestMapping("/saveTask")
    public long saveTask(@RequestBody SdmSpJobinfo sdmSpInfo) {
        return sdmDataAnalyseService.saveTask(sdmSpInfo);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteTask")
    public void deleteTask(long ssj_job_id) {
        sdmDataAnalyseService.deleteTask(ssj_job_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/selectTaskList")
    public Map<String, Object> selectTaskList(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        return sdmDataAnalyseService.selectTaskList(currPage, pageSize);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ssj_job_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "ssj_strategy", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "ssj_job_desc", value = "", dataTypeClass = String.class) })
    @RequestMapping("/updateTask")
    public void updateTask(long ssj_job_id, String ssj_job_name, String ssj_strategy, String ssj_job_desc) {
        sdmDataAnalyseService.updateTask(ssj_job_id, ssj_job_name, ssj_strategy, ssj_job_desc);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveJobSource")
    public void saveJobSource(@RequestBody Map<String, Object> req) {
        SdmJobInput sdm_job_input = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmJobInput>() {
        });
        SdmSpTextfile sdm_sp_textfile = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpTextfile>() {
        });
        SdmInputDatabase sdm_input_database = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmInputDatabase>() {
        });
        SdmSpStream sdm_sp_stream = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpStream>() {
        });
        sdmDataAnalyseService.saveJobSource(sdm_job_input, sdm_sp_textfile, sdm_input_database, sdm_sp_stream);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveJobInteriorSource")
    public void saveJobInteriorSource(@RequestBody Map<String, Object> req) {
        SdmJobInput sdm_job_input = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmJobInput>() {
        });
        String outputTableName = ReqDataUtils.getStringData(req, "outputTableName");
        String sss_consumer_offset = ReqDataUtils.getStringData(req, "sss_consumer_offset");
        sdmDataAnalyseService.saveJobInteriorSource(sdm_job_input, outputTableName, sss_consumer_offset);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_info_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "is_inner", value = "", dataTypeClass = String.class) })
    @RequestMapping("/deleteSourceInfo")
    public void deleteSourceInfo(long sdm_info_id, String is_inner) {
        sdmDataAnalyseService.deleteSourceInfo(sdm_info_id, is_inner);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_info_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "is_inner", value = "", dataTypeClass = String.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/getSourceInfo")
    public Map<String, Object> getSourceInfo(long sdm_info_id, String is_inner) {
        return sdmDataAnalyseService.getSourceInfo(sdm_info_id, is_inner);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/updateSourceInfo")
    public void updateSourceInfo(@RequestBody Map<String, Object> req) {
        SdmJobInput sdm_job_input = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmJobInput>() {
        });
        SdmSpTextfile sdm_sp_textfile = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpTextfile>() {
        });
        SdmSpStream sdm_sp_stream = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpStream>() {
        });
        SdmInputDatabase sdm_input_database = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmInputDatabase>() {
        });
        String is_inner = ReqDataUtils.getStringData(req, "is_inner");
        sdmDataAnalyseService.updateSourceInfo(sdm_job_input, sdm_sp_textfile, sdm_sp_stream, sdm_input_database, is_inner);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_input_database", value = "", dataTypeClass = SdmInputDatabase.class)
    @Return(desc = "", range = "")
    @RequestMapping("/testConnection")
    public boolean testConnection(@RequestBody SdmInputDatabase sdm_input_database) {
        return sdmDataAnalyseService.testConnection(sdm_input_database);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getDataSourceList")
    public List<SdmJobInput> getDataSourceList(long ssj_job_id) {
        return sdmDataAnalyseService.getDataSourceList(ssj_job_id);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/geKFKTreeData")
    public List<Node> geKFKTreeData() {
        return sdmDataAnalyseService.geKFKTreeData();
    }
}
