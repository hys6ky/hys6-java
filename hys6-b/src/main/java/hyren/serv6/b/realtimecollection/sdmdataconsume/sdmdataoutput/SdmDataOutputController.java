package hyren.serv6.b.realtimecollection.sdmdataconsume.sdmdataoutput;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.b.agent.tools.ReqDataUtils;
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
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataCollectionO/sdmdataconsume/sdmdataoutput")
@Api("流数据分析,数据消费输出")
@Validated
@Slf4j
public class SdmDataOutputController {

    @Autowired
    SdmDataOutputService sdmDataOutputService;

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/saveDataOutputMsg")
    public Map<String, Object> saveDataOutputMsg(@RequestBody Map<String, Object> req) {
        SdmSpOutput sdm_sp_output = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpOutput>() {
        });
        SdmSpTextfile sdm_sp_textfile = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpTextfile>() {
        });
        SdmSpDatabase sdm_sp_database = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpDatabase>() {
        });
        SdmSpStream sdm_sp_stream = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpStream>() {
        });
        StreamproSetting streampro_setting = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<StreamproSetting>() {
        });
        SdmRestDatabase sdm_rest_database = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmRestDatabase>() {
        });
        SdmRestStream sdm_rest_stream = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmRestStream>() {
        });
        List<String> rest_key = (List<String>) req.get("rest_key");
        List<String> rest_val = (List<String>) req.get("rest_val");
        long ssj_job_id = ReqDataUtils.getLongData(req, "ssj_job_id");
        long dsl_id = ReqDataUtils.getLongData(req, "dsl_id");
        return sdmDataOutputService.saveDataOutputMsg(sdm_sp_output, sdm_sp_textfile, sdm_sp_database, sdm_sp_stream, streampro_setting, ssj_job_id, dsl_id, sdm_rest_database, sdm_rest_stream, rest_key, rest_val);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/getRestList")
    public List<StreamproSetting> getRestList() {
        return sdmDataOutputService.getRestList();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "rs_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getCheckedMsg")
    public Map<String, Object> getCheckedMsg(long rs_id) {
        return sdmDataOutputService.getCheckedMsg(rs_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getSdmSpOutputMsgList")
    public List<Object> getSdmSpOutputMsgList(long ssj_job_id) {
        return sdmDataOutputService.getSdmSpOutputMsgList(ssj_job_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_info_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getSdmSpOutputMsg")
    public Map<String, Object> getSdmSpOutputMsg(long sdm_info_id) {
        return sdmDataOutputService.getSdmSpOutputMsg(sdm_info_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/updateSdmSpOutputMsg")
    public void updateSdmSpOutputMsg(@RequestBody Map<String, Object> req) {
        SdmSpOutput sdm_sp_output = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpOutput>() {
        });
        SdmSpTextfile sdm_sp_textfile = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpTextfile>() {
        });
        SdmSpDatabase sdm_sp_database = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpDatabase>() {
        });
        SdmSpStream sdm_sp_stream = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmSpStream>() {
        });
        StreamproSetting streampro_setting = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<StreamproSetting>() {
        });
        SdmRestDatabase sdm_rest_database = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmRestDatabase>() {
        });
        SdmRestStream sdm_rest_stream = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmRestStream>() {
        });
        List<String> rest_key = (List<String>) req.get("rest_key");
        List<String> rest_val = (List<String>) req.get("rest_val");
        long sdm_info_id = ReqDataUtils.getLongData(req, "sdm_info_id");
        sdmDataOutputService.updateSdmSpOutputMsg(sdm_sp_output, sdm_sp_textfile, sdm_sp_database, sdm_sp_stream, streampro_setting, sdm_info_id, sdm_rest_database, sdm_rest_stream, rest_key, rest_val);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/testOutPutConnection")
    public boolean testOutPutConnection(long dsl_id) {
        return sdmDataOutputService.testOutPutConnection(dsl_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/testMethodConnection")
    public void testMethodConnection(@RequestBody Map<String, Object> req) {
        String[] rest_key = JsonUtil.toObject(ReqDataUtils.getStringData(req, "rest_key"), new TypeReference<String[]>() {
        });
        String[] rest_val = JsonUtil.toObject(ReqDataUtils.getStringData(req, "rest_val"), new TypeReference<String[]>() {
        });
        String url = ReqDataUtils.getStringData(req, "url");
        sdmDataOutputService.testMethodConnection(url, rest_key, rest_val);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_info_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ssd_info_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/deleteSdmSpOutputMsg")
    public void deleteSdmSpOutputMsg(long sdm_info_id, long ssd_info_id) {
        sdmDataOutputService.deleteSdmSpOutputMsg(sdm_info_id, ssd_info_id);
    }
}
