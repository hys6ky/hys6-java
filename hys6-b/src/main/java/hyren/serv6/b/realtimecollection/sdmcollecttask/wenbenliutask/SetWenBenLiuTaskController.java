package hyren.serv6.b.realtimecollection.sdmcollecttask.wenbenliutask;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.SdmReceiveConf;
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
import java.util.Objects;

@RestController
@RequestMapping("/dataCollectionO/sdmcollecttask/wenbenliutask")
@Api("文件内容流任务采集")
@Validated
@Slf4j
public class SetWenBenLiuTaskController {

    @Autowired
    SetWenBenLiuTaskService setWenBenLiuTaskService;

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdmReceiveConf", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdmRecParam", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdmMessInfo", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveSdmReceiveConfManage")
    public void saveSdmReceiveConfManage(@RequestBody Map<String, Object> req) {
        String sdmReceiveConf = ReqDataUtils.getStringData(req, "sdmReceiveConf");
        String sdmRecParam = ReqDataUtils.getStringData(req, "sdmRecParam");
        String sdmMessInfo = ReqDataUtils.getStringData(req, "sdmMessInfo");
        setWenBenLiuTaskService.saveSdmReceiveConfManage(sdmReceiveConf, sdmRecParam, sdmMessInfo);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdmReceiveConf", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdmRecParam", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdmMessInfo", value = "", dataTypeClass = String.class) })
    @RequestMapping("/updateSdmReceiveConfManage")
    public void updateSdmReceiveConfManage(@RequestBody Map<String, Object> req) {
        String sdmReceiveConf = ReqDataUtils.getStringData(req, "sdmReceiveConf");
        String sdmRecParam = ReqDataUtils.getStringData(req, "sdmRecParam");
        String sdmMessInfo = ReqDataUtils.getStringData(req, "sdmMessInfo");
        setWenBenLiuTaskService.updateSdmReceiveConfManage(sdmReceiveConf, sdmRecParam, sdmMessInfo);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_name", value = "", dataTypeClass = String.class)
    @Return(desc = "", range = "")
    @RequestMapping("/topicIsValid")
    public boolean topicIsValid(String topic_name) {
        return setWenBenLiuTaskService.topicIsValid(topic_name);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "path", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "isFile", value = "", dataTypeClass = String.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/selectFile")
    public List<Object> selectFile(long sdm_agent_id, String path, String isFile) {
        return setWenBenLiuTaskService.selectFile(sdm_agent_id, path, isFile);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "file_exte_date", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "is_parse", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "data_separator", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/saveWenBenLiuTask")
    public void saveWenBenLiuTask(String file_path, String file_exte_date, String is_parse, String data_separator, long sdm_agent_id) {
        setWenBenLiuTaskService.saveWenBenLiuTask(file_path, file_exte_date, is_parse, data_separator, sdm_agent_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/downLoadFileTemplate")
    public void downLoadFileTemplate() {
        setWenBenLiuTaskService.downLoadFileTemplate();
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveMessInfo")
    public void saveMessInfo(@RequestBody Map<String, Object> req) {
        int number = Integer.parseInt(Objects.isNull(req.get("number")) ? "0" : req.get("number").toString());
        long sdm_receive_id = ReqDataUtils.getLongData(req, "sdm_receive_id");
        SdmReceiveConf sdmReceiveConf = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmReceiveConf"), new TypeReference<SdmReceiveConf>() {
        });
        setWenBenLiuTaskService.saveMessInfo(number, sdmReceiveConf, sdm_receive_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "brokerSize", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class) })
    @RequestMapping("/autoCreateTopic")
    public void autoCreateTopic(int brokerSize, String table_name) {
        setWenBenLiuTaskService.autoCreateTopic(brokerSize, table_name);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_type", value = "", dataTypeClass = String.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/selectTaskConfiguration")
    public List<AgentInfo> selectTaskConfiguration(long sdm_source_id, String sdm_agent_type) {
        return setWenBenLiuTaskService.selectTaskConfiguration(sdm_source_id, sdm_agent_type);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/selectTaskManage")
    public List<Map<String, Object>> selectTaskManage(long sdm_agent_id) {
        return setWenBenLiuTaskService.selectTaskManage(sdm_agent_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/selectWenBenTask")
    public Map<String, Object> selectWenBenTask(long sdm_agent_id, long sdm_receive_id) {
        return setWenBenLiuTaskService.selectWenBenTask(sdm_agent_id, sdm_receive_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteWenBenTask")
    public void deleteWenBenTask(long sdm_receive_id) {
        setWenBenLiuTaskService.deleteWenBenTask(sdm_receive_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/sendWenBenTask")
    public void sendWenBenTask(long sdm_receive_id) {
        setWenBenLiuTaskService.sendWenBenTask(sdm_receive_id);
    }

    @RequestMapping("/getSendDataByJobId")
    public String getSendDataByJobId(long sdm_receive_id) {
        return setWenBenLiuTaskService.getSendDataByJobId(sdm_receive_id);
    }
}
