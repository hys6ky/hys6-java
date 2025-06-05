package hyren.serv6.b.realtimecollection.sdmcollecttask.restjieshoutask;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.SdmMessInfo;
import hyren.serv6.base.entity.SdmRecParam;
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
import java.util.Map;

@RequestMapping("/dataCollectionO/sdmcollecttask/restjieshoutask")
@RestController
@Api("数据消息流采集任务")
@Slf4j
@Validated
public class SetRestJieShouTaskController {

    @Autowired
    SetRestJieShouTaskService setRestJieShouTaskService;

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdmReceiveConf", value = "", dataTypeClass = SdmReceiveConf.class), @ApiImplicitParam(name = "sdmRecParam", value = "", dataTypeClass = SdmRecParam.class), @ApiImplicitParam(name = "sdmMessInfo", value = "", dataTypeClass = SdmMessInfo.class) })
    @RequestMapping("/saveSdmRestManage")
    public void saveSdmRestManage(@RequestBody Map<String, Object> req) {
        SdmReceiveConf sdmReceiveConf = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmReceiveConf"), new TypeReference<SdmReceiveConf>() {
        });
        SdmRecParam[] sdmRecParams = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmRecParam"), new TypeReference<SdmRecParam[]>() {
        });
        SdmMessInfo[] sdmMessInfos = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmMessInfo"), new TypeReference<SdmMessInfo[]>() {
        });
        setRestJieShouTaskService.saveSdmRestManage(sdmReceiveConf, sdmRecParams, sdmMessInfos);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_name", value = "", dataTypeClass = SdmReceiveConf.class)
    @Return(desc = "", range = "")
    @RequestMapping("/topicIsValid")
    public boolean topicIsValid(String topic_name) {
        return setRestJieShouTaskService.topicIsValid(topic_name);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdmReceiveConf", value = "", dataTypeClass = SdmReceiveConf.class), @ApiImplicitParam(name = "sdmRecParam", value = "", dataTypeClass = SdmRecParam.class), @ApiImplicitParam(name = "sdmMessInfo", value = "", dataTypeClass = SdmMessInfo.class) })
    @RequestMapping("/updateSdmRestManage")
    public void updateSdmRestManage(@RequestBody Map<String, Object> req) {
        SdmReceiveConf sdmReceiveConf = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmReceiveConf"), new TypeReference<SdmReceiveConf>() {
        });
        SdmRecParam[] sdmRecParams = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmRecParam"), new TypeReference<SdmRecParam[]>() {
        });
        SdmMessInfo[] sdmMessInfos = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdmMessInfo"), new TypeReference<SdmMessInfo[]>() {
        });
        setRestJieShouTaskService.updateSdmRestManage(sdmReceiveConf, sdmRecParams, sdmMessInfos);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/selectRestJieShouTask")
    public Map<String, Object> selectRestJieShouTask(long sdm_agent_id, long sdm_receive_id) {
        return setRestJieShouTaskService.selectRestJieShouTask(sdm_agent_id, sdm_receive_id);
    }
}
