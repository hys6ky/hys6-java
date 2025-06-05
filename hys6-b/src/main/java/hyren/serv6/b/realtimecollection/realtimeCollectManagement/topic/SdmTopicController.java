package hyren.serv6.b.realtimecollection.realtimeCollectManagement.topic;

import fd.ng.core.annotation.Return;
import hyren.serv6.base.entity.SdmTopicInfo;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
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
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RequestMapping("/dataCollectionM/topic")
@RestController
@Slf4j
@Validated
@Api("sdm_topic_info增删改查")
public class SdmTopicController {

    @Autowired
    SdmTopicService sdmTopicService;

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdmTopicInfo", value = "", dataTypeClass = SdmTopicInfo.class)
    @RequestMapping("/saveExistSdmTopicInfo")
    public void saveExistSdmTopicInfo(@RequestBody SdmTopicInfo sdmTopicInfo) {
        sdmTopicService.saveExistSdmTopicInfo(sdmTopicInfo);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdmTopicInfo", value = "", dataTypeClass = SdmTopicInfo.class)
    @RequestMapping("/saveSdmTopicInfo")
    public void saveSdmTopicInfo(SdmTopicInfo sdmTopicInfo) {
        sdmTopicService.saveSdmTopicInfo(sdmTopicInfo);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteSdmTopicInfo")
    public void deleteSdmTopicInfo(@NotNull long topic_id) {
        sdmTopicService.deleteSdmTopicInfo(topic_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/searchSdmTopicInfo")
    public Map<String, Object> searchSdmTopicInfo(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        return sdmTopicService.searchSdmTopicInfo(currPage, pageSize);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_top_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_zk_host", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_top_cn_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_partition", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_replication", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_top_value", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "topic_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/updateSdmTopicInfo")
    public void updateSdmTopicInfo(String sdm_top_name, String sdm_zk_host, String sdm_top_cn_name, long sdm_partition, long sdm_replication, String sdm_top_value, long topic_id) {
        sdmTopicService.updateSdmTopicInfo(sdm_top_name, sdm_zk_host, sdm_top_cn_name, sdm_partition, sdm_replication, sdm_top_value, topic_id);
    }

    @RequestMapping("/getTopicList")
    public List<String> getTopicList() {
        return sdmTopicService.getTopicList(PropertyParaValue.getString("kafka_zk_address", "hyshf@beyondsoft.com"));
    }
}
