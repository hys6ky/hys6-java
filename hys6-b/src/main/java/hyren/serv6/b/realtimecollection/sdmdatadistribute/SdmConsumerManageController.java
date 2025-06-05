package hyren.serv6.b.realtimecollection.sdmdatadistribute;

import fd.ng.core.annotation.Return;
import hyren.serv6.base.datatree.tree.Node;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/dataCollectionO/sdmdatadistribute")
@Api
@Validated
public class SdmConsumerManageController {

    @Autowired
    SdmConsumerManageService sdmConsumerManageService;

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getConsumerTopicList")
    public List<String> getConsumerTopicList() {
        return sdmConsumerManageService.getConsumerTopicList();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "app_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/cancelApplicantion")
    public int cancelApplicantion(@NotNull long app_id) {
        return sdmConsumerManageService.cancelApplicantion(app_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "app_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/againApplicantion")
    public int againApplicantion(long app_id) {
        return sdmConsumerManageService.againApplicantion(app_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "topic_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "user_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/applyConsume")
    public void applyConsume(long topic_id, long sdm_receive_id, long user_id) {
        sdmConsumerManageService.applyConsume(topic_id, sdm_receive_id, user_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Long.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/getTopicMessInfo")
    public Map<String, Object> getTopicMessInfo(long sdm_receive_id, @RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        return sdmConsumerManageService.getTopicMessInfo(sdm_receive_id, currPage, pageSize);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getDataPreview")
    public Map<String, Object> getDataPreview(long sdm_receive_id) {
        return sdmConsumerManageService.getDataPreview(sdm_receive_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/consumerTopic")
    public HashMap<String, Object> consumerTopic(long topic_id) {
        return sdmConsumerManageService.consumerTopic(topic_id);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getKAFKATreeData")
    public List<Node> getKAFKATreeData() {
        return sdmConsumerManageService.getKAFKATreeData();
    }
}
