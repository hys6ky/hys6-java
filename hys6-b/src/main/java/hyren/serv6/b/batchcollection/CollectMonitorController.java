package hyren.serv6.b.batchcollection;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RestController
@Api("采集首页的监控信息")
@Slf4j
@RequestMapping("/dataCollectionO")
@Validated
public class CollectMonitorController {

    @Autowired
    CollectMonitorService collectMonitorService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAgentNumAndSourceNum")
    public Map<String, Object> getAgentNumAndSourceNum() {
        return collectMonitorService.getAgentNumAndSourceNum();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDatabaseSet")
    public List<Map<String, Object>> getDatabaseSet() {
        return collectMonitorService.getDatabaseSet();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDataCollectInfo")
    public List<Map<String, Object>> getDataCollectInfo() {
        return collectMonitorService.getDataCollectInfo().toList();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getHoStoryCollect")
    public List<Map<String, String>> getHoStoryCollect() {
        return collectMonitorService.getHoStoryCollect();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getCurrentTaskJob")
    @ApiImplicitParam(name = "database_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> getCurrentTaskJob(@NotNull long database_id) {
        return collectMonitorService.getCurrentTaskJob(database_id);
    }
}
