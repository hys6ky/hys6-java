package hyren.serv6.b.batchcollection.semiStructuredAgent.collectconf;

import hyren.serv6.base.entity.ObjectCollect;
import hyren.serv6.base.entity.ObjectCollectTask;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
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
@Slf4j
@RequestMapping("/dataCollectionO/semiStructuredAgent")
@Api(value = "", tags = "")
@Validated
public class CollectConfController {

    @Autowired
    public CollectConfService collectConfService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getInitObjectCollectConf")
    @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> getInitObjectCollectConf(@NotNull Long agent_id) {
        return collectConfService.getInitObjectCollectConf(agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAddObjectCollectConf")
    @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> getAddObjectCollectConf(@NotNull Long agent_id) {
        return collectConfService.getAddObjectCollectConf(agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getObjectCollectConfById")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> getObjectCollectConfById(@NotNull Long odc_id) {
        return collectConfService.getObjectCollectConfById(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/viewTable")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "is_dictionary", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "data_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "file_suffix", value = "", dataTypeClass = String.class) })
    public List<ObjectCollectTask> viewTable(@NotNull Long agent_id, @NotNull String file_path, String is_dictionary, String data_date, String file_suffix) {
        return collectConfService.viewTable(agent_id, file_path, is_dictionary, data_date, file_suffix);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveObjectCollect")
    @ApiImplicitParam(name = "object_collect", value = "", dataTypeClass = ObjectCollect.class)
    public long saveObjectCollect(ObjectCollect object_collect) {
        return collectConfService.saveObjectCollect(object_collect);
    }
}
