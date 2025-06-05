package hyren.serv6.b.batchcollection.dbAgentcollect;

import hyren.serv6.base.entity.DatabaseSet;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataCollectionO/dbAgentcollect")
@Validated
public class CollectFileController {

    @Autowired
    CollectFileService collectFileService;

    @RequestMapping("/getInitDataFileData")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class) })
    public Map<String, Object> getInitDataFileData(@NotNull Long colSetId, @NotNull Long agent_id) {
        return collectFileService.getInitDataFileData(colSetId, agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/addDataFileData")
    public Map<String, Object> addDataFileData(@NotNull Long source_id, @NotNull Long agent_id) {
        return collectFileService.addDataFileData(source_id, agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "database_set", value = "", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/saveDataFile")
    public String saveDataFile(DatabaseSet database_set) {
        return collectFileService.saveDataFile(database_set);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "database_set", value = "", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/updateDataFile")
    public String updateDataFile(DatabaseSet database_set) {
        return collectFileService.updateDataFile(database_set);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "path", value = "", dataTypeClass = String.class) })
    @RequestMapping("/selectPath")
    public List<Map> selectPath(@NotNull Long agent_id, @RequestParam(defaultValue = "") String path) {
        return collectFileService.selectPath(agent_id, path);
    }
}
