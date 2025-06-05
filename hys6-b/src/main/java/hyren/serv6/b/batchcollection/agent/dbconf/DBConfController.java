package hyren.serv6.b.batchcollection.agent.dbconf;

import hyren.serv6.base.entity.CollectJobClassify;
import hyren.serv6.base.entity.DatabaseSet;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RestController
@Api("配置源DB属性")
@RequestMapping("/dataCollectionO/agent/dbconf")
@Validated
public class DBConfController {

    @Autowired
    DBConfStepService dbConfService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "databaseId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getDBConfInfo")
    public List<Map<String, Object>> getDBConfInfo(@NotNull Long databaseId) {
        return dbConfService.getDBConfInfo(databaseId).toList();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "databaseId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/addDBConfInfo")
    public List<Map<String, Object>> addDBConfInfo(@NotNull Long databaseId, @NotNull Long agent_id) {
        return dbConfService.addDBConfInfo(databaseId, agent_id).toList();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "agentId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getHisConnection")
    public List<Map<String, Object>> getHisConnection(@NotNull Long agentId) {
        return dbConfService.getHisConnection(agentId).toList();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "classifyId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/checkClassifyId")
    public boolean checkClassifyId(@NotNull Long classifyId) {
        return dbConfService.checkClassifyId(classifyId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getClassifyInfo")
    public List<CollectJobClassify> getClassifyInfo(@NotNull Long sourceId) {
        return dbConfService.getClassifyInfo(sourceId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "classify", value = "", dataTypeClass = CollectJobClassify.class), @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/saveClassifyInfo")
    public void saveClassifyInfo(CollectJobClassify classify, @NotNull Long sourceId) {
        dbConfService.saveClassifyInfo(classify, sourceId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "classify", value = "", dataTypeClass = CollectJobClassify.class), @ApiImplicitParam(name = "sourceId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/updateClassifyInfo")
    public void updateClassifyInfo(CollectJobClassify classify, @NotNull Long sourceId) {
        dbConfService.updateClassifyInfo(classify, sourceId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "classifyId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteClassifyInfo")
    public void deleteClassifyInfo(@NotNull Long classifyId) {
        dbConfService.deleteClassifyInfo(classifyId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "databaseSet", value = "", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/saveDbConf")
    public long saveDbConf(@NotNull @RequestBody DatabaseSet databaseSet) {
        return dbConfService.saveDbConf(databaseSet);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "databaseSet", value = "", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/testConnection")
    public void testConnection(@NotNull DatabaseSet databaseSet) {
        dbConfService.testConnection(databaseSet);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agentId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "readNum", value = "", dataTypeClass = Integer.class) })
    @RequestMapping("/viewLog")
    public String viewLog(@NotNull Long agentId, @RequestParam(defaultValue = "100") Integer readNum) {
        return dbConfService.viewLog(agentId, readNum);
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getDatabaseInfo")
    public List<Object> getDatabaseInfo() {
        return dbConfService.getDatabaseInfo();
    }
}
