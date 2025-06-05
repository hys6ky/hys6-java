package hyren.serv6.b.batchcollection.agent.database;

import hyren.serv6.base.entity.DatabaseSet;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RestController
@Api("数据库采集管理类")
@RequestMapping("/dataCollectionO/agent/database")
@Validated
public class DatabaseSettingController {

    @Autowired
    DatabaseSettingService databaseSettingService;

    @RequestMapping("/getInitDatabase")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", type = "不可为空", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", type = "不可为空", value = "", dataTypeClass = Long.class) })
    public List<Map<String, Object>> getInitDatabase(@NotNull Long source_id, @NotNull Long agent_id) {
        return databaseSettingService.getInitDatabase(source_id, agent_id).toList();
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "databaseId", value = "", type = "不为空", dataTypeClass = Long.class)
    @RequestMapping("/editorDatabase")
    public String editorDatabase(@NotNull Long databaseId) {
        return databaseSettingService.editorDatabase(databaseId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "databaseId", value = "", type = "不为空", dataTypeClass = Long.class)
    @RequestMapping("/editorDatabaseSSCJ")
    public String editorDatabaseSSCJ(@NotNull Long databaseId) {
        return databaseSettingService.editorDatabaseSSCJ(databaseId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "databaseSet", value = "", type = "不可以为空", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/saveDatabaseInfo")
    public Long saveDatabaseInfo(@NotNull DatabaseSet databaseSet) {
        return databaseSettingService.saveDatabaseInfo(databaseSet);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "databaseSet", value = "", type = "不可以为空", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/updateDatabaseInfo")
    public Long updateDatabaseInfo(@NotNull DatabaseSet databaseSet) {
        return databaseSettingService.updateDatabaseInfo(databaseSet);
    }

    @RequestMapping("/getInitDatabaseSSCJ")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", type = "不可为空", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", type = "不可为空", value = "", dataTypeClass = Long.class) })
    public List<Map<String, Object>> getInitDatabaseSSCJ(@NotNull Long source_id, @NotNull Long agent_id) {
        return databaseSettingService.getInitDatabaseSSCJ(source_id, agent_id).toList();
    }
}
