package hyren.serv6.b.batchcollection.agent.register;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.DatabaseSet;
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

@Slf4j
@RequestMapping("/dataCollectionO/agent/register")
@RestController
@Api(value = "", tags = "")
@Validated
public class ResourceRecodingController {

    @Autowired
    public ResourceRecodingService recodingService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getInitStorageData")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class) })
    public Result getInitStorageData(@NotNull long databaseId, long agent_id) {
        return recodingService.getInitStorageData(databaseId, agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/editStorageData")
    @ApiImplicitParam(name = "databaseId", value = "", dataTypeClass = Long.class)
    public Result editStorageData(@NotNull Long databaseId) {
        return recodingService.editStorageData(databaseId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveRegisterData")
    @ApiImplicitParam(name = "databaseSet", value = "", dataTypeClass = DatabaseSet.class)
    public Long saveRegisterData(DatabaseSet databaseSet) {
        return recodingService.saveRegisterData(databaseSet);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateRegisterData")
    @ApiImplicitParam(name = "databaseSet", value = "", dataTypeClass = DatabaseSet.class)
    public Long updateRegisterData(DatabaseSet databaseSet) {
        return recodingService.updateRegisterData(databaseSet);
    }
}
