package hyren.serv6.f.dataRegister.source.dbconf;

import hyren.serv6.base.entity.CollectJobClassify;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import io.swagger.annotations.Api;
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

@RestController
@Api("配置源DB属性")
@RequestMapping("/dataRegister/agent/dbconf")
@Validated
public class DBConfController {

    @Autowired
    DBConfStepService dbConfService;

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getDatabaseInfo")
    public List<Object> getDatabaseInfo() {
        return dbConfService.getDatabaseInfo();
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
    @ApiImplicitParam(name = "databaseSet", value = "", dataTypeClass = DatabaseSet.class)
    @RequestMapping("/testConnection")
    public void testConnection(@NotNull DatabaseSet databaseSet) {
        dbConfService.testConnection(databaseSet);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "db_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "port", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getDBConnectionProp")
    public DBConnectionProp getDBConnectionProp(@NotNull String db_name, @RequestParam(defaultValue = "") String port) {
        return dbConfService.getDBConnectionProp(db_name, port);
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
}
