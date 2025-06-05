package hyren.serv6.f.dataRegister.source.tableconf;

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
import java.util.Map;

@RestController
@Api("定义表抽取属性")
@Validated
@RequestMapping("/dataRegister/agent/isolate")
public class CollTbConfStepController {

    @Autowired
    CollTbConfStepService collTbConfStepService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "inputString", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getTableInfo")
    public List<Map<String, Object>> getTableInfo(@NotNull Long colSetId, @NotNull String inputString) {
        return collTbConfStepService.getTableInfo(colSetId, inputString);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getAllTableInfo")
    public List<Map<String, Object>> getAllTableInfo(@NotNull Long colSetId) {
        return collTbConfStepService.getAllTableInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getColumnInfo")
    public Map<String, Object> getColumnInfo(String tableName, @NotNull Long colSetId, @RequestParam(defaultValue = "999999") Long tableId) {
        return collTbConfStepService.getColumnInfo(tableName, colSetId, tableId);
    }
}
