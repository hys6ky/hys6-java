package hyren.serv6.agent.trans.biz.semistructured;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Api("接收页面定义的参数执行object采集")
@RestController
@RequestMapping("/semistructured")
public class ObjectCollectJobController {

    @Autowired
    public ObjectCollectJobService collectJobService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/execute")
    @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class)
    public String execute(String taskInfo) {
        return collectJobService.execute(taskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/executeImmediately")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etlDate", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "taskInfo", value = "", dataTypeClass = String.class) })
    public String executeImmediately(@NotNull String etlDate, String taskInfo) {
        return collectJobService.executeImmediately(etlDate, taskInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDicTable")
    @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = String.class)
    public String getDicTable(@NotNull String file_path) {
        return collectJobService.getDicTable(file_path);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getFirstLineData")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "file_suffix", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "data_date", value = "", dataTypeClass = String.class) })
    public String getFirstLineData(@NotNull String file_path, String file_suffix, String data_date) {
        return collectJobService.getFirstLineData(file_path, file_suffix, data_date);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAllDicColumns")
    @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = String.class)
    public String getAllDicColumns(@NotNull String file_path) {
        return collectJobService.getAllDicColumns(file_path);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAllHandleType")
    @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = String.class)
    public String getAllHandleType(@NotNull String file_path) {
        return collectJobService.getAllHandleType(file_path);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/writeDictionary")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file_path", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "dictionaryParam", value = "", dataTypeClass = String.class) })
    public void writeDictionary(@NotNull String file_path, @NotNull String dictionaryParam) {
        collectJobService.writeDictionary(file_path, dictionaryParam);
    }
}
