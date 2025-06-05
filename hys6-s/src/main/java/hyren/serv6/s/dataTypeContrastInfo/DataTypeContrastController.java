package hyren.serv6.s.dataTypeContrastInfo;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataTypeContrastInfo")
@Api(tags = "")
public class DataTypeContrastController {

    @Autowired
    private DataTypeContrastService dataType;

    @ApiOperation(value = "")
    @RequestMapping("/searchContrastTypeInfo")
    public Result searchContrastTypeInfo() {
        return dataType.searchContrastTypeInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "db_name1", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "db_name2", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/getDataTypeMsg")
    public List<Map<String, Object>> getDataTypeMsg(String db_name1, String db_name2) {
        return dataType.getDataTypeMsg(db_name1, db_name2);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/generateExcel")
    public void generateExcel(String fileName) {
        dataType.generateExcel(fileName);
    }

    @ApiOperation(value = "")
    @RequestMapping("/downloadFile")
    public void downloadFile(String fileName) {
        dataType.downloadFile(fileName);
    }
}
