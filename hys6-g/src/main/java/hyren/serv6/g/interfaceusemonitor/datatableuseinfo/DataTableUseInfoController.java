package hyren.serv6.g.interfaceusemonitor.datatableuseinfo;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceManagement/interfaceusemonitor/datatableuseinfo")
public class DataTableUseInfoController {

    @Autowired
    private DataTableUseInfoService useInfoService;

    @ApiOperation(value = "", notes = "")
    @ApiResponse(code = 200, message = "")
    @PostMapping("/searchTableData")
    public Result searchTableData() {
        return useInfoService.searchTableData();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = Long.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchTableDataById")
    public Result searchTableDataById(Long user_id) {
        return useInfoService.searchTableDataById(user_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "use_id", value = "", example = "", dataTypeClass = Long.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchFieldInfoById")
    public Result searchFieldInfoById(Long use_id) {
        return useInfoService.searchFieldInfoById(use_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "use_id", value = "", example = "", dataTypeClass = Long.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/deleteDataTableUseInfo")
    public void deleteDataTableUseInfo(Long use_id) {
        useInfoService.deleteDataTableUseInfo(use_id);
    }
}
