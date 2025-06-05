package hyren.serv6.g.serviceuser;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceService/serviceuser")
public class ServiceUserController {

    @Autowired
    private ServiceUserService userService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sysreg_name", value = "", example = "", dataTypeClass = String.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchDataTableInfo")
    public Result searchDataTableInfo(String sysreg_name) {
        return userService.searchDataTableInfo(sysreg_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "interface_name", value = "", example = "", dataTypeClass = String.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchInterfaceInfo")
    public Result searchInterfaceInfo(String interface_name) {
        return userService.searchInterfaceInfo(interface_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiResponse(code = 200, message = "")
    @GetMapping("/getIpAndPort")
    public String getIpAndPort() {
        return userService.getIpAndPort();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "use_id", value = "", example = "", dataTypeClass = Long.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchColumnInfoById")
    public Result searchColumnInfoById(long use_id) {
        return userService.searchColumnInfoById(use_id);
    }
}
