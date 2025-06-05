package hyren.serv6.g.interfaceusemonitor.interfaceuserinfo;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceManagement/interfaceusemonitor/interfaceuseinfo")
public class InterfaceUseInfoController {

    @Autowired
    private InterfaceUseInfoService useInfoService;

    @ApiOperation(value = "", notes = "")
    @ApiResponse(code = 200, message = "")
    @PostMapping("/searchInterfaceInfo")
    public Result searchInterfaceInfo() {
        return useInfoService.searchInterfaceInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "use_valid_date", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = Long.class) })
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchInterfaceInfoByIdOrDate")
    public Result searchInterfaceInfoByIdOrDate(Long user_id, String use_valid_date) {
        return useInfoService.searchInterfaceInfoByIdOrDate(user_id, use_valid_date);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "interface_use_id", value = "", example = "", dataTypeClass = Long.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchInterfaceUseInfoById")
    public Map<String, Object> searchInterfaceUseInfoById(Long interface_use_id) {
        return useInfoService.searchInterfaceUseInfoById(interface_use_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "interface_use_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "use_valid_date", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "start_use_date", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/updateInterfaceUseInfo")
    public void updateInterfaceUseInfo(Long interface_use_id, String start_use_date, String use_valid_date) {
        useInfoService.updateInterfaceUseInfo(interface_use_id, start_use_date, use_valid_date);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "interface_use_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "use_state", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/interfaceDisableEnable")
    public void interfaceDisableEnable(Long interface_use_id, String use_state) {
        useInfoService.interfaceDisableEnable(interface_use_id, use_state);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "interface_use_id", value = "", example = "", dataTypeClass = Long.class)
    @GetMapping("/deleteInterfaceUseInfo")
    public void deleteInterfaceUseInfo(Long interface_use_id) {
        useInfoService.deleteInterfaceUseInfo(interface_use_id);
    }
}
