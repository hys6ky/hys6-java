package hyren.serv6.b.realtimecollection.realtimeCollectManagement.userpermission;

import fd.ng.core.annotation.Return;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@Slf4j
@RequestMapping("/dataCollectionM/userpermission")
@RestController
@Api
@Validated
public class UserPermissionController {

    @Autowired
    UserPermissionService userPermissionService;

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "app_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/userApplicationPass")
    public void userApplicationPass(@RequestBody Map<String, Object> req) {
        long app_id = ReqDataUtils.getLongData(req, "app_id");
        userPermissionService.userApplicationPass(app_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "app_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/userApplicationNoPass")
    public void userApplicationNoPass(@RequestBody Map<String, Object> req) {
        long app_id = ReqDataUtils.getLongData(req, "app_id");
        userPermissionService.userApplicationNoPass(app_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/searchUserApplication")
    public Map<String, Object> searchUserApplication(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        return userPermissionService.searchUserApplication(currPage, pageSize);
    }
}
