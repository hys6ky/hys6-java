package hyren.serv6.g.usermanage;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.SysUser;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceManagement/userManage")
public class InterfaceUserManageController {

    @Autowired
    private InterfaceUserManageService userManageService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "user_name", value = "", example = "", dataTypeClass = Integer.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/selectUserInfo")
    public Result selectUserInfo(String user_name) {
        return userManageService.selectUserInfo(user_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = long.class)
    @ApiResponse(code = 200, message = "")
    @GetMapping("/selectUserById")
    public Map<String, Object> selectUserById(long user_id) {
        return userManageService.selectUserById(user_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sys_user", value = "", example = "", dataTypeClass = SysUser.class)
    @PostMapping("/addUser")
    public void addUser(SysUser sys_user) {
        userManageService.addUser(sys_user);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = long.class)
    @PostMapping("/deleteUser")
    public void deleteUser(long user_id) {
        userManageService.deleteUser(user_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "user_name", value = ""), @ApiImplicitParam(name = "user_email", value = ""), @ApiImplicitParam(name = "user_password", value = ""), @ApiImplicitParam(name = "user_id", value = ""), @ApiImplicitParam(name = "user_remark", value = "", required = true) })
    @PostMapping("/updateUser")
    public void updateUser(String user_name, String user_email, String user_password, long user_id, String user_remark) {
        userManageService.updateUser(user_name, user_email, user_password, user_id, user_remark);
    }
}
