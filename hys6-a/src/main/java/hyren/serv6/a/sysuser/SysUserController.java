package hyren.serv6.a.sysuser;

import hyren.serv6.base.entity.SysUser;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Nullable;
import javax.annotation.Resource;
import java.util.Map;

@Api(tags = "")
@RestController()
@RequestMapping("/sysUser")
public class SysUserController {

    @Resource(name = "sysUserService")
    private SysUserService sysUserService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sysUser", value = "", example = "", dataTypeClass = SysUser.class)
    @PostMapping("/saveSysUser")
    public void saveSysUser(SysUser sysUser) {
        sysUserService.saveSysUser(sysUser);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sys_user", value = "", example = "", dataTypeClass = SysUser.class)
    @PostMapping("/updateSysUser")
    public void updateSysUser(SysUser sys_user) {
        sysUserService.updateSysUser(sys_user);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "user_id", value = "", example = "", defaultValue = "5000", dataTypeClass = long.class)
    @PostMapping("/deleteSysUser")
    public void deleteSysUser(@Nullable long user_id) {
        sysUserService.deleteSysUser(user_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "userId", value = "", example = "", dataTypeClass = long.class)
    @PostMapping("/editSysUserFunction")
    public Map<String, Object> editSysUserFunction(long userId) {
        return sysUserService.editSysUserFunction(userId);
    }
}
