package hyren.serv6.a.sysrole;

import hyren.serv6.a.sysrole.dto.UpdateSysRoleDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;

@Api(tags = "")
@RestController("sysRoleController")
@RequestMapping("/sysRole")
public class SysRoleController {

    @Resource(name = "sysRoleService")
    private SysRoleService sysRoleService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "role_menu", value = "", dataTypeClass = long.class), @ApiImplicitParam(name = "role_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "role_remark", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "is_admin", value = "", dataTypeClass = String.class) })
    @PostMapping("/saveSysRole")
    public void saveSysRole(@RequestBody UpdateSysRoleDTO updateSysRoleDTO) {
        sysRoleService.saveSysRole(updateSysRoleDTO.getRole_menu(), updateSysRoleDTO.getRole_name(), updateSysRoleDTO.getRole_remark(), updateSysRoleDTO.getIs_admin());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "role_id", value = "", dataTypeClass = long.class), @ApiImplicitParam(name = "role_menu", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "role_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "role_remark", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "is_admin", value = "", dataTypeClass = String.class) })
    @PostMapping("/updateSysRole")
    public void updateSysRole(@RequestBody UpdateSysRoleDTO upDto) {
        sysRoleService.updateSysRole(upDto.getRole_id(), upDto.getRole_menu(), upDto.getRole_name(), upDto.getRole_remark(), upDto.getIs_admin());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "role_id", value = "", example = "", dataTypeClass = long.class)
    @PostMapping("/deleteSysRole")
    public void deleteSysRole(long role_id) {
        sysRoleService.deleteSysRole(role_id);
    }
}
