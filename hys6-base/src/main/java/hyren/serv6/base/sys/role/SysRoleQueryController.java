package hyren.serv6.base.sys.role;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.MenuType;
import hyren.serv6.base.entity.ComponentMenu;
import hyren.serv6.base.entity.SysPara;
import hyren.serv6.base.entity.SysRole;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Api(tags = "")
@RestController
@RequestMapping("/sysRole")
@Configuration
@DocClass(desc = "", author = "dhw", createdate = "2023-08-23 10:53:08")
public class SysRoleQueryController {

    @ApiOperation(value = "")
    @PostMapping("/getSysRoleInfo")
    @ApiImplicitParam(name = "db", value = "", required = true)
    public Map<String, Object> getSysRoleInfo(DatabaseWrapper db) {
        List<Map<String, Object>> sysRoleList = Dbo.queryList("select * from " + SysRole.TableName + " where is_admin!=? and role_id > 99", MenuType.ChaoJiGuanLiYuan.getCode());
        setRoleMenu(sysRoleList);
        Map<String, Object> sysRoleMap = new HashMap<>();
        sysRoleMap.put("sysRoles", sysRoleList);
        return sysRoleMap;
    }

    @ApiOperation(value = "")
    @PostMapping("/getSysRoleInfoByPage")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", dataTypeClass = Integer.class) })
    public Map<String, Object> getSysRoleInfoByPage(int currPage, int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> sysRoleList = Dbo.queryPagedList(page, "select * from " + SysRole.TableName + " where is_admin!=? and role_id > 99", MenuType.ChaoJiGuanLiYuan.getCode());
        setRoleMenu(sysRoleList);
        Map<String, Object> sysRoleMap = new HashMap<>();
        sysRoleMap.put("sysRoles", sysRoleList);
        sysRoleMap.put("totalSize", page.getTotalSize());
        return sysRoleMap;
    }

    @ApiOperation(value = "")
    @PostMapping("/getUserRole")
    public List<Map<String, Object>> geAllUserRole() {
        return Dbo.queryList("select * from " + SysRole.TableName);
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "role_id", value = "", example = "", dataTypeClass = long.class)
    @PostMapping("/getRoleInfo")
    public Map<String, Object> getRoleInfo(long role_id) {
        Map<String, Object> roleInfo = Dbo.queryOneObject("select * from sys_role where role_id=?", role_id);
        List<Map<String, Object>> menus = Dbo.queryList("select menu_id from role_menu where role_id=?", role_id);
        roleInfo.put("menus", menus);
        return roleInfo;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "userIsAdmin", value = "", example = "", defaultValue = "0", dataTypeClass = SysPara.class)
    @PostMapping("/getUserFunctionMenu")
    public List<Map<String, Object>> getUserFunctionMenu(String userIsAdmin) {
        List<Map<String, Object>> menuList = Dbo.queryList("SELECT * FROM " + ComponentMenu.TableName + " WHERE menu_level='1' AND menu_type = ? ", userIsAdmin);
        for (Map<String, Object> menu : menuList) {
            List<Map<String, Object>> secMenuList = Dbo.queryList("select * from component_menu where parent_id=?", menu.get("menu_id"));
            if (null != secMenuList && !secMenuList.isEmpty()) {
                menu.put("childList", secMenuList);
            }
        }
        return menuList;
    }

    private static void setRoleMenu(List<Map<String, Object>> sysRoleList) {
        for (Map<String, Object> map : sysRoleList) {
            List<Map<String, Object>> roleMenuList = Dbo.queryList("select * from role_menu a " + "join component_menu b on a.menu_id=b.menu_id  where role_id=? and menu_level='1'", map.get("role_id"));
            if (null != roleMenuList && !roleMenuList.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < roleMenuList.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(roleMenuList.get(i).get("menu_name"));
                }
                map.put("role_menu", sb.toString());
            } else {
                map.put("role_menu", "");
            }
        }
    }
}
