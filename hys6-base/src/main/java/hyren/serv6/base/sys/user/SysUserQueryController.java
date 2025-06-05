package hyren.serv6.base.sys.user;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.MenuType;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Api(tags = "")
@RestController
@RequestMapping("/sysUser")
@Configuration
public class SysUserQueryController {

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getAllSysUserWithManager")
    public List<SysUser> getAllSysUserWithManager() {
        Dbo.Assembler asmSql = Dbo.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select su.* from " + SysUser.TableName + " su" + " join " + SysRole.TableName + " sr on sr.role_id = su.role_id" + " where sr.is_admin = ?" + " order by user_id,create_date asc,create_time asc").addParam(MenuType.GuanLiYuan.getCode());
        return Dbo.queryList(SysUser.class, asmSql.sql(), asmSql.params());
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getAllSysUserWithOperator")
    public List<SysUser> getAllSysUserWithOperator() {
        Dbo.Assembler asmSql = Dbo.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select su.* from " + SysUser.TableName + " su" + " join " + SysRole.TableName + " sr on sr.role_id = su.role_id" + " where sr.is_admin = ?" + " order by user_id,create_date asc,create_time asc").addParam(MenuType.CaoZhuoYuan.getCode());
        return Dbo.queryList(SysUser.class, asmSql.sql(), asmSql.params());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @PostMapping("/getSysUserWithOutAdminByPage")
    public Map<String, Object> getSysUserWithOutAdminByPage(int currPage, int pageSize) {
        Dbo.Assembler asmSql = Dbo.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + SysUser.TableName + " su" + " join " + SysRole.TableName + " sr on sr.role_id = su.role_id" + " where sr.is_admin != ?" + " order by user_id,create_date asc,create_time asc").addParam(MenuType.ChaoJiGuanLiYuan.getCode());
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<SysUser> sysUsers = Dbo.queryPagedList(SysUser.class, page, asmSql.sql(), asmSql.params());
        Map<String, Object> sysUserMap = new HashMap<>();
        sysUserMap.put("sysUsers", sysUsers);
        sysUserMap.put("totalSize", page.getTotalSize());
        return sysUserMap;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = Long.class)
    @PostMapping("/getSysUserByUserId")
    public SysUser getSysUserByUserId(Long user_id) {
        if (checkSysUserIsNotExist(user_id)) {
            throw new BusinessException("查询不存在的用户id，user_id=" + user_id);
        }
        return Dbo.queryOneObject(SysUser.class, "select * from " + SysUser.TableName + " where user_id = ?", user_id).orElseThrow(() -> new BusinessException("根据用户id获取用户信息的SQL失败!"));
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "menu_type", value = "", example = "", defaultValue = "0", dataTypeClass = String.class)
    @PostMapping("/getUserFunctionMenu")
    public List<Map<String, Object>> getUserFunctionMenu(@Nullable String menu_type) {
        List<Map<String, Object>> menuList = Dbo.queryList("SELECT cm.* FROM " + ComponentMenu.TableName + " cm" + " WHERE cm.menu_level='1' AND cm.menu_type = ? ", menu_type);
        for (Map<String, Object> menu : menuList) {
            List<Map<String, Object>> secMenuList = Dbo.queryList("select * from component_menu where parent_id=?", menu.get("menu_id"));
            if (null != secMenuList && !secMenuList.isEmpty()) {
                menu.put("childList", secMenuList);
            }
        }
        return menuList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    private boolean checkSysUserIsNotExist(long user_id) {
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + SysUser.TableName + " WHERE user_id = ?", user_id).orElseThrow(() -> new BusinessException("检查系统用户否存在的SQL编写错误")) != 1;
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getAllSysUserByDepartmentInfo")
    public List<SysUser> getAllSysUserByDepartmentInfo() {
        return Dbo.queryList(SysUser.class, "select * from " + SysUser.TableName + " where dep_id = ? order by user_id, create_date asc, create_time asc", UserUtil.getUser().getDepId());
    }
}
