package hyren.serv6.a.sysrole;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.SysRole;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

@Service
@DocClass(desc = "", author = "Mr.Lee")
public class SysRoleService {

    @Method(desc = "", logicStep = "")
    @Param(name = "role_menu", desc = "", range = "")
    @Param(name = "role_name", desc = "", range = "")
    @Param(name = "role_remark", desc = "", range = "", valueIfNull = "")
    @Param(name = "is_admin", desc = "", range = "")
    @Return(desc = "", range = "")
    public void saveSysRole(long[] role_menu, String role_name, String role_remark, String is_admin) {
        OptionalLong aLong = Dbo.queryNumber("select count(*) from sys_role where role_name=?", role_name);
        if (aLong.isPresent()) {
            if (aLong.getAsLong() > 0) {
                throw new BusinessException("角色名称已存在");
            }
        }
        SysRole sysRole = new SysRole();
        long role_id = PrimayKeyGener.getNextId();
        sysRole.setRole_id(role_id);
        sysRole.setRole_name(role_name);
        sysRole.setRole_remark(role_remark);
        sysRole.setIs_admin(is_admin);
        sysRole.add(Dbo.db());
        List<Object[]> params = new ArrayList<>();
        for (long roleMenu : role_menu) {
            Object[] param = new Object[] { role_id, roleMenu };
            params.add(param);
        }
        Dbo.executeBatch("insert into role_menu(role_id,menu_id) VALUES (?,?)", params);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "role_id", desc = "", range = "")
    @Param(name = "role_menu", desc = "", range = "")
    @Param(name = "role_name", desc = "", range = "")
    @Param(name = "role_remark", desc = "", range = "", valueIfNull = "")
    @Param(name = "is_admin", desc = "", range = "")
    @Return(desc = "", range = "")
    public void updateSysRole(long role_id, long[] role_menu, String role_name, String role_remark, String is_admin) {
        OptionalLong aLong = Dbo.queryNumber("select count(*) from sys_role where role_name=? and role_id!=?", role_name, role_id);
        if (aLong.isPresent()) {
            if (aLong.getAsLong() > 0) {
                throw new BusinessException("角色名称已存在");
            }
        }
        SysRole sysRole = new SysRole();
        sysRole.setRole_id(role_id);
        sysRole.setRole_name(role_name);
        sysRole.setRole_remark(role_remark);
        sysRole.setIs_admin(is_admin);
        sysRole.update(Dbo.db());
        Dbo.execute("delete from role_menu where role_id=?", role_id);
        List<Object[]> params = new ArrayList<>();
        for (long roleMenu : role_menu) {
            Object[] param = new Object[] { role_id, roleMenu };
            params.add(param);
        }
        Dbo.executeBatch("insert into role_menu(role_id,menu_id) VALUES (?,?)", params);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "role_id", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    public void deleteSysRole(long role_id) {
        OptionalLong aLong = Dbo.queryNumber("select count(*) from sys_user where role_id=?", role_id);
        if (aLong.isPresent()) {
            if (aLong.getAsLong() > 0) {
                throw new BusinessException("已有用户绑定该角色，请先删除用户");
            }
        }
        DboExecute.deletesOrThrow("删除角色失败!，role_id=" + role_id, "DELETE FROM " + SysRole.TableName + " WHERE role_id = ? ", role_id);
        Dbo.execute("delete from role_menu where role_id=?", role_id);
    }
}
