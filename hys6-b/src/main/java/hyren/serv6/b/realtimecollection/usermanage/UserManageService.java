package hyren.serv6.b.realtimecollection.usermanage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.MenuType;
import hyren.serv6.base.entity.ComponentMenu;
import hyren.serv6.base.entity.RoleMenu;
import hyren.serv6.base.entity.SysRole;
import hyren.serv6.base.entity.SysUser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;

@DocClass(desc = "", author = "yec", createdate = "2021-4-2")
@Slf4j
@Service
public class UserManageService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<SysUser> searchUser() {
        return Dbo.queryList(SysUser.class, "SELECT distinct t1.* FROM " + SysUser.TableName + " t1" + " JOIN " + SysRole.TableName + " t2 on t1.role_id = t2.role_id" + " JOIN " + RoleMenu.TableName + " t3 ON t2.role_id = t3.role_id " + " JOIN " + ComponentMenu.TableName + " t4 ON t3.menu_id = t4.menu_id where menu_type = ?", MenuType.CaoZhuoYuan.getCode());
    }
}
