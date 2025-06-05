package hyren.serv6.gateway.auth.service;

import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemRuntimeException;
import hyren.daos.base.web.UserInfoService;
import hyren.serv6.base.entity.ComponentMenu;
import hyren.serv6.base.entity.RoleMenu;
import hyren.serv6.base.entity.SysUser;
import hyren.serv6.base.user.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;

@Configuration
@Component
@Slf4j
public class UserInfoServiceImp implements UserInfoService {

    public User loadUserByUsername(String userIdString) {
        long userId = Long.parseLong(userIdString);
        SqlOperator.Assembler asmSql1 = SqlOperator.Assembler.newInstance();
        asmSql1.clean();
        asmSql1.addSql("select * from " + SysUser.TableName + " where user_id = ?");
        asmSql1.addParam(userId);
        String sql = asmSql1.sql();
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            Map<String, Object> userMapper = SqlOperator.queryOneObject(db, sql, userId);
            if (userMapper != null) {
                User.UserBuilder builder = User.builder();
                builder.userId(getValue(userMapper, "user_id", Long.class, userId));
                builder.roleId(Long.parseLong(String.valueOf(userMapper.get("role_id"))));
                builder.userEmail(getValue(userMapper, "user_email", String.class, null));
                builder.userMobile(getValue(userMapper, "user_mobile", String.class, null));
                builder.userType(getValue(userMapper, "user_type", String.class, "02"));
                builder.loginIp(getValue(userMapper, "login_ip", String.class, null));
                builder.loginDate(getValue(userMapper, "login_date", String.class, null));
                builder.userState(getValue(userMapper, "user_state", String.class, null));
                builder.depId(getValue(userMapper, "dep_id", Long.class, null));
                builder.depName(getValue(userMapper, "dep_name", String.class, null));
                builder.roleName(getValue(userMapper, "role_name", String.class, null));
                builder.userTypeGroup(getValue(userMapper, "user_type_group", String.class, null));
                builder.is_login(getValue(userMapper, "is_login", String.class, null));
                builder.limitMultiLogin(getValue(userMapper, "limitmultilogin", String.class, null));
                builder.nickname(getValue(userMapper, "user_name", String.class, null));
                User user = builder.build();
                user.setUsername(userIdString);
                user.setPassword(getValue(userMapper, "user_password", String.class, null));
                user.setResourceList(getUserAuthMenu(db, user));
                user.setAvailable(true);
                return user;
            }
        } catch (Exception e) {
            throw new SystemRuntimeException("用户信息获取失败!!!", e);
        }
        return null;
    }

    public static <K, V> V getValue(Map<K, Object> map, K key, Class<V> clazz, V defaultValue) {
        if (map != null) {
            if (map.get(key) != null && clazz.isInstance(map.get(key))) {
                return clazz.cast(map.get(key));
            }
        }
        return defaultValue;
    }

    private List<String> getUserAuthMenu(DatabaseWrapper db, User user) {
        return SqlOperator.queryOneColumnList(db, "select t2.menu_path from " + RoleMenu.TableName + " t1 join " + ComponentMenu.TableName + " t2 on  t1.menu_id" + " = t2.menu_id  where role_id = ? ", user.getRoleId());
    }
}
