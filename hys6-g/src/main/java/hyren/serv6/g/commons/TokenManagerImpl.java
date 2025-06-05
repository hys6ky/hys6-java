package hyren.serv6.g.commons;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.SysUser;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.g.bean.TokenModel;
import org.apache.commons.codec.digest.DigestUtils;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2020/3/30 17:51")
public class TokenManagerImpl implements TokenManager {

    private static final Long ENDTIME = System.currentTimeMillis();

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "token", desc = "", range = "")
    @Override
    public TokenModel createToken(DatabaseWrapper db, Long user_id, String pwd) {
        String defaultToken = TokenManagerImpl.getDefaultToken(db, user_id, pwd);
        TokenModel model;
        if (defaultToken.equals(IsFlag.Fou.getCode())) {
            String token = DigestUtils.md5Hex(user_id + pwd + System.currentTimeMillis());
            model = new TokenModel(user_id, token);
            create(db, user_id, token);
        } else {
            model = new TokenModel(user_id, defaultToken);
        }
        return model;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean checkToken(DatabaseWrapper db, String token) {
        TokenManagerImpl.isTokenExist(db, token);
        String oldValidTime = TokenManagerImpl.getValidTime(db, token);
        if (ENDTIME < Long.parseLong(oldValidTime)) {
            TokenManagerImpl.updateValidTime(db, token);
            return true;
        } else {
            return false;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    @Override
    public void deleteToken(DatabaseWrapper db, String token) {
        updateTokenToDefault(db, token);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "token", desc = "", range = "")
    public void create(DatabaseWrapper db, Long user_id, String token) {
        SysUser user = new SysUser();
        user.setToken(token);
        user.setValid_time(String.valueOf(System.currentTimeMillis() + 2 * 60 * 60 * 1000));
        user.setUser_id(user_id);
        user.update(db);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    private static void updateValidTime(DatabaseWrapper db, String token) {
        SqlOperator.execute(db, "update " + SysUser.TableName + "  set valid_time=? " + "WHERE token=?", System.currentTimeMillis() + 2 * 60 * 60 * 1000, token);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    private static void updateTokenToDefault(DatabaseWrapper db, String token) {
        SqlOperator.execute(db, "update " + SysUser.TableName + " set token = ?" + " WHERE token = ?", IsFlag.Fou.getCode(), token);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    private static void isTokenExist(DatabaseWrapper db, String token) {
        if (SqlOperator.queryNumber(db, "select count(*) FROM " + SysUser.TableName + " WHERE  token = ?", token).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("token值不能为空");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "user_password", desc = "", range = "")
    @Return(desc = "", range = "")
    private static String getDefaultToken(DatabaseWrapper db, Long user_id, String user_password) {
        Map<String, Object> userMap = SqlOperator.queryOneObject(db, "select token,valid_time FROM " + SysUser.TableName + " WHERE user_id=? and user_password=?", user_id, user_password);
        if (!userMap.isEmpty()) {
            String token = userMap.get("token").toString();
            String valid_time = userMap.get("valid_time").toString();
            if (System.currentTimeMillis() > Long.parseLong(valid_time)) {
                return IsFlag.Fou.getCode();
            }
            return token;
        } else {
            return IsFlag.Fou.getCode();
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    @Return(desc = "", range = "")
    private static String getValidTime(DatabaseWrapper db, String token) {
        List<Object> columnList = SqlOperator.queryOneColumnList(db, "select valid_time FROM " + SysUser.TableName + "  WHERE token = ?", token);
        if (!columnList.isEmpty()) {
            return columnList.get(0).toString();
        } else {
            return "";
        }
    }
}
