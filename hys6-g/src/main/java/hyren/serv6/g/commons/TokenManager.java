package hyren.serv6.g.commons;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.g.bean.TokenModel;

@DocClass(desc = "", author = "dhw", createdate = "2020/3/30 16:47")
public interface TokenManager {

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "user_password", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    TokenModel createToken(DatabaseWrapper db, Long user_id, String pwd);

    @Method(desc = "", logicStep = "")
    @Param(name = "token", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    boolean checkToken(DatabaseWrapper db, String token);

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "token", desc = "", range = "")
    void deleteToken(DatabaseWrapper db, String token);
}
