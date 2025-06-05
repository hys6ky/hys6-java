package hyren.serv6.commons.utils.database;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.commons.utils.constant.DataBaseType;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;

@DocClass(desc = "", author = "WangZhengcheng")
public class DatabaseConnUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "db_name", desc = "", range = "")
    @Param(name = "port", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static DBConnectionProp getConnParamInfo(String db_name, String port) {
        String dbName = db_name.toUpperCase();
        return DataBaseType.getDatabase(dbName).getJdbcUrl(port);
    }
}
