package hyren.serv6.commons.hadoop.sqlutils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/25 11:05")
public class HSqlExecute {

    @Method(desc = "", logicStep = "")
    @Param(name = "sqlList", desc = "", range = "")
    @Param(name = "engineName", desc = "", range = "")
    public static void executeSql(List<String> sqlList, String engineName) {
        try (DatabaseWrapper db = Dbo.db(engineName)) {
            for (String sql : sqlList) {
                log.info("执行 " + engineName + " 的sql为： " + sql);
                db.execute(sql);
            }
            db.commit();
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw (BusinessException) e;
            } else {
                throw new AppSystemException(e);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    public static void executeSql(String sql, DatabaseWrapper db) {
        if (!StringUtil.isEmpty(sql)) {
            log.info("执行的sql为: " + sql);
            db.execute(sql);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    public static void executeSql(List<String> sqlList, DatabaseWrapper db) {
        for (String sql : sqlList) {
            if (!StringUtil.isEmpty(sql)) {
                log.info("执行的sql为: " + sql);
                db.execute(sql);
            }
        }
    }
}
