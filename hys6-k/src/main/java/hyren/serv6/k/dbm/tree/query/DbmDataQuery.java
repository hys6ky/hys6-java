package hyren.serv6.k.dbm.tree.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.user.User;
import hyren.serv6.k.entity.DbmSortInfo;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/2/16 0016 下午 05:42")
public class DbmDataQuery {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Result getDbmSortInfos(User user) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DbmSortInfo.TableName + " where 1=1 ");
        return Dbo.queryResult(asmSql.sql(), asmSql.params());
    }
}
