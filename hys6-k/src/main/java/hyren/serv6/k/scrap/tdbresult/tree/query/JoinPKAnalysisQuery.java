package hyren.serv6.k.scrap.tdbresult.tree.query;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import java.util.List;
import java.util.Map;

public class JoinPKAnalysisQuery {

    public static List<Object> getJoinPKAnalysisTableCode(DatabaseWrapper db, String table_code) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        throw new BusinessException("暂未实现！");
    }

    public static List<Map<String, Object>> getJoinPkDataByTableCode(DatabaseWrapper db, String table_code) {
        throw new BusinessException("暂未实现！");
    }
}
