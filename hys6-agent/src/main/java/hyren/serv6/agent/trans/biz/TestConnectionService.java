package hyren.serv6.agent.trans.biz;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.bean.StoreConnectionBean;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.bean.DbConfBean;
import hyren.serv6.commons.collection.bean.JDBCBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import org.springframework.stereotype.Service;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Service
@DocClass(desc = "", author = "xchao", createdate = "2019-09-05 11:18")
public class TestConnectionService {

    @Method(desc = "", logicStep = "")
    @Param(name = "dbSet", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public boolean testConn(JDBCBean sourceDataConfBean) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(sourceDataConfBean)) {
            return db.isConnected();
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbSet", desc = "", range = "", isBean = true)
    @Param(name = "pageSql", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean testParallelSQL(JDBCBean jdbcBean, String pageSql) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(jdbcBean)) {
            pageSql = pageSql.replace(Constant.PARALLEL_SQL_START, "0").replace(Constant.PARALLEL_SQL_END, "1");
            String countSQL = "select count(1) as count from ( " + pageSql + " ) tmp";
            ResultSet resultSet = db.queryGetResultSet(countSQL);
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount = resultSet.getInt("count");
            }
            return rowCount != 0;
        } catch (SQLException e) {
            throw new AppSystemException(e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbSet", desc = "", range = "", isBean = true)
    @Param(name = "tableName", desc = "", range = "", nullable = true)
    @Param(name = "sql", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public String getTableCount(JDBCBean jdbcBean, String tableName, String sql) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(jdbcBean)) {
            String countSQL;
            if (StringUtil.isNotBlank(tableName)) {
                countSQL = "select count(1) as count from " + tableName;
            } else {
                countSQL = "select count(1) as count from (" + sql + ") tmp";
            }
            ResultSet resultSet = db.queryGetResultSet(countSQL);
            long rowCount = 0;
            while (resultSet.next()) {
                rowCount = resultSet.getLong("count");
            }
            return String.valueOf(rowCount);
        } catch (SQLException e) {
            throw new AppSystemException(e);
        }
    }
}
