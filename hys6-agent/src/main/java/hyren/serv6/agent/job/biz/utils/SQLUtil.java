package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.Constant;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@DocClass(desc = "", author = "WangZhengcheng")
public class SQLUtil {

    public static boolean objectIfExistForOracle(String indexName, String tableName, DatabaseWrapper db) {
        ResultSet resultSet = null;
        try {
            resultSet = db.queryGetResultSet("SELECT * FROM USER_IND_COLUMNS WHERE LOWER(INDEX_NAME) = '" + indexName.toLowerCase() + "'");
            try {
                return resultSet.next();
            } catch (SQLException e) {
                throw new AppSystemException("检查数据库下表是否存在出现异常，请联系管理员", e);
            }
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean pkIfExistForPostgresql(String tableName, DatabaseWrapper db) {
        ResultSet resultSet = null;
        try {
            resultSet = db.queryGetResultSet("SELECT " + " pg_constraint.conname AS pk_name " + " FROM " + " pg_constraint " + " INNER JOIN " + " pg_class " + " ON " + " pg_constraint.conrelid = pg_class.oid " + " WHERE " + " pg_class.relname = ? " + " AND pg_constraint.contype= ? ", tableName, "p");
            try {
                return resultSet.next();
            } catch (SQLException e) {
                throw new AppSystemException("检查数据库下表是否存在出现异常，请联系管理员", e);
            }
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean pkIfExistForDB2(String tableName, DatabaseWrapper db) {
        ResultSet resultSet = null;
        try {
            resultSet = db.queryGetResultSet("select * from syscat.indexes" + " where tabname = upper(?)" + " AND UNIQUERULE = ?", tableName, "P");
            try {
                return resultSet.next();
            } catch (SQLException e) {
                throw new AppSystemException("检查数据库下表主键是否存在出现异常，请联系管理员", e);
            }
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static boolean indexIfExistForPostgresql(String tableName, DatabaseWrapper db) {
        ResultSet resultSet = null;
        try {
            resultSet = db.queryGetResultSet("select * from pg_indexes where tablename= ? ", tableName);
            try {
                return resultSet.next();
            } catch (SQLException e) {
                throw new AppSystemException("检查数据库下表是否存在出现异常，请联系管理员", e);
            }
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void getSqlCond(List<String> columns, List<String> types, List<String> tarTypes, StringBuilder sql, Dbtype dbType) {
        columns = dbType.ofEscapedkey(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (!Constant.HYRENFIELD.contains(columns.get(i).toUpperCase())) {
                if (!tarTypes.isEmpty() && StringUtil.isNotBlank(tarTypes.get(i)) && !"NULL".equalsIgnoreCase(tarTypes.get(i))) {
                    sql.append(columns.get(i)).append(" ").append(tarTypes.get(i)).append(",");
                } else {
                    sql.append(columns.get(i)).append(" ").append(types.get(i)).append(",");
                }
            } else {
                sql.append(columns.get(i)).append(" ").append(types.get(i)).append(",");
            }
        }
    }
}
