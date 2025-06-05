package hyren.serv6.m.main.metaUtil;

import com.zaxxer.hikari.pool.HikariProxyDatabaseMetaData;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.DBException;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.meta.ColumnMeta;
import fd.ng.db.meta.TableMeta;
import hyren.serv6.m.main.entity.ColumnMetaVo;
import hyren.serv6.m.main.entity.TableMetaVo;
import lombok.extern.slf4j.Slf4j;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MetaOperatorCustomize {

    private final static String[] viewType = new String[] { "VIEW" };

    private final static String[] tableType = new String[] { "TABLE" };

    private final static String[] mviewType = new String[] { "MATERIALIZED VIEW" };

    public static List<TableMetaVo> getViewMetaInfo(DatabaseWrapper db, String viewNamePattern) {
        return getViewOrTableMetaInfo(db, viewNamePattern, viewType, false);
    }

    public static List<TableMetaVo> getTableMetaInfo(DatabaseWrapper db, String viewNamePattern) {
        return getViewOrTableMetaInfo(db, viewNamePattern, tableType, false);
    }

    public static List<TableMetaVo> getMeterViewMetaInfo(DatabaseWrapper db, String mViewNamePattern) {
        return getViewOrTableMetaInfo(db, mViewNamePattern, mviewType, false);
    }

    public static List<TableMetaVo> getViewAndColMetaInfo(DatabaseWrapper db, String viewNamePattern) {
        return getViewOrTableMetaInfo(db, viewNamePattern, viewType, true);
    }

    public static List<TableMetaVo> getTableAndColMetaInfo(DatabaseWrapper db, String viewNamePattern) {
        return getViewOrTableMetaInfo(db, viewNamePattern, tableType, true);
    }

    public static List<TableMetaVo> getMeterViewAndColMetaInfo(DatabaseWrapper db, String mViewNamePattern) {
        return getViewOrTableMetaInfo(db, mViewNamePattern, mviewType, true);
    }

    @Deprecated
    public static List<TableMeta> getFuncMetaInfo(DatabaseWrapper db, String funcNamePattern) {
        return null;
    }

    public static List<FunctionMeta> getProcMetaInfo(DatabaseWrapper db, String procNamePattern) {
        return FunctionUtil.getProcedureMeta(db, procNamePattern, false);
    }

    public static List<FunctionMeta> getProcAndDtlMetaInfo(DatabaseWrapper db, String procNamePattern) {
        return FunctionUtil.getProcedureMeta(db, procNamePattern, true);
    }

    private static List<TableMetaVo> getViewOrTableMetaInfo(DatabaseWrapper db, String objNamePattern, String[] type, boolean isConCol) {
        List<TableMetaVo> result = new ArrayList();
        ResultSet rsTables = null;
        ResultSet rsPK = null;
        ResultSet rsColumnInfo = null;
        HashSet<String> tableNameSet = new HashSet<>();
        try {
            DatabaseMetaData dbMeta = db.getConnection().getMetaData();
            String database = db.getDbtype().getDatabase(db, dbMeta);
            log.info("database 值" + database);
            if (null != database && ("".equals(database) || "%".equals(database))) {
                database = null;
            }
            if (!StringUtil.isBlank(objNamePattern)) {
                String[] split = objNamePattern.split("\\|");
                for (int i = split.length - 1; i >= 0; i--) {
                    rsTables = dbMeta.getTables(database, database, split[i], type);
                    setResult(rsTables, rsColumnInfo, isConCol, rsPK, result, dbMeta, database, db);
                }
            } else {
                rsTables = dbMeta.getTables(database, database, null, type);
                setResult(rsTables, rsColumnInfo, isConCol, rsPK, result, dbMeta, database, db);
            }
            return result;
        } catch (SQLException var24) {
            throw new DBException(db.getID(), "Get table info failed. viewNamePattern=" + objNamePattern, var24);
        } finally {
            try {
                if (rsPK != null) {
                    rsPK.close();
                }
            } catch (SQLException var23) {
                log.error(db.getID(), "Failed to close the database connection！ rsPK");
            }
            try {
                if (rsColumnInfo != null) {
                    rsColumnInfo.close();
                }
            } catch (SQLException var22) {
                log.error(db.getID(), "Failed to close the database connection！ rsColumnInfo");
            }
            try {
                if (rsTables != null) {
                    rsTables.close();
                }
            } catch (SQLException var21) {
                log.error(db.getID(), "Failed to close the database connection！ rsTables");
            }
        }
    }

    public static void setResult(ResultSet rsTables, ResultSet rsColumnInfo, boolean isConCol, ResultSet rsPK, List<TableMetaVo> result, DatabaseMetaData dbMeta, String database, DatabaseWrapper db) throws SQLException {
        while (rsTables.next()) {
            List<String> tableNames = result.stream().map(TableMetaVo::getTableName).collect(Collectors.toList());
            if (!tableNames.contains(rsTables.getString("table_name"))) {
                TableMetaVo tblMeta = new TableMetaVo();
                String tableName = rsTables.getString("table_name");
                tblMeta.setTableName(tableName);
                tblMeta.setType(rsTables.getString("TABLE_TYPE"));
                tblMeta.setDbname(rsTables.getString("TABLE_CAT"));
                tblMeta.setUsername(rsTables.getString("TABLE_SCHEM"));
                tblMeta.setRemarks(rsTables.getString("REMARKS"));
                if (isConCol) {
                    rsPK = dbMeta.getPrimaryKeys(database, database, tableName);
                    while (rsPK.next()) {
                        tblMeta.addPrimaryKey(rsPK.getString("COLUMN_NAME"));
                    }
                    rsPK.close();
                    rsColumnInfo = dbMeta.getColumns(database, database, tableName, "%");
                    fillColumnMeta(db, tblMeta, rsColumnInfo);
                    rsColumnInfo.close();
                }
                result.add(tblMeta);
            }
        }
    }

    private static void fillColumnMeta(final DatabaseWrapper db, final TableMetaVo tblMeta, final ResultSet rs) throws SQLException {
        ColumnMetaVo columnMeta;
        for (; rs.next(); tblMeta.addColumnMetaVo(columnMeta)) {
            String tableName = rs.getString("TABLE_NAME");
            columnMeta = new ColumnMetaVo();
            String colName = rs.getString("COLUMN_NAME");
            columnMeta.setName(colName);
            int type = rs.getInt("DATA_TYPE");
            if (Dbtype.SQLSERVER == db.getDbtype()) {
                if (type == -150) {
                    columnMeta.setTypeOfSQL(2005);
                } else if (type == -155) {
                    columnMeta.setTypeOfSQL(91);
                } else {
                    columnMeta.setTypeOfSQL(type);
                }
            } else {
                columnMeta.setTypeOfSQL(type);
            }
            columnMeta.setTypeName(rs.getString("TYPE_NAME"));
            columnMeta.setLength(rs.getInt("COLUMN_SIZE"));
            columnMeta.setOrdPosition(rs.getString("ORDINAL_POSITION"));
            columnMeta.setScale(rs.getInt("DECIMAL_DIGITS"));
            int isNullable = rs.getInt("NULLABLE");
            columnMeta.setNullable(1 == isNullable);
            try {
                columnMeta.setRemark(rs.getString("REMARKS"));
            } catch (SQLException var9) {
            }
        }
    }
}
