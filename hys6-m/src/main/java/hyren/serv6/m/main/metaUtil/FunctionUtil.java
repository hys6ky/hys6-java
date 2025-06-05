package hyren.serv6.m.main.metaUtil;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.meta.TableMeta;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.m.util.dbConf.ConnectionTool;
import hyren.serv6.m.vo.DatabaseSetVo;
import oracle.sql.CLOB;
import org.springframework.util.CollectionUtils;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class FunctionUtil {

    private final static Map<Dbtype, String> procSqlMap = new HashMap<>();

    static {
        procSqlMap.put(Dbtype.POSTGRESQL, " SELECT P.prosrc AS func_sql" + " FROM " + " pg_catalog.pg_namespace n, " + " pg_catalog.pg_proc P " + " LEFT JOIN pg_catalog.pg_description d ON ( P.OID = d.objoid ) " + " LEFT JOIN pg_catalog.pg_class C ON ( d.classoid = C.OID AND C.relname = 'pg_proc' ) " + " LEFT JOIN pg_catalog.pg_namespace pn ON ( C.relnamespace = pn.OID AND pn.nspname = 'pg_catalog' )  " + "WHERE " + " P.pronamespace = n.OID and n.nspname LIKE ? and P.prokind = 'p' and P.proname like ?  ");
        procSqlMap.put(Dbtype.ORACLE, " SELECT  DBMS_METADATA.GET_DDL(U.OBJECT_TYPE, U.OBJECT_NAME,U.OWNER)  AS func_sql " + " FROM ALL_OBJECTS U WHERE U.OBJECT_TYPE ='PROCEDURE' " + " AND U.OWNER IN(?)  " + " and OBJECT_NAME=? ");
        procSqlMap.put(Dbtype.MYSQL, " SHOW CREATE PROCEDURE %s ");
        String db2ProcSql = "CALL SYSCS_UTIL.SYSCS_GET_ROUTINE_DEFINITION(?, ?, 0)";
        procSqlMap.put(Dbtype.DB2V2, db2ProcSql);
        procSqlMap.put(Dbtype.DB2V1, db2ProcSql);
        procSqlMap.put(Dbtype.KINGBASE, "");
    }

    private FunctionUtil() {
    }

    protected static List<FunctionMeta> getProcedureMeta(DatabaseWrapper db, String procNamePattern, boolean isConDtl) {
        List<FunctionMeta> functionMetaList;
        functionMetaList = new ArrayList<>();
        DatabaseMetaData dbMeta;
        try {
            ResultSet rsProcs;
            dbMeta = db.getConnection().getMetaData();
            String catalog = db.getConnection().getCatalog();
            String schema = db.getDbtype().getDatabase(db, dbMeta);
            procNamePattern = StringUtil.isBlank(procNamePattern) ? null : procNamePattern;
            if (StringUtil.isBlank(procNamePattern)) {
                procNamePattern = null;
                rsProcs = dbMeta.getProcedures(catalog, schema, procNamePattern);
                setporc(rsProcs, isConDtl, db, schema, functionMetaList);
            } else {
                String[] split = procNamePattern.split("\\|");
                for (String s : split) {
                    rsProcs = dbMeta.getProcedures(catalog, schema, s);
                    setporc(rsProcs, isConDtl, db, schema, functionMetaList);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return functionMetaList;
    }

    public static void setporc(ResultSet rsProcs, boolean isConDtl, DatabaseWrapper db, String schema, List<FunctionMeta> functionMetaList) throws SQLException {
        while (rsProcs.next()) {
            Set<String> procNames = functionMetaList.stream().map(FunctionMeta::getFunc_en_name).collect(Collectors.toSet());
            if (!procNames.contains(rsProcs.getString("PROCEDURE_NAME"))) {
                FunctionMeta funcMeta = new FunctionMeta();
                String procName = rsProcs.getString("PROCEDURE_NAME");
                funcMeta.setFunc_en_name(procName);
                funcMeta.setFunc_type("p");
                if (isConDtl) {
                    String procQuerySql = procSqlMap.get(db.getDbtype());
                    if (null == procQuerySql) {
                        throw new BusinessException("暂不支持该数据库的存储存储过程sql获取");
                    }
                    List micro = null;
                    if (db.getDbtype() == Dbtype.MYSQL) {
                        micro = SqlOperator.queryList(db, String.format(procQuerySql, procName)).stream().map(map -> map.get("create procedure")).collect(Collectors.toList());
                    } else {
                        micro = SqlOperator.queryOneColumnList(db, procQuerySql, schema, procName);
                    }
                    if (!CollectionUtils.isEmpty(micro)) {
                        try {
                            Object funcSql = micro.get(0);
                            if (micro.get(0) instanceof CLOB) {
                                funcSql = ((CLOB) funcSql).getSubString(1, (int) ((CLOB) funcSql).length());
                            }
                            funcMeta.setFunc_sql(funcSql.toString());
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    }
                }
                functionMetaList.add(funcMeta);
            }
        }
    }

    public static void main(String[] args) {
        DatabaseSetVo databaseSet = new DatabaseSetVo();
        databaseSet.setJdbc_url("jdbc:db2://172.168.0.24:8999/SAMPLE");
        databaseSet.setUser_name("db2inst1");
        databaseSet.setDatabase_pad("db2");
        databaseSet.setDatabase_drive("com.ibm.db2.jcc.DB2Driver");
        databaseSet.setFetch_size(100);
        databaseSet.setDatabase_type("DB2");
        databaseSet.setDatabase_name("spdbank");
        DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(databaseSet);
        getProcedureMeta(dbWrapper, null, true);
    }
}
