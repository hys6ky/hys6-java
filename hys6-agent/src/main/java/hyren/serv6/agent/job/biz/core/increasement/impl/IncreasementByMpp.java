package hyren.serv6.agent.job.biz.core.increasement.impl;

import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.increasement.JDBCIncreasement;
import hyren.serv6.agent.job.biz.utils.SQLUtil;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.SqlBuildTool;
import hyren.serv6.commons.hadoop.sqlutils.HSqlExecute;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class IncreasementByMpp extends JDBCIncreasement {

    public IncreasementByMpp(TableBean tableBean, String hbase_name, String sysDate, DatabaseWrapper db, long dsl_id) {
        super(tableBean, hbase_name, sysDate, db, dsl_id);
    }

    @Override
    public void calculateIncrement() {
        ArrayList<String> sqlList = new ArrayList<>();
        HSqlExecute.executeSql(createTableIfNotExists(yesterdayTableName, db, columns, types), db);
        getCreateDeltaSql(sqlList);
        restore(StorageType.QuanLiang.getCode());
        getInsertDataSql(sqlList);
        getDeleteDataSql(sqlList);
        HSqlExecute.executeSql(sqlList, db);
    }

    @Override
    public void mergeIncrement() {
        List<String> list = new ArrayList<>();
        String tmpDelTa = tableNameInHBase + "hyt";
        try {
            dropTableIfExists(tmpDelTa, db, list);
            list.add(createInvalidDataSql(tmpDelTa));
            list.add(insertInvalidDataSql(tmpDelTa));
            list.add(insertDeltaDataSql(tmpDelTa, deltaTableName));
            dropTableIfExists(tableNameInHBase, db, list);
            list.add(db.getDbtype().ofRenameSql(tmpDelTa, tableNameInHBase, db));
            HSqlExecute.executeSql(list, db);
        } catch (Exception e) {
            log.error("根据临时表对mpp表做增量操作时发生错误！！", e);
            throw new AppSystemException("根据临时表对mpp表做增量操作时发生错误！！", e);
        }
    }

    @Override
    public void append() {
        if (storage_time > 0) {
            ArrayList<String> sqlList = new ArrayList<>();
            HSqlExecute.executeSql(createTableIfNotExists(yesterdayTableName, db, columns, types), db);
            restore(StorageType.ZhuiJia.getCode());
            insertAppendData(sqlList);
            HSqlExecute.executeSql(sqlList, db);
        }
    }

    @Override
    public void replace() {
        ArrayList<String> sqlList = new ArrayList<>();
        dropTableIfExists(deltaTableName, db, sqlList);
        if (storage_time > 0) {
            sqlList.add(db.getDbtype().ofCopyTableSchemasSql(todayTableName, deltaTableName, db));
            sqlList.add(db.getDbtype().ofCopyTableDataSql(todayTableName, deltaTableName, db));
            dropTableIfExists(yesterdayTableName, db, sqlList);
            sqlList.add(db.getDbtype().ofRenameSql(deltaTableName, yesterdayTableName, db));
            HSqlExecute.executeSql(sqlList, db);
        }
        ResultSet indexInfo = null;
        if (db.isExistTable(yesterdayTableName)) {
            indexInfo = SqlBuildTool.getIndexInfo(db, yesterdayTableName);
        }
        if (null == indexInfo) {
            log.info("没有额外索引配置! 表: " + yesterdayTableName);
        } else {
            SqlBuildTool.createOtherIndex(db, indexInfo, yesterdayTableName);
            log.info("创建额外索引完成! 表: " + yesterdayTableName);
        }
    }

    @Override
    public void restore(String storageType) {
        ArrayList<String> sqlList = new ArrayList<>();
        if (StorageType.ZengLiang.getCode().equals(storageType) || StorageType.QuanLiang.getCode().equals(storageType) || StorageType.LiShiLaLian.getCode().equals(storageType)) {
            sqlList.add("delete from " + kingDatabase(yesterdayTableName) + " where " + Constant._HYREN_S_DATE + "='" + sysDate + "'");
            sqlList.add("update " + kingDatabase(yesterdayTableName) + " set " + Constant._HYREN_E_DATE + " = " + Constant._MAX_DATE_8 + " where " + Constant._HYREN_E_DATE + "='" + sysDate + "'");
        } else if (StorageType.ZhuiJia.getCode().equals(storageType)) {
            sqlList.add("DELETE FROM " + kingDatabase(yesterdayTableName) + " WHERE " + Constant._HYREN_S_DATE + "='" + sysDate + "'");
        } else if (StorageType.TiHuan.getCode().equals(storageType)) {
            log.info("替换，不需要恢复当天数据");
        } else {
            throw new AppSystemException("错误的增量拉链参数代码项");
        }
        HSqlExecute.executeSql(sqlList, db);
    }

    @Override
    public void incrementalDataZipper() {
        HSqlExecute.executeSql(createTableIfNotExists(yesterdayTableName, db, columns, types), db);
        ArrayList<String> sqlList = new ArrayList<>();
        sqlList.add(db.getDbtype().ofCopyTableSchemasSql(yesterdayTableName, deltaTableName, db));
        sqlList.add(db.getDbtype().ofCopyTableDataSql(yesterdayTableName, deltaTableName, db));
        restore(StorageType.LiShiLaLian.getCode());
        updateInvalidDataSql(sqlList);
        sqlList.add(insertDeltaDataSql(deltaTableName, todayTableName));
        dropTableIfExists(yesterdayTableName, db, sqlList);
        sqlList.add(db.getDbtype().ofRenameSql(deltaTableName, yesterdayTableName, db));
        HSqlExecute.executeSql(sqlList, db);
    }

    private void updateInvalidDataSql(ArrayList<String> sqlList) {
        String updateDataSql = "UPDATE " + kingDatabase(deltaTableName) + " SET " + Constant._HYREN_E_DATE + "='" + sysDate + "'" + " WHERE EXISTS " + " ( select " + Constant._HYREN_MD5_VAL + " from " + kingDatabase(todayTableName) + " where " + kingDatabase(deltaTableName) + "." + Constant._HYREN_MD5_VAL + " = " + kingDatabase(todayTableName) + "." + Constant._HYREN_MD5_VAL + ")" + " AND " + deltaTableName + "." + Constant._HYREN_E_DATE + " = '" + Constant._MAX_DATE_8 + "'";
        sqlList.add(updateDataSql);
    }

    @Override
    public void close() {
        dropAllTmpTable();
        if (db != null) {
            db.close();
        }
    }

    private void getCreateDeltaSql(ArrayList<String> sqlList) {
        StringBuilder sql = new StringBuilder(120);
        if (db.getDbtype() == Dbtype.TERADATA) {
            sql.append("CREATE MULTISET TABLE ");
        } else {
            sql.append("CREATE TABLE ");
        }
        sql.append(kingDatabase(deltaTableName));
        sql.append("(");
        SQLUtil.getSqlCond(columns, types, tar_types, sql, db.getDbtype());
        sql.append(" action VARCHAR(2)");
        sql.append(")");
        dropTableIfExists(deltaTableName, db, sqlList);
        sqlList.add(sql.toString());
    }

    private void getDeleteDataSql(ArrayList<String> sqlList) {
        StringBuilder deleteDatasql = new StringBuilder(120);
        deleteDatasql.append("INSERT INTO ");
        deleteDatasql.append(kingDatabase(deltaTableName));
        deleteDatasql.append("(");
        columns = db.getDbtype().ofEscapedkey(columns);
        for (String col : columns) {
            deleteDatasql.append(col).append(",");
        }
        deleteDatasql.append(" action ");
        deleteDatasql.append(" ) ");
        deleteDatasql.append(" select ");
        for (String col : columns) {
            if (col.equals(Constant._HYREN_E_DATE)) {
                deleteDatasql.append(sysDate).append(" as ").append(col).append(",");
            } else {
                deleteDatasql.append(kingDatabase(yesterdayTableName)).append(".").append(col).append(",");
            }
        }
        deleteDatasql.append("'UD' ");
        deleteDatasql.append(" from ");
        deleteDatasql.append(kingDatabase(yesterdayTableName));
        deleteDatasql.append(" WHERE NOT EXISTS ");
        deleteDatasql.append(" ( select * from ");
        deleteDatasql.append(kingDatabase(todayTableName));
        deleteDatasql.append(" where ");
        deleteDatasql.append(kingDatabase(yesterdayTableName)).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDatasql.append(" = ");
        deleteDatasql.append(kingDatabase(todayTableName)).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDatasql.append(" ) AND ").append(kingDatabase(yesterdayTableName)).append(".").append(Constant._HYREN_E_DATE);
        deleteDatasql.append(" = '99991231'");
        sqlList.add(deleteDatasql.toString());
    }

    private void insertAppendData(ArrayList<String> sqlList) {
        StringBuilder insertDataSql = new StringBuilder(120);
        insertDataSql.append("INSERT INTO ");
        insertDataSql.append(kingDatabase(yesterdayTableName));
        insertDataSql.append("(");
        columns = db.getDbtype().ofEscapedkey(columns);
        for (String col : columns) {
            insertDataSql.append(col).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" ) ");
        insertDataSql.append(" select ");
        for (String col : columns) {
            insertDataSql.append(kingDatabase(todayTableName)).append(".").append(col).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" from ");
        insertDataSql.append(kingDatabase(todayTableName));
        sqlList.add(insertDataSql.toString());
    }

    private void getInsertDataSql(ArrayList<String> sqlList) {
        StringBuilder insertDataSql = new StringBuilder(120);
        insertDataSql.append("INSERT INTO ");
        insertDataSql.append(kingDatabase(deltaTableName));
        insertDataSql.append("(");
        columns = db.getDbtype().ofEscapedkey(columns);
        for (String col : columns) {
            insertDataSql.append(col).append(",");
        }
        insertDataSql.append(" action ");
        insertDataSql.append(" ) ");
        insertDataSql.append(" select ");
        for (String col : columns) {
            insertDataSql.append(kingDatabase(todayTableName)).append(".").append(col).append(",");
        }
        insertDataSql.append("'CU' ");
        insertDataSql.append(" from ");
        insertDataSql.append(kingDatabase(todayTableName));
        insertDataSql.append(" WHERE NOT EXISTS ");
        insertDataSql.append(" ( select * from ");
        insertDataSql.append(kingDatabase(yesterdayTableName));
        insertDataSql.append(" where ");
        insertDataSql.append(kingDatabase(todayTableName)).append(".").append(Constant._HYREN_MD5_VAL);
        insertDataSql.append(" = ");
        insertDataSql.append(kingDatabase(yesterdayTableName)).append(".").append(Constant._HYREN_MD5_VAL);
        insertDataSql.append(" ) ");
        sqlList.add(insertDataSql.toString());
    }

    private String insertInvalidDataSql(String tmpDelTa) {
        StringBuilder insertDataSql = new StringBuilder(120);
        insertDataSql.append("INSERT INTO ");
        insertDataSql.append(kingDatabase(tmpDelTa));
        insertDataSql.append("(");
        columns = db.getDbtype().ofEscapedkey(columns);
        for (String col : columns) {
            insertDataSql.append(col).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" ) ");
        insertDataSql.append(" select ");
        for (String col : columns) {
            insertDataSql.append(kingDatabase(tableNameInHBase)).append(".").append(col).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" from ");
        insertDataSql.append(kingDatabase(tableNameInHBase));
        insertDataSql.append(" WHERE NOT EXISTS ");
        insertDataSql.append(" ( select * from ");
        insertDataSql.append(kingDatabase(deltaTableName));
        insertDataSql.append(" where ");
        insertDataSql.append(kingDatabase(deltaTableName)).append(".").append(Constant._HYREN_MD5_VAL);
        insertDataSql.append(" = ");
        insertDataSql.append(kingDatabase(tableNameInHBase)).append(".").append(Constant._HYREN_MD5_VAL);
        insertDataSql.append(" ) ");
        return insertDataSql.toString();
    }

    private String createInvalidDataSql(String tmpDelTa) {
        StringBuilder sql = new StringBuilder(120);
        if (db.getDbtype() == Dbtype.TERADATA) {
            sql.append("CREATE MULTISET TABLE ");
        } else {
            sql.append("CREATE TABLE ");
        }
        sql.append(kingDatabase(tmpDelTa));
        sql.append("(");
        SQLUtil.getSqlCond(columns, types, tar_types, sql, db.getDbtype());
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        return sql.toString();
    }

    private String createTableIfNotExists(String tableName, DatabaseWrapper db, List<String> columns, List<String> types) {
        StringBuilder create = new StringBuilder(1024);
        if (!db.isExistTable(tableName)) {
            if (db.getDbtype() == Dbtype.TERADATA) {
                create.append("CREATE MULTISET TABLE ");
            } else {
                create.append("CREATE TABLE ");
            }
            create.append(kingDatabase(tableName));
            create.append("(");
            SQLUtil.getSqlCond(columns, types, tar_types, create, db.getDbtype());
            create.deleteCharAt(create.length() - 1);
            create.append(")");
            return create.toString();
        } else {
            return "";
        }
    }

    private void dropAllTmpTable() {
        List<String> deleteInfo = new ArrayList<>();
        dropTableIfExists(deltaTableName, db, deleteInfo);
        HSqlExecute.executeSql(deleteInfo, db);
    }

    @Override
    public void dropTodayTable() {
        List<String> deleteInfo = new ArrayList<>();
        JDBCIncreasement.dropTableIfExists(todayTableName, db, deleteInfo);
        HSqlExecute.executeSql(deleteInfo, db);
    }

    private String kingDatabase(String tableName) {
        if (db.getDbtype() == Dbtype.KINGBASE) {
            return db.getDatabaseName() + '.' + tableName;
        }
        return tableName;
    }
}
