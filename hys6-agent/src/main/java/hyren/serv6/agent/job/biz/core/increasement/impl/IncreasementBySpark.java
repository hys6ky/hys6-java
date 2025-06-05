package hyren.serv6.agent.job.biz.core.increasement.impl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.increasement.JDBCIncreasement;
import hyren.serv6.agent.job.biz.utils.SQLUtil;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.sqlutils.HSqlExecute;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class IncreasementBySpark extends JDBCIncreasement {

    public IncreasementBySpark(TableBean tableBean, String hbase_name, String sysDate, DatabaseWrapper db, long dsl_id) {
        super(tableBean, hbase_name, sysDate, db, dsl_id);
    }

    @Override
    public void calculateIncrement() {
        ArrayList<String> sqlList = new ArrayList<>();
        HSqlExecute.executeSql(createTableIfNotExists(yesterdayTableName), db);
        getCreateDeltaSql(sqlList);
        restore(StorageType.QuanLiang.getCode());
        getInsertDataSql(sqlList);
        getDeleteDataSql(sqlList);
        getDeltaDataSql(sqlList);
        HSqlExecute.executeSql(sqlList, db);
    }

    @Override
    public void mergeIncrement() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("DROP TABLE IF EXISTS " + yesterdayTableName);
        sqlList.add("alter table " + deltaTableName + " rename to " + yesterdayTableName);
        sqlList.add("ANALYZE TABLE " + yesterdayTableName + " COMPUTE STATISTICS NOSCAN");
        HSqlExecute.executeSql(sqlList, db);
    }

    @Override
    public void append() {
        if (storage_time > 0) {
            HSqlExecute.executeSql(createTableIfNotExists(yesterdayTableName), db);
            restore(StorageType.ZhuiJia.getCode());
            HSqlExecute.executeSql(insertDeltaDataSql(yesterdayTableName, todayTableName), db);
            HSqlExecute.executeSql("ANALYZE TABLE " + yesterdayTableName + " COMPUTE STATISTICS NOSCAN", db);
        }
    }

    @Override
    public void replace() {
        ArrayList<String> sqlList = new ArrayList<>();
        if (storage_time > 0) {
            getCreateDeltaSql(sqlList);
            sqlList.add(insertDeltaDataSql(deltaTableName, todayTableName));
            sqlList.add("DROP TABLE IF EXISTS " + yesterdayTableName);
            sqlList.add("ALTER TABLE " + deltaTableName + " RENAME TO " + yesterdayTableName);
            sqlList.add("ANALYZE TABLE " + yesterdayTableName + " COMPUTE STATISTICS NOSCAN");
            HSqlExecute.executeSql(sqlList, db);
        }
    }

    @Override
    public void restore(String storageType) {
        ArrayList<String> sqlList = new ArrayList<>();
        if (StorageType.QuanLiang.getCode().equals(storageType) || StorageType.ZengLiang.getCode().equals(storageType) || StorageType.LiShiLaLian == StorageType.ofEnumByCode(storageType)) {
            if (!haveTodayData(yesterdayTableName)) {
                return;
            }
            String after = "case " + Constant._HYREN_E_DATE + " when '" + sysDate + "' then '" + Constant._MAX_DATE_8 + "' else " + Constant._HYREN_E_DATE + " end " + Constant._HYREN_E_DATE;
            String join = StringUtils.join(columns, ',');
            join = StringUtils.replace(join, Constant._HYREN_E_DATE, after);
            String sql = "create table " + yesterdayTableName + "_restore as select  " + join + " from " + yesterdayTableName + " where " + Constant._HYREN_S_DATE + "<>'" + sysDate + "'";
            sqlList.add(sql);
            sqlList.add("drop table if exists " + yesterdayTableName);
            sqlList.add("alter table " + yesterdayTableName + "_restore rename to " + yesterdayTableName);
        } else if (StorageType.ZhuiJia.getCode().equals(storageType)) {
            if (!haveAppendTodayData(yesterdayTableName)) {
                return;
            }
            String join = StringUtils.join(columns, ',');
            String sql = "create table " + yesterdayTableName + "_restore as select  " + join + " from " + yesterdayTableName + " where " + Constant._HYREN_S_DATE + "<>'" + sysDate + "'";
            sqlList.add(sql);
            sqlList.add("drop table if exists " + yesterdayTableName);
            sqlList.add("alter table " + yesterdayTableName + "_restore rename to " + yesterdayTableName);
        } else if (StorageType.TiHuan.getCode().equals(storageType)) {
            log.info("替换，不需要恢复当天数据");
        } else {
            throw new AppSystemException("错误的增量拉链参数代码项");
        }
        HSqlExecute.executeSql(sqlList, db);
    }

    @Override
    public void incrementalDataZipper() {
        HSqlExecute.executeSql(createTableIfNotExists(yesterdayTableName), db);
        ArrayList<String> sqlList = new ArrayList<>();
        sqlList.add(db.getDbtype().ofCopyTableSchemasSql(yesterdayTableName, deltaTableName, db));
        restore(StorageType.LiShiLaLian.getCode());
        insertInvalidDataSql(sqlList);
        insertValidDataSql(sqlList);
        sqlList.add(insertDeltaDataSql(deltaTableName, todayTableName));
        sqlList.add("DROP TABLE IF EXISTS " + yesterdayTableName);
        sqlList.add(db.getDbtype().ofRenameSql(deltaTableName, yesterdayTableName, db));
        sqlList.add("ANALYZE TABLE " + yesterdayTableName + " COMPUTE STATISTICS NOSCAN");
        HSqlExecute.executeSql(sqlList, db);
    }

    private void insertValidDataSql(ArrayList<String> sqlList) {
        String deleteDatasql = "INSERT INTO " + this.deltaTableName + " select " + "*" + " from " + this.yesterdayTableName + " WHERE NOT EXISTS " + " ( select " + Constant._HYREN_MD5_VAL + " from " + this.todayTableName + " where " + this.yesterdayTableName + "." + Constant._HYREN_MD5_VAL + " = " + this.todayTableName + "." + Constant._HYREN_MD5_VAL + ") AND " + this.yesterdayTableName + "." + Constant._HYREN_E_DATE + " = '" + Constant._MAX_DATE_8 + "'";
        sqlList.add(deleteDatasql);
        String deleteDatasql2 = "INSERT INTO " + this.deltaTableName + " select " + "*" + " from " + this.yesterdayTableName + " WHERE " + this.yesterdayTableName + "." + Constant._HYREN_E_DATE + " <> '" + Constant._MAX_DATE_8 + "'";
        sqlList.add(deleteDatasql2);
    }

    private void insertInvalidDataSql(ArrayList<String> sqlList) {
        StringBuilder deleteDatasql = new StringBuilder(120);
        StringBuilder join = new StringBuilder(120);
        columns = db.getDbtype().ofEscapedkey(columns);
        for (String column : columns) {
            join.append(this.yesterdayTableName).append(".").append(column).append(",");
        }
        join.delete(join.length() - 1, join.length());
        String select_sql = StringUtils.replace(join.toString(), this.yesterdayTableName + "." + Constant._HYREN_E_DATE, "'" + sysDate + "'");
        deleteDatasql.append("INSERT INTO ");
        deleteDatasql.append(this.deltaTableName);
        deleteDatasql.append(" select ");
        deleteDatasql.append(select_sql);
        deleteDatasql.append(" from ");
        deleteDatasql.append(this.yesterdayTableName);
        deleteDatasql.append(" WHERE EXISTS ");
        deleteDatasql.append(" ( select ").append(Constant._HYREN_MD5_VAL).append(" from ");
        deleteDatasql.append(this.todayTableName);
        deleteDatasql.append(" where ");
        deleteDatasql.append(this.yesterdayTableName).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDatasql.append(" = ");
        deleteDatasql.append(this.todayTableName).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDatasql.append(") AND ").append(this.yesterdayTableName).append(".").append(Constant._HYREN_E_DATE);
        deleteDatasql.append(" = '").append(Constant._MAX_DATE_8).append("'");
        sqlList.add(deleteDatasql.toString());
    }

    @Override
    public void close() {
        dropAllTmpTable();
        if (db != null) {
            db.close();
        }
    }

    private void getCreateDeltaSql(ArrayList<String> sqlList) {
        sqlList.add("DROP TABLE IF EXISTS " + deltaTableName);
        sqlList.add(createTableIfNotExists(deltaTableName));
    }

    private String createTableIfNotExists(String tableName) {
        StringBuilder sql = new StringBuilder(120);
        sql.append("CREATE TABLE IF NOT EXISTS ");
        sql.append(tableName);
        sql.append("(");
        SQLUtil.getSqlCond(columns, types, tar_types, sql, db.getDbtype());
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") stored as parquet ");
        return sql.toString();
    }

    private void getDeltaDataSql(ArrayList<String> sqlList) {
        String deltaDatasql = "insert into " + deltaTableName;
        deltaDatasql += " select * from " + yesterdayTableName;
        deltaDatasql += " where ";
        deltaDatasql += yesterdayTableName + "." + Constant._HYREN_E_DATE + " <> '" + Constant._MAX_DATE_8 + "'";
        sqlList.add(deltaDatasql);
        deltaDatasql = "insert into " + deltaTableName;
        deltaDatasql += " select * from " + yesterdayTableName;
        deltaDatasql += " where ( ";
        deltaDatasql += " not exists ";
        deltaDatasql += "(";
        deltaDatasql += " select " + deltaTableName + "." + Constant._HYREN_MD5_VAL + " from " + deltaTableName + " where " + deltaTableName + "." + Constant._HYREN_E_DATE + " <> '" + Constant._MAX_DATE_8 + "'";
        deltaDatasql += " and " + yesterdayTableName + "." + Constant._HYREN_MD5_VAL + "=" + deltaTableName + "." + Constant._HYREN_MD5_VAL + ")";
        deltaDatasql += " and " + yesterdayTableName + "." + Constant._HYREN_E_DATE + " = '" + Constant._MAX_DATE_8 + "'";
        deltaDatasql += ")";
        sqlList.add(deltaDatasql);
    }

    private void getInsertDataSql(ArrayList<String> sqlList) {
        String insertDataSql = "";
        insertDataSql += "INSERT INTO ";
        insertDataSql += deltaTableName;
        insertDataSql += " select * ";
        insertDataSql += " from ";
        insertDataSql += todayTableName;
        insertDataSql += " WHERE NOT EXISTS ";
        insertDataSql += " ( select * from ";
        insertDataSql += yesterdayTableName;
        insertDataSql += " where ";
        insertDataSql += todayTableName;
        insertDataSql += ".";
        insertDataSql += Constant._HYREN_MD5_VAL;
        insertDataSql += " = ";
        insertDataSql += yesterdayTableName;
        insertDataSql += ".";
        insertDataSql += Constant._HYREN_MD5_VAL;
        insertDataSql += " AND ";
        insertDataSql += yesterdayTableName;
        insertDataSql += ".";
        insertDataSql += Constant._HYREN_E_DATE;
        insertDataSql += " = '99991231'";
        insertDataSql += " ) ";
        sqlList.add(insertDataSql);
    }

    private void getDeleteDataSql(ArrayList<String> sqlList) {
        StringBuilder deleteDatasql = new StringBuilder(120);
        StringBuilder join = new StringBuilder(120);
        columns = db.getDbtype().ofEscapedkey(columns);
        for (String column : columns) {
            join.append(this.yesterdayTableName).append(".").append(column).append(",");
        }
        join.delete(join.length() - 1, join.length());
        String select_sql = StringUtils.replace(join.toString(), this.yesterdayTableName + "." + Constant._HYREN_E_DATE, "'" + sysDate + "'");
        deleteDatasql.append("INSERT INTO ");
        deleteDatasql.append(this.deltaTableName);
        deleteDatasql.append(" select ");
        deleteDatasql.append(select_sql);
        deleteDatasql.append(" from ");
        deleteDatasql.append(this.yesterdayTableName);
        deleteDatasql.append(" WHERE NOT EXISTS ");
        deleteDatasql.append(" ( select * from ");
        deleteDatasql.append(this.todayTableName);
        deleteDatasql.append(" where ");
        deleteDatasql.append(this.yesterdayTableName).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDatasql.append(" = ");
        deleteDatasql.append(this.todayTableName).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDatasql.append(") AND ").append(this.yesterdayTableName).append(".").append(Constant._HYREN_E_DATE);
        deleteDatasql.append(" = '99991231'");
        sqlList.add(deleteDatasql.toString());
    }

    private boolean haveTodayData(String tableName) {
        ResultSet resultSet;
        ResultSet resultSet2;
        try {
            resultSet = db.queryGetResultSet("select " + Constant._HYREN_S_DATE + " from " + tableName + " where " + Constant._HYREN_S_DATE + " = '" + sysDate + "' limit 1");
            resultSet2 = db.queryGetResultSet("select " + Constant._HYREN_E_DATE + " from " + tableName + " where " + Constant._HYREN_E_DATE + " = '" + sysDate + "' limit 1");
            return resultSet.next() || resultSet2.next();
        } catch (Exception e) {
            throw new AppSystemException("执行查询当天增量是否有进数");
        }
    }

    private boolean haveAppendTodayData(String tableName) {
        ResultSet resultSet;
        try {
            resultSet = db.queryGetResultSet("select " + Constant._HYREN_S_DATE + " from " + tableName + " where " + Constant._HYREN_S_DATE + " = '" + sysDate + "' limit 1");
            log.info(resultSet.next() + "###################haveAppendTodayData####################");
            return resultSet.next();
        } catch (Exception e) {
            throw new AppSystemException("执行查询当天增量是否有进数");
        }
    }

    private void dropAllTmpTable() {
        List<String> deleteInfo = new ArrayList<>();
        deleteInfo.add("DROP TABLE IF EXISTS " + deltaTableName);
        HSqlExecute.executeSql(deleteInfo, db);
    }

    @Override
    public void dropTodayTable() {
        List<String> deleteInfo = new ArrayList<>();
        JDBCIncreasement.dropTableIfExists(todayTableName, db, deleteInfo);
        HSqlExecute.executeSql(deleteInfo, db);
    }
}
