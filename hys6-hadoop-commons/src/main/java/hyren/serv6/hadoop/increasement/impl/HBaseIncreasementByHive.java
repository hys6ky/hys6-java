package hyren.serv6.hadoop.increasement.impl;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.hadoop.increasement.HBaseIncreasement;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import java.sql.ResultSet;

@Slf4j
public class HBaseIncreasementByHive extends HBaseIncreasement {

    public HBaseIncreasementByHive(TableBean tableBean, String hbase_name, String sysDate, String dsl_name, String hadoop_user_name, String platform, String prncipal_name, Long dsl_id, DatabaseWrapper db) {
        super(tableBean, hbase_name, sysDate, dsl_name, hadoop_user_name, platform, prncipal_name, dsl_id, db);
    }

    @Override
    public void calculateIncrement() {
        hiveMapHBase(todayTableName);
        if (!hBaseOperator.existsTable(tableNameInHBase)) {
            createDefaultPrePartTable(hBaseOperator, tableNameInHBase, false);
        }
        hiveMapHBase(tableNameInHBase);
        getCreateDeltaSql();
        restore(StorageType.QuanLiang.getCode());
        getInsertDataSql();
        getDeleteDataSql();
        getDeltaDataSql();
    }

    @Override
    public void mergeIncrement() {
        hBaseOperator.dropTable(tableNameInHBase);
        hBaseOperator.renameTable(deltaTableName, tableNameInHBase);
        db.execute("DROP TABLE IF EXISTS " + deltaTableName);
    }

    @Override
    public void append() {
        hiveMapHBase(todayTableName);
        if (!hBaseOperator.existsTable(tableNameInHBase)) {
            createDefaultPrePartTable(hBaseOperator, tableNameInHBase, false);
        }
        hiveMapHBase(tableNameInHBase);
        restore(StorageType.ZhuiJia.getCode());
        db.execute(insertAppendSql(tableNameInHBase, todayTableName));
    }

    @Override
    public void replace() {
        if (storage_time > 0) {
            if (hBaseOperator.existsTable(tableNameInHBase)) {
                hBaseOperator.dropTable(tableNameInHBase);
            }
            hBaseOperator.cloneTable(todayTableName, tableNameInHBase);
        }
        hiveMapHBase(tableNameInHBase);
    }

    @Override
    public void incrementalDataZipper() {
        hiveMapHBase(todayTableName);
        if (!hBaseOperator.existsTable(tableNameInHBase)) {
            createDefaultPrePartTable(hBaseOperator, tableNameInHBase, false);
        }
        hiveMapHBase(tableNameInHBase);
        getCreateDeltaSql();
        restore(StorageType.ZengLiang.getCode());
        insertInvalidDataSql();
        insertValidDataSql();
        String insertDataSql = "INSERT INTO " + deltaTableName + "(" + Constant.HIVEMAPPINGROWKEY + "," + StringUtils.join(columns, ',').toLowerCase() + ") SELECT " + getConcatSelectColumn(todayTableName) + "  FROM " + todayTableName;
        db.execute(insertDataSql);
        hBaseOperator.dropTable(tableNameInHBase);
        hBaseOperator.renameTable(deltaTableName, tableNameInHBase);
        db.execute("DROP TABLE IF EXISTS " + deltaTableName);
    }

    private void insertValidDataSql() {
        String deleteDatasql = "INSERT INTO " + this.deltaTableName + " select *  from " + this.yesterdayTableName + " WHERE NOT EXISTS " + " ( select " + Constant._HYREN_MD5_VAL + " from " + this.todayTableName + " where " + this.yesterdayTableName + "." + Constant._HYREN_MD5_VAL + " = " + this.todayTableName + "." + Constant._HYREN_MD5_VAL + ") AND " + this.yesterdayTableName + "." + Constant._HYREN_E_DATE + " = '" + Constant._MAX_DATE_8 + "'";
        db.execute(deleteDatasql);
        String deleteDatasql2 = "INSERT INTO " + this.deltaTableName + " select *  from " + this.yesterdayTableName + " WHERE " + this.yesterdayTableName + "." + Constant._HYREN_E_DATE + " <> '" + Constant._MAX_DATE_8 + "'";
        db.execute(deleteDatasql2);
    }

    private void insertInvalidDataSql() {
        StringBuilder deleteDatasql = new StringBuilder(120);
        StringBuilder join = new StringBuilder(120);
        join.append(this.yesterdayTableName).append(".").append(Constant.HIVEMAPPINGROWKEY).append(",");
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
        db.execute(deleteDatasql.toString());
    }

    @Override
    public void restore(String storageType) {
        String join = Constant.HIVEMAPPINGROWKEY + "," + StringUtils.join(columns, ',').toLowerCase();
        try {
            if (StorageType.QuanLiang.getCode().equals(storageType) || StorageType.ZengLiang.getCode().equals(storageType)) {
                if (!haveTodayData(yesterdayTableName)) {
                    return;
                }
                createDefaultPrePartTable(hBaseOperator, yesterdayTableName + "_restore", false);
                hiveMapHBase(yesterdayTableName + "_restore");
                String after = "case " + Constant._HYREN_E_DATE + " when '" + sysDate + "' then '" + Constant._MAX_DATE_8 + "' else " + Constant._HYREN_E_DATE + " end " + Constant._HYREN_E_DATE;
                join = StringUtils.replace(join, Constant._HYREN_E_DATE, after);
                db.execute("INSERT INTO TABLE " + yesterdayTableName + "_restore (" + join + ") SELECT " + join + " FROM " + yesterdayTableName + " where " + Constant._HYREN_S_DATE + "<>'" + sysDate + "'");
                hBaseOperator.dropTable(yesterdayTableName);
                hBaseOperator.renameTable(yesterdayTableName + "_restore", yesterdayTableName);
                db.execute("DROP TABLE IF EXISTS " + yesterdayTableName + "_restore");
            } else if (StorageType.ZhuiJia.getCode().equals(storageType)) {
                if (!haveAppendTodayData(yesterdayTableName)) {
                    return;
                }
                createDefaultPrePartTable(hBaseOperator, yesterdayTableName + "_restore", false);
                hiveMapHBase(yesterdayTableName + "_restore");
                db.execute("INSERT INTO TABLE " + yesterdayTableName + "_restore (" + join + ") SELECT " + join + " FROM " + yesterdayTableName + " where " + Constant._HYREN_S_DATE + "<>'" + sysDate + "'");
                hBaseOperator.dropTable(yesterdayTableName);
                hBaseOperator.renameTable(yesterdayTableName + "_restore", yesterdayTableName);
                db.execute("DROP TABLE IF EXISTS " + yesterdayTableName + "_restore");
            } else if (StorageType.TiHuan.getCode().equals(storageType)) {
                log.info("替换，不需要恢复当天数据");
            } else {
                throw new AppSystemException("错误的增量拉链参数代码项");
            }
        } catch (Exception e) {
            throw new AppSystemException("恢复当天表的数据失败");
        }
    }

    private void getDeltaDataSql() {
        String insertColumn = Constant.HIVEMAPPINGROWKEY + "," + StringUtils.join(columns, ',').toLowerCase();
        String deleteDataSql = "INSERT INTO " + deltaTableName + "(" + insertColumn + ") SELECT " + getSelectColumn(yesterdayTableName) + " FROM " + yesterdayTableName + " where " + yesterdayTableName + "." + Constant._HYREN_E_DATE + " <> '" + Constant._MAX_DATE_8 + "'";
        db.execute(deleteDataSql);
        String deleteDataSql2 = "INSERT INTO " + deltaTableName + "(" + insertColumn + ") SELECT " + getSelectColumn(yesterdayTableName) + " FROM " + yesterdayTableName + " where (  not exists ( select " + deltaTableName + "." + Constant._HYREN_MD5_VAL + " from " + deltaTableName + " where " + deltaTableName + "." + Constant._HYREN_E_DATE + " <> '" + Constant._MAX_DATE_8 + "' and " + yesterdayTableName + "." + Constant._HYREN_MD5_VAL + "=" + deltaTableName + "." + Constant._HYREN_MD5_VAL + ") and " + yesterdayTableName + "." + Constant._HYREN_E_DATE + " = '" + Constant._MAX_DATE_8 + "')";
        db.execute(deleteDataSql2);
    }

    private void getDeleteDataSql() {
        StringBuilder deleteDataSql = new StringBuilder(120);
        String insertColumn = Constant.HIVEMAPPINGROWKEY + "," + StringUtils.join(columns, ',').toLowerCase();
        String selectColumn = getSelectColumn(yesterdayTableName);
        selectColumn = StringUtils.replace(selectColumn, yesterdayTableName + "." + Constant._HYREN_E_DATE, "'" + sysDate + "'");
        deleteDataSql.append("INSERT INTO ").append(deltaTableName).append("(").append(insertColumn).append(")");
        deleteDataSql.append(" SELECT ").append(selectColumn).append(" FROM ").append(yesterdayTableName);
        deleteDataSql.append(" WHERE NOT EXISTS ").append(" ( SELECT ").append(getSelectColumn(todayTableName)).append(" FROM ").append(todayTableName);
        deleteDataSql.append(" WHERE ").append(yesterdayTableName).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDataSql.append(" = ").append(todayTableName).append(".").append(Constant._HYREN_MD5_VAL);
        deleteDataSql.append(") AND ").append(yesterdayTableName).append(".").append(Constant._HYREN_E_DATE);
        deleteDataSql.append(" = '").append(Constant._MAX_DATE_8).append("'");
        db.execute(deleteDataSql.toString());
    }

    private void getInsertDataSql() {
        String insertColumn = Constant.HIVEMAPPINGROWKEY + "," + StringUtils.join(columns, ',').toLowerCase();
        String insertDataSql = "INSERT INTO " + deltaTableName + "(" + insertColumn + ") SELECT " + getConcatSelectColumn(todayTableName) + "  FROM " + todayTableName + " WHERE NOT EXISTS  ( SELECT " + getSelectColumn(yesterdayTableName) + " FROM " + yesterdayTableName + " where " + todayTableName + "." + Constant._HYREN_MD5_VAL + " = " + yesterdayTableName + "." + Constant._HYREN_MD5_VAL + " AND " + yesterdayTableName + "." + Constant._HYREN_E_DATE + " = '" + Constant._MAX_DATE_8 + "'" + " ) ";
        db.execute(insertDataSql);
    }

    private String getConcatSelectColumn(String todayTableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("concat(").append(todayTableName).append(".").append(Constant.HIVEMAPPINGROWKEY).append(",'_").append(sysDate).append("'").append(") as ").append(Constant.HIVEMAPPINGROWKEY).append(",");
        for (String column : columns) {
            sb.append(todayTableName).append(".").append(column).append(" as ").append(column).append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
        return sb.toString();
    }

    private String getSelectColumn(String todayTableName) {
        StringBuilder sb = new StringBuilder();
        sb.append(todayTableName).append(".").append(Constant.HIVEMAPPINGROWKEY).append(" as ").append(Constant.HIVEMAPPINGROWKEY).append(",");
        for (String column : columns) {
            sb.append(todayTableName).append(".").append(column).append(" as ").append(column).append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
        return sb.toString();
    }

    private void getCreateDeltaSql() {
        if (hBaseOperator.existsTable(deltaTableName)) {
            hBaseOperator.dropTable(deltaTableName);
        }
        createDefaultPrePartTable(hBaseOperator, deltaTableName, false);
        hiveMapHBase(deltaTableName);
    }

    private void hiveMapHBase(String tableName) {
        try {
            StringBuilder sql = new StringBuilder(1024);
            db.execute("DROP TABLE IF EXISTS " + tableName);
            sql.append("CREATE EXTERNAL TABLE IF NOT EXISTS ").append(tableName).append(" ( ").append(Constant.HIVEMAPPINGROWKEY).append(" string , ");
            for (int i = 0; i < columns.size(); i++) {
                if (!Constant.HYRENFIELD.contains(columns.get(i).toUpperCase())) {
                    if (!tar_types.isEmpty() && StringUtil.isNotBlank(tar_types.get(i)) && !"NULL".equalsIgnoreCase(tar_types.get(i))) {
                        sql.append("`").append(columns.get(i)).append("` ").append(tar_types.get(i)).append(",");
                    } else {
                        sql.append("`").append(columns.get(i)).append("` ").append(types.get(i)).append(",");
                    }
                } else {
                    sql.append("`").append(columns.get(i)).append("` ").append(types.get(i)).append(",");
                }
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(") row format delimited fields terminated by '\\t' ");
            sql.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' ");
            sql.append("WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key , ");
            for (String column : columns) {
                sql.append(Bytes.toString(Constant.HBASE_COLUMN_FAMILY)).append(":").append(column).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append("\") TBLPROPERTIES (\"hbase.table.name\" = \"").append(tableName).append("\")");
            db.execute(sql.toString());
        } catch (Exception e) {
            throw new AppSystemException("hive映射HBase表失败", e);
        }
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
            return resultSet.next();
        } catch (Exception e) {
            throw new AppSystemException("执行查询当天增量是否有进数");
        }
    }

    private String insertAppendSql(String targetTableName, String sourceTableName) {
        StringBuilder insertDataSql = new StringBuilder(120);
        insertDataSql.append("INSERT INTO ");
        insertDataSql.append(targetTableName);
        insertDataSql.append("(").append(Constant.HIVEMAPPINGROWKEY).append(",");
        for (String col : columns) {
            insertDataSql.append(col.toLowerCase()).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" ) ");
        insertDataSql.append(" select ").append("concat(").append(sourceTableName).append(".").append(Constant.HIVEMAPPINGROWKEY).append(",'_").append(sysDate).append("'").append("),");
        for (String col : columns) {
            insertDataSql.append(sourceTableName).append(".").append(col.toLowerCase()).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" from ");
        insertDataSql.append(sourceTableName);
        return insertDataSql.toString();
    }
}
