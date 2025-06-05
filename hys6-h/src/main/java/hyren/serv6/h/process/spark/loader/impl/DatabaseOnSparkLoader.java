package hyren.serv6.h.process.spark.loader.impl;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.DataBaseType;
import hyren.serv6.h.process.args.DatabaseHandleArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import java.util.List;
import java.util.Properties;
import static hyren.serv6.commons.utils.constant.Constant.*;

@Slf4j
public class DatabaseOnSparkLoader extends AbsSparkLoader {

    private static final long serialVersionUID = 3834997003197645388L;

    private final DatabaseHandleArgs databaseHandleArgs;

    private final Properties connProperties = new Properties();

    public DatabaseOnSparkLoader(SparkSession sparkSession, Dataset<Row> rowDataset, DatabaseHandleArgs handleArgs) {
        super(sparkSession, rowDataset, handleArgs);
        this.databaseHandleArgs = handleArgs;
        this.jobNameParam = databaseHandleArgs.getJobNameParam();
        connProperties.setProperty("user", databaseHandleArgs.getUser());
        connProperties.setProperty("password", databaseHandleArgs.getPassword());
        Dbtype dbtype = DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDbtype();
        if (dbtype == Dbtype.KINGBASE) {
            tableName = databaseHandleArgs.getDatabase() + '.' + tableName;
        }
    }

    @Override
    public void append() {
        DataBaseType dataBaseType = DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType());
        DataFrameWriter<Row> rowDataFrameWriter = rowDataset.write().option("driver", dataBaseType.getDriver()).mode(SaveMode.Append);
        rowDataFrameWriter.jdbc(databaseHandleArgs.getUrl(), tableName, connProperties);
    }

    @Override
    public void replace() {
        DataBaseType dataBaseType = DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType());
        DataFrameWriter<Row> rowDataFrameWriter = rowDataset.write().option("driver", dataBaseType.getDriver()).mode(SaveMode.Append);
        rowDataFrameWriter.jdbc(databaseHandleArgs.getUrl(), tableName, connProperties);
    }

    @Override
    public void upSert() {
        sparkSession.read().option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), tableName, connProperties).createOrReplaceTempView(tableName);
        rowDataset.createOrReplaceTempView(currentTempTable);
        dealF2CurrentJobUndeleteData();
        dealF2OtherJobData();
        dealF2CurrentDeltaData();
    }

    private void dealF2CurrentJobUndeleteData() {
        StringBuilder upsetCurrentJobUndeleteDataSql = new StringBuilder();
        try {
            upsetCurrentJobUndeleteDataSql.append("SELECT * FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" = ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F2 undelete data sql: [ {} ]", upsetCurrentJobUndeleteDataSql);
            sparkSession.sql(upsetCurrentJobUndeleteDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F2 undelete data (" + zipperTempTable + ") failed! " + " Execute sql: " + upsetCurrentJobUndeleteDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF2OtherJobData() {
        StringBuilder upsetOtherJobDataSql = new StringBuilder();
        try {
            upsetOtherJobDataSql.append("SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" != ").append("'").append(jobNameParam).append("'");
            log.info("F2 other job data sql: [ {} ]", upsetOtherJobDataSql);
            sparkSession.sql(upsetOtherJobDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F2 other job data (" + zipperTempTable + ") failed! " + " Execute sql: " + upsetOtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF2CurrentDeltaData() {
        StringBuilder upsetCurrentDeltaDataSql = new StringBuilder();
        try {
            upsetCurrentDeltaDataSql.append("SELECT * FROM ").append(currentTempTable);
            log.info("F2 current delta data sql: [ {} ]", upsetCurrentDeltaDataSql);
            sparkSession.sql(upsetCurrentDeltaDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F2 current delta data (" + zipperTempTable + ") failed! " + " Execute sql: " + upsetCurrentDeltaDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void historyZipperFullLoading() {
        sparkSession.read().option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), tableName, connProperties).createOrReplaceTempView(tableName);
        rowDataset.createOrReplaceTempView(currentTempTable);
        dealF5AddData();
        dealF5DelData();
        dealF5CurrentJobValidData();
        dealF5CurrentJobInvalidData();
        dealF5OtherJobData();
    }

    private void dealF5AddData() {
        StringBuilder f5AddDataSql = new StringBuilder();
        try {
            f5AddDataSql.append("SELECT * FROM ").append(currentTempTable).append(" WHERE NOT EXISTS").append(" (").append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" = ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 add data sql: [ {} ]", f5AddDataSql);
            sparkSession.sql(f5AddDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F5 add data into incremental table [ " + zipperTempTable + " ] failed!" + " Execute sql: " + f5AddDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5DelData() {
        StringBuilder f5DelDataSql = new StringBuilder();
        try {
            String invalid_insert_sql = getProcessFieldInsertionSplicingSQL(tableName);
            f5DelDataSql.append("SELECT ").append(invalid_insert_sql).append(" FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" ( SELECT * FROM ").append(currentTempTable).append(" WHERE").append(" ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" = ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(")").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 del data sql: [ {} ]", f5DelDataSql);
            sparkSession.sql(f5DelDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F5 del data into incremental table [ " + zipperTempTable + " ] failed!" + " Execute sql: " + f5DelDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5CurrentJobValidData() {
        StringBuilder f5CurrentJobValidDataSql = new StringBuilder();
        try {
            f5CurrentJobValidDataSql.append("SELECT * FROM ").append(tableName).append(" WHERE (").append(" EXISTS (").append("SELECT ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" = ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(") AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(")").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 current job valid data sql: [ {} ]", f5CurrentJobValidDataSql);
            sparkSession.sql(f5CurrentJobValidDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F5 current job valid data into the incremental table" + " ( historically valid ) [ " + zipperTempTable + " ] failed! " + " Execute sql: " + f5CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5CurrentJobInvalidData() {
        StringBuilder f5CurrentJobInvalidDataSql = new StringBuilder();
        try {
            f5CurrentJobInvalidDataSql.append("SELECT * FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(_HYREN_E_DATE).append(" <> ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 current job invalid data sql: [ {} ]", f5CurrentJobInvalidDataSql);
            sparkSession.sql(f5CurrentJobInvalidDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F5 current job invalid data into the incremental temporary table" + " (historical linked data) [ " + zipperTempTable + " ] failed! " + " Execute sql: " + f5CurrentJobInvalidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5OtherJobData() {
        StringBuilder f5OtherJobDataSql = new StringBuilder();
        try {
            f5OtherJobDataSql.append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" != ").append("'").append(jobNameParam).append("'");
            log.info("F5 other job data sql: [ {} ]", f5OtherJobDataSql);
            sparkSession.sql(f5OtherJobDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F5 other job data into the incremental table [ " + zipperTempTable + " ] failed! " + " Execute sql: " + f5OtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void historyZipperIncrementLoading() {
        sparkSession.read().option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), tableName, connProperties).createOrReplaceTempView(tableName);
        rowDataset.createOrReplaceTempView(currentTempTable);
        dealF3InvalidData();
        dealF3CurrentJobValidData();
        dealF3CurrentJobInvalidData();
        dealF3OtherJobData();
        dealF3CurrentDeltaData();
    }

    private void dealF3InvalidData() {
        String invalid_insert_sql = getProcessFieldInsertionSplicingSQL(tableName);
        StringBuilder dealF3InvalidDataSql = new StringBuilder(120);
        try {
            dealF3InvalidDataSql.append(" SELECT ").append(invalid_insert_sql).append(" FROM ").append(tableName).append(" WHERE EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" = ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = '").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
            log.info("F3 invalid data sql: [ {} ]", dealF3InvalidDataSql);
            sparkSession.sql(dealF3InvalidDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F3 invalid data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3InvalidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3CurrentJobValidData() {
        StringBuilder f3CurrentJobValidDataSql = new StringBuilder();
        try {
            f3CurrentJobValidDataSql.append(" SELECT * FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append("=").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
            log.info("F3 current job valid data sql: [ {} ]", f3CurrentJobValidDataSql);
            sparkSession.sql(f3CurrentJobValidDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F3 current job valid data  (" + zipperTempTable + ") 的SQLfailed! " + " Execute sql: " + f3CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3CurrentJobInvalidData() {
        StringBuilder f3CurrentJobValidDataSql = new StringBuilder();
        try {
            f3CurrentJobValidDataSql.append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_E_DATE).append(" <> ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F3 current job invalid data sql: [ {} ]", f3CurrentJobValidDataSql);
            sparkSession.sql(f3CurrentJobValidDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F3 current job invalid data (" + zipperTempTable + ") 的SQLfailed! " + " Execute sql: " + f3CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3OtherJobData() {
        StringBuilder fOtherJobDataSql = new StringBuilder();
        try {
            fOtherJobDataSql.append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" <> ").append("'").append(jobNameParam).append("'");
            log.info("F3 other job data sql: [ {} ]", fOtherJobDataSql);
            sparkSession.sql(fOtherJobDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F3 other job data sql (" + zipperTempTable + ") failed! " + " Execute sql: " + fOtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    protected void dealF3CurrentDeltaData() {
        StringBuilder f3CurrentDeltaDataSql = new StringBuilder();
        try {
            f3CurrentDeltaDataSql.append("SELECT * FROM ").append(currentTempTable);
            log.info("F3 delta data sql: [ {} ]", f3CurrentDeltaDataSql);
            sparkSession.sql(f3CurrentDeltaDataSql.toString()).write().mode(SaveMode.Append).option("driver", DataBaseType.getDatabase(databaseHandleArgs.getDatabaseType()).getDriver()).jdbc(databaseHandleArgs.getUrl(), zipperTempTable, connProperties);
        } catch (Exception e) {
            throw new AppSystemException("F3 delta data sql (" + zipperTempTable + ") failed! " + " Execute sql: " + f3CurrentDeltaDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void incrementalDataZipper() {
        super.incrementalDataZipper();
    }

    private String getProcessFieldInsertionSplicingSQL(String srcTableName) {
        List<String> columns = StringUtil.split(databaseHandleArgs.getSrcColumn(), ",");
        StringBuilder select_sql_sb = new StringBuilder();
        for (String column : columns) {
            if (HYRENFIELD.contains(column.toUpperCase())) {
                if (column.equalsIgnoreCase(_HYREN_E_DATE)) {
                    select_sql_sb.append("'").append(etlDateWith8).append("'").append(" as ").append(column);
                } else {
                    select_sql_sb.append(srcTableName).append(".").append(column);
                }
            } else {
                select_sql_sb.append(srcTableName).append(".").append(column);
            }
            select_sql_sb.append(",");
        }
        return select_sql_sb.delete(select_sql_sb.length() - 1, select_sql_sb.length()).toString();
    }
}
