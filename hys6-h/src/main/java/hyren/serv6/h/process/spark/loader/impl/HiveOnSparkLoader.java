package hyren.serv6.h.process.spark.loader.impl;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.h.process.args.HiveHandleArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.List;
import static hyren.serv6.commons.utils.constant.Constant.*;

@Slf4j
public class HiveOnSparkLoader extends AbsSparkLoader {

    private static final long serialVersionUID = -6931449333729372079L;

    private final HiveHandleArgs hiveHandleArgs;

    protected List<String> partitionFields;

    protected String jobNameParam;

    public HiveOnSparkLoader(SparkSession sparkSession, Dataset<Row> rowDataset, HiveHandleArgs handleArgs) {
        super(sparkSession, rowDataset, handleArgs);
        this.hiveHandleArgs = handleArgs;
        this.partitionFields = hiveHandleArgs.getPartitionFields();
        this.jobNameParam = hiveHandleArgs.getJobNameParam();
    }

    @Override
    public void append() {
        super.append();
    }

    @Override
    public void replace() {
        super.replace();
    }

    @Override
    public void upSert() {
        rowDataset.createOrReplaceTempView(currentTempTable);
        dealUpsetCurrentJobUndeleteData();
        dealUpsetOtherJobData();
        dealUpsetCurrentDeltaData();
    }

    private void dealUpsetCurrentJobUndeleteData() {
        StringBuilder dealUpsetCurrentJobUndeleteDataSql = new StringBuilder();
        try {
            dealUpsetCurrentJobUndeleteDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" = ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("Upset insert undelete data sql: [ {} ]", dealUpsetCurrentJobUndeleteDataSql);
            sparkSession.sql(dealUpsetCurrentJobUndeleteDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Upset insert undelete data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealUpsetCurrentJobUndeleteDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealUpsetOtherJobData() {
        StringBuilder dealUpsetOtherJobDataSql = new StringBuilder();
        try {
            dealUpsetOtherJobDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" != ").append("'").append(jobNameParam).append("'");
            log.info("Upset insert other job data sql: [ {} ]", dealUpsetOtherJobDataSql);
            sparkSession.sql(dealUpsetOtherJobDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Upset insert other job data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealUpsetOtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealUpsetCurrentDeltaData() {
        StringBuilder dealUpsetCurrentDeltaDataSql = new StringBuilder();
        try {
            dealUpsetCurrentDeltaDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(currentTempTable);
            sparkSession.sql(dealUpsetCurrentDeltaDataSql.toString());
            log.info("Upset insert current delta data sql: [ {} ]", dealUpsetCurrentDeltaDataSql);
        } catch (Exception e) {
            throw new AppSystemException("Upset insert current delta data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealUpsetCurrentDeltaDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void historyZipperFullLoading() {
        rowDataset.createOrReplaceTempView(currentTempTable);
        dealF5AddData();
        dealF5DelData();
        dealF5CurrentJobValidData();
        dealF5CurrentJobInvalidData();
        dealF5OtherJobData();
    }

    private void dealF5AddData() {
        StringBuilder dealF5AddDataSql = new StringBuilder();
        try {
            dealF5AddDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(currentTempTable).append(" WHERE NOT EXISTS").append(" (").append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" = ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 insert add data sql: [ {} ]", dealF5AddDataSql);
            sparkSession.sql(dealF5AddDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Insert add data into incremental table [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5AddDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5DelData() {
        StringBuilder dealF5DelDataSql = new StringBuilder();
        try {
            String colsMaxEdate = getProcessFieldInsertionSplicingSQL(tableName);
            dealF5DelDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT ").append(colsMaxEdate).append(" FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" ( SELECT * FROM ").append(currentTempTable).append(" WHERE").append(" ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(")").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 insert del data sql: [ {} ]", dealF5DelDataSql);
            sparkSession.sql(dealF5DelDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Insert add data into incremental table [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5DelDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5CurrentJobValidData() {
        StringBuilder dealF5CurrentJobValidDataSql = new StringBuilder();
        try {
            dealF5CurrentJobValidDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE (").append(" EXISTS (").append("SELECT ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(") AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(")").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 deal current job valid data sql: [ {} ]", dealF5CurrentJobValidDataSql);
            sparkSession.sql(dealF5CurrentJobValidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Insert current job valid data into the incremental table" + " ( historically valid ) [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5CurrentJobInvalidData() {
        StringBuilder dealF5CurrentJobInvalidDataSql = new StringBuilder();
        try {
            dealF5CurrentJobInvalidDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(_HYREN_E_DATE).append(" <> ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.info("F5 deal current job invalid data sql: [ {} ]", dealF5CurrentJobInvalidDataSql);
            sparkSession.sql(dealF5CurrentJobInvalidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F5 Insert expired data into the incremental temporary table" + " (historical linked data) [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5CurrentJobInvalidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5OtherJobData() {
        StringBuilder dealF5OtherJobDataSql = new StringBuilder();
        try {
            dealF5OtherJobDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" != ").append("'").append(jobNameParam).append("'");
            log.info("F5 deal other job data sql: [ {} ]", dealF5OtherJobDataSql);
            sparkSession.sql(dealF5OtherJobDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Insert other job data into the incremental table" + " [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5OtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void historyZipperIncrementLoading() {
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
            dealF3InvalidDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT ").append(invalid_insert_sql).append(" FROM ").append(tableName).append(" WHERE EXISTS").append(" (").append(" SELECT ").append(_HYREN_JOB_NAME).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = '").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
            log.info("F3 insert invalid data sql: [ {} ]", dealF3InvalidDataSql);
            sparkSession.sql(dealF3InvalidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert invalid data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3InvalidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3CurrentJobValidData() {
        StringBuilder dealF3CurrentJobValidDataSql = new StringBuilder();
        try {
            dealF3CurrentJobValidDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" (").append(" SELECT ").append(_HYREN_JOB_NAME).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_JOB_NAME).append("=").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
            log.info("F3 insert current job valid data sql: [ {} ]", dealF3CurrentJobValidDataSql);
            sparkSession.sql(dealF3CurrentJobValidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert current job valid data  (" + zipperTempTable + ") 的SQLfailed! " + " Execute sql: " + dealF3CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3CurrentJobInvalidData() {
        StringBuilder dealF3CurrentJobValidDataSql = new StringBuilder();
        dealF3CurrentJobValidDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_E_DATE).append(" <> ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
        try {
            log.info("F3 insert current job invalid data sql: [ {} ]", dealF3CurrentJobValidDataSql);
            sparkSession.sql(dealF3CurrentJobValidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert current job invalid data (" + zipperTempTable + ") 的SQLfailed! " + " Execute sql: " + dealF3CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3OtherJobData() {
        StringBuilder dealF3OtherJobDataSql = new StringBuilder();
        try {
            dealF3OtherJobDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" <> ").append("'").append(jobNameParam).append("'");
            log.info("F3 insert other job data sql: [ {} ]", dealF3OtherJobDataSql);
            sparkSession.sql(dealF3OtherJobDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert other job data sql (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3OtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    protected void dealF3CurrentDeltaData() {
        StringBuilder dealF3CurrentDeltaDataSql = new StringBuilder();
        try {
            dealF3CurrentDeltaDataSql.append("INSERT INTO ").append(zipperTempTable).append(getParttitionSplicingSQL()).append(" SELECT * FROM ").append(currentTempTable);
            log.info("F3 insert delta data sql: [ {} ]", dealF3CurrentDeltaDataSql);
            sparkSession.sql(dealF3CurrentDeltaDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert delta data sql (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3CurrentDeltaDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void incrementalDataZipper() {
        super.incrementalDataZipper();
    }

    private String getProcessFieldInsertionSplicingSQL(String srcTableName) {
        List<String> columns = StringUtil.split(hiveHandleArgs.getSrcColumn(), ",");
        StringBuilder select_sql_sb = new StringBuilder();
        for (String column : columns) {
            if (partitionFields.contains(column))
                continue;
            if (HYRENFIELD.contains(column.toUpperCase())) {
                if (column.equalsIgnoreCase(_HYREN_E_DATE)) {
                    select_sql_sb.append("'").append(etlDateWith8).append("'");
                } else {
                    select_sql_sb.append(srcTableName).append(".").append(column);
                }
            } else {
                select_sql_sb.append(srcTableName).append(".").append(column);
            }
            select_sql_sb.append(",");
        }
        if (!partitionFields.isEmpty()) {
            for (String partitionField : partitionFields) {
                select_sql_sb.append(srcTableName).append(".").append(partitionField).append(",");
            }
        }
        return select_sql_sb.delete(select_sql_sb.length() - 1, select_sql_sb.length()).toString();
    }

    private String getParttitionSplicingSQL() {
        StringBuilder parttitionSplicingSQL = new StringBuilder();
        if (!partitionFields.isEmpty()) {
            parttitionSplicingSQL.append(" PARTITION (");
            parttitionSplicingSQL.append(StringUtil.join(partitionFields, ","));
            parttitionSplicingSQL.append(")");
        }
        return parttitionSplicingSQL.toString();
    }
}
