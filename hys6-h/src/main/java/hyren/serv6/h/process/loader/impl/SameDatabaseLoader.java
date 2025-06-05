package hyren.serv6.h.process.loader.impl;

import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.ProcessType;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.DataBaseType;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import static hyren.serv6.commons.utils.constant.Constant.*;

@Slf4j
public class SameDatabaseLoader extends AbsLoaderImpl {

    private static final ThreadLocal<DatabaseWrapper> _dbBox = new ThreadLocal<>();

    private final DataBaseType databaseType;

    public SameDatabaseLoader(ProcessJobTableConfBean processJobTableConfBean) {
        super(processJobTableConfBean);
        createTableColumnTypes = buildCreateTableColumnTypes();
        databaseType = DataBaseType.getDatabase(tableLayerAttrs.get(StorageTypeKey.database_type));
        String srcColumn = srcFieldInfos.stream().map(DmJobTableFieldInfo::getJobtab_field_en_name).collect(Collectors.joining(","));
        isZipperFlag = processJobTableConfBean.getIsZipperFlag();
    }

    @Override
    public void init() {
        this.dropTableIfExists(getDataBaseDB(), currentTempTable);
        this.dropTableIfExists(getDataBaseDB(), restoreTempTable);
        this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
    }

    @Override
    public void ensureVersionRelation() {
        try (DatabaseWrapper db = getDataBaseDB()) {
            boolean isVersionExpire = versionManager.isVersionExpire();
            log.info("表 {} 版本信息是否过期: [ {} ] !", tableName, isVersionExpire);
            if (isVersionExpire) {
                if (!db.isExistTable(versionManager.getRenameTableName()) && db.isExistTable(tableName)) {
                    log.info("表 {} 版本过期, 确保版本关系!", tableName);
                    renameTable(db, tableName, versionManager.getRenameTableName());
                }
            }
            createTableIfNotExists(db, tableName);
        }
    }

    @Override
    public void restore() {
        log.info("作业回滚阶段,回滚表 [ {} ] 数据,开始", processJobTableConfBean.getTarTableName());
        DatabaseWrapper dsl_db;
        try {
            dsl_db = getDataBaseDB();
        } catch (Exception e) {
            throw new AppSystemException("初始化存储层DB对象失败! e: " + e);
        }
        String tableName = processJobTableConfBean.getTarTableName();
        IsFlag isZipperFlag = processJobTableConfBean.getIsZipperFlag();
        String restoreTempTable = processJobTableConfBean.getTarTableName() + "_restore";
        if (dsl_db.isExistTable(tableName)) {
            if (haveTodayData(processJobTableConfBean, dsl_db)) {
                String case_sql = "CASE " + _HYREN_E_DATE + " WHEN '" + etlDateWith8 + "' THEN '" + _MAX_DATE_8 + "'" + " ELSE " + _HYREN_E_DATE + " END AS " + _HYREN_E_DATE;
                if (dsl_db.isExistTable(restoreTempTable)) {
                    dsl_db.execute("DROP TABLE " + restoreTempTable);
                }
                createTableIfNotExists(dsl_db, restoreTempTable);
                StringBuilder selectSql = buildSelectSqlSplicing(dsl_db, case_sql);
                StringBuilder restore_sql_1 = new StringBuilder();
                restore_sql_1.append("INSERT INTO ").append(restoreTempTable).append(" SELECT ").append(selectSql).append(" FROM ").append(tableName).append(" WHERE ");
                restore_sql_1.append(_HYREN_JOB_NAME).append(" != '").append(jobNameParam).append("'");
                log.info("加工,执行回滚 [ 非当前作业数据 ] 数据的SQL: {}", restore_sql_1);
                dsl_db.execute(String.valueOf(restore_sql_1));
                StringBuilder restore_sql_2 = new StringBuilder();
                restore_sql_2.append("INSERT INTO ").append(restoreTempTable).append(" SELECT ").append(selectSql).append(" FROM ").append(tableName).append(" WHERE ");
                StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
                if (isZipperFlag == IsFlag.Shi) {
                    restore_sql_2.append(_HYREN_S_DATE).append(" <> '").append(etlDateWith8).append("'").append(" AND ");
                } else if (isZipperFlag == IsFlag.Fou) {
                    if (storageType != StorageType.TiHuan) {
                        restore_sql_2.append(_HYREN_S_DATE).append(" <> '").append(etlDateWith8).append("'").append(" AND ");
                    }
                }
                restore_sql_2.append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
                if (storageType != StorageType.TiHuan) {
                    log.info("加工,执行回滚 [ 当前作业非当天数据 ] 数据的SQL: {}", restore_sql_2);
                    dsl_db.execute(String.valueOf(restore_sql_2));
                }
                if (dsl_db.isExistTable(tableName)) {
                    dsl_db.execute("DROP TABLE " + tableName);
                }
                dsl_db.execute("ALTER TABLE " + restoreTempTable + " RENAME TO " + tableName);
            }
        } else {
            log.info("检查是否存在跑批日期数据时,表不存在,执行创建表操作 tableName: [ {} ]", tableName);
            createTableIfNotExists(dsl_db, tableName);
        }
        log.info("作业回滚阶段,回滚表 [ {} ] 数据,结束", processJobTableConfBean.getTarTableName());
    }

    private boolean haveTodayData(ProcessJobTableConfBean processJobTableConfBean, DatabaseWrapper db) {
        String tableName = processJobTableConfBean.getTarTableName();
        IsFlag isZipperFlag = processJobTableConfBean.getIsZipperFlag();
        String haveTodayDataSql = "";
        try {
            log.info("执行检查表 [ {} ] 当天是否进数!", tableName);
            Dbtype dbtype = db.getDbtype();
            ResultSet resultSet;
            if (isZipperFlag == IsFlag.Shi) {
                if (dbtype == Dbtype.ORACLE) {
                    haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' AND ROWNUM = 1", tableName, _HYREN_S_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    log.info("检查表: [ {} ] 存在当前新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                    resultSet = db.queryGetResultSet(haveTodayDataSql);
                    if (resultSet.next())
                        return true;
                    haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' AND ROWNUM = 1", tableName, _HYREN_E_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    log.info("检查表: [ {} ] 存在当前失效数据SQL: [ {} ]", tableName, haveTodayDataSql);
                    resultSet = db.queryGetResultSet(haveTodayDataSql);
                } else {
                    haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1", tableName, _HYREN_S_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    log.info("检查表: [ {} ] 存在当前新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                    resultSet = db.queryGetResultSet(haveTodayDataSql);
                    if (resultSet.next())
                        return true;
                    haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1", tableName, _HYREN_E_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    log.info("检查表: [ {} ] 存在当前失效数据SQL: [ {} ]", tableName, haveTodayDataSql);
                    resultSet = db.queryGetResultSet(haveTodayDataSql);
                }
                return resultSet.next();
            } else if (isZipperFlag == IsFlag.Fou) {
                StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
                if (storageType == StorageType.TiHuan) {
                    if (dbtype == Dbtype.ORACLE) {
                        haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND ROWNUM = 1", tableName, _HYREN_JOB_NAME, jobNameParam);
                    } else {
                        haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' LIMIT 1", tableName, _HYREN_JOB_NAME, jobNameParam);
                    }
                    log.info("检查表: [ {} ] 存在当前作业的新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                } else {
                    if (dbtype == Dbtype.ORACLE) {
                        haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' AND ROWNUM = 1", tableName, _HYREN_S_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    } else {
                        haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1", tableName, _HYREN_S_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    }
                    log.info("检查表: [ {} ] 存在当前日期的新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                }
                return db.queryGetResultSet(haveTodayDataSql).next();
            }
        } catch (SQLException throwables) {
            throw new AppSystemException("检查是否拥有今日数据的SQL执行失败! sql: " + haveTodayDataSql);
        }
        return false;
    }

    private StringBuilder buildSelectSqlSplicing(DatabaseWrapper dsl_db, String case_sql) {
        StringBuilder select_sql_sb = new StringBuilder();
        Dbtype dbtype = dsl_db.getDbtype();
        for (DmJobTableFieldInfo dmJobTableFieldInfo : processJobTableConfBean.getDmJobTableFieldInfos()) {
            String field_en_name = dmJobTableFieldInfo.getJobtab_field_en_name();
            if (HYRENFIELD.contains(field_en_name.toUpperCase())) {
                if (field_en_name.equalsIgnoreCase(_HYREN_E_DATE)) {
                    select_sql_sb.append(case_sql);
                } else {
                    select_sql_sb.append(field_en_name);
                }
            } else {
                select_sql_sb.append(dbtype.ofEscapedkey(field_en_name));
            }
            select_sql_sb.append(",");
        }
        select_sql_sb.delete(select_sql_sb.length() - 1, select_sql_sb.length());
        return select_sql_sb;
    }

    @Override
    public void append() {
        try {
            insertData(tableName);
            jobRunStatus = ProcessJobRunStatusEnum.FINISHED;
        } catch (Exception e) {
            jobRunStatus = ProcessJobRunStatusEnum.FAILED;
        }
    }

    @Override
    public void replace() {
        try {
            insertData(tableName);
            jobRunStatus = ProcessJobRunStatusEnum.FINISHED;
        } catch (Exception e) {
            jobRunStatus = ProcessJobRunStatusEnum.FAILED;
        }
    }

    private void insertData(String tableName) {
        String excuteSql = "INSERT INTO %s ( %s ) SELECT %s FROM ( %s ) a";
        try (DatabaseWrapper db = getDataBaseDB()) {
            if (db.getDbtype() == Dbtype.MYSQL) {
                excuteSql += ",(SELECT @row_number:=0) AS t";
            }
            String columns = processJobTableConfBean.getDmJobTableFieldInfos().stream().map(DmJobTableFieldInfo::getJobtab_field_en_name).collect(Collectors.joining(","));
            db.execute(String.format(excuteSql, tableName, columns, realSelectExpr(), processJobTableConfBean.getCompleteSql()));
            db.commit();
        } catch (Exception e) {
            throw new AppSystemException("插入数据失败：e：%s", e);
        }
    }

    private String realSelectExpr() {
        final StringBuilder selectExpr = new StringBuilder(120);
        final StringBuilder md5Cols = new StringBuilder(120);
        processJobTableConfBean.getDmJobTableFieldInfos().stream().filter(field -> !Constant.HYRENFIELD.contains(field.getJobtab_field_en_name().toUpperCase())).forEach(field -> {
            String processCode = field.getJobtab_field_process();
            if (ProcessType.ZiZeng.getCode().equals(processCode)) {
                if (getDataBaseDB().getDbtype() == Dbtype.MYSQL) {
                    selectExpr.append(mySql8Following());
                } else {
                    selectExpr.append(autoIncreasingExpr());
                }
            } else if (ProcessType.YingShe.getCode().equals(processCode) || ProcessType.FenZhuYingShe.getCode().equals(processCode) || ProcessType.HanShuYingShe.getCode().equals(processCode) || ProcessType.DingZhi.getCode().equals(processCode)) {
                selectExpr.append(field.getJobtab_field_en_name());
                md5Cols.append(field.getJobtab_field_en_name()).append(',');
            } else {
                throw new AppSystemException("不支持处理方式码：" + processCode);
            }
            selectExpr.append(',');
        });
        selectExpr.deleteCharAt(selectExpr.length() - 1);
        selectExpr.append(',').append("'").append(jobNameParam).append("'");
        selectExpr.append(',').append("'").append(etlDateWith8).append("'");
        if (processJobTableConfBean.getIsZipperFlag() == IsFlag.Shi) {
            selectExpr.append(',').append(_MAX_DATE_8);
            selectExpr.append(',').append(lineMd5Expr(md5Cols.deleteCharAt(md5Cols.length() - 1).toString()));
        }
        return selectExpr.toString();
    }

    @Override
    public void upSert() {
        try {
            this.createTableIfNotExists(getDataBaseDB(), tableName);
            this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
            this.createTableIfNotExists(getDataBaseDB(), zipperTempTable);
            this.dropTableIfExists(getDataBaseDB(), currentTempTable);
            this.createTableIfNotExists(getDataBaseDB(), currentTempTable);
            this.generateCurrentTempTable(getDataBaseDB(), currentTempTable);
            this.dealUpsetCurrentJobUndeleteData();
            this.dealUpsetOtherJobData();
            this.dealUpsetCurrentDeltaData();
            this.dropTableIfExists(getDataBaseDB(), tableName);
            this.mergeIncrement(getDataBaseDB(), zipperTempTable, tableName);
            jobRunStatus = ProcessJobRunStatusEnum.FINISHED;
        } catch (Exception e) {
            jobRunStatus = ProcessJobRunStatusEnum.FAILED;
        }
    }

    private void dealUpsetCurrentJobUndeleteData() {
        StringBuilder dealUpsetCurrentJobUndeleteDataSql = new StringBuilder();
        try {
            dealUpsetCurrentJobUndeleteDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append("=").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.debug("Upset insert undelete data sql: [ {} ]", dealUpsetCurrentJobUndeleteDataSql);
            getDataBaseDB().execute(dealUpsetCurrentJobUndeleteDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Upset insert undelete data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealUpsetCurrentJobUndeleteDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealUpsetOtherJobData() {
        StringBuilder dealUpsetOtherJobDataSql = new StringBuilder();
        try {
            dealUpsetOtherJobDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" != ").append("'").append(jobNameParam).append("'");
            log.debug("Upset insert other job data sql: [ {} ]", dealUpsetOtherJobDataSql);
            getDataBaseDB().execute(dealUpsetOtherJobDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("Upset insert other job data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealUpsetOtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealUpsetCurrentDeltaData() {
        StringBuilder dealUpsetCurrentDeltaDataSql = new StringBuilder();
        try {
            dealUpsetCurrentDeltaDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(currentTempTable);
            getDataBaseDB().execute(dealUpsetCurrentDeltaDataSql.toString());
            log.debug("Upset insert current delta data sql: [ {} ]", dealUpsetCurrentDeltaDataSql);
        } catch (Exception e) {
            throw new AppSystemException("Upset insert current delta data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealUpsetCurrentDeltaDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void historyZipperFullLoading() {
        try {
            this.createTableIfNotExists(getDataBaseDB(), tableName);
            this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
            this.createTableIfNotExists(getDataBaseDB(), zipperTempTable);
            this.dropTableIfExists(getDataBaseDB(), currentTempTable);
            this.createTableIfNotExists(getDataBaseDB(), currentTempTable);
            this.generateCurrentTempTable(getDataBaseDB(), currentTempTable);
            this.dealF5AddData();
            this.dealF5DelData();
            this.dealF5CurrentJobValidData();
            this.dealF5CurrentJobInvalidData();
            this.dealF5OtherJobData();
            this.mergeIncrement(getDataBaseDB(), zipperTempTable, tableName);
            jobRunStatus = ProcessJobRunStatusEnum.FINISHED;
        } catch (Exception e) {
            jobRunStatus = ProcessJobRunStatusEnum.FAILED;
        }
    }

    private void dealF5AddData() {
        StringBuilder dealF5AddDataSql = new StringBuilder();
        try {
            dealF5AddDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(currentTempTable).append(" WHERE NOT EXISTS").append(" (").append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" = ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.debug("F5 insert add data sql: [ {} ]", dealF5AddDataSql);
            getDataBaseDB().execute(dealF5AddDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F5 insert add data into incremental table [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5AddDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5DelData() {
        StringBuilder dealF5DelDataSql = new StringBuilder();
        try {
            String colsMaxEdate = getProcessFieldInsertionSplicingSQL(tableName);
            dealF5DelDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT ").append(colsMaxEdate).append(" FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" ( SELECT * FROM ").append(currentTempTable).append(" WHERE").append(" ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" = ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(")").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.debug("F5 insert del data sql: [ {} ]", dealF5DelDataSql);
            getDataBaseDB().execute(dealF5DelDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F5 insert add data into incremental table [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5DelDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5CurrentJobValidData() {
        StringBuilder dealF5CurrentJobValidDataSql = new StringBuilder();
        try {
            dealF5CurrentJobValidDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE (").append(" EXISTS (").append("SELECT ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" = ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(") AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(")").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.debug("F5 deal current job valid data sql: [ {} ]", dealF5CurrentJobValidDataSql);
            getDataBaseDB().execute(dealF5CurrentJobValidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F5 insert current job valid data into the incremental table" + " ( historically valid ) [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5CurrentJobInvalidData() {
        StringBuilder dealF5CurrentJobInvalidDataSql = new StringBuilder();
        try {
            dealF5CurrentJobInvalidDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(_HYREN_E_DATE).append(" <> ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
            log.debug("F5 deal current job invalid data sql: [ {} ]", dealF5CurrentJobInvalidDataSql);
            getDataBaseDB().execute(dealF5CurrentJobInvalidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F5 insert expired data into the incremental temporary table" + " (historical linked data) [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5CurrentJobInvalidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF5OtherJobData() {
        StringBuilder dealF5OtherJobDataSql = new StringBuilder();
        try {
            dealF5OtherJobDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" != ").append("'").append(jobNameParam).append("'");
            log.debug("F5 deal other job data sql: [ {} ]", dealF5OtherJobDataSql);
            getDataBaseDB().execute(dealF5OtherJobDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F5 insert other job data into the incremental table" + " [ " + zipperTempTable + " ] failed! " + " Execute sql: " + dealF5OtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void historyZipperIncrementLoading() {
        try {
            this.createTableIfNotExists(getDataBaseDB(), tableName);
            this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
            this.createTableIfNotExists(getDataBaseDB(), zipperTempTable);
            this.dropTableIfExists(getDataBaseDB(), currentTempTable);
            this.createTableIfNotExists(getDataBaseDB(), currentTempTable);
            this.generateCurrentTempTable(getDataBaseDB(), currentTempTable);
            this.dealF3InvalidData();
            this.dealF3CurrentJobValidData();
            this.dealF3CurrentJobInvalidData();
            this.dealF3OtherJobData();
            this.dealF3CurrentDeltaData();
            this.mergeIncrement(getDataBaseDB(), zipperTempTable, tableName);
            jobRunStatus = ProcessJobRunStatusEnum.FINISHED;
        } catch (Exception e) {
            jobRunStatus = ProcessJobRunStatusEnum.FAILED;
        }
    }

    private void dealF3InvalidData() {
        String invalid_insert_sql = getProcessFieldInsertionSplicingSQL(tableName);
        StringBuilder dealF3InvalidDataSql = new StringBuilder(120);
        try {
            dealF3InvalidDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT ").append(invalid_insert_sql).append(" FROM ").append(tableName).append(" WHERE EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append(" = ").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = '").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
            log.debug("F3 insert invalid data sql: [ {} ]", dealF3InvalidDataSql);
            getDataBaseDB().execute(dealF3InvalidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert invalid data (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3InvalidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3CurrentJobValidData() {
        StringBuilder dealF3CurrentJobValidDataSql = new StringBuilder();
        try {
            dealF3CurrentJobValidDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE NOT EXISTS").append(" (").append(" SELECT ").append(_HYREN_MD5_VAL).append(" FROM ").append(currentTempTable).append(" WHERE ").append(tableName).append(".").append(_HYREN_MD5_VAL).append("=").append(currentTempTable).append(".").append(_HYREN_MD5_VAL).append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append(currentTempTable).append(".").append(_HYREN_JOB_NAME).append(" )").append(" AND ").append(tableName).append(".").append(_HYREN_E_DATE).append(" = ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = '").append(jobNameParam).append("'");
            log.debug("F3 insert current job valid data sql: [ {} ]", dealF3CurrentJobValidDataSql);
            getDataBaseDB().execute(dealF3CurrentJobValidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert current job valid data  (" + zipperTempTable + ") 的SQLfailed! " + " Execute sql: " + dealF3CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3CurrentJobInvalidData() {
        StringBuilder dealF3CurrentJobValidDataSql = new StringBuilder();
        dealF3CurrentJobValidDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_E_DATE).append(" <> ").append("'").append(_MAX_DATE_8).append("'").append(" AND ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" = ").append("'").append(jobNameParam).append("'");
        try {
            log.debug("F3 insert current job invalid data sql: [ {} ]", dealF3CurrentJobValidDataSql);
            getDataBaseDB().execute(dealF3CurrentJobValidDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert current job invalid data (" + zipperTempTable + ") 的SQLfailed! " + " Execute sql: " + dealF3CurrentJobValidDataSql + " Exception: " + e.getMessage());
        }
    }

    private void dealF3OtherJobData() {
        StringBuilder dealF3OtherJobDataSql = new StringBuilder();
        try {
            dealF3OtherJobDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(tableName).append(" WHERE").append(" ").append(tableName).append(".").append(_HYREN_JOB_NAME).append(" <> ").append("'").append(jobNameParam).append("'");
            log.debug("F3 insert other job data sql: [ {} ]", dealF3OtherJobDataSql);
            getDataBaseDB().execute(dealF3OtherJobDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert other job data sql (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3OtherJobDataSql + " Exception: " + e.getMessage());
        }
    }

    protected void dealF3CurrentDeltaData() {
        StringBuilder dealF3CurrentDeltaDataSql = new StringBuilder();
        try {
            dealF3CurrentDeltaDataSql.append("INSERT INTO ").append(zipperTempTable).append(" SELECT * FROM ").append(currentTempTable);
            log.debug("F3 insert delta data sql: [ {} ]", dealF3CurrentDeltaDataSql);
            getDataBaseDB().execute(dealF3CurrentDeltaDataSql.toString());
        } catch (Exception e) {
            throw new AppSystemException("F3 insert delta data sql (" + zipperTempTable + ") failed! " + " Execute sql: " + dealF3CurrentDeltaDataSql + " Exception: " + e.getMessage());
        }
    }

    @Override
    public void handleException() {
        getDataBaseDB().rollback();
    }

    @Override
    public void clean() {
        this.dropTableIfExists(getDataBaseDB(), currentTempTable);
        this.dropTableIfExists(getDataBaseDB(), restoreTempTable);
        this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
    }

    @Override
    public void close() {
        try {
            getDataBaseDB().close();
            _dbBox.remove();
            log.info("SameDatabaseLoader 关闭 db !");
        } catch (Exception e) {
            throw new AppSystemException("DatabaseLoader 关闭db发生异常! " + e);
        }
    }

    @Override
    public ProcessJobRunStatusEnum getJobRunStatus() {
        return jobRunStatus;
    }

    private String getProcessFieldInsertionSplicingSQL(String srcTableName) {
        List<String> columns = processJobTableConfBean.getDmJobTableFieldInfos().stream().map(DmJobTableFieldInfo::getJobtab_field_en_name).collect(Collectors.toList());
        StringBuilder select_sql_sb = new StringBuilder();
        for (String column : columns) {
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
        return select_sql_sb.delete(select_sql_sb.length() - 1, select_sql_sb.length()).toString();
    }

    private DatabaseWrapper getDataBaseDB() {
        DatabaseWrapper db = _dbBox.get();
        if (db == null || !db.isConnected()) {
            try {
                log.info("SameDatabaseLoader 的 db 为 null 初始化创建 db");
                db = super.getDB();
                _dbBox.set(db);
            } catch (Exception e) {
                throw new AppSystemException("SameDatabaseLoader 初始化创建 db 失败!" + e);
            }
        } else {
            try {
                db.isConnected();
            } catch (Exception e) {
                try {
                    log.info("SameDatabaseLoader 的 db 可能失效! 重新创建 db !");
                    db = this.getDB();
                    log.info("SameDatabaseLoader 重新创建 db 成功 !");
                } catch (Exception ex) {
                    throw new AppSystemException("DatabaseLoader 重新创建 db 失败!" + ex);
                }
                _dbBox.set(db);
            }
        }
        return db;
    }

    private String autoIncreasingExpr() {
        List<String> additionalAttrs = processJobTableConfBean.getFieldAdditionalInfoMap().get(StoreLayerAdded.ZhuJian.getCode());
        String orderByCol = "";
        if (additionalAttrs != null) {
            orderByCol = additionalAttrs.get(0);
        } else {
            List<DmJobTableFieldInfo> dmJobTableFieldInfos = processJobTableConfBean.getDmJobTableFieldInfos();
            for (DmJobTableFieldInfo field_info : dmJobTableFieldInfos) {
                if (!field_info.getJobtab_field_en_name().startsWith("hyren_") && !ProcessType.ZiZeng.getCode().equals(field_info.getJobtab_field_process())) {
                    orderByCol = field_info.getJobtab_field_en_name();
                    break;
                }
            }
        }
        if (StringUtil.isBlank(orderByCol)) {
            throw new AppSystemException("找不到自增函数的排序列");
        }
        return " row_number() over (order by " + orderByCol + ")";
    }

    private String mySql8Following() {
        return " (@row_number:=@row_number + 1) ";
    }

    private String lineMd5Expr(String columnsJoin) {
        List<String> columnList = StringUtil.split(columnsJoin, ",");
        if (columnList == null || columnList.isEmpty()) {
            throw new AppSystemException("SameDatabaseLoader 计算MD5的字段列表不能空!");
        }
        List<String> columnNames = new ArrayList<>(columnList);
        return databaseType.getDbtype().ofColMd5(getDataBaseDB(), columnNames);
    }

    private String buildCreateTableColumnTypes() {
        final StringBuilder column_type_sql = new StringBuilder();
        for (DmJobTableFieldInfo field : processJobTableConfBean.getDmJobTableFieldInfos()) {
            column_type_sql.append(field.getJobtab_field_en_name()).append(" ").append(field.getJobtab_field_type());
            String fieldLength = field.getJobtab_field_length();
            if (StringUtil.isNotBlank(fieldLength)) {
                column_type_sql.append("(").append(fieldLength).append(")");
            }
            column_type_sql.append(",");
        }
        column_type_sql.deleteCharAt(column_type_sql.length() - 1);
        return column_type_sql.toString();
    }

    private void generateCurrentTempTable(DatabaseWrapper db, String tarTableName) {
        StringBuilder select_sql = new StringBuilder();
        for (DmJobTableFieldInfo fieldInfo : srcFieldInfos) {
            String finalColName = getDataBaseDB().getDbtype().ofEscapedkey(fieldInfo.getJobtab_field_en_name());
            select_sql.append(finalColName).append(",");
        }
        select_sql.deleteCharAt(select_sql.length() - 1);
        select_sql.append("," + "'").append(jobNameParam).append("'");
        String colMd5 = "";
        Dbtype dbtype = db.getDbtype();
        if (isZipperFlag == IsFlag.Shi) {
            if (storageType == StorageType.QuanLiang) {
                List<String> finalColumnForMD5 = processJobTableConfBean.getDmJobTableFieldInfos().stream().map(DmJobTableFieldInfo::getJobtab_field_en_name).filter(jobtabFieldEnName -> !HYRENFIELD.contains(jobtabFieldEnName.toUpperCase())).sorted().collect(Collectors.toList());
                log.info("table: [ {} ], storageType: [ {} ], final calc finalColumnForMD5: [ {} ]", tarTableName, storageType.getValue(), JsonUtil.toJson(finalColumnForMD5));
                colMd5 = dbtype.ofColMd5(db, finalColumnForMD5);
                select_sql.append(",'").append(etlDateWith8).append("','").append(_MAX_DATE_8).append("'");
            }
            if (storageType == StorageType.LiShiLaLian) {
                if (primaryKeyInfos.isEmpty()) {
                    throw new AppSystemException("进数方式为: " + storageType.getValue() + " 时, 主键配置不能为空!");
                }
                List<String> calc_md5_col_s = primaryKeyInfos;
                List<String> finalColumnForMD5 = calc_md5_col_s.stream().sorted().collect(Collectors.toList());
                colMd5 = dbtype.ofColMd5(db, finalColumnForMD5);
                log.info("table: [ {} ], storageType: [ {} ], final calc finalColumnForMD5: [ {} ]", tarTableName, storageType.getValue(), JsonUtil.toJson(finalColumnForMD5));
                select_sql.append(",'").append(etlDateWith8).append("','").append(_MAX_DATE_8).append("'");
            }
            if (storageType == StorageType.ZengLiang) {
                throw new AppSystemException("进数方式: " + storageType.getValue() + " ,暂未实现!");
            }
            select_sql.append(",").append(colMd5);
        } else {
            if (storageType == StorageType.ZhuiJia || storageType == StorageType.UpSet) {
                select_sql.append(",'").append(etlDateWith8).append("'");
                if (storageType == StorageType.UpSet) {
                    if (primaryKeyInfos.isEmpty()) {
                        throw new AppSystemException("进数方式为: " + storageType.getValue() + " 时, 主键配置不能为空!");
                    }
                    List<String> calc_md5_col_s = primaryKeyInfos;
                    List<String> finalColumnForMD5 = calc_md5_col_s.stream().sorted().collect(Collectors.toList());
                    colMd5 = dbtype.ofColMd5(db, finalColumnForMD5);
                    log.info("table: [ {} ], storageType: [ {} ], final calc finalColumnForMD5: [ {} ]", tarTableName, storageType.getValue(), JsonUtil.toJson(finalColumnForMD5));
                    select_sql.append(",").append(colMd5);
                }
            }
        }
        StringBuilder copy_data_sql = new StringBuilder();
        try {
            log.debug("生成当天数据表 [ " + tarTableName + " ] 时的查询字段列表[ " + select_sql + " ]");
            copy_data_sql.append("INSERT INTO ").append(tarTableName);
            copy_data_sql.append(" SELECT ").append(select_sql).append(" FROM ").append("( ").append(processJobTableConfBean.getCompleteSql()).append(" ) a");
            db.execute(copy_data_sql.toString());
        } catch (Exception e) {
            throw new AppSystemException("执行复制当前批次表数据到增量表 (" + tarTableName + ") 失败!" + " 执行sql: " + copy_data_sql + " 异常: " + e);
        }
    }

    private void renameTable(DatabaseWrapper db, String srcTableName, String destTableName) {
        boolean srcTable_is_exist = db.isExistTable(srcTableName);
        log.info("临时表名称: " + srcTableName + " 是否存在: " + srcTable_is_exist);
        if (!srcTable_is_exist) {
            throw new AppSystemException("表" + srcTableName + "不存在,无法重命名成" + destTableName);
        }
        boolean destTable_is_exist = db.isExistTable(destTableName);
        log.info("最终表名称: " + destTableName + " 是否存在: " + destTable_is_exist);
        if (destTable_is_exist) {
            throw new AppSystemException("表" + destTableName + "已存在,无法重命名成" + destTableName);
        }
        String renameSql = db.getDbtype().ofRenameSql(srcTableName, destTableName, db);
        db.ExecDDL(renameSql);
    }

    public void createTableIfNotExists(DatabaseWrapper db, String tableName) {
        log.debug("创建表 [ " + tableName + " ] 时的字段片段信息 [ " + createTableColumnTypes + " ] ");
        StringBuilder create_table_sql = new StringBuilder();
        try {
            if (!db.isExistTable(tableName)) {
                create_table_sql.append("CREATE TABLE ");
                create_table_sql.append(tableName);
                create_table_sql.append("(");
                create_table_sql.append(createTableColumnTypes);
                create_table_sql.append(")");
                log.debug("创建表 [ " + tableName + " ] 时生成的建表语句 [ " + create_table_sql + " ] ");
                db.execute(create_table_sql.toString());
            }
        } catch (Exception e) {
            throw new AppSystemException("执行创建表 (" + tableName + ") 的SQL执行失败! " + " 执行sql: " + create_table_sql + " 异常: " + e.getMessage());
        }
    }

    private void dropTableIfExists(DatabaseWrapper db, String tableName) {
        try {
            if (db.isExistTable(tableName)) {
                log.info("需要删除的表: " + tableName + " 已经存在,执行删除!");
                db.ExecDDL("DROP TABLE " + tableName);
            }
        } catch (Exception e) {
            throw new AppSystemException("删除表失败: " + tableName, e);
        }
    }

    private void mergeIncrement(DatabaseWrapper db, String incrementTempTable, String finalTable) {
        String alter_table_sql = "", analyze_table_sql = "";
        try {
            dropTableIfExists(db, finalTable);
            alter_table_sql = "ALTER TABLE " + incrementTempTable + " RENAME TO " + finalTable;
            db.execute(alter_table_sql);
        } catch (Exception e) {
            throw new AppSystemException("执行据临时增量表合并出新的增量表 (" + finalTable + ") 的SQL执行失败! " + " ,重命名增量临时表为增量结果表sql: " + alter_table_sql + " ,异常: " + e.getMessage());
        }
    }
}
