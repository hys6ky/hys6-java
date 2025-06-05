package hyren.serv6.h.process.loader.impl;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.h.process.args.DatabaseHandleArgs;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.spark.launcher.SparkLauncherRunner;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;
import static hyren.serv6.commons.utils.constant.Constant.*;
import static hyren.serv6.commons.utils.storagelayer.StorageTypeKey.*;

@Slf4j
public class DatabaseLoader extends AbsLoaderImpl {

    private final DatabaseHandleArgs databaseHandleArgs = new DatabaseHandleArgs();

    private static final ThreadLocal<DatabaseWrapper> _dbBox = new ThreadLocal<>();

    public DatabaseLoader(ProcessJobTableConfBean processJobTableConfBean) {
        super(processJobTableConfBean);
        createTableColumnTypes = buildCreateTableColumnTypes();
        databaseHandleArgs.setModuleTableId_JobTableId(processJobTableConfBean.getModuleTableId() + "_" + processJobTableConfBean.getJobTableId());
        databaseHandleArgs.setStoreType(Store_type.DATABASE);
        databaseHandleArgs.setEtlDateWith8(this.etlDateWith8);
        databaseHandleArgs.setTableName(tableName);
        String srcColumn = processJobTableConfBean.getDmJobTableFieldInfos().stream().map(DmJobTableFieldInfo::getJobtab_field_en_name).collect(Collectors.joining(","));
        databaseHandleArgs.setSrcColumn(srcColumn);
        databaseHandleArgs.setDriver(tableLayerAttrs.get(database_driver));
        databaseHandleArgs.setUrl(tableLayerAttrs.get(jdbc_url));
        databaseHandleArgs.setUser(tableLayerAttrs.get(user_name));
        databaseHandleArgs.setPassword(tableLayerAttrs.get(database_pwd));
        databaseHandleArgs.setDatabaseType(tableLayerAttrs.get(database_type));
        databaseHandleArgs.setDatabase(tableLayerAttrs.get(StorageTypeKey.database_name));
        databaseHandleArgs.setIsTempFlag(processJobTableConfBean.getIsTempFlag());
        isZipperFlag = processJobTableConfBean.getIsZipperFlag();
        databaseHandleArgs.setJobNameParam(processJobTableConfBean.getJobNameParam());
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
                this.createTableIfNotExists(dsl_db, restoreTempTable);
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
            this.createTableIfNotExists(dsl_db, tableName);
        }
        log.info("作业回滚阶段,回滚表 [ {} ] 数据,结束", processJobTableConfBean.getTarTableName());
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
        } catch (SQLException e) {
            String message = String.format("检查是否拥有今日数据的SQL执行失败! sql: %s, e:%s", haveTodayDataSql, e);
            log.error(message);
            throw new BusinessException(message);
        }
        return false;
    }

    @Override
    public void append() {
        this.createTableIfNotExists(getDataBaseDB(), tableName);
        databaseHandleArgs.setStorageType(StorageType.ZhuiJia);
        databaseHandleArgs.setTableName(tableName);
        jobRunStatus = SparkLauncherRunner.runJob(databaseHandleArgs);
    }

    @Override
    public void replace() {
        this.createTableIfNotExists(getDataBaseDB(), tableName);
        databaseHandleArgs.setStorageType(StorageType.TiHuan);
        databaseHandleArgs.setTableName(tableName);
        jobRunStatus = SparkLauncherRunner.runJob(databaseHandleArgs);
    }

    @Override
    public void upSert() {
        this.createTableIfNotExists(getDataBaseDB(), tableName);
        this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
        this.createTableIfNotExists(getDataBaseDB(), zipperTempTable);
        databaseHandleArgs.setStorageType(StorageType.UpSet);
        jobRunStatus = SparkLauncherRunner.runJob(databaseHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            this.dropTableIfExists(getDataBaseDB(), tableName);
            mergeIncrement(getDataBaseDB(), zipperTempTable, tableName);
        }
    }

    @Override
    public void historyZipperFullLoading() {
        this.createTableIfNotExists(getDataBaseDB(), tableName);
        this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
        this.createTableIfNotExists(getDataBaseDB(), zipperTempTable);
        databaseHandleArgs.setStorageType(StorageType.QuanLiang);
        jobRunStatus = SparkLauncherRunner.runJob(databaseHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            this.dropTableIfExists(getDataBaseDB(), tableName);
            mergeIncrement(getDataBaseDB(), zipperTempTable, tableName);
        }
    }

    @Override
    public void historyZipperIncrementLoading() {
        this.createTableIfNotExists(getDataBaseDB(), tableName);
        this.dropTableIfExists(getDataBaseDB(), zipperTempTable);
        this.createTableIfNotExists(getDataBaseDB(), zipperTempTable);
        databaseHandleArgs.setStorageType(StorageType.LiShiLaLian);
        jobRunStatus = SparkLauncherRunner.runJob(databaseHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            this.dropTableIfExists(getDataBaseDB(), tableName);
            mergeIncrement(getDataBaseDB(), zipperTempTable, tableName);
        }
    }

    @Override
    public void incrementalDataZipper() {
        super.incrementalDataZipper();
    }

    @Override
    public ProcessJobRunStatusEnum getJobRunStatus() {
        return jobRunStatus;
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
            DatabaseWrapper db = getDataBaseDB();
            db.close();
            _dbBox.remove();
            log.info("DatabaseLoader 关闭 db : {}", db.getID());
        } catch (Exception e) {
            throw new AppSystemException("DatabaseLoader 关闭db失败! " + e);
        }
    }

    private DatabaseWrapper getDataBaseDB() {
        DatabaseWrapper db = _dbBox.get();
        if (db == null || !db.isConnected()) {
            try {
                log.info("DatabaseLoader 的 db 为 null 初始化创建 DatabaseLoader DB");
                db = super.getDB();
                _dbBox.set(db);
            } catch (Exception e) {
                throw new AppSystemException("DatabaseLoader 初始化创建 db 失败!" + e);
            }
        } else {
            try {
                db.isConnected();
            } catch (Exception e) {
                try {
                    log.info("DatabaseLoader 的 db 可能失效! 重新创建 db !");
                    db = super.getDB();
                    log.info("DatabaseLoader 重新创建 db 成功 !");
                } catch (Exception ex) {
                    throw new AppSystemException("DatabaseLoader 重新创建 db 失败!" + ex);
                }
                _dbBox.set(db);
            }
        }
        return db;
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
