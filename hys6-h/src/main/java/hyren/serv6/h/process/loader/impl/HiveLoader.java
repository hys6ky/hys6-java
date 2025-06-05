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
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.args.HiveHandleArgs;
import hyren.serv6.h.process.spark.launcher.SparkLauncherRunner;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import static hyren.serv6.commons.utils.constant.Constant.*;

@Slf4j
public class HiveLoader extends AbsLoaderImpl {

    private final HiveHandleArgs hiveHandleArgs = new HiveHandleArgs();

    private static final ThreadLocal<DatabaseWrapper> _dbBox = new ThreadLocal<>();

    public HiveLoader(ProcessJobTableConfBean processJobTableConfBean) {
        super(processJobTableConfBean);
        initHiveHandleArgs();
        createTableColumnTypes = buildCreateTableColumnTypes();
        tableLayerAttrs.put(StorageTypeKey.database_type, Dbtype.HIVE.name());
    }

    private void initHiveHandleArgs() {
        hiveHandleArgs.setStoreType(Store_type.HIVE);
        hiveHandleArgs.setEtlDateWith8(etlDateWith8);
        hiveHandleArgs.setTableName(tableName);
        hiveHandleArgs.setDatatableId(moduleTableId);
        String srcColumns = processJobTableConfBean.getDmJobTableFieldInfos().stream().map(DmJobTableFieldInfo::getJobtab_field_en_name).collect(Collectors.joining(","));
        hiveHandleArgs.setSrcColumn(srcColumns);
        hiveHandleArgs.setDatabase(tableLayerAttrs.get(StorageTypeKey.database_name));
        hiveHandleArgs.setIsTempFlag(processJobTableConfBean.getIsTempFlag());
        hiveHandleArgs.setModuleTableId_JobTableId(moduleTableId_JobTableId);
        hiveHandleArgs.setDoiId(jobTableId);
        isZipperFlag = processJobTableConfBean.getIsZipperFlag();
        hiveHandleArgs.setPartitionFields(partitionFields);
        hiveHandleArgs.setJobNameParam(processJobTableConfBean.getJobNameParam());
    }

    @Override
    public void init() {
        this.dropTableIfExists(getHiveDB(), currentTempTable);
        this.dropTableIfExists(getHiveDB(), restoreTempTable);
        this.dropTableIfExists(getHiveDB(), zipperTempTable);
    }

    @Override
    public void ensureVersionRelation() {
        try (DatabaseWrapper db = getHiveDB()) {
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
            dsl_db = getHiveDB();
        } catch (Exception e) {
            throw new AppSystemException("初始化存储层DB对象失败! e: " + e);
        }
        String tableName = processJobTableConfBean.getTarTableName();
        IsFlag isZipperFlag = processJobTableConfBean.getIsZipperFlag();
        List<String> partitionFields = processJobTableConfBean.getPartitionFields();
        String restoreTempTable = processJobTableConfBean.getTarTableName() + "_restore";
        if (dsl_db.isExistTable(tableName)) {
            if (haveTodayData(processJobTableConfBean, dsl_db)) {
                String case_sql = "CASE " + _HYREN_E_DATE + " WHEN '" + etlDateWith8 + "' THEN '" + _MAX_DATE_8 + "'" + " ELSE " + _HYREN_E_DATE + " END AS " + _HYREN_E_DATE;
                dsl_db.execute("DROP TABLE IF EXISTS " + restoreTempTable);
                this.createTableIfNotExists(dsl_db, restoreTempTable);
                StringBuilder selectSql = buildSelectSqlSplicing(dsl_db, partitionFields, case_sql);
                StringBuilder restore_sql_1 = new StringBuilder();
                restore_sql_1.append("INSERT INTO ").append(restoreTempTable).append(getParttitionSplicingSQL()).append(" SELECT ").append(selectSql).append(" FROM ").append(tableName).append(" WHERE ");
                restore_sql_1.append(_HYREN_JOB_NAME).append(" != '").append(jobNameParam).append("'");
                log.info("加工,执行回滚 [ 非当前作业数据 ] 数据的SQL: {}", restore_sql_1);
                dsl_db.execute(String.valueOf(restore_sql_1));
                StringBuilder restore_sql_2 = new StringBuilder();
                restore_sql_2.append("INSERT INTO ").append(restoreTempTable).append(getParttitionSplicingSQL()).append(" SELECT ").append(selectSql).append(" FROM ").append(tableName).append(" WHERE ");
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
                dsl_db.execute("DROP TABLE IF EXISTS " + tableName);
                dsl_db.execute("ALTER TABLE " + restoreTempTable + " RENAME TO " + tableName);
            }
        } else {
            log.info("检查是否存在跑批日期数据时,表不存在,执行创建表操作 tableName: [ {} ]", tableName);
            this.createTableIfNotExists(dsl_db, tableName);
        }
        log.info("作业回滚阶段,回滚表 [ {} ] 数据,结束", processJobTableConfBean.getTarTableName());
    }

    private StringBuilder buildSelectSqlSplicing(DatabaseWrapper dsl_db, List<String> partitionFields, String case_sql) {
        StringBuilder select_sql_sb = new StringBuilder();
        Dbtype dbtype = dsl_db.getDbtype();
        for (DmJobTableFieldInfo dmJobTableFieldInfo : processJobTableConfBean.getDmJobTableFieldInfos()) {
            String field_en_name = dmJobTableFieldInfo.getJobtab_field_en_name();
            if (partitionFields.contains(field_en_name))
                continue;
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
        if (!partitionFields.isEmpty()) {
            for (String partitionField : partitionFields) {
                for (DmJobTableFieldInfo dmJobTableFieldInfo : processJobTableConfBean.getDmJobTableFieldInfos()) {
                    if (partitionField.equalsIgnoreCase(dmJobTableFieldInfo.getJobtab_field_en_name())) {
                        select_sql_sb.append(",");
                        select_sql_sb.append(dbtype.ofEscapedkey(partitionField));
                    }
                }
            }
        }
        return select_sql_sb;
    }

    private boolean haveTodayData(ProcessJobTableConfBean processJobTableConfBean, DatabaseWrapper db) {
        String tableName = processJobTableConfBean.getTarTableName();
        IsFlag isZipperFlag = processJobTableConfBean.getIsZipperFlag();
        String haveTodayDataSql = "";
        try {
            log.info("执行检查表 [ {} ] 当天是否进数!", tableName);
            if (isZipperFlag == IsFlag.Shi) {
                haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1", tableName, _HYREN_S_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                log.info("检查表: [ {} ] 存在当前新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                ResultSet resultSet = db.queryGetResultSet(haveTodayDataSql);
                if (resultSet.next())
                    return true;
                haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1", tableName, _HYREN_E_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                log.info("检查表: [ {} ] 存在当前失效数据SQL: [ {} ]", tableName, haveTodayDataSql);
                resultSet = db.queryGetResultSet(haveTodayDataSql);
                return resultSet.next();
            } else if (isZipperFlag == IsFlag.Fou) {
                StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
                if (storageType == StorageType.TiHuan) {
                    haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' LIMIT 1", tableName, _HYREN_JOB_NAME, jobNameParam);
                    log.info("检查表: [ {} ] 存在当前作业的新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                } else {
                    haveTodayDataSql = String.format("SELECT * FROM %s WHERE %s = '%s' AND %s = '%s' LIMIT 1", tableName, _HYREN_S_DATE, etlDateWith8, _HYREN_JOB_NAME, jobNameParam);
                    log.info("检查表: [ {} ] 存在当前日期的新增数据SQL: [ {} ]", tableName, haveTodayDataSql);
                }
                return db.queryGetResultSet(haveTodayDataSql).next();
            }
        } catch (SQLException throwables) {
            throw new BusinessException("检查是否拥有今日数据的SQL执行失败! sql: " + haveTodayDataSql);
        }
        return false;
    }

    @Override
    public void append() {
        this.createTableIfNotExists(getHiveDB(), tableName);
        hiveHandleArgs.setStorageType(StorageType.ZhuiJia);
        hiveHandleArgs.setTableName(tableName);
        jobRunStatus = SparkLauncherRunner.runJob(hiveHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            String analyze_table_sql = "";
            try {
                analyze_table_sql = getAnalyzeTableSQL(tableName);
                getHiveDB().execute(analyze_table_sql);
            } catch (Exception e) {
                throw new AppSystemException("执行表分析 (" + tableName + ") 的SQL执行失败! " + " ,表分析sql: " + analyze_table_sql + " ,异常: " + e.getMessage());
            }
        }
    }

    @Override
    public void replace() {
        this.createTableIfNotExists(getHiveDB(), tableName);
        hiveHandleArgs.setStorageType(StorageType.TiHuan);
        hiveHandleArgs.setTableName(tableName);
        jobRunStatus = SparkLauncherRunner.runJob(hiveHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            String analyze_table_sql = "";
            try {
                analyze_table_sql = getAnalyzeTableSQL(tableName);
                getHiveDB().execute(analyze_table_sql);
            } catch (Exception e) {
                throw new AppSystemException("执行表分析 (" + tableName + ") 的SQL执行失败! " + " ,表分析sql: " + analyze_table_sql + " ,异常: " + e.getMessage());
            }
        }
    }

    @Override
    public void upSert() {
        this.createTableIfNotExists(getHiveDB(), tableName);
        this.dropTableIfExists(getHiveDB(), zipperTempTable);
        this.createTableIfNotExists(getHiveDB(), zipperTempTable);
        hiveHandleArgs.setStorageType(StorageType.UpSet);
        jobRunStatus = SparkLauncherRunner.runJob(hiveHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            this.dropTableIfExists(getHiveDB(), tableName);
            mergeIncrement(getHiveDB(), zipperTempTable, tableName);
        }
    }

    @Override
    public void historyZipperFullLoading() {
        this.createTableIfNotExists(getHiveDB(), tableName);
        this.dropTableIfExists(getHiveDB(), zipperTempTable);
        this.createTableIfNotExists(getHiveDB(), zipperTempTable);
        hiveHandleArgs.setStorageType(StorageType.QuanLiang);
        jobRunStatus = SparkLauncherRunner.runJob(hiveHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            this.dropTableIfExists(getHiveDB(), tableName);
            mergeIncrement(getHiveDB(), zipperTempTable, tableName);
        }
    }

    @Override
    public void historyZipperIncrementLoading() {
        this.createTableIfNotExists(getHiveDB(), tableName);
        this.dropTableIfExists(getHiveDB(), zipperTempTable);
        this.createTableIfNotExists(getHiveDB(), zipperTempTable);
        hiveHandleArgs.setStorageType(StorageType.LiShiLaLian);
        jobRunStatus = SparkLauncherRunner.runJob(hiveHandleArgs);
        if (jobRunStatus == ProcessJobRunStatusEnum.FINISHED) {
            this.dropTableIfExists(getHiveDB(), tableName);
            mergeIncrement(getHiveDB(), zipperTempTable, tableName);
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
    }

    @Override
    public void clean() {
        this.dropTableIfExists(getHiveDB(), currentTempTable);
        this.dropTableIfExists(getHiveDB(), restoreTempTable);
        this.dropTableIfExists(getHiveDB(), zipperTempTable);
    }

    @Override
    public void close() {
        try {
            DatabaseWrapper db = getHiveDB();
            db.close();
            _dbBox.remove();
            log.info("HiveLoader 关闭 db : {}", db.getID());
        } catch (Exception e) {
            throw new AppSystemException("HiveLoader 关闭db失败! " + e);
        }
    }

    private String buildCreateTableColumnTypes() {
        final StringBuilder column_type_sql = new StringBuilder(300);
        for (DmJobTableFieldInfo field : processJobTableConfBean.getDmJobTableFieldInfos()) {
            if (this.partitionFields.isEmpty()) {
                column_type_sql.append(field.getJobtab_field_en_name()).append(" ").append(field.getJobtab_field_type());
                String fieldLength = field.getJobtab_field_length();
                if (StringUtil.isNotBlank(fieldLength)) {
                    column_type_sql.append("(").append(fieldLength).append(")");
                }
                column_type_sql.append(",");
            } else {
                for (String partitionField : partitionFields) {
                    if (partitionField.equalsIgnoreCase(field.getJobtab_field_en_name()))
                        continue;
                    column_type_sql.append(field.getJobtab_field_en_name()).append(" ").append(field.getJobtab_field_type());
                    String fieldLength = field.getJobtab_field_length();
                    if (StringUtil.isNotBlank(fieldLength)) {
                        column_type_sql.append("(").append(fieldLength).append(")");
                    }
                    column_type_sql.append(",");
                }
            }
        }
        column_type_sql.deleteCharAt(column_type_sql.length() - 1);
        return column_type_sql.toString();
    }

    private void mergeIncrement(DatabaseWrapper db, String incrementTempTable, String finalTable) {
        String alter_table_sql = "", analyze_table_sql = "";
        try {
            dropTableIfExists(db, finalTable);
            alter_table_sql = "ALTER TABLE " + incrementTempTable + " RENAME TO " + finalTable;
            db.execute(alter_table_sql);
            analyze_table_sql = getAnalyzeTableSQL(finalTable);
            db.execute(analyze_table_sql);
        } catch (Exception e) {
            throw new AppSystemException("执行据临时增量表合并出新的增量表 (" + finalTable + ") 的SQL执行失败! " + " ,重命名增量临时表为增量结果表sql: " + alter_table_sql + " ,表分析sql: " + analyze_table_sql + " ,异常: " + e.getMessage());
        }
    }

    private String getAnalyzeTableSQL(String tableName) {
        StringBuilder analyzeTableSQL = new StringBuilder();
        analyzeTableSQL.append("ANALYZE TABLE ").append(tableName);
        if (!partitionFields.isEmpty()) {
            analyzeTableSQL.append(getParttitionSplicingSQL());
        }
        analyzeTableSQL.append(" COMPUTE STATISTICS NOSCAN");
        return analyzeTableSQL.toString();
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

    private String getParttitionSplicingSQL() {
        StringBuilder parttitionSplicingSQL = new StringBuilder();
        if (!partitionFields.isEmpty()) {
            parttitionSplicingSQL.append(" PARTITION (");
            parttitionSplicingSQL.append(StringUtil.join(partitionFields, ","));
            parttitionSplicingSQL.append(")");
        }
        return parttitionSplicingSQL.toString();
    }

    public void createTableIfNotExists(DatabaseWrapper db, String tableName) {
        log.debug("创建表 [ " + tableName + " ] 时的字段片段信息 [ " + createTableColumnTypes + " ] ");
        StringBuilder create_table_sql = new StringBuilder();
        try {
            create_table_sql.append("CREATE TABLE IF NOT EXISTS ");
            create_table_sql.append(tableName);
            create_table_sql.append("(");
            create_table_sql.append(createTableColumnTypes);
            create_table_sql.append(")");
            create_table_sql.append(buildPartitionSqlFragment());
            create_table_sql.append(" STORED AS PARQUET ");
            log.debug("创建表 [ " + tableName + " ] 时生成的建表语句 [ " + create_table_sql + " ] ");
            db.execute(create_table_sql.toString());
        } catch (Exception e) {
            throw new AppSystemException("执行创建表 (" + tableName + ") 的SQL执行失败! " + " 执行sql: " + create_table_sql + " 异常: " + e.getMessage());
        }
    }

    private StringBuilder buildPartitionSqlFragment() {
        StringBuilder partitionSqlFragment = new StringBuilder();
        if (partitionFields.isEmpty()) {
            return partitionSqlFragment;
        } else {
            partitionSqlFragment.append(" PARTITIONED BY ( ");
            for (String partitionField : partitionFields) {
                for (DmJobTableFieldInfo field : processJobTableConfBean.getDmJobTableFieldInfos()) {
                    if (partitionField.equalsIgnoreCase(field.getJobtab_field_en_name())) {
                        partitionSqlFragment.append(partitionField).append(" ").append(field.getJobtab_field_type());
                        String fieldLength = field.getJobtab_field_length();
                        if (StringUtil.isNotBlank(fieldLength)) {
                            partitionSqlFragment.append("(").append(fieldLength).append(")");
                        }
                        partitionSqlFragment.append(",");
                    }
                }
            }
            partitionSqlFragment.deleteCharAt(partitionSqlFragment.length() - 1);
            partitionSqlFragment.append(" ) ");
        }
        return partitionSqlFragment;
    }

    private DatabaseWrapper getHiveDB() {
        DatabaseWrapper db = _dbBox.get();
        if (db == null || !db.isConnected()) {
            try {
                log.info("HiveLoader 的 db 为 null 初始化创建 HiveDB");
                db = super.getDB();
                _dbBox.set(db);
            } catch (Exception e) {
                throw new AppSystemException("HiveLoader 初始化创建 db 失败!" + e);
            }
        } else {
            try {
                db.execute("show databases");
            } catch (Exception e) {
                try {
                    log.info("HiveLoader 的 db 可能失效! 重新创建 db !");
                    db = super.getDB();
                    log.info("HiveLoader 重新创建 db 成功 !");
                } catch (Exception ex) {
                    throw new AppSystemException("HiveLoader 重新创建 db 失败!" + ex);
                }
                _dbBox.set(db);
            }
        }
        return db;
    }
}
