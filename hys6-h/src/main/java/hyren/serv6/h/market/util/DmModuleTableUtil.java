package hyren.serv6.h.market.util;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.entity.DmModuleTable;
import hyren.serv6.base.entity.DmModuleTableFieldInfo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public class DmModuleTableUtil {

    public static void createModuleTableIfNotExist(long dslId, List<DmJobTableFieldInfo> dmJobTableFieldInfos, List<String> partitionFields, String module_table_en_name) {
        DataStoreLayer storeLayer = Dbo.queryOneObject(DataStoreLayer.class, "select * from " + DataStoreLayer.TableName + " where dsl_id = ?", dslId).orElseThrow(() -> new BusinessException("查询存储层信息失败"));
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), dslId)) {
            Store_type storeType = Store_type.ofEnumByCode(storeLayer.getStore_type());
            if (Store_type.HIVE == storeType) {
                String hiveColumnTypes = DmModuleTableUtil.getHiveColumnTypes(dmJobTableFieldInfos, partitionFields);
                DmModuleTableUtil.createHiveTable(module_table_en_name, hiveColumnTypes, partitionFields, dmJobTableFieldInfos, db);
            } else if (Store_type.DATABASE == storeType) {
                String column_type_sql = DmModuleTableUtil.getColumnSqlPart(dmJobTableFieldInfos);
                DmModuleTableUtil.createDataBaseTable(db, module_table_en_name, column_type_sql);
                db.commit();
            } else {
                log.warn(String.format("暂不支持此存储层 【%s】 创建模型表", storeType.name()));
            }
        }
    }

    public static void createModuleTableIfNotExistByModuleFields(long dslId, List<DmModuleTableFieldInfo> dmModuleTableFieldInfos, List<String> partitionFields, String module_table_en_name) {
        DataStoreLayer storeLayer = Dbo.queryOneObject(DataStoreLayer.class, "select * from " + DataStoreLayer.TableName + " where dsl_id = ?", dslId).orElseThrow(() -> new BusinessException("查询存储层信息失败"));
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), dslId)) {
            Store_type storeType = Store_type.ofEnumByCode(storeLayer.getStore_type());
            if (Store_type.HIVE == storeType) {
                String hiveColumnTypes = DmModuleTableUtil.getHiveColumnTypesByModuleFields(dmModuleTableFieldInfos, partitionFields);
                DmModuleTableUtil.createHiveTableByModuleFields(module_table_en_name, hiveColumnTypes, partitionFields, dmModuleTableFieldInfos, db);
            } else if (Store_type.DATABASE == storeType) {
                String column_type_sql = DmModuleTableUtil.getColumnSqlPartByModuleField(dmModuleTableFieldInfos);
                DmModuleTableUtil.createDataBaseTable(db, module_table_en_name, column_type_sql);
                db.commit();
            } else {
                log.warn(String.format("暂不支持此存储层 【%s】 创建模型表", storeType.name()));
            }
        }
    }

    public static void createHiveTable(String table_name, String hiveColumnTypes, List<String> partitionFields, List<DmJobTableFieldInfo> dmJobTableFieldInfos, DatabaseWrapper db) {
        log.debug("创建表 [ " + table_name + " ] 时的字段片段信息 [ " + hiveColumnTypes + " ] ");
        StringBuilder create_table_sql = new StringBuilder();
        try {
            create_table_sql.append("CREATE TABLE IF NOT EXISTS ");
            create_table_sql.append(table_name);
            create_table_sql.append("(");
            create_table_sql.append(hiveColumnTypes);
            create_table_sql.append(")");
            create_table_sql.append(buildPartitionSqlFragment(partitionFields, dmJobTableFieldInfos));
            create_table_sql.append(" STORED AS PARQUET ");
            log.debug("创建表 [ " + table_name + " ] 时生成的建表语句 [ " + create_table_sql + " ] ");
            if (!db.isExistTable(table_name)) {
                db.execute(create_table_sql.toString());
            }
        } catch (Exception e) {
            throw new AppSystemException("执行创建表 (" + table_name + ") 的SQL执行失败! " + " 执行sql: " + create_table_sql + " 异常: " + e.getMessage());
        }
    }

    public static void createHiveTableByModuleFields(String table_name, String hiveColumnTypes, List<String> partitionFields, List<DmModuleTableFieldInfo> dmModuleTableFieldInfos, DatabaseWrapper db) {
        log.debug("创建表 [ " + table_name + " ] 时的字段片段信息 [ " + hiveColumnTypes + " ] ");
        StringBuilder create_table_sql = new StringBuilder();
        try {
            create_table_sql.append("CREATE TABLE IF NOT EXISTS ");
            create_table_sql.append(table_name);
            create_table_sql.append("(");
            create_table_sql.append(hiveColumnTypes);
            create_table_sql.append(")");
            create_table_sql.append(buildPartitionSqlFragmentByModuleFields(partitionFields, dmModuleTableFieldInfos));
            create_table_sql.append(" STORED AS PARQUET ");
            log.debug("创建表 [ " + table_name + " ] 时生成的建表语句 [ " + create_table_sql + " ] ");
            if (!db.isExistTable(table_name)) {
                db.execute(create_table_sql.toString());
            }
        } catch (Exception e) {
            throw new AppSystemException("执行创建表 (" + table_name + ") 的SQL执行失败! " + " 执行sql: " + create_table_sql + " 异常: " + e.getMessage());
        }
    }

    public static StringBuilder buildPartitionSqlFragment(List<String> partitionFields, List<DmJobTableFieldInfo> dmJobTableFieldInfos) {
        StringBuilder partitionSqlFragment = new StringBuilder();
        if (partitionFields.isEmpty()) {
            return partitionSqlFragment;
        } else {
            partitionSqlFragment.append(" PARTITIONED BY ( ");
            for (String partitionField : partitionFields) {
                for (DmJobTableFieldInfo field : dmJobTableFieldInfos) {
                    String field_en_name = field.getJobtab_field_en_name();
                    if (partitionField.equalsIgnoreCase(field_en_name)) {
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

    public static StringBuilder buildPartitionSqlFragmentByModuleFields(List<String> partitionFields, List<DmModuleTableFieldInfo> dmModuleTableFieldInfos) {
        StringBuilder partitionSqlFragment = new StringBuilder();
        if (partitionFields.isEmpty()) {
            return partitionSqlFragment;
        } else {
            partitionSqlFragment.append(" PARTITIONED BY ( ");
            for (String partitionField : partitionFields) {
                for (DmModuleTableFieldInfo field : dmModuleTableFieldInfos) {
                    String field_en_name = field.getField_en_name();
                    if (partitionField.equalsIgnoreCase(field_en_name)) {
                        partitionSqlFragment.append(partitionField).append(" ").append(field.getField_type());
                        String fieldLength = field.getField_length();
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

    public static String getHiveColumnTypes(List<DmJobTableFieldInfo> dmJobTableFieldInfos, List<String> partitionFields) {
        final StringBuilder column_type_sql = new StringBuilder(300);
        for (DmJobTableFieldInfo field : dmJobTableFieldInfos) {
            String field_en_name = field.getJobtab_field_en_name();
            if (partitionFields.isEmpty()) {
                column_type_sql.append(field_en_name).append(" ").append(field.getJobtab_field_type());
                String fieldLength = field.getJobtab_field_length();
                if (StringUtil.isNotBlank(fieldLength)) {
                    column_type_sql.append("(").append(fieldLength).append(")");
                }
                column_type_sql.append(",");
            } else {
                for (String partitionField : partitionFields) {
                    if (partitionField.equalsIgnoreCase(field_en_name))
                        continue;
                    column_type_sql.append(field_en_name).append(" ").append(field.getJobtab_field_type());
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

    public static String getHiveColumnTypesByModuleFields(List<DmModuleTableFieldInfo> dmModuleTableFieldInfos, List<String> partitionFields) {
        final StringBuilder column_type_sql = new StringBuilder(300);
        for (DmModuleTableFieldInfo field : dmModuleTableFieldInfos) {
            String field_en_name = field.getField_en_name();
            if (partitionFields.isEmpty()) {
                column_type_sql.append(field_en_name).append(" ").append(field.getField_type());
                String fieldLength = field.getField_length();
                if (StringUtil.isNotBlank(fieldLength)) {
                    column_type_sql.append("(").append(fieldLength).append(")");
                }
                column_type_sql.append(",");
            } else {
                for (String partitionField : partitionFields) {
                    if (partitionField.equalsIgnoreCase(field_en_name))
                        continue;
                    column_type_sql.append(field_en_name).append(" ").append(field.getField_type());
                    String fieldLength = field.getField_length();
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

    public static void createDataBaseTable(DatabaseWrapper db, String table_name, String column_type_sql) {
        StringBuilder create_table_sql = new StringBuilder();
        try {
            if (!db.isExistTable(table_name)) {
                create_table_sql.append("CREATE TABLE ");
                create_table_sql.append(table_name);
                create_table_sql.append("(");
                create_table_sql.append(column_type_sql);
                create_table_sql.append(")");
                log.debug("创建表 [ " + table_name + " ] 时生成的建表语句 [ " + create_table_sql + " ] ");
                db.execute(create_table_sql.toString());
            }
        } catch (Exception e) {
            throw new AppSystemException("执行创建表 (" + table_name + ") 的SQL执行失败! " + " 执行sql: " + create_table_sql + " 异常: " + e.getMessage());
        }
    }

    public static String getColumnSqlPart(List<DmJobTableFieldInfo> dmJobTableFieldInfos) {
        final StringBuilder column_type_sql = new StringBuilder();
        for (DmJobTableFieldInfo field : dmJobTableFieldInfos) {
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

    public static String getColumnSqlPartByModuleField(List<DmModuleTableFieldInfo> dmModuleTableFieldInfos) {
        final StringBuilder column_type_sql = new StringBuilder();
        for (DmModuleTableFieldInfo field : dmModuleTableFieldInfos) {
            column_type_sql.append(field.getField_en_name()).append(" ").append(field.getField_type());
            String fieldLength = field.getField_length();
            if (StringUtil.isNotBlank(fieldLength)) {
                column_type_sql.append("(").append(fieldLength).append(")");
            }
            column_type_sql.append(",");
        }
        column_type_sql.deleteCharAt(column_type_sql.length() - 1);
        return column_type_sql.toString();
    }

    public static void addHyrenFieldsByModuleFields(List<DmModuleTableFieldInfo> dmModuleTableFieldInfos, DmModuleTable moduleTable) {
        DmModuleTableFieldInfo dmModuleTableFieldInfo = new DmModuleTableFieldInfo();
        dmModuleTableFieldInfo.setField_en_name(Constant._HYREN_JOB_NAME);
        dmModuleTableFieldInfo.setField_type(Constant._VARCAHR_300);
        dmModuleTableFieldInfos.add(dmModuleTableFieldInfo);
        StorageType storageType = StorageType.ofEnumByCode(moduleTable.getStorage_type());
        if (storageType == StorageType.QuanLiang || storageType == StorageType.LiShiLaLian || storageType == StorageType.ZengLiang) {
            DmModuleTableFieldInfo hyren_s_date_tfi = new DmModuleTableFieldInfo();
            hyren_s_date_tfi.setField_en_name(Constant._HYREN_S_DATE);
            hyren_s_date_tfi.setField_type(Constant._VARCAHR_8);
            dmModuleTableFieldInfos.add(hyren_s_date_tfi);
            DmModuleTableFieldInfo hyren_e_date_tfi = new DmModuleTableFieldInfo();
            hyren_e_date_tfi.setField_en_name(Constant._HYREN_E_DATE);
            hyren_e_date_tfi.setField_type(Constant._VARCAHR_8);
            dmModuleTableFieldInfos.add(hyren_e_date_tfi);
            DmModuleTableFieldInfo hyren_md5_val_tfi = new DmModuleTableFieldInfo();
            hyren_md5_val_tfi.setField_en_name(Constant._HYREN_MD5_VAL);
            hyren_md5_val_tfi.setField_type(Constant._VARCAHR_32);
            dmModuleTableFieldInfos.add(hyren_md5_val_tfi);
        } else if (storageType == StorageType.ZhuiJia || storageType == StorageType.TiHuan || storageType == StorageType.UpSet) {
            DmModuleTableFieldInfo data_insr_dt_tfi = new DmModuleTableFieldInfo();
            data_insr_dt_tfi.setField_en_name(Constant._HYREN_S_DATE);
            data_insr_dt_tfi.setField_type(Constant._VARCAHR_8);
            dmModuleTableFieldInfos.add(data_insr_dt_tfi);
            if (storageType == StorageType.UpSet) {
                DmModuleTableFieldInfo hyren_md5_val_tfi = new DmModuleTableFieldInfo();
                hyren_md5_val_tfi.setField_en_name(Constant._HYREN_MD5_VAL);
                hyren_md5_val_tfi.setField_type(Constant._VARCAHR_32);
                dmModuleTableFieldInfos.add(hyren_md5_val_tfi);
            }
        } else {
            throw new BusinessException("设置 IsZipperFlag 未知的进数方式! StorageType: " + storageType.getValue());
        }
        dmModuleTableFieldInfos.forEach(moduleTableFieldInfo -> moduleTableFieldInfo.setField_en_name(moduleTableFieldInfo.getField_en_name().toLowerCase()));
    }

    public static void addHyrenFields(List<DmJobTableFieldInfo> dmJobTableFieldInfos, DmModuleTable moduleTable) {
        DmJobTableFieldInfo dmJobTableFieldInfo = new DmJobTableFieldInfo();
        dmJobTableFieldInfo.setJobtab_field_en_name(Constant._HYREN_JOB_NAME);
        dmJobTableFieldInfo.setJobtab_field_type(Constant._VARCAHR_300);
        dmJobTableFieldInfos.add(dmJobTableFieldInfo);
        StorageType storageType = StorageType.ofEnumByCode(moduleTable.getStorage_type());
        if (storageType == StorageType.QuanLiang || storageType == StorageType.LiShiLaLian || storageType == StorageType.ZengLiang) {
            DmJobTableFieldInfo hyren_s_date_tfi = new DmJobTableFieldInfo();
            hyren_s_date_tfi.setJobtab_field_en_name(Constant._HYREN_S_DATE);
            hyren_s_date_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(hyren_s_date_tfi);
            DmJobTableFieldInfo hyren_e_date_tfi = new DmJobTableFieldInfo();
            hyren_e_date_tfi.setJobtab_field_en_name(Constant._HYREN_E_DATE);
            hyren_e_date_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(hyren_e_date_tfi);
            DmJobTableFieldInfo hyren_md5_val_tfi = new DmJobTableFieldInfo();
            hyren_md5_val_tfi.setJobtab_field_en_name(Constant._HYREN_MD5_VAL);
            hyren_md5_val_tfi.setJobtab_field_type(Constant._VARCAHR_32);
            dmJobTableFieldInfos.add(hyren_md5_val_tfi);
        } else if (storageType == StorageType.ZhuiJia || storageType == StorageType.TiHuan || storageType == StorageType.UpSet) {
            DmJobTableFieldInfo data_insr_dt_tfi = new DmJobTableFieldInfo();
            data_insr_dt_tfi.setJobtab_field_en_name(Constant._HYREN_S_DATE);
            data_insr_dt_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(data_insr_dt_tfi);
            if (storageType == StorageType.UpSet) {
                DmJobTableFieldInfo hyren_md5_val_tfi = new DmJobTableFieldInfo();
                hyren_md5_val_tfi.setJobtab_field_en_name(Constant._HYREN_MD5_VAL);
                hyren_md5_val_tfi.setJobtab_field_type(Constant._VARCAHR_32);
                dmJobTableFieldInfos.add(hyren_md5_val_tfi);
            }
        } else {
            throw new BusinessException("设置 IsZipperFlag 未知的进数方式! StorageType: " + storageType.getValue());
        }
        dmJobTableFieldInfos.forEach(jobTableFieldInfo -> jobTableFieldInfo.setJobtab_field_en_name(jobTableFieldInfo.getJobtab_field_en_name().toLowerCase()));
    }
}
