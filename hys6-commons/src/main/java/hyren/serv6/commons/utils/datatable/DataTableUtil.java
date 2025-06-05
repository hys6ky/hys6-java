package hyren.serv6.commons.utils.datatable;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.datatree.background.query.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.constant.Constant;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "BY-HLL", createdate = "2019/11/4 0004 下午 02:35")
public class DataTableUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    public static List<String> getAllTableNameByPlatform(DatabaseWrapper db) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT * FROM (");
        asmSql.addSql("SELECT hyren_name AS table_name FROM " + DataStoreReg.TableName);
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT en_name AS table_name FROM " + ObjectCollectTask.TableName);
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT module_table_en_name AS table_name FROM" + " " + DmModuleTable.TableName + " dmd" + " JOIN " + DtabRelationStore.TableName + " dtab_rs ON dmd.module_table_id=dtab_rs.tab_id" + " WHERE dtab_rs.is_successful=?").addParam(JobExecuteState.WanCheng.getCode());
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT table_name AS table_name FROM " + DqIndex3record.TableName);
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT table_name AS table_name FROM " + DqTableInfo.TableName);
        asmSql.addSql(") tmp ");
        return SqlOperator.queryOneColumnList(db, asmSql.sql(), asmSql.params());
    }

    public static List<String> getAllTableNameByPlatform() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getAllTableNameByPlatform(db);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_layer", desc = "", range = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getTableInfoByFileId(String data_layer, String file_id) {
        Map<String, Object> tableInfoMap = new HashMap<>();
        String table_id, table_name, table_ch_name, create_date;
        DataSourceType dataSourceType = DataSourceType.ofEnumByCode(data_layer);
        if (dataSourceType == DataSourceType.ISL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DCL) {
            Map<String, Object> table_info = DCLDataQuery.getDCLBatchTableInfo(file_id);
            if (table_info.isEmpty()) {
                throw new BusinessException("表登记信息已经不存在!");
            }
            table_id = table_info.get("table_id").toString();
            table_name = table_info.get("hyren_name").toString();
            table_ch_name = table_info.get("table_ch_name").toString();
            create_date = table_info.get("original_update_date").toString();
        } else if (dataSourceType == DataSourceType.DPL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DML) {
            DmModuleTable dmModuleTable = DMLDataQuery.getDMLTableInfo(file_id);
            if (StringUtil.isBlank(dmModuleTable.getModule_table_id().toString())) {
                throw new BusinessException("表登记信息已经不存在!");
            }
            table_id = dmModuleTable.getModule_table_id().toString();
            table_name = dmModuleTable.getModule_table_en_name();
            table_ch_name = dmModuleTable.getModule_table_cn_name();
            create_date = dmModuleTable.getModule_table_c_date();
        } else if (dataSourceType == DataSourceType.SFL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.AML) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DQC) {
            DqIndex3record dq_index3record = DQCDataQuery.getDQCTableInfo(file_id);
            table_id = dq_index3record.getRecord_id().toString();
            table_name = dq_index3record.getTable_name();
            table_ch_name = dq_index3record.getTable_name();
            create_date = dq_index3record.getRecord_date();
        } else if (dataSourceType == DataSourceType.UDL) {
            DqTableInfo dq_table_info = UDLDataQuery.getUDLTableInfo(file_id);
            table_id = dq_table_info.getTable_id().toString();
            table_name = dq_table_info.getTable_name();
            table_ch_name = dq_table_info.getCh_name();
            create_date = dq_table_info.getCreate_date();
        } else {
            throw new BusinessException("未找到匹配的数据层!" + data_layer);
        }
        tableInfoMap.put("file_id", file_id);
        tableInfoMap.put("table_id", table_id);
        tableInfoMap.put("data_layer", data_layer);
        tableInfoMap.put("table_name", table_name);
        tableInfoMap.put("table_ch_name", table_ch_name);
        tableInfoMap.put("create_date", create_date);
        return tableInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_layer", desc = "", range = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getTableInfoAndColumnInfo(String data_layer, String file_id) {
        Map<String, Object> data_meta_info = new HashMap<>();
        List<Map<String, String>> column_info_list;
        String table_id, table_name, table_ch_name, create_date, hyren_name;
        DataSourceType dataSourceType = DataSourceType.ofEnumByCode(data_layer);
        if (dataSourceType == DataSourceType.ISL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DCL) {
            Map<String, Object> table_info = DCLDataQuery.getDCLBatchTableInfo(file_id);
            if (table_info.isEmpty()) {
                throw new BusinessException("表登记信息已经不存在!");
            }
            table_id = table_info.get("table_id").toString();
            table_name = table_info.get("table_name").toString();
            table_ch_name = table_info.get("table_ch_name").toString();
            hyren_name = table_info.get("hyren_name").toString();
            create_date = table_info.get("original_update_date").toString();
            column_info_list = DataTableFieldUtil.metaInfoToList(DCLDataQuery.getDCLBatchTableColumns(file_id));
        } else if (dataSourceType == DataSourceType.DPL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DML) {
            DmModuleTable dmModuleTable = DMLDataQuery.getDMLTableInfo(file_id);
            Long moduleTableId = dmModuleTable.getModule_table_id();
            String moduleTableEnName = dmModuleTable.getModule_table_en_name();
            if (StringUtil.isBlank(moduleTableId.toString())) {
                throw new BusinessException("表登记信息已经不存在!");
            }
            table_id = moduleTableId.toString();
            table_name = moduleTableEnName;
            table_ch_name = dmModuleTable.getModule_table_cn_name();
            hyren_name = moduleTableEnName;
            create_date = dmModuleTable.getModule_table_c_date();
            List<Map<String, Object>> dmlTableColumns = new ArrayList<>();
            DMLDataQuery.getDMLTableColumns(table_id).forEach(table_column -> {
                List<String> columnNames = dmlTableColumns.stream().map(map -> map.get("column_name").toString()).collect(Collectors.toList());
                Map<String, Object> map = new HashMap<>();
                map.put("column_id", table_column.get("column_id").toString());
                map.put("column_name", table_column.get("column_name").toString());
                map.put("column_ch_name", table_column.get("column_ch_name").toString());
                map.put("column_type", table_column.get("column_type").toString());
                map.put("is_primary_key", table_column.get("is_primary_key").toString());
                if (!columnNames.contains(table_column.get("column_name").toString())) {
                    dmlTableColumns.add(map);
                }
                ProcessType processType = ProcessType.ofEnumByCode(table_column.get("field_process").toString());
                if (processType == ProcessType.FenZhuYingShe) {
                    map = new HashMap<>();
                    map.put("column_id", table_column.get("column_id"));
                    map.put("column_name", table_column.get("group_mapping").toString().split("=")[0]);
                    map.put("column_ch_name", table_column.get("column_ch_name").toString());
                    map.put("column_type", table_column.get("column_type").toString());
                    map.put("is_primary_key", table_column.get("is_primary_key").toString());
                    if (!columnNames.contains(table_column.get("group_mapping").toString().split("=")[0])) {
                        dmlTableColumns.add(map);
                    }
                }
            });
            column_info_list = DataTableFieldUtil.metaInfoToList(dmlTableColumns.stream().collect(Collectors.collectingAndThen(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(o -> o.get("column_name").toString()))), ArrayList::new)));
        } else if (dataSourceType == DataSourceType.SFL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.AML) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DQC) {
            DqIndex3record dq_index3record = DQCDataQuery.getDQCTableInfo(file_id);
            table_id = dq_index3record.getRecord_id().toString();
            table_name = dq_index3record.getTable_name();
            table_ch_name = dq_index3record.getTable_name();
            hyren_name = dq_index3record.getTable_name();
            create_date = dq_index3record.getRecord_date();
            List<Map<String, Object>> table_column_list = new ArrayList<>();
            String[] columns = dq_index3record.getTable_col().split(",");
            for (String column : columns) {
                Map<String, Object> map = new HashMap<>();
                String is_primary_key = IsFlag.Fou.getCode();
                map.put("column_id", table_id);
                map.put("column_name", column);
                map.put("column_ch_name", column);
                map.put("column_type", "varchar(--)");
                map.put("is_primary_key", is_primary_key);
                table_column_list.add(map);
            }
            column_info_list = DataTableFieldUtil.metaInfoToList(table_column_list);
        } else if (dataSourceType == DataSourceType.UDL) {
            DqTableInfo dq_table_info = UDLDataQuery.getUDLTableInfo(file_id);
            table_id = dq_table_info.getTable_id().toString();
            table_name = dq_table_info.getTable_name();
            table_ch_name = dq_table_info.getCh_name();
            hyren_name = dq_table_info.getTable_name();
            create_date = dq_table_info.getCreate_date();
            column_info_list = DataTableFieldUtil.metaInfoToList(UDLDataQuery.getUDLTableColumns(table_id));
        } else if (dataSourceType == DataSourceType.KFK) {
            List<Map<String, Object>> kafka_column_list = new ArrayList<>();
            List<String> table_ids = StringUtil.split(file_id, ",");
            DqTableInfo dq_table_info = KFKDataQuery.getTableMsgInfo(table_ids.get(0));
            table_id = dq_table_info.getTable_id().toString();
            table_name = dq_table_info.getTable_name();
            table_ch_name = dq_table_info.getCh_name();
            hyren_name = dq_table_info.getTable_name();
            create_date = dq_table_info.getCreate_date();
            List<String> kfKcolums = getKFKcolums(table_ids.get(1), table_name.toLowerCase());
            for (String column : kfKcolums) {
                Map<String, Object> map = new HashMap<>();
                map.put("column_id", table_id);
                map.put("column_type", "varchar(--)");
                if (StringUtil.split(column, "`").size() > 1) {
                    map.put("is_primary_key", IsFlag.Shi.getCode());
                    map.put("column_name", StringUtil.split(column, "`").get(0));
                    map.put("column_ch_name", StringUtil.split(column, "`").get(0));
                } else {
                    map.put("is_primary_key", IsFlag.Fou.getCode());
                    map.put("column_name", column);
                    map.put("column_ch_name", column);
                }
                kafka_column_list.add(map);
            }
            column_info_list = DataTableFieldUtil.metaInfoToList(kafka_column_list);
        } else {
            throw new BusinessException("未找到匹配的数据层!" + data_layer);
        }
        data_meta_info.put("file_id", file_id);
        data_meta_info.put("table_id", table_id);
        data_meta_info.put("data_layer", data_layer);
        data_meta_info.put("table_name", table_name);
        data_meta_info.put("table_ch_name", table_ch_name);
        data_meta_info.put("hyren_name", hyren_name);
        data_meta_info.put("create_date", create_date);
        data_meta_info.put("column_info_list", column_info_list);
        return data_meta_info;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_layer", desc = "", range = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getColumnInfoByTableName(DatabaseWrapper db, String table_name) {
        List<LayerBean> layerBeans = ProcessingData.getLayerByTable(table_name, db);
        if (layerBeans.isEmpty()) {
            return null;
        }
        Map<String, Object> tableAndColumnInfo = getTableAndColumnInfoByName(db, table_name);
        List<Map<String, Object>> column_info_s = JsonUtil.toObject(JsonUtil.toJson(tableAndColumnInfo.get("column_info_list")), new TypeReference<List<Map<String, Object>>>() {
        });
        String table_cn_name = "";
        if (null != tableAndColumnInfo.get("table_ch_name")) {
            table_cn_name = tableAndColumnInfo.get("table_ch_name").toString();
        }
        List<Map<String, Object>> tableList = new ArrayList<>();
        for (LayerBean layerBean : layerBeans) {
            Map<String, Object> tableMap = new HashMap<>();
            tableMap.put("table_name", table_name);
            tableMap.put("table_cn_name", table_cn_name);
            tableMap.put("store_type", layerBean.getStore_type());
            tableMap.put("dst", layerBean.getDst());
            tableMap.put("dsl_name", layerBean.getDsl_name());
            tableMap.put("columns", column_info_s);
            tableList.add(tableMap);
        }
        return tableList;
    }

    public static List<Map<String, Object>> getColumnInfoByTableName(String table_name) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getColumnInfoByTableName(db, table_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_layer", desc = "", range = "")
    @Param(name = "data_own_type", desc = "", range = "", nullable = true)
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getColumnByFileId(String data_layer, String data_own_type, String file_id) {
        List<Map<String, Object>> col_info_s;
        DataSourceType dataSourceType = DataSourceType.ofEnumByCode(data_layer);
        if (dataSourceType == DataSourceType.ISL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DCL) {
            if (Constant.DCL_BATCH.equals(data_own_type)) {
                col_info_s = DCLDataQuery.getDCLBatchTableColumns(file_id);
            } else if (Constant.DCL_REALTIME.equals(data_own_type)) {
                throw new BusinessException("获取实时数据表的字段信息暂未实现!");
            } else {
                throw new BusinessException("数据表类型错误! dcl_batch:批量数据,dcl_realtime:实时数据");
            }
        } else if (dataSourceType == DataSourceType.DPL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DML) {
            col_info_s = DMLDataQuery.getDMLTableColumns(file_id);
        } else if (dataSourceType == DataSourceType.SFL) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.AML) {
            throw new BusinessException(data_layer + "层暂未实现!");
        } else if (dataSourceType == DataSourceType.DQC) {
            DqIndex3record dq_index3record = DQCDataQuery.getDQCTableInfo(file_id);
            List<Map<String, Object>> table_column_list = new ArrayList<>();
            String[] columns = dq_index3record.getTable_col().split(",");
            for (String column : columns) {
                Map<String, Object> map = new HashMap<>();
                String is_primary_key = IsFlag.Fou.getCode();
                map.put("column_name", column);
                map.put("column_ch_name", column);
                map.put("column_type", "varchar(--)");
                map.put("is_primary_key", is_primary_key);
                table_column_list.add(map);
            }
            col_info_s = table_column_list;
        } else if (dataSourceType == DataSourceType.UDL) {
            col_info_s = UDLDataQuery.getUDLTableColumns(file_id);
        } else if (dataSourceType == DataSourceType.KFK) {
            List<Map<String, Object>> kafka_column_list = new ArrayList<>();
            List<String> table_ids = StringUtil.split(file_id, ",");
            DqTableInfo dq_table_info = KFKDataQuery.getTableMsgInfo(table_ids.get(0));
            List<String> kfKcolums = getKFKcolums(table_ids.get(1), dq_table_info.getTable_name().toLowerCase());
            for (String column : kfKcolums) {
                Map<String, Object> map = new HashMap<>();
                map.put("column_type", "varchar(--)");
                if (StringUtil.split(column, "`").size() > 1) {
                    map.put("is_primary_key", IsFlag.Shi.getCode());
                    map.put("column_name", StringUtil.split(column, "`").get(0));
                    map.put("column_ch_name", StringUtil.split(column, "`").get(0));
                } else {
                    map.put("is_primary_key", IsFlag.Fou.getCode());
                    map.put("column_name", column);
                    map.put("column_ch_name", column);
                }
                kafka_column_list.add(map);
            }
            col_info_s = kafka_column_list;
        } else {
            throw new BusinessException("未找到匹配的数据层!" + data_layer);
        }
        return col_info_s;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<String> getKFKcolums(String dsl_id, String table_name) {
        try (DatabaseWrapper dbWrapper = new DatabaseWrapper()) {
            try (DatabaseWrapper db = ConnectionTool.getDBWrapper(dbWrapper, Long.parseLong(dsl_id))) {
                List<String> columnEnNameList = new ArrayList<>();
                List<String> primaryKeysList = new ArrayList<>();
                DatabaseMetaData data = db.getConnection().getMetaData();
                ResultSet columnsList = data.getColumns(null, "%", table_name, "%");
                ResultSet primaryKeys = data.getPrimaryKeys("", "", table_name);
                while (columnsList.next()) {
                    columnEnNameList.add(columnsList.getString("COLUMN_NAME"));
                }
                while (primaryKeys.next()) {
                    primaryKeysList.add(primaryKeys.getString("COLUMN_NAME"));
                }
                if (!primaryKeysList.isEmpty() && new HashSet<>(columnEnNameList).containsAll(primaryKeysList)) {
                    columnEnNameList.remove(primaryKeysList.get(0));
                    columnEnNameList.add(primaryKeysList.get(0) + "`1");
                }
                return columnEnNameList;
            } catch (Exception e) {
                throw new BusinessException(e.getMessage());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getColumnByTableName(DatabaseWrapper db, String table_name) {
        Map<String, Object> tableAndColumnInfo = getTableAndColumnInfoByName(db, table_name);
        return JsonUtil.toObject(JsonUtil.toJson(tableAndColumnInfo.get("column_info_list")), new TypeReference<List<Map<String, Object>>>() {
        });
    }

    public static List<Map<String, Object>> getColumnByTableName(String table_name) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getColumnByTableName(db, table_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getTableAndColumnInfoByName(DatabaseWrapper db, String table_name) {
        Map<String, Object> tableMap = getDataLayerByTableName(db, table_name);
        String file_id = tableMap.get("file_id").toString();
        String data_layer = tableMap.get("data_layer").toString();
        return DataTableUtil.getTableInfoAndColumnInfo(data_layer, file_id);
    }

    public static Map<String, Object> getTableAndColumnInfoByName(String table_name) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getTableAndColumnInfoByName(db, table_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getDataLayerByTableName(DatabaseWrapper db, String table_name) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        Map<String, Object> tableMap = SqlOperator.queryOneObject(db, "SELECT file_id,'DCL' AS data_layer FROM " + DataStoreReg.TableName + " WHERE lower(hyren_name) = lower(?)", table_name);
        if (tableMap.isEmpty()) {
            asmSql.clean();
            asmSql.addSql("SELECT module_table_id AS file_id,'DML' AS data_layer " + " FROM " + DmModuleTable.TableName + " WHERE LOWER(module_table_en_name) = LOWER(?)").addParam(table_name);
            asmSql.addSql("UNION");
            asmSql.addSql("SELECT table_id as file_id,'UDL' AS data_layer FROM " + DqTableInfo.TableName + " WHERE LOWER(table_name) = LOWER(?)").addParam(table_name);
            asmSql.addSql("UNION");
            asmSql.addSql("SELECT record_id as file_id,'DQC' as data_layer FROM " + DqIndex3record.TableName + " WHERE table_name = LOWER(?)").addParam(table_name);
            tableMap = SqlOperator.queryOneObject(db, asmSql.sql(), asmSql.params());
        }
        if (tableMap.isEmpty()) {
            throw new BusinessException("根据表名" + table_name + "没有找到对应的表信息");
        }
        return tableMap;
    }

    public static Map<String, Object> getDataLayerByTableName(String table_name) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getDataLayerByTableName(db, table_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean tableIsRepeat(DatabaseWrapper db, String tableName) {
        boolean isRepeat = Boolean.FALSE;
        if (tableIsExistInDCL(db, tableName)) {
            isRepeat = Boolean.TRUE;
        }
        if (tableIsExistInDML(db, tableName)) {
            isRepeat = Boolean.TRUE;
        }
        if (tableIsExistInUDL(db, tableName)) {
            isRepeat = Boolean.TRUE;
        }
        return isRepeat;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean tableExistByDslId(long dslId, String tableName) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(new DatabaseWrapper(), dslId)) {
            return db.isExistTable(tableName);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void operateTableByDslId(long dslId, String sql) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(new DatabaseWrapper(), dslId)) {
            db.execute(sql);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    private static boolean tableIsExistInDCL(DatabaseWrapper db, String tableName) {
        return SqlOperator.queryNumber(db, "SELECT count(1) count FROM " + DataStoreReg.TableName + " WHERE lower(hyren_name) = ? AND collect_type IN (?,?)", tableName.toLowerCase(), AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode()).orElseThrow(() -> new BusinessException("检查表名称否重复在源文件信息表的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    private static boolean tableIsExistInDML(DatabaseWrapper db, String tableName) {
        return SqlOperator.queryNumber(db, "SELECT count(1) count FROM " + DmModuleTable.TableName + " WHERE lower(module_table_en_name) = lower(?)", tableName.toLowerCase()).orElseThrow(() -> new BusinessException("检查表名称否重复在集市数据表的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    private static boolean tableIsExistInUDL(DatabaseWrapper db, String tableName) {
        return SqlOperator.queryNumber(db, "SELECT count(1) count FROM " + DqTableInfo.TableName + " WHERE lower(table_name) = lower(?)", tableName.toLowerCase()).orElseThrow(() -> new BusinessException("检查表名称否重复在系统表创建信息表的SQL编写错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "search_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> bloodlineDateInfo(DatabaseWrapper db, String table_name, String search_type) {
        String sqlStr = "select * from (" + "SELECT dmt.module_table_en_name AS TABLE_NAME,tfi.field_en_name AS target_column_name," + "djti.jobtab_en_name AS source_table_name,djtfi.jobtab_field_en_name AS source_fields_name,'' AS mapping " + "FROM dm_module_table dmt JOIN " + "dm_module_table_field_info tfi ON dmt.module_table_id = tfi.module_table_id JOIN " + "dm_job_table_field_info djtfi ON tfi.module_field_id = djtfi.module_field_id JOIN " + "dm_job_table_info djti ON djtfi.jobtab_id = djti.jobtab_id WHERE LOWER(dmt.module_table_en_name) = LOWER('" + table_name + "')" + " ) aa order by source_table_name,target_column_name";
        List<Map<String, Object>> bloodline_data_s = SqlOperator.queryList(db, sqlStr);
        List<Map<String, Object>> children_s = new ArrayList<>();
        if (!bloodline_data_s.isEmpty()) {
            IsFlag is_st = IsFlag.ofEnumByCode(search_type);
            if (is_st == IsFlag.Fou) {
                Set<String> set = new HashSet<>();
                bloodline_data_s.forEach(bloodline_data -> {
                    String tableName = bloodline_data.get("source_table_name").toString();
                    if (!set.contains(tableName)) {
                        Map<String, Object> map = new HashMap<>();
                        set.add(tableName);
                        map.put("name", tableName);
                        children_s.add(map);
                    }
                });
            } else if (is_st == IsFlag.Shi) {
                Map<String, List<Map<String, Object>>> children_map = new HashMap<>();
                bloodline_data_s.forEach(bloodline_data -> {
                    String source_table_name = bloodline_data.get("source_table_name").toString();
                    String source_fields_name = bloodline_data.get("source_fields_name").toString();
                    String target_column_name = bloodline_data.get("target_column_name").toString();
                    if (!children_map.containsKey(target_column_name)) {
                        List<Map<String, Object>> map_col_list = new ArrayList<>();
                        Map<String, Object> source_table_map = new HashMap<>();
                        source_table_map.put("name", source_table_name);
                        List<Map<String, Object>> source_table_col_list = new ArrayList<>();
                        Map<String, Object> source_table_col_map = new HashMap<>();
                        source_table_col_map.put("name", source_fields_name);
                        source_table_col_list.add(source_table_col_map);
                        source_table_map.put("children", source_table_col_list);
                        map_col_list.add(source_table_map);
                        children_map.put(target_column_name, map_col_list);
                    } else {
                        Map<String, Object> source_table_map = new HashMap<>();
                        source_table_map.put("name", source_table_name);
                        List<Map<String, Object>> source_table_col_list = new ArrayList<>();
                        Map<String, Object> source_table_col_map = new HashMap<>();
                        source_table_col_map.put("name", source_fields_name);
                        source_table_col_list.add(source_table_col_map);
                        source_table_map.put("children", source_table_col_list);
                        children_map.get(target_column_name).add(source_table_map);
                    }
                });
                if (!children_map.isEmpty()) {
                    Set<Map.Entry<String, List<Map<String, Object>>>> entrySet = children_map.entrySet();
                    for (Map.Entry<String, List<Map<String, Object>>> entry : entrySet) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("name", entry.getKey());
                        map.put("children", entry.getValue());
                        children_s.add(map);
                    }
                }
            } else {
                throw new BusinessException("搜索类型不匹配! search_type=" + search_type);
            }
        }
        Map<String, Object> bloodlineDateInfoMap = new HashMap<>();
        bloodlineDateInfoMap.put("name", table_name);
        bloodlineDateInfoMap.put("children", children_s);
        return bloodlineDateInfoMap;
    }
}
