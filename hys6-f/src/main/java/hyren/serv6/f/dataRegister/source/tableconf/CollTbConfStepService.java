package hyren.serv6.f.dataRegister.source.tableconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.meta.ColumnMeta;
import fd.ng.db.meta.MetaOperator;
import fd.ng.db.meta.TableMeta;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.ExecuteState;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.utils.xlstoxml.Platform;
import hyren.serv6.f.source.tools.SendMsgUtil;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@Slf4j
@Api("定义表抽取属性")
@Service
@DocClass(desc = "", author = "WangZhengcheng")
public class CollTbConfStepService {

    private static final long DEFAULT_TABLE_ID = 999999L;

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "inputString", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTableInfo(long colSetId, String inputString) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId);
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        DatabaseSet databaseSet = SendMsgUtil.getLegalParam(databaseInfo);
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), databaseSet.getDsl_id())) {
            List<String> names = StringUtil.split(inputString, "|");
            for (String name : names) {
                List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(db, "%" + name + "%");
                for (TableMeta tableMeta : tableMetas) {
                    TableInfo tableInfo = new TableInfo();
                    tableInfo.setTable_name(tableMeta.getTableName());
                    tableInfo.setTable_ch_name(!StringUtil.isEmpty(tableMeta.getRemarks()) ? tableMeta.getRemarks() : tableMeta.getTableName());
                    tableInfos.add(tableInfo);
                }
            }
            return getTableInfoByTableName(tableInfos, colSetId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAllTableInfo(long colSetId) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId);
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        DatabaseSet databaseSet = SendMsgUtil.getLegalParam(databaseInfo);
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), databaseSet.getDsl_id())) {
            List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(db);
            List<TableInfo> tableNames = new ArrayList<TableInfo>();
            for (TableMeta tableMeta : tableMetas) {
                TableInfo tableInfo = new TableInfo();
                tableInfo.setTable_name(tableMeta.getTableName());
                tableInfo.setTable_ch_name(!StringUtil.isEmpty(tableMeta.getRemarks()) ? tableMeta.getRemarks() : tableMeta.getTableName());
                tableNames.add(tableInfo);
            }
            return getTableInfoByTableName(tableNames, colSetId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableId", desc = "", range = "", nullable = true, valueIfNull = "999999")
    @Return(desc = "", range = "")
    public Map<String, Object> getColumnInfo(String tableName, long colSetId, long tableId) {
        Map<String, Object> returnMap = new HashMap<>();
        returnMap.put("tableName", tableName);
        if (tableId == DEFAULT_TABLE_ID) {
            Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId);
            if (databaseInfo.isEmpty()) {
                throw new BusinessException("未找到数据库采集任务");
            }
            DatabaseSet databaseSet = SendMsgUtil.getLegalParam(databaseInfo);
            try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), databaseSet.getDsl_id())) {
                List<TableMeta> tablesWithColumns = MetaOperator.getTablesWithColumns(db, tableName);
                TableMeta tableMeta = tablesWithColumns.get(0);
                Map<String, ColumnMeta> columnMetas = tableMeta.getColumnMetas();
                Set<String> primaryKeys = tableMeta.getPrimaryKeys();
                List<TableColumn> tableColumns = new ArrayList<TableColumn>();
                for (String key : columnMetas.keySet()) {
                    TableColumn tableColumn = new TableColumn();
                    ColumnMeta columnMeta = columnMetas.get(key);
                    String colName = columnMeta.getName();
                    String colCnName = columnMeta.getRemark();
                    String typeName = columnMeta.getTypeName();
                    int precision = columnMeta.getLength();
                    int dataType = columnMeta.getTypeOfSQL();
                    int scale = columnMeta.getScale();
                    String column_type = new Platform().getColType(dataType, typeName, precision, scale, 0);
                    boolean primaryKey = primaryKeys.contains(colName);
                    tableColumn.setColumn_name(colName);
                    tableColumn.setColumn_ch_name(!StringUtil.isEmpty(colCnName) ? colCnName : colName);
                    tableColumn.setColumn_type(column_type);
                    tableColumn.setIs_primary_key(primaryKey ? IsFlag.Shi.getCode() : IsFlag.Fou.getCode());
                    tableColumn.setIs_get(IsFlag.Shi.getCode());
                    tableColumn.setIs_alive(IsFlag.Shi.getCode());
                    tableColumn.setIs_new(IsFlag.Fou.getCode());
                    tableColumn.setTc_remark("");
                    tableColumns.add(tableColumn);
                }
                tableColumns.forEach(table_column -> table_column.setIs_zipper_field(IsFlag.Fou.getCode()));
                returnMap.put("columnInfo", tableColumns);
            }
        } else {
            List<TableColumn> tableColumns = Dbo.queryList(TableColumn.class, " SELECT * FROM " + TableColumn.TableName + " WHERE table_id = ?", tableId);
            returnMap.put("columnInfo", tableColumns);
        }
        long editCount = Dbo.queryNumber("select count(1) from " + DataStoreReg.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (editCount > 0) {
            returnMap.put("editFlag", IsFlag.Fou.getCode());
        } else {
            returnMap.put("editFlag", IsFlag.Shi.getCode());
        }
        return returnMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getDatabaseSetInfo(long colSetId) {
        long databaseNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (databaseNum == 0) {
            throw new BusinessException("任务(" + colSetId + ")不存在!!!");
        }
        return Dbo.queryOneObject(" select t1.dsl_id, t1.fetch_size," + " t1.agent_id, t1.db_agent, t1.plane_url" + " from " + DatabaseSet.TableName + " t1 where t1.database_id = ? ", colSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableNames", desc = "", range = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> getTableInfoByTableName(List<TableInfo> tableNames, long colSetId) {
        if (tableNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> results = new ArrayList<>();
        tableNames.sort(Comparator.comparing(table_info -> table_info.getTable_name().toLowerCase()));
        List<String> tableNameList = new ArrayList<>();
        for (TableInfo table_info : tableNames) {
            String tableName = table_info.getTable_name();
            if (tableNameList.contains(tableName)) {
                log.info("跳过此次重复的表名: " + tableName);
                continue;
            }
            tableNameList.add(tableName);
            Map<String, Object> tableResult = Dbo.queryOneObject(" select *" + " FROM " + TableInfo.TableName + " ti " + " WHERE ti.database_id = ? AND ti.table_name = ? " + " AND ti.is_user_defined = ? ", colSetId, tableName, IsFlag.Fou.getCode());
            if (tableResult.size() == 0) {
                log.warn("为了检测map没有数据，但是显示size大于0，result=" + tableResult);
            }
            if (tableResult.size() == 0 || tableResult.get("table_id") == null) {
                Map<String, Object> map = new HashMap<>();
                map.put("table_name", tableName);
                map.put("table_ch_name", table_info.getTable_ch_name());
                map.put("is_md5", IsFlag.Fou.getCode());
                map.put("is_parallel", IsFlag.Fou.getCode());
                map.put("collectState", true);
                map.put("interval_time", "");
                map.put("over_date", "");
                map.put("tc_id", "");
                results.add(map);
            } else {
                List<Object> tableStateList = checkTableCollectState(colSetId, tableName);
                if (tableStateList.contains(ExecuteState.YunXingWanCheng.getCode()) || tableStateList.contains(ExecuteState.KaiShiYunXing.getCode())) {
                    tableResult.put("collectState", false);
                } else {
                    tableResult.put("collectState", true);
                }
                Map<String, Object> tableCycle = getTableCycle(tableResult.get("table_id"));
                tableResult.put("interval_time", tableCycle.get("interval_time"));
                tableResult.put("over_date", tableCycle.get("over_date"));
                tableResult.put("tc_id", tableCycle.get("tc_id"));
                results.add(tableResult);
            }
        }
        return results;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    private void deleteDirtyDataOfTb(long tableId) {
        List<Object> columnIds = Dbo.queryOneColumnList("select column_id from " + TableColumn.TableName + " WHERE table_id = ?", tableId);
        if (!columnIds.isEmpty()) {
            for (Object columnId : columnIds) {
                deleteDirtyDataOfCol((long) columnId);
            }
        }
        Dbo.execute(" DELETE FROM " + TableCycle.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + TableColumn.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + TakeRelationEtl.TableName + " WHERE take_id in (" + "SELECT take_id FROM " + DataExtractionDef.TableName + " WHERE table_id =?) ", tableId);
        Dbo.execute(" DELETE FROM " + DataExtractionDef.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + DtabRelationStore.TableName + " WHERE tab_id = " + "(SELECT storage_id FROM " + TableStorageInfo.TableName + " WHERE table_id = ?) AND data_source = ? ", tableId, StoreLayerDataSource.DB.getCode());
        Dbo.execute(" DELETE FROM " + TableStorageInfo.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + ColumnMerge.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + TableClean.TableName + " WHERE table_id = ? ", tableId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    private void deleteDirtyDataOfCol(long columnId) {
        Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ? ", columnId);
        Dbo.execute("delete from " + ColumnClean.TableName + " where column_id = ?", columnId);
        Dbo.execute("delete from " + ColumnSplit.TableName + " where column_id = ?", columnId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Object> checkTableCollectState(long colSetId, Object table_name) {
        return Dbo.queryOneColumnList("SELECT t1.execute_state FROM " + CollectCase.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.collect_set_id = t2.database_id " + "AND t1.task_classify = t2.table_name  WHERE t1.collect_set_id = ? AND t2.table_name = ?", colSetId, table_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    Map<String, Object> getTableCycle(Object table_id) {
        return Dbo.queryOneObject("SELECT * FROM " + TableCycle.TableName + " WHERE table_id = ?", table_id);
    }
}
