package hyren.serv6.b.batchcollection.agent.tableconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.agent.bean.CollTbConfParam;
import hyren.serv6.b.agent.bean.StoreConnectionBean;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.b.batchcollection.agent.dbconf.DBConfStepService;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Slf4j
@Api("定义表抽取属性")
@Service
@DocClass(desc = "", author = "WangZhengcheng")
public class CollTbConfStepService {

    private static final long DEFAULT_TABLE_ID = 999999L;

    @Autowired
    DBConfStepService dbConfStepService;

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getInitInfo(long colSetId) {
        List<Map<String, Object>> tableList = Dbo.queryList(" SELECT *, " + " (CASE WHEN ( SELECT COUNT(1) FROM table_column WHERE " + " table_id = ti.table_id AND is_primary_key = ? ) > 0 " + " THEN 'true' ELSE 'false' END ) is_primary_key FROM " + TableInfo.TableName + " ti " + " WHERE ti.database_id = ? AND ti.is_user_defined = ? ORDER BY table_name", IsFlag.Shi.getCode(), colSetId, IsFlag.Fou.getCode());
        tableList.forEach(itemMap -> {
            List<Object> tableStateList = checkTableCollectState(colSetId, itemMap.get("table_name"));
            if (tableStateList.contains(ExecuteState.YunXingWanCheng.getCode()) || tableStateList.contains(ExecuteState.KaiShiYunXing.getCode())) {
                itemMap.put("collectState", false);
            } else {
                itemMap.put("collectState", true);
            }
            Map<String, Object> tableCycle = getTableCycle(itemMap.get("table_id"));
            itemMap.put("interval_time", tableCycle.get("interval_time"));
            itemMap.put("over_date", tableCycle.get("over_date"));
            itemMap.put("tc_id", tableCycle.get("tc_id"));
        });
        return tableList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "inputString", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTableInfo(long colSetId, String inputString) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId, getUserId());
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        String methodName = AgentActionUtil.GETDATABASETABLE;
        long agentId = (long) databaseInfo.get("agent_id");
        String respMsg = SendMsgUtil.searchTableName(agentId, getUserId(), databaseInfo, inputString, methodName);
        List<TableInfo> rightTables = JsonUtil.toObject(respMsg, new TypeReference<List<TableInfo>>() {
        });
        return getTableInfoByTableName(rightTables, colSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAllTableInfo(long colSetId) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId, getUserId());
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        String methodName = AgentActionUtil.GETDATABASETABLE;
        long agentId = (long) databaseInfo.get("agent_id");
        String respMsg = SendMsgUtil.getAllTableName(agentId, getUserId(), databaseInfo, methodName);
        List<TableInfo> tableNames = JsonUtil.toObject(respMsg, new TypeReference<List<TableInfo>>() {
        });
        return getTableInfoByTableName(tableNames, colSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getPageSQL(long tableId) {
        return AesUtil.encrypt(Dbo.queryResult("select ti.table_id, ti.page_sql,ti.is_customize_sql, ti.table_count, ti.pageparallels, ti.dataincrement " + " from " + TableInfo.TableName + " ti where ti.table_id = ?", tableId).toJSON());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfoArray", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "tableColumn", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableCycles", desc = "", range = "", nullable = true)
    public long saveAllSQL(String tableInfoArray, long colSetId, String tableColumn, String tableCycles) {
        long dbSetCount = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (dbSetCount != 1) {
            throw new BusinessException("数据库采集任务未找到");
        }
        Map<String, Object> map = Dbo.queryOneObject("select collect_type from " + DatabaseSet.TableName + " where database_id = ?", colSetId);
        List<Object> tableIds = Dbo.queryOneColumnList("select table_id from " + TableInfo.TableName + " where database_id = ? and is_user_defined = ?", colSetId, IsFlag.Shi.getCode());
        Dbo.execute("delete from " + TableInfo.TableName + " where database_id = ? AND is_user_defined = ? ", colSetId, IsFlag.Shi.getCode());
        List<TableInfo> tableInfos = new ArrayList<>();
        if (tableInfoArray != null && !tableInfoArray.equals("")) {
            tableInfos = JsonUtil.toObject(AesUtil.desEncrypt(tableInfoArray), new TypeReference<List<TableInfo>>() {
            });
        }
        if (tableInfos != null && tableInfos.size() != 0) {
            for (int i = 0; i < tableInfos.size(); i++) {
                TableInfo tableInfo = tableInfos.get(i);
                tableInfo.setIs_customize_sql(IsFlag.Fou.getCode());
                tableInfo.setPage_sql("");
                if (StringUtil.isBlank(tableInfo.getTable_name())) {
                    throw new BusinessException("保存SQL抽取数据配置，第" + (i + 1) + "条数据表名不能为空!");
                }
                if (StringUtil.isBlank(tableInfo.getTable_ch_name())) {
                    throw new BusinessException("保存SQL抽取数据配置，第" + (i + 1) + "条数据表中文名不能为空!");
                }
                if (StringUtil.isBlank(tableInfo.getUnload_type())) {
                    throw new BusinessException("保存SQL抽取数据配置，第" + (i + 1) + "条数据卸数方式不能为空!");
                }
                if (StringUtil.isBlank(tableInfo.getIs_md5())) {
                    throw new BusinessException("保存SQL抽取数据配置，第" + (i + 1) + "条数据 MD5 不能为空!");
                }
                if (UnloadType.ofEnumByCode(tableInfo.getUnload_type()) == UnloadType.ZengLiangXieShu) {
                    if (StringUtil.isBlank(tableInfo.getSql())) {
                        throw new BusinessException("保存采集表 " + tableInfo.getTable_name() + " 配置,第 " + (i + 1) + " 条,增量SQL不能为空!");
                    }
                    tableInfo.setIs_parallel(IsFlag.Fou.getCode());
                    tableInfo.setTable_count("");
                    tableInfo.setDataincrement(0);
                    tableInfo.setPageparallels(0);
                } else if (UnloadType.ofEnumByCode(tableInfo.getUnload_type()) == UnloadType.QuanLiangXieShu) {
                    if (StringUtil.isBlank(tableInfo.getIs_parallel())) {
                        throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据并行方式不能为空!");
                    }
                    if (StringUtil.isBlank(tableInfo.getSql())) {
                        throw new BusinessException("保存采集表 " + tableInfo.getTable_name() + " 配置,第 " + (i + 1) + " 条,全量SQL不能为空!");
                    }
                    if (IsFlag.ofEnumByCode(tableInfo.getIs_parallel()) == IsFlag.Shi) {
                        if (tableInfo.getPageparallels() == null) {
                            throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据分页并行数不能为空!");
                        }
                        if (tableInfo.getDataincrement() == null) {
                            throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条每日数据增量不能为空!");
                        }
                        if (StringUtil.isBlank(tableInfo.getTable_count())) {
                            throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据总量不能为空!");
                        }
                    }
                } else {
                    throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据的卸数方式不存在, 得到的卸数方式代码项值是 :" + tableInfo.getUnload_type());
                }
                if (StringUtil.isBlank(tableInfo.getRec_num_date())) {
                    tableInfo.setRec_num_date(DateUtil.getSysDate());
                }
                tableInfo.setDatabase_id(colSetId);
                tableInfo.setValid_s_date(DateUtil.getSysDate());
                tableInfo.setValid_e_date(Constant._MAX_DATE_8);
                tableInfo.setIs_user_defined(IsFlag.Shi.getCode());
                tableInfo.setIs_register(IsFlag.Fou.getCode());
                if (tableInfo.getTable_id() == null) {
                    tableInfo.setTable_id(PrimayKeyGener.getNextId());
                    tableInfo.setTi_or(JsonUtil.toJson(Constant.DEFAULT_TABLE_CLEAN_ORDER));
                    tableInfo.add(Dbo.db());
                    if (StringUtil.isNotBlank(tableColumn)) {
                        Map tableColumnMap = JsonUtil.toObjectSafety(tableColumn, Map.class).orElseThrow(() -> new BusinessException("解析自定义列信息失败"));
                        if (!tableColumnMap.containsKey(tableInfo.getTable_name())) {
                            saveCustomSQLColumnInfoForAdd(tableInfo, colSetId);
                        } else {
                            saveCustomizeColumn(JsonUtil.toJson(tableColumnMap.get(tableInfo.getTable_name())), tableInfo);
                        }
                    } else {
                        saveCustomSQLColumnInfoForAdd(tableInfo, colSetId);
                    }
                    if (map.get("collect_type").equals(CollectType.ShuJuKuCaiJi.getCode())) {
                        DataExtractionDef extraction_def = new DataExtractionDef();
                        extraction_def.setDed_id(PrimayKeyGener.getNextId());
                        extraction_def.setTable_id(tableInfo.getTable_id());
                        extraction_def.setData_extract_type(DataExtractType.YuanShuJuGeShi.getCode());
                        extraction_def.setIs_header(IsFlag.Fou.getCode());
                        extraction_def.setDatabase_code(DataBaseCode.UTF_8.getCode());
                        extraction_def.setDbfile_format(FileFormat.PARQUET.getCode());
                        extraction_def.setIs_archived(IsFlag.Fou.getCode());
                        extraction_def.add(Dbo.db());
                    }
                } else {
                    long oldID = tableInfo.getTable_id();
                    long newID = PrimayKeyGener.getNextId();
                    tableInfo.setTable_id(newID);
                    tableInfo.setTi_or(JsonUtil.toJson(Constant.DEFAULT_TABLE_CLEAN_ORDER));
                    tableInfo.add(Dbo.db());
                    updateTableId(newID, oldID);
                    if (StringUtil.isNotBlank(tableColumn)) {
                        Map tableColumnMap = JsonUtil.toObjectSafety(tableColumn, Map.class).orElseThrow(() -> new BusinessException("解析自定义列信息失败"));
                        if (tableColumnMap.containsKey(tableInfo.getTable_name())) {
                            saveCustomizeColumn(JsonUtil.toJson(tableColumnMap.get(tableInfo.getTable_name())), tableInfo);
                        }
                    }
                }
                if (map.get("collect_type").equals(CollectType.ShuJuKuCaiJi.getCode())) {
                    saveTableCycle(tableCycles, tableInfo);
                }
            }
        } else {
            if (!tableIds.isEmpty()) {
                for (Object tableId : tableIds) {
                    deleteDirtyDataOfTb((long) tableId);
                }
            }
        }
        return colSetId;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableColumn", desc = "", range = "")
    @Param(name = "tableInfo", desc = "", range = "", isBean = true)
    private void saveCustomizeColumn(String tableColumn, TableInfo tableInfo) {
        List<TableColumn> tableColumnList = JsonUtil.toObject(tableColumn, new TypeReference<List<TableColumn>>() {
        });
        if (UnloadType.ofEnumByCode(tableInfo.getUnload_type()) == UnloadType.ZengLiangXieShu) {
            List<Boolean> primary = new ArrayList<>();
            tableColumnList.forEach(table_column -> {
                if (IsFlag.ofEnumByCode(table_column.getIs_primary_key()) == IsFlag.Shi) {
                    primary.add(true);
                }
            });
            if (!primary.contains(true)) {
                throw new BusinessException("当前表(" + tableInfo.getTable_name() + ")的卸数方式为增量, 未设置主键,请检查");
            }
        }
        tableColumnList.forEach(table_column -> {
            if (table_column.getColumn_id() != null) {
                Dbo.execute("UPDATE " + TableColumn.TableName + " SET is_get = ?, is_primary_key = ?, column_name = ?, column_type = ?, column_ch_name = ?," + "	table_id = ?, valid_s_date = ?, valid_e_date = ?, is_alive = ?, is_new = ?, tc_or = ?," + " tc_remark = ?,is_zipper_field = ?  WHERE column_id = ?", table_column.getIs_get(), table_column.getIs_primary_key(), table_column.getColumn_name(), table_column.getColumn_type(), table_column.getColumn_ch_name(), tableInfo.getTable_id(), table_column.getValid_s_date(), table_column.getValid_e_date(), table_column.getIs_alive(), table_column.getIs_new(), table_column.getTc_or(), table_column.getTc_remark(), table_column.getIs_zipper_field(), table_column.getColumn_id());
            } else {
                table_column.setColumn_id(PrimayKeyGener.getNextId());
                table_column.setTable_id(tableInfo.getTable_id());
                table_column.add(Dbo.db());
            }
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "pageSql", desc = "", range = "")
    public void testParallelExtraction(long colSetId, String pageSql) {
        Map<String, Object> resultMap = Dbo.queryOneObject("select dsl_id, agent_id," + " database_type, fetch_size from " + DatabaseSet.TableName + " where database_id = ?", colSetId);
        if (resultMap.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        String url = AgentActionUtil.getUrl((Long) resultMap.get("agent_id"), getUserId(), AgentActionUtil.TESTPARALLELSQL);
        StoreConnectionBean storeConnectionBean = dbConfStepService.setStoreConnectionBean(Long.valueOf(resultMap.get("dsl_id").toString()));
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("database_drive", storeConnectionBean.getDatabase_driver()).addData("jdbc_url", storeConnectionBean.getJdbc_url()).addData("user_name", storeConnectionBean.getUser_name()).addData("database_pad", storeConnectionBean.getDatabase_pwd()).addData("database_type", storeConnectionBean.getDatabase_type()).addData("fetch_size", resultMap.get("fetch_size").toString()).addData("pageSql", AesUtil.desEncrypt(pageSql)).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("并行抽取SQL(" + pageSql + ")测试失败");
            }
            boolean resultData = (boolean) ar.getData();
            if (!resultData) {
                throw new BusinessException("根据并行抽取SQL(" + pageSql + ")未能获取到数据");
            }
        } catch (Exception e) {
            throw new BusinessException("与Agent端交互异常!!!" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "sql", desc = "", range = "", nullable = true, valueIfNull = "")
    @Return(desc = "", range = "")
    public long getTableDataCount(long colSetId, String tableName, String sql) {
        Map<String, Object> resultMap = Dbo.queryOneObject("select dsl_id, agent_id," + " fetch_size  from " + DatabaseSet.TableName + " where database_id = ?", colSetId);
        if (resultMap.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        String url = AgentActionUtil.getUrl((Long) resultMap.get("agent_id"), getUserId(), AgentActionUtil.GETTABLECOUNT);
        StoreConnectionBean storeConnectionBean = dbConfStepService.setStoreConnectionBean(Long.valueOf(resultMap.get("dsl_id").toString()));
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("database_drive", storeConnectionBean.getDatabase_driver()).addData("jdbc_url", storeConnectionBean.getJdbc_url()).addData("user_name", storeConnectionBean.getUser_name()).addData("database_pad", storeConnectionBean.getDatabase_pwd()).addData("database_type", storeConnectionBean.getDatabase_type()).addData("tableName", tableName).addData("fetch_size", (Integer) resultMap.get("fetch_size")).addData("sql", StringUtil.isNotBlank(sql) ? AesUtil.desEncrypt(sql) : "").post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("获取" + tableName + "表数据量失败");
            }
            Long count = Long.parseLong((String) ar.getData());
            return count;
        } catch (Exception e) {
            throw new BusinessException("与Agent端交互异常!!!" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getAllSQLs(long colSetId) {
        List<Map<String, Object>> tableList = Dbo.queryList(" SELECT * " + " FROM " + TableInfo.TableName + " WHERE database_id = ? AND is_user_defined = ? order by table_id", colSetId, IsFlag.Shi.getCode());
        tableList.forEach(itemMap -> {
            List<Object> tableStateList = checkTableCollectState(colSetId, itemMap.get("table_name"));
            if (tableStateList.contains(ExecuteState.YunXingWanCheng.getCode()) || tableStateList.contains(ExecuteState.KaiShiYunXing.getCode())) {
                itemMap.put("collectState", false);
            } else {
                itemMap.put("collectState", true);
            }
            Map<String, Object> tableCycle = getTableCycle(itemMap.get("table_id"));
            itemMap.put("interval_time", tableCycle.get("interval_time"));
            itemMap.put("over_date", tableCycle.get("over_date"));
            itemMap.put("tc_id", tableCycle.get("tc_id"));
        });
        return AesUtil.encrypt(JsonUtil.toJson(tableList));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getSingleTableSQL(long colSetId, String tableName) {
        return Dbo.queryResult("SELECT * " + " FROM " + TableInfo.TableName + " WHERE database_id = ? AND table_name = ? ", colSetId, tableName);
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
            List<TableColumn> tableColumns = getColumnInfoByTableName(colSetId, getUserId(), tableName);
            tableColumns.forEach(table_column -> table_column.setIs_zipper_field(IsFlag.Fou.getCode()));
            returnMap.put("columnInfo", tableColumns);
        } else {
            List<TableColumn> tableColumns = Dbo.queryList(TableColumn.class, " SELECT * FROM " + TableColumn.TableName + " WHERE table_id = ? order by cast(tc_remark as integer)", tableId);
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
    @Return(desc = "", range = "")
    public Result getSQLInfoByColSetId(long colSetId) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        return Dbo.queryResult("select * " + " from " + TableInfo.TableName + " where database_id = ? and is_user_defined = ?", colSetId, IsFlag.Fou.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, List<Map<String, Object>>> getColumnInfoByColSetId(long colSetId) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        Result tableInfos = Dbo.queryResult("select table_id, table_name from " + TableInfo.TableName + " where database_id = ? and is_user_defined = ?", colSetId, IsFlag.Fou.getCode());
        if (tableInfos.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<Map<String, Object>>> returnMap = new HashMap<>();
        for (int i = 0; i < tableInfos.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + TableColumn.TableName + " where table_id = ?", tableInfos.getLong(i, "table_id"));
            if (result.isEmpty()) {
                throw new BusinessException("获取表字段信息失败");
            }
            returnMap.put(tableInfos.getString(i, "table_name"), result.toList());
        }
        return returnMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfo", desc = "", range = "")
    @Param(name = "collColumn", desc = "", range = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "columnCleanOrder", desc = "", range = "")
    private void saveTableColumnInfoForAdd(TableInfo tableInfo, String collColumn, long colSetId, String columnCleanOrder) {
        List<TableColumn> tableColumns;
        if (StringUtil.isBlank(collColumn)) {
            tableColumns = getColumnInfoByTableName(colSetId, getUserId(), tableInfo.getTable_name());
            if (!tableColumns.isEmpty()) {
                for (TableColumn tableColumn : tableColumns) {
                    tableColumn.setIs_get(IsFlag.Shi.getCode());
                    if (tableColumn.getIs_primary_key() != null) {
                        if (IsFlag.ofEnumByCode(tableColumn.getIs_primary_key()) == IsFlag.Shi) {
                            tableColumn.setIs_primary_key(IsFlag.Shi.getCode());
                        } else {
                            tableColumn.setIs_primary_key(IsFlag.Fou.getCode());
                        }
                    } else {
                        tableColumn.setIs_primary_key(IsFlag.Fou.getCode());
                    }
                }
            }
        } else {
            tableColumns = JsonUtil.toObject(collColumn, new TypeReference<List<TableColumn>>() {
            });
        }
        if (tableColumns != null && !tableColumns.isEmpty()) {
            for (TableColumn tableColumn : tableColumns) {
                Validator.notBlank(tableColumn.getColumn_name(), "保存" + tableColumn.getColumn_name() + "采集列时，字段名不能为空");
                Validator.notBlank(tableColumn.getColumn_type(), "保存" + tableColumn.getColumn_type() + "采集列时，字段类型不能为空");
                Validator.notBlank(tableColumn.getIs_get(), "保存" + tableColumn.getIs_get() + "采集列时，是否采集标识位不能为空");
                IsFlag.ofEnumByCode(tableColumn.getIs_get());
                Validator.notBlank(tableColumn.getIs_primary_key(), "保存" + tableColumn.getIs_primary_key() + "采集列时，是否主键标识位不能为空");
                tableColumn.setColumn_id(PrimayKeyGener.getNextId());
                tableColumn.setTable_id(tableInfo.getTable_id());
                tableColumn.setValid_s_date(DateUtil.getSysDate());
                tableColumn.setValid_e_date(Constant._MAX_DATE_8);
                tableColumn.setTc_or(columnCleanOrder);
                tableColumn.add(Dbo.db());
            }
        } else {
            throw new BusinessException("保存" + tableInfo.getTable_name() + "的采集字段失败");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfo", desc = "", range = "", isBean = true)
    @Param(name = "collColumn", desc = "", range = "")
    private void saveTableColumnInfoForUpdate(TableInfo tableInfo, String collColumn) {
        List<TableColumn> tableColumns = JsonUtil.toObject(collColumn, new TypeReference<List<TableColumn>>() {
        });
        if (tableColumns == null || tableColumns.isEmpty()) {
            throw new BusinessException("未获取到" + tableInfo.getTable_name() + "表的字段信息");
        }
        for (TableColumn tableColumn : tableColumns) {
            tableColumn.setTable_id(tableInfo.getTable_id());
            try {
                tableColumn.update(Dbo.db());
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException("保存时更新列信息失败");
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<TableColumn> getColumnInfoByTableName(long colSetId, long userId, String tableName) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId, userId);
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        long agentId = (long) databaseInfo.get("agent_id");
        String respMsg = SendMsgUtil.getColInfoByTbName(agentId, getUserId(), databaseInfo, tableName, AgentActionUtil.GETTABLECOLUMN);
        return JsonUtil.toObject(respMsg, new TypeReference<List<TableColumn>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfoString", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "collTbConfParamString", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "delTbString", desc = "", range = "", nullable = true)
    @Param(name = "tableCycles", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public long saveCollTbInfo(String tableInfoString, long colSetId, String collTbConfParamString, String delTbString, String tableCycles) {
        Map<String, Object> resultMap = Dbo.queryOneObject("select * from " + DatabaseSet.TableName + " where database_id = ?", colSetId);
        if (resultMap.isEmpty()) {
            throw new BusinessException("未找到数据库采集任务");
        }
        Dbo.execute(" DELETE FROM " + TableInfo.TableName + " WHERE database_id = ? AND is_user_defined = ? ", colSetId, IsFlag.Fou.getCode());
        try {
            List<TableInfo> tableInfos = JsonUtil.toObject(AesUtil.desEncrypt(tableInfoString), new TypeReference<List<TableInfo>>() {
            });
            List<CollTbConfParam> tbConfParams = JsonUtil.toObject(collTbConfParamString, new TypeReference<List<CollTbConfParam>>() {
            });
            if (tableInfos != null && tbConfParams != null) {
                if (tableInfos.size() != tbConfParams.size()) {
                    throw new BusinessException("请在传参时确保采集表数据和配置采集字段信息一一对应");
                }
                for (int i = 0; i < tableInfos.size(); i++) {
                    TableInfo tableInfo = tableInfos.get(i);
                    Validator.notBlank(tableInfo.getTable_name(), "保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据表名不能为空!");
                    Validator.notBlank(tableInfo.getTable_ch_name(), "保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据表中文名不能为空!");
                    Validator.notBlank(tableInfo.getUnload_type(), "保存采集表 " + tableInfo.getTable_name() + " 配置,第 " + (i + 1) + " 条,卸数方式不能为空!");
                    if (UnloadType.ofEnumByCode(tableInfo.getUnload_type()) == UnloadType.ZengLiangXieShu) {
                        Validator.notBlank(tableInfo.getSql(), "保存采集表 " + tableInfo.getTable_name() + " 配置,第 " + (i + 1) + " 条,增量SQL不能为空!");
                        tableInfo.setIs_parallel(IsFlag.Fou.getCode());
                        tableInfo.setIs_customize_sql(IsFlag.Fou.getCode());
                        tableInfo.setPage_sql("");
                        tableInfo.setTable_count("");
                        tableInfo.setDataincrement("");
                        tableInfo.setPageparallels("");
                    } else if (UnloadType.ofEnumByCode(tableInfo.getUnload_type()) == UnloadType.QuanLiangXieShu) {
                        Validator.notBlank(tableInfo.getIs_parallel(), "保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据并行方式不能为空!");
                        IsFlag isFlag = IsFlag.ofEnumByCode(tableInfo.getIs_parallel());
                        if (isFlag == IsFlag.Shi) {
                            Validator.notBlank(tableInfo.getIs_customize_sql(), "保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据自定义SQL方式不能为空!");
                            if (IsFlag.ofEnumByCode(tableInfo.getIs_customize_sql()) == IsFlag.Shi) {
                                tableInfo.setTable_count("");
                                tableInfo.setDataincrement("");
                                tableInfo.setPageparallels("");
                                Validator.notBlank(tableInfo.getPage_sql(), "保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据分页抽取SQL不能为空!");
                            } else {
                                tableInfo.setPage_sql("");
                                if (tableInfo.getPageparallels() == null) {
                                    throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据分页并行数不能为空!");
                                }
                                if (tableInfo.getDataincrement() == null) {
                                    throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条每日数据增量不能为空!");
                                }
                            }
                        } else {
                            tableInfo.setIs_customize_sql(IsFlag.Fou.getCode());
                        }
                    } else {
                        throw new BusinessException("保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据的卸数方式不存在, 得到的卸数方式是 :" + tableInfo.getUnload_type());
                    }
                    if (StringUtil.isBlank(tableInfo.getRec_num_date())) {
                        tableInfo.setRec_num_date(DateUtil.getSysDate());
                    }
                    Validator.notBlank(tableInfo.getIs_md5(), "保存采集表" + tableInfo.getTable_name() + "配置，第 " + (i + 1) + "条数据是否算MD5不能为空!");
                    tableInfo.setValid_s_date(DateUtil.getSysDate());
                    tableInfo.setValid_e_date(Constant._MAX_DATE_8);
                    tableInfo.setIs_user_defined(IsFlag.Fou.getCode());
                    tableInfo.setIs_register(IsFlag.Fou.getCode());
                    String collect_type = resultMap.get("collect_type").toString();
                    log.info("=============collect_type:{}", collect_type);
                    if (tableInfo.getTable_id() == null || StringUtil.isBlank(tableInfo.getTable_id().toString())) {
                        tableInfo.setTable_id(PrimayKeyGener.getNextId());
                        tableInfo.setTi_or(JsonUtil.toJson(Constant.DEFAULT_TABLE_CLEAN_ORDER));
                        tableInfo.add(Dbo.db());
                        saveTableColumnInfoForAdd(tableInfo, tbConfParams.get(i).getCollColumnString(), colSetId, JsonUtil.toJson(Constant.DEFAULT_COLUMN_CLEAN_ORDER));
                        if (collect_type.equals(CollectType.ShuJuKuCaiJi.getCode()) || collect_type.equals(CollectType.ShiShiCaiJi.getCode())) {
                            Long tableId = tableInfo.getTable_id();
                            addDataExtractionDefIfExists(tableId);
                        }
                    } else {
                        long oldID = tableInfo.getTable_id();
                        long newID = PrimayKeyGener.getNextId();
                        tableInfo.setTable_id(newID);
                        tableInfo.setTi_or(JsonUtil.toJson(Constant.DEFAULT_TABLE_CLEAN_ORDER));
                        tableInfo.add(Dbo.db());
                        updateTableId(newID, oldID);
                        saveTableColumnInfoForUpdate(tableInfo, tbConfParams.get(i).getCollColumnString());
                        if (collect_type.equals(CollectType.ShuJuKuCaiJi.getCode()) || collect_type.equals(CollectType.ShiShiCaiJi.getCode())) {
                            addDataExtractionDefIfExists(newID);
                        }
                    }
                    if (collect_type.equals(CollectType.ShuJuKuCaiJi.getCode()) || collect_type.equals(CollectType.ShiShiCaiJi.getCode())) {
                        saveTableCycle(tableCycles, tableInfo);
                    }
                }
            }
            String replace = delTbString.replaceAll("tableId", "table_id");
            List<TableInfo> delTables = JsonUtil.toObject(replace, new TypeReference<List<TableInfo>>() {
            });
            if (delTables != null && !delTables.isEmpty()) {
                for (TableInfo tableInfo : delTables) {
                    deleteDirtyDataOfTb(tableInfo.getTable_id());
                }
            }
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
        return colSetId;
    }

    private void addDataExtractionDefIfExists(Long tableId) {
        long num = Dbo.queryNumber(" select count(*) from " + DataExtractionDef.TableName + " where table_id = ? ", tableId).orElseThrow(() -> new BusinessException(String.format("根据新表ID %s 查询数据抽取定义表信息失败", tableId)));
        if (num == 0) {
            DataExtractionDef extraction_def = new DataExtractionDef();
            extraction_def.setDed_id(PrimayKeyGener.getNextId());
            extraction_def.setTable_id(tableId);
            extraction_def.setData_extract_type(DataExtractType.YuanShuJuGeShi.getCode());
            extraction_def.setIs_header(IsFlag.Fou.getCode());
            extraction_def.setDatabase_code(DataBaseCode.UTF_8.getCode());
            extraction_def.setDbfile_format(FileFormat.PARQUET.getCode());
            extraction_def.setIs_archived(IsFlag.Fou.getCode());
            extraction_def.add(Dbo.db());
        }
    }

    private void saveTableCycle(String tableCycles, TableInfo tableInfo) {
        if (StringUtil.isNotBlank(tableCycles)) {
            Map<String, Object> jsonObject = JsonUtil.toObject(tableCycles, new TypeReference<Map<String, Object>>() {
            });
            Map data = (Map<String, Object>) jsonObject.get(tableInfo.getTable_name());
            if (jsonObject.get(tableInfo.getTable_name()) != null && StringUtil.isNotBlank(data.get("over_date").toString())) {
                TableCycle tableCycle = JsonUtil.toObjectSafety(jsonObject.get(tableInfo.getTable_name()).toString(), TableCycle.class).orElseThrow(() -> new BusinessException("解析表" + tableInfo.getTable_name() + "的采集周期信息失败"));
                Validator.notBlank(tableCycle.getOver_date(), "采集表" + tableInfo.getTable_name() + "的采集周期结束日期不能为空");
                tableCycle.setTable_id(tableInfo.getTable_id());
                if (tableCycle.getTc_id() != null && tableCycle.getTc_id() != 0) {
                    try {
                        tableCycle.update(Dbo.db());
                    } catch (Exception e) {
                        if (!(e instanceof ProEntity.EntityDealZeroException)) {
                            CheckParam.throwErrorMsg(e.getMessage());
                        }
                    }
                } else {
                    tableCycle.setTc_id(PrimayKeyGener.getNextId());
                    tableCycle.setTable_id(tableInfo.getTable_id());
                    tableCycle.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getDatabaseSetInfo(long colSetId, long userId) {
        long databaseNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (databaseNum == 0) {
            throw new BusinessException("任务(" + colSetId + ")不存在!!!");
        }
        return Dbo.queryOneObject(" select t1.fetch_size," + " t1.agent_id, t1.db_agent, t1.plane_url,t1.dsl_id" + " from " + DatabaseSet.TableName + " t1" + " join " + AgentInfo.TableName + " ai on ai.agent_id = t1.agent_id" + " where t1.database_id = ? and ai.user_id = ? ", colSetId, userId);
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
    @Param(name = "newID", desc = "", range = "")
    @Param(name = "oldID", desc = "", range = "")
    private void updateTableId(long newID, long oldID) {
        List<Object> storageIdList = Dbo.queryOneColumnList(" select storage_id from " + TableStorageInfo.TableName + " where table_id = ? ", oldID);
        if (storageIdList.size() > 1) {
            throw new BusinessException("表存储信息不唯一");
        }
        if (!storageIdList.isEmpty()) {
            long storageId = (long) storageIdList.get(0);
            DboExecute.updatesOrThrow("更新表存储信息失败", "update " + TableStorageInfo.TableName + " set table_id = ? where storage_id = ?", newID, storageId);
        }
        List<Object> tableCleanIdList = Dbo.queryOneColumnList(" select table_clean_id from " + TableClean.TableName + " where table_id = ? ", oldID);
        if (!tableCleanIdList.isEmpty()) {
            StringBuilder tableCleanBuilder = new StringBuilder("update " + TableClean.TableName + " set table_id = ? where table_clean_id in ( ");
            for (int j = 0; j < tableCleanIdList.size(); j++) {
                tableCleanBuilder.append((long) tableCleanIdList.get(j));
                if (j != tableCleanIdList.size() - 1) {
                    tableCleanBuilder.append(",");
                }
            }
            tableCleanBuilder.append(" )");
            Dbo.execute(tableCleanBuilder.toString(), newID);
        }
        List<Object> tableColumnIdList = Dbo.queryOneColumnList(" select column_id from " + TableColumn.TableName + " where table_id = ? ", oldID);
        if (!tableColumnIdList.isEmpty()) {
            StringBuilder tableCleanBuilder = new StringBuilder("update " + TableColumn.TableName + " set table_id = ? where column_id in ( ");
            for (int j = 0; j < tableColumnIdList.size(); j++) {
                tableCleanBuilder.append((long) tableColumnIdList.get(j));
                if (j != tableColumnIdList.size() - 1) {
                    tableCleanBuilder.append(",");
                }
            }
            tableCleanBuilder.append(" )");
            Dbo.execute(tableCleanBuilder.toString(), newID);
        }
        List<Object> extractDefIdList = Dbo.queryOneColumnList(" select ded_id from " + DataExtractionDef.TableName + " where table_id = ? ", oldID);
        if (!extractDefIdList.isEmpty()) {
            StringBuilder data_extraction_def = new StringBuilder("update " + DataExtractionDef.TableName + " set table_id = ? where ded_id in ( ");
            for (int j = 0; j < extractDefIdList.size(); j++) {
                data_extraction_def.append(extractDefIdList.get(j));
                if (j != extractDefIdList.size() - 1) {
                    data_extraction_def.append(",");
                }
            }
            data_extraction_def.append(" )");
            Dbo.execute(data_extraction_def.toString(), newID);
        }
        List<Object> colMergeIdList = Dbo.queryOneColumnList(" select col_merge_id from " + ColumnMerge.TableName + " where table_id = ? ", oldID);
        if (!colMergeIdList.isEmpty()) {
            StringBuilder colMergeBuilder = new StringBuilder("update " + ColumnMerge.TableName + " set table_id = ? where col_merge_id in ( ");
            for (int j = 0; j < colMergeIdList.size(); j++) {
                colMergeBuilder.append((long) colMergeIdList.get(j));
                if (j != colMergeIdList.size() - 1) {
                    colMergeBuilder.append(",");
                }
            }
            colMergeBuilder.append(" )");
            Dbo.execute(colMergeBuilder.toString(), newID);
        }
        Optional<String> optional_table_id = Dbo.queryOneObject(String.class, "select table_id from " + DataStoreReg.TableName + " where table_id = ?", oldID);
        if (optional_table_id.isPresent()) {
            DboExecute.updatesOrThrow("更新" + DataStoreReg.TableName + "表失败!!!", "update " + DataStoreReg.TableName + " set table_id = ? where table_id = ?", newID, oldID);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfo", desc = "", range = "", isBean = true)
    @Param(name = "colSetId", desc = "", range = "")
    private void saveCustomSQLColumnInfoForAdd(TableInfo tableInfo, long colSetId) {
        Set<TableColumn> tableColumns = getSqlColumnData(colSetId, tableInfo.getUnload_type(), tableInfo.getSql(), tableInfo.getTable_id(), tableInfo.getTable_name());
        if (UnloadType.ofEnumByCode(tableInfo.getUnload_type()) == UnloadType.ZengLiangXieShu) {
            List<Boolean> primary = new ArrayList<>();
            tableColumns.forEach(table_column -> {
                if (IsFlag.ofEnumByCode(table_column.getIs_primary_key()) == IsFlag.Shi) {
                    primary.add(true);
                }
            });
            if (!primary.contains(true)) {
                throw new BusinessException("当前表(" + tableInfo.getTable_name() + ")的卸数方式为增量, 未设置主键,请检查");
            }
        }
        for (TableColumn tableColumn : tableColumns) {
            tableColumn.setColumn_id(PrimayKeyGener.getNextId());
            tableColumn.setTable_id(tableInfo.getTable_id());
            tableColumn.add(Dbo.db());
        }
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
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Optional<TableInfo> getTableSetUnloadData(long table_id) {
        return Dbo.queryOneObject(TableInfo.class, "SELECT * FROM " + TableInfo.TableName + " WHERE table_id = ?", table_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableNames", desc = "", range = "")
    @Param(name = "tableIds", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Boolean> checkTablePrimary(long colSetId, String[] tableNames, String tableIds) {
        List<TableColumn> tableColumns;
        Map<String, Long> tableIdMap = null;
        if (StringUtil.isNotBlank(tableIds)) {
            tableIdMap = JsonUtil.toObject(tableIds, new TypeReference<Map<String, Long>>() {
            });
        }
        Map<String, Boolean> checkPrimaryMap = new HashMap<>();
        for (String table_name : tableNames) {
            if (tableIdMap != null && StringUtil.isNotBlank(String.valueOf(tableIdMap.get(table_name)))) {
                getCheckPrimaryByTableId(colSetId, table_name, tableIdMap.get(table_name), checkPrimaryMap);
            } else {
                tableColumns = getColumnInfoByTableName(colSetId, getUserId(), table_name);
                for (TableColumn tableColumn : tableColumns) {
                    if (IsFlag.ofEnumByCode(tableColumn.getIs_primary_key()) == IsFlag.Shi) {
                        checkPrimaryMap.put(table_name, true);
                        break;
                    } else {
                        checkPrimaryMap.put(table_name, false);
                    }
                }
            }
        }
        return checkPrimaryMap;
    }

    private void getCheckPrimaryByTableId(long colSetId, String tableName, long table_id, Map<String, Boolean> checkPrimaryMap) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TableColumn.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.table_id = t2.table_id WHERE t2.database_id = ? AND t2.table_name = ? AND t1.table_id = ?", colSetId, tableName, table_id).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            throw new BusinessException("任务( " + colSetId + "),不存在表( " + tableName + " )");
        }
        List<Map<String, Object>> list = Dbo.queryList("SELECT (case t1.is_primary_key when ? then 'true' else 'false' end) is_primary_key FROM " + TableColumn.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.table_id = t2.table_id WHERE t2.database_id = ? AND t2.table_name = ? AND t1.table_id = ?", IsFlag.Shi.getCode(), colSetId, tableName, table_id);
        for (Map<String, Object> map : list) {
            if (map.get("is_primary_key").equals("true")) {
                checkPrimaryMap.put(tableName, true);
                break;
            } else {
                checkPrimaryMap.put(tableName, false);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "unloadType", desc = "", range = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "tableId", desc = "", range = "", nullable = true, valueIfNull = "0")
    @Param(name = "tableName", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Set<TableColumn> getSqlColumnData(long colSetId, String unloadType, String sql, long tableId, String tableName) {
        if (StringUtil.isBlank(unloadType)) {
            throw new BusinessException("请指定卸数方式");
        }
        if (StringUtil.isBlank(sql)) {
            throw new BusinessException("SQL不能为空");
        }
        Set<TableColumn> columnDataSet = new LinkedHashSet<>();
        if (UnloadType.ofEnumByCode(unloadType) == UnloadType.ZengLiangXieShu) {
            @SuppressWarnings("unchecked")
            Map<String, Object> incrementSqlMap = JsonUtil.toObjectSafety(sql, Map.class).orElseThrow(() -> new BusinessException("增量SQL解析出现错误"));
            incrementSqlMap.forEach((k, v) -> {
                if (v != null && StringUtil.isNotBlank(v.toString())) {
                    getTableColumns(colSetId, v.toString(), columnDataSet);
                }
            });
        } else {
            getTableColumns(colSetId, sql, columnDataSet);
        }
        if (tableId != 0) {
            Map<String, Object> columnInfo = getColumnInfo(tableName, colSetId, tableId);
            @SuppressWarnings("unchecked")
            List<TableColumn> tableColumnList = (List<TableColumn>) columnInfo.get("columnInfo");
            List<String> collect = tableColumnList.stream().map(TableColumn::getColumn_name).collect(Collectors.toList());
            columnDataSet.removeIf(item -> collect.contains(item.getColumn_name()));
            columnDataSet.addAll(tableColumnList);
        }
        return columnDataSet;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "sql", desc = "", range = "")
    @Return(desc = "", range = "")
    private void getTableColumns(long colSetId, String sql, Set<TableColumn> tableColumnSet) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId, getUserId());
        String respMsg = SendMsgUtil.getCustColumn(((long) databaseInfo.get("agent_id")), getUserId(), databaseInfo, sql, AgentActionUtil.GETCUSTCOLUMN);
        List<TableColumn> tableColumnList = JsonUtil.toObject(respMsg, new TypeReference<List<TableColumn>>() {
        });
        tableColumnList.forEach(table_column -> {
            table_column.setIs_get(IsFlag.Shi.getCode());
            table_column.setIs_primary_key(IsFlag.Fou.getCode());
            table_column.setIs_new(IsFlag.Fou.getCode());
            table_column.setTc_or(JsonUtil.toJson(Constant.DEFAULT_COLUMN_CLEAN_ORDER));
            table_column.setIs_alive(IsFlag.Shi.getCode());
            table_column.setIs_new(IsFlag.Fou.getCode());
            table_column.setValid_s_date(DateUtil.getSysDate());
            table_column.setValid_e_date(Constant._MAX_DATE_8);
        });
        tableColumnSet.addAll(tableColumnList);
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

    @Method(desc = "", logicStep = "")
    @Param(name = "database_id", desc = "", range = "")
    @Param(name = "table_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    public void deleteTableSql(long database_id, long table_id, String table_name) {
        long num = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai on ai.source_id = ds.source_id " + " join " + DatabaseSet.TableName + " dbs on ai.Agent_id = dbs.Agent_id " + " where ai.user_id = ? and dbs.database_id = ?", getUserId(), database_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (num != 1) {
            throw new BusinessException("根据" + database_id + "没有找到对应采集任务，请检查是否存在");
        } else {
            deleteDirtyDataOfTb(table_id);
            DboExecute.deletesOrThrow("根据表ID" + table_id + "删除表信息失败", "delete from " + TableInfo.TableName + " where table_id = ?", table_id);
            Dbo.execute("delete from " + CollectCase.TableName + " where collect_set_id = ? and task_classify = ?", database_id, table_name);
        }
    }
}
