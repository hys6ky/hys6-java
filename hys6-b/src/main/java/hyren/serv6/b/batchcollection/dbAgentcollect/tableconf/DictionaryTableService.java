package hyren.serv6.b.batchcollection.dbAgentcollect.tableconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator.Assembler;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Slf4j
@Service
@Api("数据文件采集表配置")
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-04-13 14:41")
public class DictionaryTableService {

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getTableData(long colSetId) {
        Map<String, Object> resultMap = new HashMap<>();
        long countNum = Dbo.queryNumber("SELECT count(1) FROM database_set WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("采集任务( %s ),不存在!!!", colSetId);
        }
        List<TableInfo> databaseTableList = getTableInfo(colSetId);
        List<TableInfo> dirTableList = getDirTableData(colSetId);
        if (!databaseTableList.isEmpty()) {
            if (dirTableList == null || dirTableList.isEmpty()) {
                CheckParam.throwErrorMsg("数据字典中的表信息为空");
            }
            Map<String, List<String>> differenceInfo = getDifferenceInfo(getTableName(dirTableList), getTableName(databaseTableList));
            if (!differenceInfo.get("delete").isEmpty()) {
                removeTableBean(differenceInfo.get("delete"), databaseTableList, true);
                List<String> deleteTableList = new ArrayList<>();
                differenceInfo.get("delete").forEach(tableName -> {
                    long tableCountNum = Dbo.queryNumber("SELECT count(1) FROM  " + DataStoreReg.TableName + "  WHERE database_id = ? AND table_name  = ?", colSetId, tableName).orElseThrow(() -> new BusinessException("SQL填写错误"));
                    if (tableCountNum != 0) {
                        deleteTableList.add(tableName);
                    } else {
                        List<String> deleteTableNameList = new ArrayList<>();
                        deleteTableNameList.add(tableName);
                        deleteTableInfoByTableName(colSetId, deleteTableNameList);
                    }
                    if (deleteTableList.size() != 0) {
                        resultMap.put("existsTable", String.format("删除的表(%s)在上次任务采集中已经完成,此次配置却被删除了...请检查数据字典在进行操作", StringUtil.join(deleteTableList, ",")));
                    }
                });
            }
            if (!differenceInfo.get("exists").isEmpty()) {
                removeTableBean(differenceInfo.get("exists"), dirTableList, true);
            }
            if (!differenceInfo.get("add").isEmpty()) {
                removeTableBean(differenceInfo.get("add"), dirTableList, false);
                setDirTableDefaultData(dirTableList, colSetId);
            }
            dirTableList.addAll(databaseTableList);
        } else {
            if (dirTableList == null || dirTableList.isEmpty()) {
                CheckParam.throwErrorMsg("数据字典与数据库中的表信息为空");
            }
            setDirTableDefaultData(dirTableList, colSetId);
        }
        resultMap.put("dirTableList", dirTableList);
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<TableColumn> getTableColumnByTableId(long colSetId, long table_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TableInfo.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("任务ID( %s )不存在采集表ID(%s)信息!!!", colSetId, table_id);
        }
        return Dbo.queryList(TableColumn.class, "SELECT t1.* from " + TableColumn.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.table_id = t2.table_id WHERE t2.database_id = ? AND t1.table_id = ?" + " AND t2.valid_e_date = ? AND t1.valid_e_date = ? ORDER BY t1.column_name", colSetId, table_id, Constant._MAX_DATE_8, Constant._MAX_DATE_8);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<TableColumn> getTableColumnByTableName(long colSetId, String table_name) {
        Map<String, List<TableColumn>> dicTableAllColumnMap = getDicTableAllColumn(colSetId);
        if (dicTableAllColumnMap == null || dicTableAllColumnMap.isEmpty()) {
            CheckParam.throwErrorMsg("数据字典中未找到表(%s)列信息", table_name);
        }
        return dicTableAllColumnMap.get(table_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, List<TableColumn>> getDicTableAllColumn(long colSetId) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId);
        String respMsg = SendMsgUtil.getAllTableName((Long) databaseInfo.get("agent_id"), getUserId(), databaseInfo, AgentActionUtil.GETAlLLTABLECOLUMN);
        return JsonUtil.toObject(respMsg, new TypeReference<Map<String, List<TableColumn>>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Param(name = "tableColumns", desc = "", range = "", isBean = true)
    public void updateColumnByTableId(long table_id, TableColumn[] tableColumns) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TableColumn.TableName + " WHERE table_id = ?", table_id).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("根据表ID(%s),未找到列信息", table_id);
        }
        for (TableColumn tableColumn : tableColumns) {
            tableColumn.update(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tableInfos", desc = "", range = "", isBean = true)
    @Param(name = "tableColumns", desc = "", range = "", nullable = true, example = "")
    @Return(desc = "", range = "")
    public long saveTableData(long colSetId, TableInfo[] tableInfos, Map<String, List<TableColumn>> tableColumns) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("任务ID(%s),不存在", colSetId);
        }
        for (TableInfo tableInfo : tableInfos) {
            if (tableInfo.getTable_id() == null) {
                long table_id = PrimayKeyGener.getNextId();
                tableInfo.setTable_id(table_id);
                tableInfo.setDatabase_id(colSetId);
                tableInfo.setTi_or(JsonUtil.toJson(Constant.DEFAULT_TABLE_CLEAN_ORDER));
                tableInfo.setValid_s_date(DateUtil.getSysDate());
                tableInfo.setValid_e_date(Constant._MAX_DATE_8);
                tableInfo.add(Dbo.db());
                if (tableColumns == null) {
                    Map<String, List<TableColumn>> dicTableAllColumnMap = getDicTableAllColumn(colSetId);
                    setColumnDefaultData(dicTableAllColumnMap.get(tableInfo.getTable_name()), table_id);
                } else {
                    String tableName = tableInfo.getTable_name();
                    List<TableColumn> columnListt = tableColumns.get(tableName);
                    Map<String, List<TableColumn>> tableColumnList = new HashMap<>();
                    tableColumnList.put(tableName, columnListt);
                    if (tableColumnList.isEmpty()) {
                        CheckParam.throwErrorMsg("为获取到表的列信息");
                    }
                    setColumnDefaultData(tableColumnList.get(tableInfo.getTable_name()), table_id);
                }
            } else {
                if (tableColumns != null && tableColumns.size() != 0) {
                    tableColumns.forEach((s, table_columns) -> {
                        table_columns.forEach(table_column -> {
                            if (table_column.getColumn_id() == null) {
                                table_column.setColumn_id(PrimayKeyGener.getNextId());
                                table_column.setTable_id(tableInfo.getTable_id());
                                table_column.setValid_s_date(DateUtil.getSysDate());
                                table_column.setValid_e_date(Constant._MAX_DATE_8);
                                table_column.add(Dbo.db());
                            } else {
                                Dbo.execute(" UPDATE " + TableColumn.TableName + " SET valid_e_date = ? where table_id = ? and column_id = ?", DateUtil.getSysDate(), table_column.getTable_id(), table_column.getColumn_id());
                                Dbo.execute("UPDATE " + TableColumn.TableName + " SET column_name = ?,colume_ch_name = ?,column_type = ?,is_primary_key = ?," + "is_get = ?,is_alive = ?,is_new = ?,valid_e_date = ? where table_id = ? and column_id = ?", table_column.getColumn_name(), table_column.getColumn_ch_name(), table_column.getColumn_type(), table_column.getIs_primary_key(), table_column.getIs_get(), table_column.getIs_alive(), table_column.getIs_new(), Constant._MAX_DATE_8, table_column.getTable_id(), table_column.getColumn_id());
                            }
                        });
                    });
                }
            }
        }
        List<String> databaseTableList = getTableInfo(colSetId).stream().map(TableInfo::getTable_name).collect(Collectors.toList());
        List<String> dirTableList = getDirTableData(colSetId).stream().map(TableInfo::getTable_name).collect(Collectors.toList());
        Map<String, List<String>> differenceInfo = getDifferenceInfo(dirTableList, databaseTableList);
        deleteTableInfoByTableName(colSetId, differenceInfo.get("delete"));
        return colSetId;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableColumnList", desc = "", range = "", isBean = true)
    @Param(name = "table_id", desc = "", range = "")
    private void setColumnDefaultData(List<TableColumn> tableColumnList, long table_id) {
        if (null != tableColumnList && !tableColumnList.isEmpty()) {
            tableColumnList.forEach(table_column -> {
                table_column.setTable_id(table_id);
                table_column.setColumn_id(PrimayKeyGener.getNextId());
                table_column.setTc_or(JsonUtil.toJson(Constant.DEFAULT_COLUMN_CLEAN_ORDER));
                table_column.setValid_s_date(DateUtil.getSysDate());
                table_column.setValid_e_date(Constant._MAX_DATE_8);
                table_column.add(Dbo.db());
            });
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<TableInfo> getDirTableData(long colSetId) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId);
        String respMsg = SendMsgUtil.getAllTableName(Long.parseLong(String.valueOf(databaseInfo.get("agent_id"))), getUserId(), databaseInfo, AgentActionUtil.GETDATABASETABLE);
        return JsonUtil.toObject(respMsg, new TypeReference<List<TableInfo>>() {
        });
    }

    @Param(name = "dirTableList", desc = "", range = "", isBean = true)
    @Param(name = "colSetId", desc = "", range = "")
    private void setDirTableDefaultData(List<TableInfo> dirTableList, long colSetId) {
        dirTableList.forEach((table_info) -> {
            table_info.setDatabase_id(colSetId);
            table_info.setRec_num_date(DateUtil.getSysDate());
            table_info.setValid_s_date(DateUtil.getSysDate());
            table_info.setIs_md5(IsFlag.Fou.getCode());
            table_info.setIs_register(IsFlag.Shi.getCode());
            table_info.setIs_customize_sql(IsFlag.Fou.getCode());
            table_info.setIs_parallel(IsFlag.Fou.getCode());
            table_info.setIs_user_defined(IsFlag.Fou.getCode());
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getDatabaseSetInfo(long colSetId) {
        long databaseNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (databaseNum == 0) {
            CheckParam.throwErrorMsg("任务(%s)不存在!!!", colSetId);
        }
        return Dbo.queryOneObject(" select t1.dsl_id, t1.fetch_size," + " t1.agent_id, t1.db_agent, t1.plane_url" + " from " + DatabaseSet.TableName + " t1" + " left join " + AgentInfo.TableName + " ai on ai.agent_id = t1.agent_id" + " where t1.database_id = ? and ai.user_id = ? ", colSetId, getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableBeanList", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<String> getTableName(List<TableInfo> tableBeanList) {
        List<String> tableNameList = new ArrayList<>();
        tableBeanList.forEach(table_info -> tableNameList.add(table_info.getTable_name()));
        return tableNameList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dicTableList", desc = "", range = "")
    @Param(name = "databaseTableNames", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, List<String>> getDifferenceInfo(List<String> dicTableList, List<String> databaseTableNames) {
        log.info("数据字典的 " + dicTableList);
        log.info("数据库的 " + databaseTableNames);
        List<String> exists = new ArrayList<>();
        List<String> delete = new ArrayList<>();
        Map<String, List<String>> differenceMap = new HashMap<>();
        for (String databaseTableName : databaseTableNames) {
            if (dicTableList.contains(databaseTableName)) {
                exists.add(databaseTableName);
                dicTableList.remove(databaseTableName);
            } else {
                delete.add(databaseTableName);
            }
        }
        log.info("数据字典存在的===>" + exists);
        differenceMap.put("exists", exists);
        log.info("数据字典删除的===>" + delete);
        differenceMap.put("delete", delete);
        log.info("数据字典新增的===>" + dicTableList);
        differenceMap.put("add", dicTableList);
        return differenceMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableNameList", desc = "", range = "")
    @Param(name = "tableList", desc = "", range = "")
    @Param(name = "deleteType", desc = "", range = "")
    private void removeTableBean(List<String> tableNameList, List<TableInfo> tableList, boolean deleteType) {
        Iterator<TableInfo> iterator = tableList.iterator();
        while (iterator.hasNext()) {
            TableInfo table_info = iterator.next();
            if (deleteType) {
                if (tableNameList.contains(table_info.getTable_name())) {
                    iterator.remove();
                }
            } else {
                if (!tableNameList.contains(table_info.getTable_name())) {
                    iterator.remove();
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<TableInfo> getTableInfo(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("当前任务(%s),不存在!!!");
        }
        return Dbo.queryList(TableInfo.class, "SELECT * FROM " + TableInfo.TableName + " WHERE database_id = ? AND valid_e_date = ? ORDER BY table_name", colSetId, Constant._MAX_DATE_8);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "deleteTableName", desc = "", range = "")
    private void deleteTableInfoByTableName(long colSetId, List<String> deleteTableName) {
        if (deleteTableName == null || deleteTableName.size() == 0) {
            return;
        }
        Assembler assembler = Assembler.newInstance().addSql("SELECT table_id FROM table_info t1 JOIN database_set t2 ON t1.database_id = t2.database_id WHERE " + "not EXISTS (SELECT * FROM  " + DataStoreReg.TableName + " t2  WHERE t1.table_name = t2.table_name) AND t1.database_id = ? ").addParam(colSetId).addORParam("table_name", deleteTableName.toArray());
        Object[] deleteTableId = Dbo.queryOneColumnList(assembler.sql(), assembler.params()).toArray(new Object[0]);
        if (deleteTableId.length == 0) {
            return;
        }
        assembler.clean();
        assembler = Assembler.newInstance().addSql("SELECT t2.column_id FROM " + TableInfo.TableName + " t1 LEFT JOIN " + TableColumn.TableName + " t2 ON t1.table_id = t2.table_id " + "WHERE t1.database_id = ?").addParam(colSetId).addORParam("t1.table_id", deleteTableId);
        Object[] deleteTableColumnId = Dbo.queryOneColumnList(assembler.sql(), assembler.params()).toArray(new Object[0]);
        assembler.clean();
        StringBuilder sql = new StringBuilder("DELETE FROM " + TableInfo.TableName + " WHERE table_id in (");
        for (Object object : deleteTableId) {
            sql.append(object).append(",");
        }
        sql.delete(sql.length() - 1, sql.length());
        sql.append(")");
        execute(sql.toString(), new ArrayList<>());
        StringBuilder sql1 = new StringBuilder("DELETE FROM " + TableColumn.TableName + " WHERE column_id in (");
        for (Object object : deleteTableColumnId) {
            sql1.append(object).append(",");
        }
        sql1.delete(sql1.length() - 1, sql1.length());
        sql1.append(")");
        execute(sql1.toString(), new ArrayList<>());
        StringBuilder sql2 = new StringBuilder("DELETE FROM " + ColumnSplit.TableName + " WHERE column_id in (");
        for (Object object : deleteTableColumnId) {
            sql2.append(object).append(",");
        }
        sql2.delete(sql2.length() - 1, sql2.length());
        sql2.append(")");
        execute(sql2.toString(), new ArrayList<>());
        StringBuilder sql3 = new StringBuilder("DELETE FROM " + ColumnClean.TableName + " WHERE column_id in (");
        for (Object object : deleteTableColumnId) {
            sql3.append(object).append(",");
        }
        sql3.delete(sql3.length() - 1, sql3.length());
        sql3.append(")");
        execute(sql3.toString(), new ArrayList<>());
        StringBuilder sql4 = new StringBuilder("DELETE FROM " + DataExtractionDef.TableName + " WHERE table_id in (");
        for (Object object : deleteTableId) {
            sql4.append(object).append(",");
        }
        sql4.delete(sql4.length() - 1, sql4.length());
        sql4.append(")");
        execute(sql4.toString(), new ArrayList<>());
        StringBuilder sql5 = new StringBuilder("DELETE FROM " + TableStorageInfo.TableName + " WHERE table_id in (");
        for (Object object : deleteTableId) {
            sql5.append(object).append(",");
        }
        sql5.delete(sql5.length() - 1, sql5.length());
        sql5.append(")");
        execute(sql5.toString(), new ArrayList<>());
        StringBuilder sql6 = new StringBuilder("DELETE FROM " + TableClean.TableName + " WHERE table_id in (");
        for (Object object : deleteTableId) {
            sql6.append(object).append(",");
        }
        sql6.delete(sql6.length() - 1, sql6.length());
        sql6.append(")");
        execute(sql6.toString(), new ArrayList<>());
        StringBuilder sql7 = new StringBuilder("DELETE FROM " + ColumnMerge.TableName + " WHERE table_id in (");
        for (Object object : deleteTableId) {
            sql7.append(object).append(",");
        }
        sql7.delete(sql7.length() - 1, sql7.length());
        sql7.append(")");
        execute(sql7.toString(), new ArrayList<>());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "parasList", desc = "", range = "")
    private void execute(String sql, List<Object> parasList) {
        Dbo.execute(sql, parasList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Object> checkTableCollectState(long colSetId, String table_name) {
        long checkTableNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TableInfo.TableName + " WHERE database_id = ? AND table_name = ?", colSetId, table_name).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (checkTableNum == 0) {
            throw new BusinessException("任务(" + colSetId + ")下不存在表 (" + table_name + ")");
        }
        return Dbo.queryOneColumnList("SELECT t1.execute_state FROM " + CollectCase.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.collect_set_id = t2.database_id " + "AND t1.task_classify = t2.table_name  WHERE t1.collect_set_id = ? AND t2.table_name = ?", colSetId, table_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_id", desc = "", range = "")
    @Param(name = "database_type", desc = "", range = "")
    public void saveDatabaseType(long database_id, String database_type) {
        if (Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", database_id).orElseThrow(() -> new BusinessException("sql查询异常")) == 0) {
            throw new BusinessException("当前任务" + database_id + "已不存在！！！");
        }
        DboExecute.updatesOrThrow("更新源系统数据库设置表数据类型失败", "update " + DatabaseSet.TableName + " set database_type = ? where database_id = ?", database_type, database_id);
    }
}
