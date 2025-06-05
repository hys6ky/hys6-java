package hyren.serv6.b.batchcollection.agent.tableregister;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-07-07 11:04")
public class TableRegisterService {

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "tableInfos", desc = "", range = "", isBean = true)
    @Param(name = "tableColumns", desc = "", range = "", nullable = true)
    @Param(name = "dsl_id", desc = "", range = "")
    public void saveTableData(long source_id, long agent_id, long databaseId, TableInfo[] tableInfos, String tableColumns, long dsl_id) {
        Object collect_type = checkDatabaseSetExist(databaseId);
        Map<String, List<Map<String, Object>>> tableColumnObj = null;
        if (StringUtil.isNotBlank(tableColumns)) {
            try {
                tableColumnObj = JsonUtil.toObject(tableColumns, new TypeReference<Map<String, List<Map<String, Object>>>>() {
                });
            } catch (Exception e) {
                throw new BusinessException("表字段信息有误，请检查。");
            }
        }
        for (TableInfo tableInfo : tableInfos) {
            saveTableInfo(databaseId, tableInfo, collect_type);
            List<TableColumn> tableColumnList;
            if (tableColumnObj != null && tableColumnObj.containsKey(tableInfo.getTable_name())) {
                if (tableColumnObj.get(tableInfo.getTable_name()) == null) {
                    CheckParam.throwErrorMsg("表名称(%s)未设置列信息", tableInfo.getTable_name());
                }
                tableColumnList = JsonUtil.toObject(JsonUtil.toJson(tableColumnObj.get(tableInfo.getTable_name())), new TypeReference<List<TableColumn>>() {
                });
            } else {
                tableColumnList = databaseTableColumnInfo(databaseId, tableInfo.getTable_name());
            }
            setTableColumnInfo(tableInfo.getTable_id(), tableInfo.getTable_name(), tableColumnList);
            if (!collect_type.equals(CollectType.ShuJuKuCaiJi.getCode())) {
                saveStorageData(source_id, agent_id, databaseId, dsl_id, tableInfo);
            }
        }
        if (!collect_type.equals(CollectType.ShuJuKuCaiJi.getCode())) {
            DboExecute.updatesOrThrow("更新的数据超出了范围", "UPDATE " + DatabaseSet.TableName + " SET is_sendok = ? WHERE database_id = ?", IsFlag.Shi.getCode(), databaseId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<TableInfo> getTableData(long databaseId) {
        checkDatabaseSetExist(databaseId);
        return Dbo.queryList(TableInfo.class, "SELECT * FROM " + TableInfo.TableName + " WHERE database_id = ? ", databaseId);
    }

    private Object checkDatabaseSetExist(long databaseId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", databaseId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("任务ID(%s)不存在", databaseId);
        }
        return Dbo.queryOneObject("SELECT collect_type FROM " + DatabaseSet.TableName + " WHERE database_id = ?", databaseId).get("collect_type");
    }

    @Method(desc = "", logicStep = "")
    private void saveStorageData(long source_id, long agent_id, long databaseId, long dsl_id, TableInfo tableInfo) {
        Map<String, Object> classifyAndSourceNum = getClassifyAndSourceNum(databaseId);
        String hyren_name = String.format("%s_%s_%s", classifyAndSourceNum.get("datasource_number"), classifyAndSourceNum.get("classify_num"), tableInfo.getTable_name());
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DataStoreReg.TableName + " WHERE hyren_name = ? ", hyren_name).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum != 0) {
            CheckParam.throwErrorMsg("数据源(%s),分类(%s)下已存在当前表(%s)", classifyAndSourceNum.get("datasource_number"), classifyAndSourceNum.get("classify_num"), tableInfo.getTable_name());
        }
        TableStorageInfo table_storage_info = new TableStorageInfo();
        Long storage_id = PrimayKeyGener.getNextId();
        table_storage_info.setStorage_id(storage_id);
        table_storage_info.setFile_format(FileFormat.CSV.getCode());
        table_storage_info.setStorage_type(StorageType.ZhuiJia.getCode());
        table_storage_info.setIs_zipper(IsFlag.Fou.getCode());
        table_storage_info.setStorage_time(1L);
        table_storage_info.setHyren_name(hyren_name);
        table_storage_info.setTable_id(tableInfo.getTable_id());
        table_storage_info.add(Dbo.db());
        DtabRelationStore dtab_relation_store = new DtabRelationStore();
        dtab_relation_store.setDsl_id(dsl_id);
        dtab_relation_store.setTab_id(storage_id);
        dtab_relation_store.setData_source(StoreLayerDataSource.DBA.getCode());
        dtab_relation_store.setIs_successful(JobExecuteState.WanCheng.getCode());
        dtab_relation_store.add(Dbo.db());
        DataStoreReg data_store_reg = new DataStoreReg();
        data_store_reg.setFile_id(UUID.randomUUID().toString());
        data_store_reg.setCollect_type(AgentType.ShuJuKu.getCode());
        data_store_reg.setOriginal_update_date(DateUtil.getSysDate());
        data_store_reg.setOriginal_update_time(DateUtil.getSysTime());
        data_store_reg.setOriginal_name(StringUtil.isNotBlank(tableInfo.getTable_ch_name()) ? tableInfo.getTable_ch_name() : tableInfo.getTable_name());
        data_store_reg.setTable_name(tableInfo.getTable_name());
        data_store_reg.setHyren_name(hyren_name);
        data_store_reg.setStorage_date(DateUtil.getSysDate());
        data_store_reg.setStorage_time(DateUtil.getSysTime());
        data_store_reg.setFile_size(0L);
        data_store_reg.setAgent_id(agent_id);
        data_store_reg.setDatabase_id(databaseId);
        data_store_reg.setSource_id(source_id);
        data_store_reg.setTable_id(tableInfo.getTable_id());
        data_store_reg.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_info", desc = "", range = "", isBean = true)
    void checklistInformation(TableInfo table_info) {
        Validator.notBlank(table_info.getTable_name(), "表名称不能为空");
        Validator.notBlank(table_info.getTable_ch_name(), String.format("表(%s)中文名称不能为空", table_info.getTable_name()));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_info", desc = "", range = "", isBean = true)
    void checkColumnInformation(TableColumn table_column) {
        Validator.notBlank(table_column.getColumn_name(), "列名称不能为空");
        Validator.notBlank(table_column.getColumn_ch_name(), String.format("列(%s)中文名称不能为空", table_column.getColumn_name()));
        Validator.notBlank(table_column.getIs_primary_key(), String.format("列(%s)主键信息不能为空", table_column.getColumn_name()));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<TableColumn> databaseTableColumnInfo(long databaseId, String table_name) {
        long databaseNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", databaseId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (databaseNum == 0) {
            throw new BusinessException("任务(" + databaseId + ")不存在!!!");
        }
        Map<String, Object> databaseSetInfo = Dbo.queryOneObject(" select t1.dsl_id, t1.fetch_size," + " t1.agent_id, t1.db_agent, t1.plane_url" + " from " + DatabaseSet.TableName + " t1 " + " join " + AgentInfo.TableName + " ai on ai.agent_id = t1.agent_id" + " where t1.database_id = ? and ai.user_id = ? ", databaseId, UserUtil.getUserId());
        long agent_id = Long.parseLong(databaseSetInfo.get("agent_id").toString());
        String respMsg = SendMsgUtil.getColInfoByTbName(agent_id, UserUtil.getUserId(), databaseSetInfo, table_name, AgentActionUtil.GETTABLECOLUMN);
        return JsonUtil.toObject(respMsg, new TypeReference<List<TableColumn>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    private void setTableColumnInfo(long table_id, String table_name, List<TableColumn> tableColumnList) {
        tableColumnList.forEach(table_column -> {
            Validator.notBlank(table_column.getColumn_name(), String.format("表(%s)的列名称未设置", table_name));
            Validator.notBlank(table_column.getColumn_ch_name(), String.format("表(%s)的列(%s)中文名称未设置", table_name, table_column.getColumn_name()));
            setTableColumnDefaultData(table_id, table_column);
            table_column.add(Dbo.db());
        });
    }

    void setTableColumnDefaultData(long table_id, TableColumn table_column) {
        table_column.setColumn_id(PrimayKeyGener.getNextId());
        table_column.setTable_id(table_id);
        table_column.setValid_s_date(DateUtil.getSysDate());
        table_column.setValid_e_date(Constant._MAX_DATE_8);
        table_column.setIs_alive(IsFlag.Shi.getCode());
        table_column.setIs_new(IsFlag.Fou.getCode());
        table_column.setTc_or(Constant.DEFAULT_COLUMN_CLEAN_ORDER.toString());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Param(name = "tableInfos", desc = "", range = "", isBean = true)
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "tableColumns", desc = "", range = "", nullable = true)
    public void updateTableData(long source_id, long agent_id, long databaseId, long dsl_id, TableInfo[] tableInfos, String tableColumns) {
        Object collect_type = checkDatabaseSetExist(databaseId);
        if (!collect_type.equals(CollectType.ShuJuKuCaiJi.getCode())) {
            Dbo.execute("DELETE FROM " + DtabRelationStore.TableName + " WHERE tab_id in (SELECT storage_id FROM " + TableStorageInfo.TableName + " WHERE table_id in (SELECT table_id FROM " + TableInfo.TableName + " WHERE database_id = ?))", databaseId);
            Dbo.execute("DELETE FROM " + TableStorageInfo.TableName + " WHERE table_id in (SELECT table_id FROM " + TableInfo.TableName + " WHERE database_id = ?)", databaseId);
        }
        Dbo.execute("DELETE FROM " + TableInfo.TableName + " WHERE database_id = ?", databaseId);
        Dbo.execute("DELETE FROM " + DataStoreReg.TableName + " WHERE database_id = ?", databaseId);
        Map<String, Object> tableColumnObj = null;
        if (StringUtil.isNotBlank(tableColumns)) {
            tableColumnObj = JsonUtil.toObject(tableColumns, new TypeReference<Map<String, Object>>() {
            });
        }
        List<TableColumn> tableColumnList;
        for (TableInfo tableInfo : tableInfos) {
            checklistInformation(tableInfo);
            if (tableInfo.getTable_id() == null) {
                if (tableColumnObj != null && tableColumnObj.containsKey(tableInfo.getTable_name())) {
                    if (tableColumnObj.get(tableInfo.getTable_name()) == null) {
                        CheckParam.throwErrorMsg("表名称(%s)未设置列信息", tableInfo.getTable_name());
                    }
                    tableColumnList = JsonUtil.toObject(JsonUtil.toJson(tableColumnObj.get(tableInfo.getTable_name())), new TypeReference<List<TableColumn>>() {
                    });
                } else {
                    tableColumnList = databaseTableColumnInfo(databaseId, tableInfo.getTable_name());
                }
                saveTableInfo(databaseId, tableInfo, collect_type);
                setTableColumnInfo(tableInfo.getTable_id(), tableInfo.getTable_name(), tableColumnList);
                if (!collect_type.equals(CollectType.ShuJuKuCaiJi.getCode())) {
                    saveStorageData(source_id, agent_id, databaseId, dsl_id, tableInfo);
                }
            } else {
                try {
                    tableInfo.add(Dbo.db());
                    if (tableColumnObj != null && tableColumnObj.containsKey(tableInfo.getTable_name())) {
                        tableColumnList = JsonUtil.toObject(JsonUtil.toJson(tableColumnObj.get(tableInfo.getTable_name())), new TypeReference<List<TableColumn>>() {
                        });
                        updateTableColumn(tableInfo.getTable_id(), tableColumnList);
                    }
                    long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DataExtractionDef.TableName + " WHERE table_id = ?", tableInfo.getTable_id()).orElseThrow(() -> new BusinessException("SQL错误"));
                    if (countNum == 0) {
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
                    if (!collect_type.equals(CollectType.ShuJuKuCaiJi.getCode())) {
                        saveStorageData(source_id, agent_id, databaseId, dsl_id, tableInfo);
                    }
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            }
        }
    }

    private void saveTableInfo(long databaseId, TableInfo tableInfo, Object collect_type) {
        checklistInformation(tableInfo);
        tableInfo.setTable_id(PrimayKeyGener.getNextId());
        tableInfo.setDatabase_id(databaseId);
        tableInfo.setValid_s_date(DateUtil.getSysDate());
        tableInfo.setValid_e_date(Constant._MAX_DATE_8);
        tableInfo.setIs_md5(IsFlag.Fou.getCode());
        tableInfo.setIs_register(IsFlag.Fou.getCode());
        tableInfo.setIs_customize_sql(IsFlag.Fou.getCode());
        tableInfo.setIs_parallel(IsFlag.Fou.getCode());
        tableInfo.setIs_user_defined(IsFlag.Fou.getCode());
        tableInfo.setTi_or(Constant.DEFAULT_TABLE_CLEAN_ORDER.toString());
        tableInfo.setRec_num_date(DateUtil.getSysDate());
        tableInfo.add(Dbo.db());
        if (collect_type.equals(CollectType.ShuJuKuCaiJi.getCode())) {
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
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableColumns", desc = "", range = "", isBean = true)
    @Param(name = "table_id", desc = "", range = "")
    void updateTableColumn(long table_id, List<TableColumn> tableColumnList) {
        Dbo.execute("DELETE FROM " + TableColumn.TableName + " WHERE table_id = ?", table_id);
        tableColumnList.forEach(table_column -> {
            if (table_column.getColumn_id() == null) {
                setTableColumnDefaultData(table_id, table_column);
            }
            checkColumnInformation(table_column);
            table_column.add(Dbo.db());
        });
    }

    Map<String, Object> getClassifyAndSourceNum(long database_id) {
        return Dbo.queryOneObject("SELECT t2.classify_num,t4.datasource_number FROM " + DatabaseSet.TableName + " t1 JOIN " + CollectJobClassify.TableName + " t2 ON t1.classify_id = t2.classify_id JOIN " + AgentInfo.TableName + " t3 ON t1.agent_id = t3.agent_id JOIN " + DataSource.TableName + " t4 ON t3.source_id = t4.source_id WHERE t1.database_id = ?", database_id);
    }
}
