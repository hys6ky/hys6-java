package hyren.serv6.h.market.dmjobtable;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.market.dmmoduletable.DmModuleTableService;
import hyren.serv6.h.market.dmtaskinfo.DmTaskInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DmJobTableInfoService {

    private static final String[] NO_LENGTH_COLUMN_TYPES = { "string", "text", "bigint" };

    public List<Map<String, Object>> findDmJobTableInfoByTaskId(String task_id) {
        return Dbo.queryList("select * from " + DmJobTableInfo.TableName + " where task_id = ?", Long.parseLong(task_id));
    }

    public List<Map<String, Object>> getDataBySQL(String querysql, String sqlparameter) {
        try {
            List<Map<String, Object>> dataBySQL_rs = new ArrayList<>();
            querysql = StringUtil.replace(querysql, "  ", Constant.SPACE);
            querysql = StringUtil.replace(querysql, "\r\n", Constant.SPACE);
            querysql = StringUtil.replace(querysql, "\n", Constant.SPACE);
            querysql = StringUtil.replace(querysql, "\r", Constant.SPACE);
            DruidParseQuerySql dpqs = new DruidParseQuerySql();
            querysql = dpqs.GetNewSql(querysql);
            dpqs.getBloodRelationMap(querysql);
            if (!StringUtil.isEmpty(sqlparameter)) {
                List<String> sql_param_s = StringUtil.split(sqlparameter, ";");
                for (String sql_param : sql_param_s) {
                    List<String> param_s = StringUtil.split(sql_param, "=");
                    if (param_s.size() > 1) {
                        querysql = StringUtil.replace(querysql, "#{" + param_s.get(0).trim() + "}", param_s.get(1));
                    }
                }
            }
            querysql = querysql.trim();
            if (querysql.endsWith(";")) {
                querysql = querysql.substring(0, querysql.length() - 1);
            }
            try (DatabaseWrapper db = new DatabaseWrapper()) {
                new ProcessingData() {

                    @Override
                    public void dealLine(Map<String, Object> map) {
                        dataBySQL_rs.add(map);
                    }
                }.getPageDataLayer(querysql, db, 1, 10);
            }
            return dataBySQL_rs;
        } catch (Exception e) {
            throw new BusinessException("根据sql预览数据发生异常! " + e);
        }
    }

    public Map<String, Object> getColumnBySql(String querysql, String datatable_id, String sqlparameter) {
        Map<String, Object> resultmap = new HashMap<>();
        List<Map<String, Object>> resultlist = new ArrayList<>();
        if (StringUtil.isBlank(querysql)) {
            throw new BusinessException(" query sql is null.");
        }
        long datatableId;
        try {
            datatableId = Long.parseLong(datatable_id);
        } catch (NumberFormatException e) {
            throw new BusinessException(" datatable_id is number format ." + e);
        }
        String storeType;
        Long dsl_id;
        List<Map<String, Object>> storeTypeList = Dbo.queryList("select t1.store_type,t1.dsl_id from " + DataStoreLayer.TableName + " t1 left join " + DtabRelationStore.TableName + " t2 on t1.dsl_id = t2.dsl_id " + "where t2.tab_id = ? and t2.data_source = ?", datatableId, StoreLayerDataSource.DM.getCode());
        if (storeTypeList.isEmpty() || storeTypeList.get(0).get("store_type") == null || storeTypeList.get(0).get("dsl_id") == null) {
            throw new BusinessException("查询当前加工存储目的地错误，请检查");
        }
        try {
            storeType = storeTypeList.get(0).get("store_type").toString();
            dsl_id = Long.parseLong(storeTypeList.get(0).get("dsl_id").toString());
        } catch (BusinessException | NumberFormatException e) {
            throw new BusinessException("dsl_id is number format ..." + e);
        }
        String field_type = getDefaultFieldType(storeType);
        List<String> columnNameList = new ArrayList<>();
        HashMap<String, Object> bloodRelationMap = new HashMap<>();
        try {
            querysql = StringUtil.replace(querysql, "  ", Constant.SPACE);
            DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(querysql);
            columnNameList = druidParseQuerySql.parseSelectAliasField();
            DruidParseQuerySql dpqs = new DruidParseQuerySql();
            bloodRelationMap = dpqs.getBloodRelationMap(querysql);
        } catch (Exception e) {
            if (!StringUtil.isEmpty(e.getMessage())) {
                log.error(e.getMessage());
                throw e;
            } else {
                getDataBySQL(querysql, sqlparameter);
            }
        }
        String targetfield_type;
        String field_length = "100";
        List<Map<String, Object>> columnlist = new ArrayList<>();
        for (int i = 0; i < columnNameList.size(); i++) {
            String everyColumnName = columnNameList.get(i);
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("field_en_name", everyColumnName);
            map.put("field_cn_name", everyColumnName);
            map.put("process_mapping", everyColumnName);
            map.put("field_seq", i);
            map.put("group_mapping", "");
            Object object = bloodRelationMap.get(everyColumnName);
            if (null == object) {
                targetfield_type = field_type;
            } else {
                @SuppressWarnings("unchecked")
                ArrayList<HashMap<String, Object>> list = (ArrayList<HashMap<String, Object>>) object;
                if (list.size() == 1) {
                    HashMap<String, Object> stringObjectHashMap = list.get(0);
                    String sourcetable = stringObjectHashMap.get(DruidParseQuerySql.sourcetable).toString();
                    String sourcecolumn = stringObjectHashMap.get(DruidParseQuerySql.sourcecolumn).toString();
                    Map<String, String> fieldType = getFieldType(sourcetable, sourcecolumn, field_type, dsl_id);
                    targetfield_type = fieldType.get("targettype");
                    field_length = fieldType.get("field_length");
                    if (field_length == null || Arrays.asList(NO_LENGTH_COLUMN_TYPES).contains(targetfield_type)) {
                        field_length = StringUtil.EMPTY;
                    }
                } else {
                    targetfield_type = field_type;
                }
            }
            map.put("field_type", targetfield_type);
            map.put("field_length", field_length);
            List<Map<String, Object>> dslaStorelayerList = Dbo.queryList("select dslad_id,dsla_storelayer from " + DataStoreLayerAdded.TableName + " t1 " + "left join " + DtabRelationStore.TableName + " t2 on t1.dsl_id = t2.dsl_id " + "where t2.tab_id = ? and t2.data_source = ? order by dsla_storelayer", datatableId, StoreLayerDataSource.DM.getCode());
            for (Map<String, Object> dslaStorelayeMap : dslaStorelayerList) {
                map.put(StoreLayerAdded.ofValueByCode(dslaStorelayeMap.get("dsla_storelayer").toString()), false);
            }
            resultlist.add(map);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("value", columnNameList.get(i));
            resultMap.put("code", i);
            columnlist.add(resultMap);
        }
        resultmap.put("result", resultlist);
        resultmap.put("columnlist", columnlist);
        return resultmap;
    }

    private Map<String, String> getFieldType(String sourcetable, String sourcecolumn, String field_type, Long dsl_id) {
        Map<String, String> resultmap = new HashMap<>();
        List<LayerBean> layerByTable;
        try {
            layerByTable = ProcessingData.getLayerByTable(sourcetable, Dbo.db());
        } catch (Exception e) {
            resultmap.put("sourcetype", field_type);
            resultmap.put("targettype", field_type);
            return resultmap;
        }
        if (layerByTable.isEmpty()) {
            resultmap.put("sourcetype", field_type);
            resultmap.put("targettype", field_type);
            return resultmap;
        } else {
            String dataSourceType = layerByTable.get(0).getDst();
            if (dataSourceType.equals(DataSourceType.DCL.getCode())) {
                List<Map<String, Object>> maps = Dbo.queryList("select t2.column_type,t4.dsl_id from " + TableStorageInfo.TableName + " t1 left join " + TableColumn.TableName + " t2 on t1.table_id = t2.table_id" + " left join " + TableStorageInfo.TableName + " t3 on t1.table_id = t3.table_id join " + DtabRelationStore.TableName + " t4 on t4.tab_id = t3.storage_id " + "where lower(t2.column_name) = ? and lower(t1.hyren_name) = ? ", sourcecolumn.toLowerCase(), sourcetable.toLowerCase());
                if (maps.isEmpty()) {
                    resultmap.put("sourcetype", field_type);
                    resultmap.put("targettype", field_type);
                } else {
                    String column_type = maps.get(0).get("column_type").toString();
                    Long DCLdsl_id = Long.parseLong(maps.get(0).get("dsl_id").toString());
                    resultmap.put("sourcetype", column_type.toLowerCase());
                    if (column_type.contains("(") && column_type.contains(")") && column_type.indexOf("(") < column_type.indexOf(")")) {
                        String field_length = column_type.substring(column_type.indexOf("(") + 1, column_type.indexOf(")"));
                        resultmap.put("field_length", field_length);
                    }
                    column_type = transFormColumnType(column_type, DCLdsl_id);
                    column_type = transFormColumnType(column_type, dsl_id);
                    resultmap.put("targettype", column_type.toLowerCase());
                }
                return resultmap;
            } else if (dataSourceType.equals(DataSourceType.DML.getCode())) {
                List<Map<String, Object>> maps = Dbo.queryList("select field_length,field_type from " + DmModuleTableFieldInfo.TableName + " t1 left join " + DmModuleTable.TableName + " t2 on t1.module_table_id = t2.module_table_id where lower(t2.module_table_en_name) = ? and lower(t1.field_en_name) = ?", sourcetable.toLowerCase(), sourcecolumn.toLowerCase());
                if (maps.isEmpty()) {
                    resultmap.put("sourcetype", field_type);
                    resultmap.put("targettype", field_type);
                } else {
                    String DMLfield_type = maps.get(0).get("field_type").toString();
                    if (maps.get(0).get("field_length") == null) {
                        resultmap.put("field_length", null);
                    } else {
                        resultmap.put("field_length", maps.get(0).get("field_length").toString());
                        DMLfield_type = DMLfield_type + "(" + maps.get(0).get("field_length").toString() + ")";
                    }
                    resultmap.put("sourcetype", DMLfield_type.toLowerCase());
                    DMLfield_type = transFormColumnType(DMLfield_type, dsl_id);
                    resultmap.put("targettype", DMLfield_type.toLowerCase());
                }
                return resultmap;
            } else {
                resultmap.put("sourcetype", field_type);
                resultmap.put("targettype", field_type);
                return resultmap;
            }
        }
    }

    private String transFormColumnType(String column_type, Long dsl_id) {
        if (dsl_id == null) {
            return column_type;
        }
        column_type = column_type.toLowerCase();
        if (column_type.contains("(")) {
            column_type = column_type.substring(0, column_type.indexOf("("));
        }
        DataStoreLayer data_store_layer = new DataStoreLayer();
        data_store_layer.setDsl_id(dsl_id);
        List<String> type_contrasts = Dbo.queryOneColumnList("select distinct case" + " when position('(' in t1.target_type) != 0" + " 	then lower(SUBSTR(t1.target_type, 0, position('(' in t1.target_type)))" + " else LOWER(t1.target_type) end as target_type" + " from (" + "	select *, dtm.database_type2 as target_type" + " 	from " + DatabaseTypeMapping.TableName + " dtm" + " 		inner join " + DatabaseInfo.TableName + " di on upper(dtm.database_name2) = upper(di.database_name)" + "		inner join " + DataStoreLayer.TableName + " dsl on upper(di.database_name) = upper(dsl.database_name)" + " 	where upper(substr(dtm.database_type1, 0, position('(' in dtm.database_type1))) = upper(?)" + "	union all" + "	select *, dtm.database_type1 as target_type" + " 	from " + DatabaseTypeMapping.TableName + " dtm" + "		inner join " + DatabaseInfo.TableName + " di on upper(dtm.database_name1) = upper(di.database_name)" + "		inner join " + DataStoreLayer.TableName + " dsl on upper(di.database_name) = upper(dsl.database_name)" + "	where upper(substr(dtm.database_type2, 0, position('(' in dtm.database_type2))) = upper(?)" + " ) as t1" + " where t1.dsl_id = ?", column_type, column_type, data_store_layer.getDsl_id());
        log.error(column_type + "\t" + data_store_layer.getDsl_id());
        if (type_contrasts.isEmpty()) {
            return column_type;
        } else {
            if (type_contrasts.contains(column_type)) {
                return column_type;
            } else {
                String target_type = type_contrasts.get(0);
                target_type = target_type.toLowerCase();
                if (target_type.contains("(")) {
                    target_type = target_type.substring(0, target_type.indexOf("("));
                }
                return target_type;
            }
        }
    }

    private String getDefaultFieldType(String storeType) {
        String field_type = "";
        if (storeType.equals(Store_type.DATABASE.getCode())) {
            field_type = "varchar";
        } else if (storeType.equals(Store_type.HIVE.getCode())) {
            field_type = "string";
        } else if (storeType.equals(Store_type.HBASE.getCode())) {
            field_type = "string";
        }
        return field_type;
    }

    @Autowired
    DmModuleTableService dmModuleTableService;

    public List<DmJobTableInfo> findJobs(String taskId) {
        return Dbo.queryList(DmJobTableInfo.class, " select * from " + DmJobTableInfo.TableName + " where task_id = ?", Long.parseLong(taskId));
    }

    @Autowired
    DmTaskInfoService dmTaskInfoService;

    public List<DmJobTableInfo> findJobsByModuleTableId(Long module_table_id) {
        List<DmJobTableInfo> jobTableInfos = new ArrayList<>();
        List<DmTaskInfo> dmTaskInfos = dmTaskInfoService.findDmTaskInfosByTableId(module_table_id);
        for (DmTaskInfo dmTaskInfo : dmTaskInfos) {
            jobTableInfos.addAll(findJobs(dmTaskInfo.getTask_id().toString()));
        }
        return jobTableInfos;
    }

    public boolean delJobTableByJobTableId(Long jobTableId) {
        try {
            Dbo.execute(" delete from " + DmJobTableInfo.TableName + " where jobtab_id = ?", jobTableId);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean delJobFieldByJobTableId(Long jobTableId) {
        try {
            Dbo.execute(" delete from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ?", jobTableId);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean checkTableName(String tableName, String moduleTableId) {
        Optional<DmJobTableInfo> dmJobTableInfo = Dbo.queryOneObject(DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where jobtab_en_name = ? and module_table_id = ?", tableName, Long.parseLong(moduleTableId));
        if (dmJobTableInfo.isPresent()) {
            throw new BusinessException(" 该表名已在此模型表任务下存在，请重新编辑");
        }
        return true;
    }

    public DmJobTableInfo findDmTaskDataTableByTaskId(String jobtabId) {
        return Dbo.queryOneObject(DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where jobtab_id = ?", Long.parseLong(jobtabId)).orElseThrow(() -> new BusinessException("data failed.."));
    }

    public List<DmJobTableFieldInfo> findFieldsByJobTabId(String jobtabId) {
        return Dbo.queryList(DmJobTableFieldInfo.class, "select * from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ?", Long.parseLong(jobtabId));
    }
}
