package hyren.serv6.b.batchcollection.agent.stodestconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.agent.bean.ColStoParam;
import hyren.serv6.b.agent.bean.DataStoRelaParam;
import hyren.serv6.b.agent.bean.StoreConnectionBean;
import hyren.serv6.b.batchcollection.agent.dbconf.DBConfStepService;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.datastorage.dcl.LengthMapping;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Api("定义存储目的地配置")
@Service
@DocClass(desc = "", author = "WangZhengcheng")
public class StoDestStepService {

    @Autowired
    DBConfStepService dbConfStepService;

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getInitInfo(long colSetId) {
        Result result = Dbo.queryResult(" select ti.table_id, ti.table_name, ti.table_ch_name, ti.unload_type, tsi.is_zipper, tsi.storage_type," + " tsi.storage_time,ded.data_extract_type,'0' as destflag,tsi.hyren_name,tsi.is_prefix" + " ,tsi.is_md5, ded.is_archived from " + TableInfo.TableName + " ti" + " left join " + DatabaseSet.TableName + " ds on ti.database_id = ds.database_id" + " left join " + TableStorageInfo.TableName + " tsi on ti.table_id = tsi.table_id" + " left join " + DataExtractionDef.TableName + " ded on ti.table_id = ded.table_id" + " where ti.database_id = ? ORDER BY ti.table_name", colSetId);
        List<Object> list = Dbo.queryOneColumnList("select table_id from " + TableInfo.TableName + " where database_id = ?", colSetId);
        if (list.isEmpty()) {
            throw new BusinessException("未获取到数据库采集表");
        }
        for (Object obj : list) {
            Long tableIdFromTI = (Long) obj;
            for (int j = 0; j < result.getRowCount(); j++) {
                long tableIdFromResult = result.getLong(j, "table_id");
                if (tableIdFromTI.equals(tableIdFromResult)) {
                    long count = Dbo.queryNumber("select count(1) from " + TableStorageInfo.TableName + " where table_id = ?", tableIdFromTI).orElseThrow(() -> new BusinessException("SQL查询错误"));
                    if (count > 0) {
                        result.setObject(j, "destflag", IsFlag.Shi.getCode());
                    }
                }
                if (StringUtil.isBlank(result.getString(j, "hyren_name"))) {
                    result.setObject(j, "is_prefix", IsFlag.Shi.getCode());
                }
            }
        }
        Map<String, Object> collectMapData = Dbo.queryOneObject("SELECT t3.datasource_number,t4.classify_num FROM " + DatabaseSet.TableName + "" + " t1 JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id" + " JOIN " + DataSource.TableName + " t3 ON t2.source_id = t3.source_id" + " join " + CollectJobClassify.TableName + " t4 on t1.classify_id = t4.classify_id" + " WHERE t1.database_id = ?", colSetId);
        collectMapData.put("storageTableData", result.toList());
        return collectMapData;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTbStoDestByColSetId(long colSetId) {
        List<Object> tableIds = Dbo.queryOneColumnList("select ti.table_id from " + TableInfo.TableName + " ti" + " join " + DataExtractionDef.TableName + " ded on ti.table_id = ded.table_id" + " where ti.database_id = ? and ded.data_extract_type = ? ORDER BY ti.table_name", colSetId, DataExtractType.YuanShuJuGeShi.getCode());
        if (tableIds.isEmpty()) {
            throw new BusinessException("未获取到数据库采集表");
        }
        List<Map<String, Object>> returnList = new ArrayList<>();
        for (Object tableId : tableIds) {
            Map<String, Object> returnMap = new LinkedHashMap<>();
            List<Object> list = Dbo.queryOneColumnList("SELECT drt.dsl_id FROM " + DtabRelationStore.TableName + " drt" + " WHERE drt.tab_id = (" + "   SELECT storage_id FROM " + TableStorageInfo.TableName + "   WHERE table_id = ?" + " ) AND drt.data_source = ?", tableId, StoreLayerDataSource.DB.getCode());
            returnMap.put("tableId", tableId);
            returnMap.put("dslIds", list);
            if (list.isEmpty()) {
                returnMap.put("hyren_name", "");
                returnMap.put("storage_type", StorageType.TiHuan.getCode());
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append("select t1.hyren_name,t1.storage_type,t1.is_md5 from " + TableStorageInfo.TableName + " t1 join " + DtabRelationStore.TableName + " t2 on t1.storage_id = t2.tab_id where t1.table_id = ").append(tableId).append(" AND t2.dsl_id in (");
                for (Object obj : list) {
                    sb.append(obj).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(") LIMIT 1");
                Map<String, Object> map = Dbo.queryOneObject(sb.toString());
                if (map.isEmpty()) {
                    returnMap.put("hyren_name", "");
                    returnMap.put("storage_type", StorageType.TiHuan.getCode());
                    returnMap.put("is_md5", IsFlag.Fou.getCode());
                } else {
                    returnMap.put("hyren_name", map.get("hyren_name"));
                    returnMap.put("storage_type", map.get("storage_type"));
                    returnMap.put("is_md5", map.get("is_md5"));
                }
            }
            returnList.add(returnMap);
        }
        return returnList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dslId", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getStoDestDetail(long dslId) {
        return AesUtil.encrypt(Dbo.queryResult(" select t1.storage_property_key, t1.storage_property_val,t2.store_type " + " FROM " + DataStoreLayerAttr.TableName + " t1 JOIN " + DataStoreLayer.TableName + " t2 ON t1.dsl_id = t2.dsl_id  WHERE t1.dsl_id = ?", dslId).toJSON());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getStoDestForOnlyExtract(long tableId) {
        long count = Dbo.queryNumber("select count(1) from " + DataExtractionDef.TableName + " where table_id = ? and data_extract_type = ?", tableId, DataExtractType.ShuJuKuChouQuLuoDi.getCode()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("获取该表数据抽取信息异常");
        }
        return Dbo.queryResult("select plane_url from " + DataExtractionDef.TableName + " where table_id = ? and data_extract_type = ?", tableId, DataExtractType.ShuJuKuChouQuLuoDi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Param(name = "stoDest", desc = "", range = "")
    public void saveStoDestForOnlyExtract(long tableId, String stoDest) {
        long count = Dbo.queryNumber("select count(1) from " + DataExtractionDef.TableName + " where table_id = ? and data_extract_type = ?", tableId, DataExtractType.ShuJuKuChouQuLuoDi.getCode()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("获取该表数据抽取信息异常");
        }
        DboExecute.updatesOrThrow("保存存储目的地失败", "update " + DataExtractionDef.TableName + " set plane_url = ? where table_id = ?", stoDest, tableId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getStoDestByTableId(long tableId) {
        Result storageData = getStorageDataByTarget();
        Map<String, Object> resultMap = new HashMap<>();
        Result tbStoRela = Dbo.queryResult("SELECT t2.dsl_id,t1.hyren_name FROM " + TableStorageInfo.TableName + " t1" + " JOIN " + DtabRelationStore.TableName + " t2 ON  t1.storage_id = t2.tab_id" + " WHERE t1.table_id = ? AND t2.data_source = ?", tableId, StoreLayerDataSource.DB.getCode());
        if (tbStoRela.isEmpty()) {
            resultMap.put("hyren_name", "");
            resultMap.put("tableStorage", storageData.toList());
            return resultMap;
        }
        for (int i = 0; i < tbStoRela.getRowCount(); i++) {
            long dslId = tbStoRela.getLong(i, "dsl_id");
            for (int j = 0; j < storageData.getRowCount(); j++) {
                long dslIdFromResult = storageData.getLong(j, "dsl_id");
                if (dslId == dslIdFromResult) {
                    storageData.setObject(j, "usedflag", IsFlag.Shi.getCode());
                    resultMap.put("hyren_name", tbStoRela.getString(i, "hyren_name"));
                }
            }
        }
        resultMap.put("tableStorage", storageData.toList());
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getStorageData() {
        Result result = Dbo.queryResult("SELECT dsl_id, dsl_name, store_type, '0' as usedflag FROM " + DataStoreLayer.TableName);
        if (result.isEmpty()) {
            throw new BusinessException("系统中未定义存储目的地信息，请联系管理员");
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            Store_type storeType = Store_type.ofEnumByCode(result.getString(i, "store_type"));
            if (storeType == Store_type.DATABASE) {
                Map<String, Object> map = Dbo.queryOneObject("SELECT storage_property_val FROM " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND dsl_id = ?", StorageTypeKey.database_type, result.getLong(i, "dsl_id"));
                if (Objects.isNull(map.get("storage_property_val"))) {
                    throw new BusinessException("storage_property_key and dsl_id find storage_property_val is null.");
                }
                result.setObject(i, "store_name", map.get("storage_property_val").toString());
            } else {
                result.setObject(i, "store_name", storeType.getValue());
            }
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dslId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, String> getColumnHeader(long dslId) {
        Map<String, String> header = new HashMap<>();
        header.put("column_name", "列名");
        header.put("column_ch_name", "列中文名");
        List<Object> list = Dbo.queryOneColumnList("select dsla_storelayer from " + DataStoreLayerAdded.TableName + " where dsl_id = ?", dslId);
        if (!list.isEmpty()) {
            for (Object obj : list) {
                header.put(StoreLayerAdded.ofValueByCode((String) obj), StoreLayerAdded.ofValueByCode((String) obj));
            }
        }
        return header;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dslId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Long> getDataStoreLayerAddedId(long dslId) {
        Map<String, Long> storeAddedId = new HashMap<>();
        Result result = Dbo.queryResult("select dslad_id, dsla_storelayer from " + DataStoreLayerAdded.TableName + " where dsl_id = ?", dslId);
        if (result.isEmpty()) {
            return storeAddedId;
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            storeAddedId.put(result.getString(i, "dsla_storelayer"), result.getLong(i, "dslad_id"));
        }
        return storeAddedId;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Param(name = "dslId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getColumnStoInfo(long tableId, long dslId) {
        Result resultOne = Dbo.queryResult("select column_id, column_name, column_ch_name, is_primary_key,column_type " + " from " + TableColumn.TableName + " where table_id = ? and is_get = ? order by column_id", tableId, IsFlag.Shi.getCode());
        if (resultOne.isEmpty()) {
            throw new BusinessException("未找到属于该表的字段");
        }
        for (int i = 0; i < resultOne.getRowCount(); i++) {
            long column_id = resultOne.getLong(i, "column_id");
            Map<String, Object> tbcol_srctgt_map = Dbo.queryOneObject("select column_tar_type from " + TbcolSrctgtMap.TableName + " where dsl_id = ? and column_id = ?", dslId, column_id);
            if (!tbcol_srctgt_map.isEmpty()) {
                resultOne.setObject(i, "column_tar_type", tbcol_srctgt_map.get("column_tar_type"));
            }
        }
        setColumnTypeContrast(tableId, dslId, resultOne);
        DatabaseSet database_set = Dbo.queryOneObject(DatabaseSet.class, "select * from " + DatabaseSet.TableName + " where database_id = " + "(select database_id from " + TableInfo.TableName + " where table_id = ?)", tableId).orElseThrow(() -> new BusinessException("sql查询错误或查询获取任务发送状态失败"));
        if (IsFlag.Fou == IsFlag.ofEnumByCode(database_set.getIs_sendok())) {
            Result resultTwo = Dbo.queryResult("select dsla.dsla_storelayer from " + DataStoreLayerAdded.TableName + " dsla" + " join " + DataStoreLayer.TableName + " dsl on dsla.dsl_id = dsl.dsl_id" + " where dsl.dsl_id = ?", dslId);
            if (!resultTwo.isEmpty()) {
                for (int i = 0; i < resultTwo.getRowCount(); i++) {
                    for (int j = 0; j < resultOne.getRowCount(); j++) {
                        StoreLayerAdded dsla_storelayer = StoreLayerAdded.ofEnumByCode(resultTwo.getString(i, "dsla_storelayer"));
                        if (StoreLayerAdded.ZhuJian == dsla_storelayer) {
                            resultOne.setObject(j, dsla_storelayer.getValue(), resultOne.getString(j, "is_primary_key"));
                        } else {
                            resultOne.setObject(j, dsla_storelayer.getValue(), IsFlag.Fou.getCode());
                        }
                    }
                }
            } else {
                return resultOne;
            }
        }
        Result resultThree = Dbo.queryResult("select csi.col_id column_id, dsla.dsla_storelayer, csi_number" + " from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla on dsla.dslad_id = csi.dslad_id" + " where csi.col_id in (select column_id from " + TableColumn.TableName + " where table_id = ?)" + " and dsla.dsl_id = ? AND  csi.data_source = ? ", tableId, dslId, StoreLayerDataSource.DB.getCode());
        for (int i = 0; i < resultThree.getRowCount(); i++) {
            long columnIdFromCSI = resultThree.getLong(i, "column_id");
            for (int j = 0; j < resultOne.getRowCount(); j++) {
                long columnIdFromTC = resultOne.getLong(j, "column_id");
                if (columnIdFromCSI == columnIdFromTC) {
                    resultOne.setObject(j, StoreLayerAdded.ofValueByCode(resultThree.getString(i, "dsla_storelayer")), IsFlag.Shi.getCode());
                    resultOne.setObject(j, "csi_number", resultThree.getLong(i, "csi_number"));
                }
            }
        }
        return resultOne;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Param(name = "dslId", desc = "", range = "")
    @Param(name = "resultOne", desc = "", range = "")
    @Return(desc = "", range = "")
    private void setColumnTypeContrast(long tableId, long dslId, Result resultOne) {
        String database_name1 = getDatabase_name1(tableId);
        Map<String, Object> databaseInfoMap = Dbo.queryOneObject(Dbo.db(), "select * from " + DatabaseInfo.TableName + " where upper(database_name) like ?", "%" + database_name1.toUpperCase() + "%");
        if (databaseInfoMap.isEmpty()) {
            throw new BusinessException(database_name1 + "数据库类型未定义在表" + DatabaseInfo.TableName + "中，请联系管理员！！！");
        }
        String database_name2 = getDatabase_name2(dslId);
        log.info("从" + database_name1 + "到" + database_name2);
        if (!database_name1.equalsIgnoreCase(database_name2)) {
            List<Map<String, Object>> columnTypeMapping = Dbo.queryList(Dbo.db(), "select upper(database_type1) as database_type1,upper(database_type2) as database_type2,is_default" + " from " + DatabaseTypeMapping.TableName + " where lower(database_name1) = lower(?) and lower(database_name2) = lower(?)" + " UNION ALL" + " select upper(database_type2) as database_type1,upper(database_type1) as database_type2,is_default" + " from " + DatabaseTypeMapping.TableName + " where lower(database_name1) = lower(?) and lower(database_name2) = lower(?)", database_name1, database_name2, database_name2, database_name1).stream().distinct().collect(Collectors.toList());
            Map<String, Map<String, Object>> defaultLengthMap = LengthMapping.getDefaultLengthMap();
            Map<String, Object> typeLengthMap = new HashMap<>();
            if (!defaultLengthMap.isEmpty()) {
                typeLengthMap = defaultLengthMap.get(database_name1.toUpperCase());
            }
            log.info("========typeLengthMap=======" + JsonUtil.toJson(typeLengthMap));
            for (int i = 0; i < resultOne.getRowCount(); i++) {
                String columnType = resultOne.getString(i, "column_type");
                String column_name = resultOne.getString(i, "column_name");
                String len = "";
                if (columnType.contains(Constant.LXKH) && columnType.contains(Constant.RXKH)) {
                    len = columnType.substring(columnType.indexOf(Constant.LXKH) + 1, columnType.indexOf(Constant.RXKH));
                    if (StringUtil.isNotBlank(columnType.substring(columnType.indexOf(Constant.RXKH) + 1))) {
                        columnType = columnType.substring(0, columnType.indexOf(Constant.LXKH) + 1) + len + Constant.RXKH + columnType.substring(columnType.indexOf(Constant.RXKH) + 1);
                    } else {
                        columnType = columnType.substring(0, columnType.indexOf(Constant.LXKH));
                    }
                }
                List<String> columnTypeMappingList = new ArrayList<>();
                for (Map<String, Object> map : columnTypeMapping) {
                    String database_type1 = map.get("database_type1").toString();
                    if (database_type1.contains(Constant.LXKH) && database_type1.contains(Constant.RXKH)) {
                        database_type1 = database_type1.substring(0, database_type1.indexOf(Constant.LXKH));
                    }
                    String database_type2 = map.get("database_type2").toString();
                    if (database_type1.equalsIgnoreCase(columnType)) {
                        if (database_type2.contains(Constant.LXKH) && database_type2.contains(Constant.RXKH)) {
                            String begin = database_type2.substring(0, database_type2.indexOf(Constant.LXKH) + 1);
                            String end = database_type2.substring(database_type2.indexOf(Constant.LXKH) + 1);
                            String mapping_len = database_type2.substring(database_type2.indexOf(Constant.LXKH) + 1, database_type2.indexOf(Constant.RXKH));
                            if (StringUtil.isNotBlank(mapping_len)) {
                                throw new BusinessException("数据类型映射表不可以配置精确长度:" + database_type2);
                            }
                            if (StringUtil.isNotBlank(len)) {
                                database_type2 = begin + len + end;
                            } else {
                                if (null != typeLengthMap && !typeLengthMap.isEmpty() && null != typeLengthMap.get(database_type1)) {
                                    database_type2 = begin + typeLengthMap.get(database_type1) + end;
                                } else {
                                    throw new BusinessException("列名为：" + column_name + "，列字段类型为" + database_type1 + "没有配置默认字段长度，请联系管理员配置！！！");
                                }
                            }
                        }
                        columnTypeMappingList.add(database_type2);
                        String defaultColumnTarType = getDefaultColumnTarType(columnTypeMapping, database_type1);
                        if (database_type2.contains(Constant.LXKH) && database_type2.contains(Constant.RXKH)) {
                            String substring = database_type2.substring(0, database_type2.indexOf(Constant.LXKH) + 1) + Constant.RXKH;
                            if (StringUtil.isNotBlank(defaultColumnTarType) && substring.equalsIgnoreCase(defaultColumnTarType)) {
                                resultOne.setObject(i, "column_tar_type", database_type2);
                            }
                        }
                    }
                }
                resultOne.setObject(i, "column_type_mapping", columnTypeMappingList);
            }
        }
    }

    private String getDefaultColumnTarType(List<Map<String, Object>> columnTypeMapping, String database_type1) {
        Map<Object, List<Map<String, Object>>> databaseTypeMap = columnTypeMapping.stream().collect(Collectors.groupingBy(map -> map.get("database_type1")));
        for (Map.Entry<Object, List<Map<String, Object>>> entry : databaseTypeMap.entrySet()) {
            String key = entry.getKey().toString();
            if (key.contains(Constant.LXKH) && key.contains(Constant.RXKH)) {
                key = key.substring(0, key.indexOf(Constant.LXKH));
            }
            if (key.equals(database_type1)) {
                List<Map<String, Object>> mapList = entry.getValue();
                if (mapList.size() > 1) {
                    Map<String, List<Map<String, Object>>> isDefaultMap = mapList.stream().collect(Collectors.groupingBy(map -> map.get("is_default").toString()));
                    for (Map.Entry<String, List<Map<String, Object>>> listEntry : isDefaultMap.entrySet()) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(listEntry.getKey())) {
                            List<Map<String, Object>> list = listEntry.getValue();
                            if (list.size() == 1) {
                                return list.get(0).get("database_type2").toString();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private String getDatabase_name2(long dslId) {
        DataStoreLayer data_store_layer = Dbo.queryOneObject(DataStoreLayer.class, "select store_type from " + DataStoreLayer.TableName + " where dsl_id = ?", dslId).orElseThrow(() -> new BusinessException("sql查询错误"));
        String store_type = data_store_layer.getStore_type();
        String database_name2 = Store_type.ofValueByCode(store_type);
        if (Store_type.ofEnumByCode(store_type) == Store_type.DATABASE) {
            Map<String, Object> map = Dbo.queryOneObject("SELECT storage_property_val FROM " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND dsl_id = ?", StorageTypeKey.database_type, dslId);
            database_name2 = map.get("storage_property_val").toString();
        }
        return database_name2;
    }

    private String getDatabase_name1(long tableId) {
        TableInfo table_info = Dbo.queryOneObject(TableInfo.class, "select * FROM " + TableInfo.TableName + " where table_id = ?", tableId).orElseThrow(() -> new BusinessException("sql查询错误！"));
        String database_name1 = table_info.getDatabase_type();
        if (StringUtil.isBlank(database_name1)) {
            DatabaseSet database_set = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, "SELECT dsl_id from " + DatabaseSet.TableName + " where database_id = ?", table_info.getDatabase_id()).orElseThrow(() -> new BusinessException("sql查询错误"));
            StoreConnectionBean storeConnectionBean = dbConfStepService.setStoreConnectionBean(database_set.getDsl_id());
            database_name1 = storeConnectionBean.getDatabase_type();
        }
        return database_name1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colStoInfoString", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "tableId", desc = "", range = "")
    public void saveColStoInfo(String colStoInfoString, long tableId) {
        List<ColStoParam> colStoParams = JsonUtil.toObject(colStoInfoString, new TypeReference<List<ColStoParam>>() {
        });
        Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id in (select column_id from " + TableColumn.TableName + " where table_id = ? )" + " AND data_source = ?", tableId, StoreLayerDataSource.DB.getCode());
        if (!colStoParams.isEmpty()) {
            for (ColStoParam param : colStoParams) {
                Long columnId = param.getColumnId();
                Long[] dsladIds = param.getDsladIds();
                if (dsladIds == null || !(dsladIds.length > 0)) {
                    throw new BusinessException("请检查配置信息，并为待保存的字段选择其是否具有特殊性质");
                }
                for (long dsladId : dsladIds) {
                    DcolRelationStore columnStorageInfo = new DcolRelationStore();
                    columnStorageInfo.setCol_id(columnId);
                    columnStorageInfo.setDslad_id(dsladId);
                    columnStorageInfo.setData_source(StoreLayerDataSource.DB.getCode());
                    List<Object> list = Dbo.queryOneColumnList("select dsl.store_type from " + DataStoreLayer.TableName + " dsl, " + DataStoreLayerAdded.TableName + " dsla" + " where dsl.dsl_id = dsla.dsl_id and dsla.dslad_id = ?", dsladId);
                    if (list.size() != 1) {
                        throw new BusinessException("通过字段存储附加信息获得存储目的地信息出错");
                    }
                    if (param.getCsiNumber() != null && Store_type.HBASE.getCode().equalsIgnoreCase((String) list.get(0))) {
                        columnStorageInfo.setCsi_number(param.getCsiNumber());
                    }
                    columnStorageInfo.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnString", desc = "", range = "")
    public void updateColumnZhName(List<TableColumn> tableColumns) {
        if (tableColumns == null || tableColumns.isEmpty()) {
            throw new BusinessException("获取字段信息失败");
        }
        for (int i = 0; i < tableColumns.size(); i++) {
            TableColumn tableColumn = tableColumns.get(i);
            if (tableColumn.getColumn_id() == null) {
                throw new BusinessException("保存第" + (i + 1) + "个字段的中文名必须关联字段ID");
            }
            DboExecute.updatesOrThrow("保存第" + (i + 1) + "个字段的中文名与字段类型失败", "update " + TableColumn.TableName + " set column_ch_name = ? where column_id = ?", tableColumn.getColumn_ch_name(), tableColumn.getColumn_id());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tbcol_srctgt_maps", desc = "", range = "", isBean = true)
    @Param(name = "dslId", desc = "", range = "")
    public void saveTbColSrctgtMapInfo(TbcolSrctgtMap[] tbcol_srctgt_maps, long dslId) {
        if (null != tbcol_srctgt_maps) {
            List<Long> colIdList = Arrays.stream(tbcol_srctgt_maps).map(TbcolSrctgtMap::getColumn_id).collect(Collectors.toList());
            StringBuilder sb = new StringBuilder();
            sb.append("delete from " + TbcolSrctgtMap.TableName + " where dsl_id = ").append(dslId).append(" AND column_id in(");
            for (Long colId : colIdList) {
                sb.append(colId).append(",");
            }
            sb.deleteCharAt(sb.length() - 1).append(")");
            Dbo.execute(sb.toString());
            for (TbcolSrctgtMap tbcol_srctgt_map : tbcol_srctgt_maps) {
                Validator.notNull(tbcol_srctgt_map.getDsl_id(), "存储层id不能为空");
                Validator.notNull(tbcol_srctgt_map.getColumn_id(), "存储层id不能为空");
                Validator.notNull(tbcol_srctgt_map.getColumn_tar_type(), "目标类型不能为空");
                tbcol_srctgt_map.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableString", desc = "", range = "")
    public void updateTableName(String tableString) {
        List<TableInfo> tableInfos = JsonUtil.toObject(tableString, new TypeReference<List<TableInfo>>() {
        });
        if (tableInfos == null || tableInfos.isEmpty()) {
            throw new BusinessException("获取表信息失败");
        }
        for (int i = 0; i < tableInfos.size(); i++) {
            TableInfo tableInfo = tableInfos.get(i);
            if (tableInfo.getTable_id() == null) {
                throw new BusinessException("保存第" + (i + 1) + "张表的名称信息必须关联字段ID");
            }
            if (StringUtil.isBlank(tableInfo.getTable_name())) {
                throw new BusinessException("第" + (i + 1) + "张表的表名必须填写");
            }
            if (StringUtil.isBlank(tableInfo.getTable_ch_name())) {
                throw new BusinessException("第" + (i + 1) + "张表的表中文名必须填写");
            }
            DboExecute.updatesOrThrow("保存第" + i + "张表名称信息失败", "update " + TableInfo.TableName + " set table_name = ?, table_ch_name = ? where table_id = ?", tableInfo.getTable_name(), tableInfo.getTable_ch_name(), tableInfo.getTable_id());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableStorageInfos", desc = "", range = "")
    private void verifyTbStoConf(List<TableStorageInfo> tableStorageInfos) {
        for (int i = 0; i < tableStorageInfos.size(); i++) {
            TableStorageInfo storageInfo = tableStorageInfos.get(i);
            if (storageInfo.getTable_id() == null) {
                throw new BusinessException("第" + (i + 1) + "条数据保存表存储配置时，请关联表");
            }
            if (StringUtil.isNotBlank(storageInfo.getIs_zipper())) {
                IsFlag isFlag = IsFlag.ofEnumByCode(storageInfo.getIs_zipper());
                if (IsFlag.Shi == isFlag) {
                    if (StringUtil.isBlank(storageInfo.getStorage_type())) {
                        throw new BusinessException("第" + (i + 1) + "条数据保存表存储配置时，请选择进数方式");
                    }
                    StorageType.ofEnumByCode(storageInfo.getStorage_type());
                }
            } else {
                throw new BusinessException("第" + (i + 1) + "条数据保存表存储配置时，请选择是否拉链存储");
            }
            if (storageInfo.getStorage_time() == null) {
                throw new BusinessException("第" + (i + 1) + "条数据保存表存储配置时，请填写存储期限");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tbStoInfoString", desc = "", range = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "dslIdString", desc = "", range = "")
    @Return(desc = "", range = "")
    public long saveTbStoInfo(String tbStoInfoString, long colSetId, String dslIdString) {
        List<TableStorageInfo> tableStorageInfos = JsonUtil.toObject(tbStoInfoString, new TypeReference<List<TableStorageInfo>>() {
        });
        if (tableStorageInfos == null || tableStorageInfos.isEmpty()) {
            throw new BusinessException("未获取到表存储信息");
        }
        verifyTbStoConf(tableStorageInfos);
        List<DataStoRelaParam> dataStoRelaParams = JsonUtil.toObject(dslIdString, new TypeReference<List<DataStoRelaParam>>() {
        });
        if (dataStoRelaParams == null || dataStoRelaParams.isEmpty()) {
            throw new BusinessException("未获取到表存储目的地信息");
        }
        if (tableStorageInfos.size() != dataStoRelaParams.size()) {
            throw new BusinessException("保存表存储信息失败，请确保入库的表都选择了存储目的地");
        }
        List<Object> storeLayerDataByOracle = getStoreLayerDataByOracle();
        Map<String, Object> classifyAndSourceData = getClassifyAndSourceData(colSetId);
        for (TableStorageInfo storageInfo : tableStorageInfos) {
            List<Object> hyrenNameList = getHyrenNameList(classifyAndSourceData.get("classify_id"), colSetId, storageInfo.getIs_prefix());
            long count = Dbo.queryNumber("select count(1) from " + TableStorageInfo.TableName + " where table_id = ?", storageInfo.getTable_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (count == 1) {
                Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id in (select storage_id from " + TableStorageInfo.TableName + " where table_id=?)" + " AND data_source = ?", storageInfo.getTable_id(), StoreLayerDataSource.DB.getCode());
                DboExecute.deletesOrThrow("删除表存储信息异常，一张表入库信息只能在表存储信息表中出现一条记录", "delete from " + TableStorageInfo.TableName + " where table_id = ?", storageInfo.getTable_id());
            }
            long storageId = PrimayKeyGener.getNextId();
            storageInfo.setStorage_id(storageId);
            List<Object> list = Dbo.queryOneColumnList("select dbfile_format from " + DataExtractionDef.TableName + " where table_id = ?", storageInfo.getTable_id());
            if (list.size() != 1) {
                throw new BusinessException("获取采集表卸数文件格式失败");
            }
            storageInfo.setFile_format((String) list.get(0));
            Long tableIdFromTSI = storageInfo.getTable_id();
            for (DataStoRelaParam param : dataStoRelaParams) {
                if (StringUtil.isBlank(param.getHyren_name())) {
                    throw new BusinessException("落地表名未填写");
                }
                Long tableIdFromParam = param.getTableId();
                if (tableIdFromTSI.equals(tableIdFromParam)) {
                    Long[] dslIds = param.getDslIds();
                    if (dslIds == null || !(dslIds.length > 0)) {
                        throw new BusinessException("请检查配置信息，并为每张入库的表选择至少一个存储目的地");
                    }
                    for (long dslId : dslIds) {
                        if (storeLayerDataByOracle.contains(dslId)) {
                            if (param.getHyren_name().length() > 27) {
                                throw new BusinessException("表名称(" + param.getHyren_name() + "),长度超过了27个字符请修改!!!");
                            }
                        }
                        if (hyrenNameList.contains(param.getHyren_name())) {
                            CheckParam.throwErrorMsg("海云系统中已存在当前表(%s), 请重新填写表名!!!", param.getHyren_name());
                        }
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(storageInfo.getIs_prefix())) {
                            String table_prefix = classifyAndSourceData.get("datasource_number") + "_" + classifyAndSourceData.get("classify_num") + "_";
                            String hyren_name = table_prefix + param.getHyren_name();
                            if (param.getHyren_name().startsWith(table_prefix)) {
                                storageInfo.setHyren_name(param.getHyren_name());
                            } else {
                                storageInfo.setHyren_name(hyren_name);
                            }
                        } else {
                            String hyren_name = StringUtil.replace(param.getHyren_name(), classifyAndSourceData.get("datasource_number") + "_", "");
                            hyren_name = StringUtil.replace(hyren_name, classifyAndSourceData.get("classify_num") + "_", "");
                            storageInfo.setHyren_name(hyren_name);
                        }
                        DtabRelationStore relationTable = new DtabRelationStore();
                        relationTable.setTab_id(storageId);
                        relationTable.setDsl_id(dslId);
                        relationTable.setData_source(StoreLayerDataSource.DB.getCode());
                        relationTable.add(Dbo.db());
                        long countNum = Dbo.queryNumber("select  count(1)  from " + DataStoreReg.TableName + " where  table_id = ?", tableIdFromParam).orElse(0);
                        if (countNum != 0) {
                            DboExecute.updatesOrThrow("采集表名称被修改, 更新数据存储登记信息表名失败", "update " + DataStoreReg.TableName + " set hyren_name = ?  where table_id = ?", storageInfo.getHyren_name(), tableIdFromParam);
                        }
                    }
                }
            }
            if (IsFlag.Fou == IsFlag.ofEnumByCode(storageInfo.getIs_zipper())) {
                Dbo.execute("UPDATE " + TableColumn.TableName + " SET is_zipper_field = ? WHERE table_id = ?", IsFlag.Fou.getCode(), storageInfo.getTable_id());
            }
            storageInfo.add(Dbo.db());
        }
        return colSetId;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    private List<Object> getStoreLayerDataByOracle() {
        return Dbo.queryOneColumnList("select dsl_id from " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND (storage_property_val like upper(?))", StorageTypeKey.database_type, "%ORACLE%");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getClassifyAndSourceData(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("查询异常"));
        if (countNum == 0) {
            throw new BusinessException("检测到任务(" + colSetId + ")不存在");
        }
        return Dbo.queryOneObject("SELECT t1.classify_id,t1.classify_num,t1.classify_name,t4.datasource_number,t4.datasource_name  FROM " + CollectJobClassify.TableName + " t1 JOIN " + DatabaseSet.TableName + " t2 ON t1.classify_id = t2.classify_id JOIN " + AgentInfo.TableName + " t3 ON t3.agent_id = t1.agent_id JOIN " + DataSource.TableName + " t4 ON t3.source_id = t4.source_id WHERE t2.database_id = ?", colSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Object> getHyrenNameList(Object classify_id, long colSetId, String is_prefix) {
        if (IsFlag.Shi == IsFlag.ofEnumByCode(is_prefix)) {
            return Dbo.queryOneColumnList("SELECT hyren_name FROM " + TableInfo.TableName + " t1 LEFT JOIN " + TableStorageInfo.TableName + " t2 ON t1.table_id = t2.table_id JOIN " + DatabaseSet.TableName + " t3 ON t1.database_id = t3.database_id WHERE t3.is_sendok = ? AND t3.db_agent = ? AND t3.classify_id = ? " + " AND t3.database_id != ?", IsFlag.Shi.getCode(), IsFlag.Shi.getCode(), classify_id, colSetId);
        } else {
            return Dbo.queryOneColumnList("SELECT hyren_name FROM " + TableInfo.TableName + " t1 LEFT JOIN " + TableStorageInfo.TableName + " t2 ON t1.table_id = t2.table_id JOIN " + DatabaseSet.TableName + " t3 ON t1.database_id = t3.database_id WHERE t3.is_sendok = ? " + " AND t3.database_id != ? AND t2.is_prefix = ?", IsFlag.Shi.getCode(), colSetId, IsFlag.Fou.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getStorageDataForKafka() {
        Result result = Dbo.queryResult("SELECT dsl_id, dsl_name, store_type, '0' as usedflag FROM " + DataStoreLayer.TableName + " where store_type=?", Store_type.KAFKA.getCode());
        if (result.isEmpty()) {
            throw new BusinessException("系统中未定义存储目的地信息，请联系管理员");
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    public void saveDatabaseFinish(String databaseId) {
        DboExecute.updatesOrThrow("此次采集任务配置完成,更新状态失败", "UPDATE " + DatabaseSet.TableName + " SET is_sendok = ? WHERE database_id = ?", IsFlag.Shi.getCode(), Long.parseLong(databaseId));
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getStorageDataBySource() {
        Result result = Dbo.queryResult("SELECT dsl_id, dsl_name, store_type, '0' as usedflag FROM " + DataStoreLayer.TableName + " where dsl_source =?", IsFlag.Shi.getCode());
        if (result.isEmpty()) {
            throw new BusinessException("系统中未定义存储目的地信息，请联系管理员");
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            Store_type storeType = Store_type.ofEnumByCode(result.getString(i, "store_type"));
            if (storeType == Store_type.DATABASE) {
                Map<String, Object> map = Dbo.queryOneObject("SELECT storage_property_val FROM " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND dsl_id = ?", StorageTypeKey.database_type, result.getLong(i, "dsl_id"));
                if (Objects.isNull(map.get("storage_property_val"))) {
                    throw new BusinessException("storage_property_key and dsl_id find storage_property_val is null.");
                }
                result.setObject(i, "store_name", map.get("storage_property_val").toString());
            } else {
                result.setObject(i, "store_name", storeType.getValue());
            }
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getStorageDataByTarget() {
        Result result = Dbo.queryResult("SELECT dsl_id, dsl_name, store_type, '0' as usedflag FROM " + DataStoreLayer.TableName + " where dsl_goal =?", IsFlag.Shi.getCode());
        if (result.isEmpty()) {
            throw new BusinessException("系统中未定义存储目的地信息，请联系管理员");
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            Store_type storeType = Store_type.ofEnumByCode(result.getString(i, "store_type"));
            if (storeType == Store_type.DATABASE) {
                Map<String, Object> map = Dbo.queryOneObject("SELECT storage_property_val FROM " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND dsl_id = ?", StorageTypeKey.database_type, result.getLong(i, "dsl_id"));
                if (Objects.isNull(map.get("storage_property_val"))) {
                    throw new BusinessException("storage_property_key and dsl_id find storage_property_val is null.");
                }
                result.setObject(i, "store_name", map.get("storage_property_val").toString());
            } else {
                result.setObject(i, "store_name", storeType.getValue());
            }
        }
        return result;
    }
}
