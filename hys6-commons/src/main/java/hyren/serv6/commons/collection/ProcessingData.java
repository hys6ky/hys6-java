package hyren.serv6.commons.collection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.*;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.collection.bean.LayerTypeBean;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@DocClass(desc = "", author = "xchao", createdate = "2020年3月31日 16:32:43")
@Slf4j
public abstract class ProcessingData {

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> getDataLayer(String sql, DatabaseWrapper db) {
        return getPageDataLayer(sql, db, 0, 0, false);
    }

    public List<String> getDataLayer(String sql) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getPageDataLayer(sql, db, 0, 0, false);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "intoLayerBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> getDataLayer(String sql, DatabaseWrapper db, long dsl_id) {
        return getPageDataLayer(sql, db, 0, 0, dsl_id, false);
    }

    public List<String> getDataLayer(String sql, long dsl_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getPageDataLayer(sql, db, 0, 0, dsl_id, false);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "")
    @Param(name = "end", desc = "", range = "")
    @Return(desc = "", range = "")
    public void getPageDataLayer(String sql, DatabaseWrapper db, int begin, int end) {
        getPageDataLayer(sql, db, begin, end, false);
    }

    public void getPageDataLayer(String sql, int begin, int end) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            getPageDataLayer(sql, db, begin, end, false);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "")
    @Param(name = "end", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public void getPageDataLayer(String sql, DatabaseWrapper db, int begin, int end, long dsl_id) {
        getPageDataLayer(sql, db, begin, end, dsl_id, false);
    }

    public void getPageDataLayer(String sql, int begin, int end, long dsl_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            getPageDataLayer(sql, db, begin, end, dsl_id, false);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "")
    @Param(name = "end", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "isCountTotal", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> getPageDataLayer(String sql, DatabaseWrapper db, int begin, int end, long dsl_id, boolean isCountTotal) {
        return getResultSet(sql, db, dsl_id, begin, end, isCountTotal);
    }

    public List<String> getPageDataLayer(String sql, int begin, int end, long dsl_id, boolean isCountTotal) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getResultSet(sql, db, dsl_id, begin, end, isCountTotal);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "")
    @Param(name = "end", desc = "", range = "")
    @Param(name = "isCountTotal", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> getPageDataLayer(String sql, DatabaseWrapper db, int begin, int end, boolean isCountTotal) {
        LayerTypeBean ltb = getAllTableIsLayer(sql, db);
        String ofSql = getdStoreReg(sql, db);
        if (ltb.getConnType() == LayerTypeBean.ConnType.oneJdbc) {
            long dsl_id = ltb.getLayerBean().getDsl_id();
            return getResultSet(ofSql, db, dsl_id, begin, end, isCountTotal);
        } else if (ltb.getConnType() == LayerTypeBean.ConnType.oneOther) {
            long dsl_id = ltb.getLayerBean().getDsl_id();
            return getResultSet(ofSql, db, dsl_id, begin, end, isCountTotal);
        } else if (ltb.getConnType() == LayerTypeBean.ConnType.moreJdbc) {
            return getMoreJdbcResult(sql, begin, end, isCountTotal);
        } else if (ltb.getConnType() == LayerTypeBean.ConnType.moreOther) {
            return getMoreJdbcResult(ofSql, begin, end, isCountTotal);
        }
        return null;
    }

    public List<String> getPageDataLayer(String sql, int begin, int end, boolean isCountTotal) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getPageDataLayer(sql, db, begin, end, isCountTotal);
        }
    }

    public static String getdStoreReg(String sql, DatabaseWrapper db) {
        Map<String, String> map = new HashMap<>();
        List<String> listTable = DruidParseQuerySql.parseSqlTableToList(sql);
        for (String tableName : listTable) {
            Map<String, Object> objectMap = SqlOperator.queryOneObject(db, "select a.collect_type,a.table_name from " + DataStoreReg.TableName + " a join" + " " + DatabaseSet.TableName + " b on a.database_id = b.database_id " + " where a.collect_type in (?,?) and lower(hyren_name) = ? and b.collect_type = ?", AgentType.DBWenJian.getCode(), AgentType.ShuJuKu.getCode(), tableName.toLowerCase(), CollectType.TieYuanDengJi.getCode());
            if (!objectMap.isEmpty()) {
                if (objectMap.get("table_name") == null) {
                    return sql;
                }
                Pattern p = Pattern.compile("([\\s*|\\t|\\r|\\n+])" + tableName);
                Matcher m = p.matcher(sql);
                sql = m.replaceAll(" " + objectMap.get("table_name").toString());
            }
        }
        return sql;
    }

    public static String getdStoreReg(String sql) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getdStoreReg(sql, db);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public static LayerBean getLayerBean(long dsl_id, DatabaseWrapper db) {
        LayerBean layerBean = SqlOperator.queryOneObject(db, LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL执行失败!")));
        layerBean.setLayerAttr(ConnectionTool.getLayerMap(db, layerBean.getDsl_id()));
        return layerBean;
    }

    public static LayerBean getLayerBean(long dsl_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getLayerBean(dsl_id, db);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<LayerBean> getTableLayer(String tableName, DatabaseWrapper db) {
        List<LayerBean> tableStorageLayerBeans = new ArrayList<>();
        Optional<DataStoreReg> dsr_optional = SqlOperator.queryOneObject(db, DataStoreReg.class, "select * from " + DataStoreReg.TableName + " where collect_type in (?,?,?)" + " and lower(hyren_name) = ? " + " union " + " select a.* from " + DataStoreReg.TableName + " a join " + DatabaseSet.TableName + " b on a.database_id = b.database_id where " + " a.collect_type = ? and  lower(a.hyren_name)" + " = ? and b.collect_type = ?", AgentType.DBWenJian.getCode(), AgentType.ShuJuKu.getCode(), AgentType.DuiXiang.getCode(), tableName.toLowerCase(), AgentType.ShuJuKu.getCode(), tableName.toLowerCase(), CollectType.TieYuanDengJi.getCode());
        if (dsr_optional.isPresent()) {
            DataStoreReg dsr = dsr_optional.get();
            List<LayerBean> dcl_layerBeans;
            if (AgentType.DuiXiang == AgentType.ofEnumByCode(dsr.getCollect_type())) {
                dcl_layerBeans = SqlOperator.queryList(db, LayerBean.class, "select dsl.*,'" + DataSourceType.DCL.getCode() + "' as dst" + " from " + DtabRelationStore.TableName + " dtrs" + " join " + DataStoreLayer.TableName + " dsl on dtrs.dsl_id = dsl.dsl_id" + " where dtrs.tab_id = ? and dtrs.data_source =?", dsr.getTable_id(), StoreLayerDataSource.OBJ.getCode());
            } else {
                dcl_layerBeans = SqlOperator.queryList(db, LayerBean.class, "select dsl.*,'" + DataSourceType.DCL.getCode() + "' as dst from " + TableStorageInfo.TableName + " tsi" + " join " + DtabRelationStore.TableName + " dtrs on tsi.storage_id = dtrs.tab_id" + " join " + DataStoreLayer.TableName + " dsl on dtrs.dsl_id = dsl.dsl_id" + " where lower(tsi.hyren_name) = lower(?) and dtrs.data_source in (?,?)", dsr.getHyren_name(), StoreLayerDataSource.DB.getCode(), StoreLayerDataSource.DBA.getCode());
            }
            tableStorageLayerBeans.addAll(dcl_layerBeans);
        }
        List<LayerBean> dml_layerBeans = SqlOperator.queryList(db, LayerBean.class, "select dsl.*,'" + DataSourceType.DML.getCode() + "' as dst" + " from " + DmModuleTable.TableName + " dd" + " join  " + DtabRelationStore.TableName + " dtrs on dd.module_table_id = dtrs.tab_id" + " join " + DataStoreLayer.TableName + " dsl on dtrs.dsl_id = dsl.dsl_id" + " where lower(module_table_en_name) = ? and dtrs.data_source = ?" + " union " + " select dsl.*,'" + DataSourceType.DML.getCode() + "' as dst" + " from " + DmJobTableInfo.TableName + " djti" + " join  " + DtabRelationStore.TableName + " dtrs on djti.module_table_id = dtrs.tab_id" + " join " + DataStoreLayer.TableName + " dsl on dtrs.dsl_id = dsl.dsl_id" + " where lower(jobtab_en_name) = ? and dtrs.data_source = ?", tableName.toLowerCase(), StoreLayerDataSource.DM.getCode(), tableName.toLowerCase(), StoreLayerDataSource.DM.getCode());
        tableStorageLayerBeans.addAll(dml_layerBeans);
        List<LayerBean> dqc_layerBeans = SqlOperator.queryList(db, LayerBean.class, "select dsl.*,'" + DataSourceType.DQC.getCode() + "' as dst from " + DqIndex3record.TableName + " di3" + " join " + DataStoreLayer.TableName + " dsl on di3.dsl_id=dsl.dsl_id" + " where lower(di3.table_name) = ?", tableName.toLowerCase());
        tableStorageLayerBeans.addAll(dqc_layerBeans);
        List<LayerBean> udl_layerBeans = SqlOperator.queryList(db, LayerBean.class, "select dsl.*,'" + DataSourceType.UDL.getCode() + "' as dst from " + DqTableInfo.TableName + " dqti" + " join  " + DtabRelationStore.TableName + " dtrs on dqti.table_id = dtrs.tab_id" + " join " + DataStoreLayer.TableName + " dsl on dtrs.dsl_id = dsl.dsl_id" + " where lower(dqti.table_name) = ? and dtrs.data_source = ?", tableName.toLowerCase(), StoreLayerDataSource.UD.getCode());
        tableStorageLayerBeans.addAll(udl_layerBeans);
        List<LayerBean> kafka_layerBeans = SqlOperator.queryList(db, LayerBean.class, "select dsl.*,'" + DataSourceType.KFK.getCode() + "' as dst from " + DqTableInfo.TableName + " dqti" + " join  " + DtabRelationStore.TableName + " dtrs on dqti.table_id = dtrs.tab_id" + " join " + DataStoreLayer.TableName + " dsl on dtrs.dsl_id = dsl.dsl_id" + " where lower(dqti.table_name) = ? and dtrs.data_source = ?", tableName.toLowerCase(), StoreLayerDataSource.SD.getCode());
        tableStorageLayerBeans.addAll(kafka_layerBeans);
        if (!tableStorageLayerBeans.isEmpty()) {
            for (LayerBean layerBean : tableStorageLayerBeans) {
                layerBean.setLayerAttr(ConnectionTool.getLayerMap(db, layerBean.getDsl_id()));
            }
        }
        if (tableStorageLayerBeans.isEmpty()) {
            throw new AppSystemException("表: " + tableName + " 未在任何存储层中存在!");
        }
        return tableStorageLayerBeans;
    }

    public static List<LayerBean> getLayerByTable(String tableName, DatabaseWrapper db) {
        return getTableLayer(tableName, db);
    }

    public static List<LayerBean> getLayerByTable(String tableName) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return getTableLayer(tableName, db);
        }
    }

    public static Map<String, List<LayerBean>> getLayerByTable(List<String> tableNameList, DatabaseWrapper db) {
        Map<String, List<LayerBean>> laytable = new HashMap<>();
        for (String tableName : tableNameList) {
            List<LayerBean> layerByTable = getLayerByTable(tableName, db);
            laytable.put(tableName, layerByTable);
        }
        return laytable;
    }

    public static Map<String, List<LayerBean>> getLayerByTable(List<String> tableNameList) {
        return getLayerByTable(tableNameList);
    }

    public static LayerTypeBean getAllTableIsLayer(String sql, DatabaseWrapper db) {
        List<String> listTable = DruidParseQuerySql.parseSqlTableToList(sql);
        return getAllTableIsLayer(listTable, db);
    }

    public static LayerTypeBean getAllTableIsLayer(String sql) {
        return getAllTableIsLayer(sql);
    }

    public static LayerTypeBean getAllTableIsLayer(List<String> allTableList, DatabaseWrapper db) {
        Map<String, LayerBean> allTableLayer = new HashMap<>();
        for (String tableName : allTableList) {
            List<LayerBean> tableLayer = getLayerByTable(tableName, db);
            for (LayerBean objectMap : tableLayer) {
                String layer_id = String.valueOf(objectMap.getDsl_id());
                LayerBean layerBean = allTableLayer.get(layer_id) == null ? objectMap : allTableLayer.get(layer_id);
                Set<String> tableNameList = layerBean.getTableNameList() == null ? new HashSet<>() : layerBean.getTableNameList();
                tableNameList.add(tableName);
                layerBean.setTableNameList(tableNameList);
                allTableLayer.put(layer_id, layerBean);
            }
        }
        LayerTypeBean layerTypeBean = new LayerTypeBean();
        List<LayerBean> list = new ArrayList<>();
        Iterator<String> iter = allTableLayer.keySet().iterator();
        Set<LayerTypeBean.ConnType> setconn = new HashSet<>();
        while (iter.hasNext()) {
            String key = iter.next();
            LayerBean objectMap = allTableLayer.get(key);
            String store_type = objectMap.getStore_type();
            Set<String> tableNameList = objectMap.getTableNameList();
            if (tableNameList.size() == allTableList.size()) {
                if (Store_type.DATABASE == Store_type.ofEnumByCode(store_type) || Store_type.HIVE == Store_type.ofEnumByCode(store_type) || Store_type.CARBONDATA == Store_type.ofEnumByCode(store_type)) {
                    layerTypeBean.setConnType(LayerTypeBean.ConnType.oneJdbc);
                } else {
                    layerTypeBean.setConnType(LayerTypeBean.ConnType.oneOther);
                }
                layerTypeBean.setLayerBean(objectMap);
                return layerTypeBean;
            }
            if (Store_type.DATABASE == Store_type.ofEnumByCode(store_type) || Store_type.HIVE == Store_type.ofEnumByCode(store_type)) {
                setconn.add(LayerTypeBean.ConnType.moreJdbc);
            } else {
                setconn.add(LayerTypeBean.ConnType.moreOther);
            }
            list.add(objectMap);
            layerTypeBean.setLayerBeanList(list);
        }
        if (setconn.size() == 1 && setconn.contains(LayerTypeBean.ConnType.moreJdbc)) {
            layerTypeBean.setConnType(LayerTypeBean.ConnType.moreJdbc);
        } else {
            layerTypeBean.setConnType(LayerTypeBean.ConnType.moreOther);
        }
        return layerTypeBean;
    }

    public static LayerTypeBean getAllTableIsLayer(List<String> allTableList) {
        return getAllTableIsLayer(allTableList);
    }

    private List<String> getResultSet(String sql, DatabaseWrapper db, long dsl_id, int begin, int end, boolean isCountTotal) {
        String is_query_spark = PropertyParaValue.getString("isQuerySpark", "false");
        if ("true".equals(is_query_spark)) {
            return getMoreJdbcResult(sql, begin, end, isCountTotal);
        } else {
            try (DatabaseWrapper dbDataConn = ConnectionTool.getDBWrapper(db, dsl_id)) {
                return getSQLData(sql, dbDataConn, begin, end, isCountTotal);
            } catch (Exception e) {
                throw new AppSystemException("sql查询出错: ", e);
            }
        }
    }

    private List<String> getMoreJdbcResult(String sql, int begin, int end, boolean isCountTotal) {
        try (DatabaseWrapper db = new DatabaseWrapper.Builder().dbname("Hive").create()) {
            return getSQLData(sql, db, begin, end, isCountTotal);
        } catch (Exception e) {
            throw new AppSystemException("sql查询出错: ", e);
        }
    }

    private List<String> getSQLData(String sql, DatabaseWrapper db, int begin, int end, boolean isCountTotal) throws Exception {
        List<String> colArray = new ArrayList<>();
        List<Map<String, String>> colNameTypeList = new ArrayList<>();
        ResultSet rs;
        if (begin == 0 && end == 0) {
            rs = db.queryGetResultSet(sql);
        } else {
            rs = db.queryPagedGetResultSet(sql, begin, end, isCountTotal);
        }
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        for (int i = 0; i < cols; i++) {
            Map<String, String> map = new HashMap<>();
            String colName = meta.getColumnName(i + 1).toLowerCase();
            String columnType = meta.getColumnTypeName(i + 1);
            map.put(colName, columnType);
            colArray.add(colName);
            colNameTypeList.add(map);
        }
        while (rs.next()) {
            Map<String, Object> result = new HashMap<>();
            for (Map<String, String> colNameTypeMap : colNameTypeList) {
                for (Map.Entry<String, String> entry : colNameTypeMap.entrySet()) {
                    if (!Constant.HYRENFIELD.contains(entry.getKey().toUpperCase())) {
                        if ("clob".equalsIgnoreCase(entry.getValue()) || "nclob".equalsIgnoreCase(entry.getValue())) {
                            Object clobToString = rs.getObject(entry.getKey());
                            if (null != clobToString) {
                                clobToString = clobToString(rs.getClob(entry.getKey()));
                            }
                            result.put(entry.getKey(), clobToString == null ? "NULL" : clobToString);
                        } else {
                            result.put(entry.getKey(), rs.getObject(entry.getKey()) == null ? "NULL" : rs.getObject(entry.getKey()));
                        }
                    }
                }
            }
            dealLine(result);
        }
        return colArray;
    }

    private static String clobToString(Clob clob) throws SQLException, IOException {
        try (Reader stream = clob.getCharacterStream();
            BufferedReader br = new BufferedReader(stream)) {
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    public abstract void dealLine(Map<String, Object> map) throws Exception;
}
