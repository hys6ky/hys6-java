package hyren.serv6.q.websqlquery;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.datatree.WebTreeData;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.cache.CacheConfBean;
import hyren.serv6.commons.cache.CacheObj;
import hyren.serv6.commons.cache.ConcurrentHashMapCacheUtil;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Api("WebSql查询处理类")
@Service
@DocClass(desc = "", author = "BY-HLL", createdate = "2019/10/25 0025 下午 05:51")
public class WebsqlqueryServiceImpl {

    private static final ConcurrentHashMapCacheUtil platformAllTableInfoCache = initPlatformAllTableInfo();

    @Method(desc = "", logicStep = "")
    private static ConcurrentHashMapCacheUtil initPlatformAllTableInfo() {
        log.info("Start to initialize all table information of the platform.");
        CacheConfBean cacheConfBean = new CacheConfBean();
        cacheConfBean.setCache_time(10 * 60 * 1000L);
        cacheConfBean.setCache_cleaning_frequency(10 * 60 * 1000L);
        cacheConfBean.setCache_max_number(10000);
        ConcurrentHashMapCacheUtil platformAllTableInfoCache = new ConcurrentHashMapCacheUtil(cacheConfBean);
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            StringBuilder sb_sql = new StringBuilder();
            try {
                sb_sql.append("SELECT * FROM (");
                sb_sql.append("SELECT ti.table_id AS table_id,dsr.hyren_name AS table_name," + " tc.column_name AS column_name, tc.column_ch_name AS column_ch_name," + " tc.column_type AS column_type FROM " + DataStoreReg.TableName + " dsr JOIN " + TableInfo.TableName + " ti ON dsr.database_id = ti.database_id" + " AND dsr.table_name = ti.table_name JOIN " + TableColumn.TableName + " tc" + " ON ti.table_id = tc.table_id");
                sb_sql.append(" UNION");
                sb_sql.append(" SELECT oct.ocs_id AS table_id,oct.en_name AS table_name," + "ocs.column_name AS column_name, ocs.data_desc AS column_ch_name," + "ocs.column_type AS column_type FROM " + ObjectCollectTask.TableName + " oct" + " JOIN " + ObjectCollectStruct.TableName + " ocs ON oct.ocs_id=ocs.ocs_id" + " JOIN " + DtabRelationStore.TableName + " dtab_rs ON dtab_rs.tab_id=oct.ocs_id");
                sb_sql.append(" UNION");
                sb_sql.append(" SELECT dd.module_table_id AS table_id, dd.module_table_en_name AS table_name," + " dfi.field_en_name AS column_name, dfi.field_cn_name AS column_ch_name," + " concat(field_type,'(',field_length,')') AS" + " column_type FROM " + DmModuleTableFieldInfo.TableName + " dfi" + " JOIN " + DmModuleTable.TableName + " dd" + " ON dd.module_table_id = dfi.module_table_id");
                sb_sql.append(" UNION");
                sb_sql.append(" SELECT dti.table_id AS table_id, dti.table_name AS table_name," + " dtc.column_name AS column_name, dtc.field_ch_name AS column_ch_name," + " dtc.column_type AS column_type FROM " + DqTableInfo.TableName + " dti" + " JOIN " + DqTableColumn.TableName + " dtc ON dti.table_id=dtc.table_id");
                sb_sql.append(")").append(" tmp group by table_id,table_name,column_name,column_ch_name,column_type").append(" order by table_name");
                List<Map<String, Object>> tableInfoByPlatform = SqlOperator.queryList(db, sb_sql.toString());
                List<DqIndex3record> di_3_s = SqlOperator.queryList(db, DqIndex3record.class, "SELECT * FROM " + DqIndex3record.TableName);
                di_3_s.forEach(di_3 -> {
                    String table_col_s = di_3.getTable_col();
                    String[] column_s = table_col_s.split(",");
                    for (String column : column_s) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("table_id", di_3.getRecord_id());
                        map.put("table_name", di_3.getTable_name());
                        map.put("column_name", column);
                        map.put("column_ch_name", column);
                        map.put("column_type", "VARCHAR(--)");
                        tableInfoByPlatform.add(map);
                    }
                });
                if (tableInfoByPlatform.isEmpty()) {
                    log.warn("初始化sql补全的缓存数据时,平台没有成功登记的表!");
                }
                Map<String, List<Object>> cache_map = new HashMap<>();
                tableInfoByPlatform.forEach(table_info -> {
                    String table_name = table_info.get("table_name").toString();
                    List<Object> column_s;
                    if (cache_map.containsKey(table_name)) {
                        column_s = cache_map.get(table_name);
                        column_s.add(table_info.get("column_name"));
                    } else {
                        column_s = new ArrayList<>();
                        column_s.add(table_info.get("column_name"));
                        cache_map.put(table_name, column_s);
                    }
                });
                cache_map.forEach(platformAllTableInfoCache::setCache);
            } catch (Exception e) {
                throw new BusinessException("初始化平台表及字段的缓存信息失败! e: " + e);
            }
        }
        log.info("Successfully initialized all table information of the platform");
        return platformAllTableInfoCache;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public Object getTableInfoByTableName_cache(String table_name) {
        CacheObj cacheObj;
        Validator.notBlank(table_name, "查询表名不能为空!");
        cacheObj = platformAllTableInfoCache.getCache(table_name);
        if (null == cacheObj) {
            List<Map<String, Object>> columnsByTableName = DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
            List<Object> column_s = new ArrayList<>();
            if (!columnsByTableName.isEmpty()) {
                columnsByTableName.forEach(column_info -> column_s.add(column_info.get("column_name")));
                platformAllTableInfoCache.setCache(table_name, column_s);
                cacheObj = platformAllTableInfoCache.getCache(table_name);
            }
        }
        return cacheObj == null ? new Object() : cacheObj.getCacheValue();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "", valueIfNull = "1")
    @Param(name = "end", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> queryDataBasedOnTableName(String tableName, int begin, int end) {
        String sql = "select * from " + tableName;
        List<Map<String, Object>> query_list = new ArrayList<>();
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            new ProcessingData() {

                @Override
                public void dealLine(Map<String, Object> map) {
                    query_list.add(map);
                }
            }.getPageDataLayer(sql, db, begin, end);
        }
        return query_list;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "querySQL", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "", valueIfNull = "1")
    @Param(name = "end", desc = "", range = "", valueIfNull = "100")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> queryDataBasedOnSql(String querySQL, int begin, int end) {
        Validator.notBlank(querySQL, "请填写SQL信息");
        querySQL = AesUtil.desEncrypt(querySQL);
        querySQL = new DruidParseQuerySql().GetNewSql(querySQL);
        List<Map<String, Object>> query_list = new ArrayList<>();
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            new ProcessingData() {

                @Override
                public void dealLine(Map<String, Object> map) {
                    query_list.add(map);
                }
            }.getPageDataLayer(querySQL, db, begin, end);
        }
        return query_list;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> getWebSQLTreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        treeConf.setIsIntoHBase("");
        treeConf.setShowDCLRealtime(Boolean.TRUE);
        return new WebTreeData().getTreeData(TreePageSource.WEB_SQL, treeConf);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<String> getAllTableNameByPlatform() {
        return DataTableUtil.getAllTableNameByPlatform(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    @Deprecated
    public List<Map<String, Object>> getColumnsByTableName(String table_name) {
        Validator.notBlank(table_name, "查询表名不能为空!");
        return DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTableColumnInfoBySql(String sql) {
        Validator.notBlank(sql, "解析sql不能为空");
        List<Map<String, Object>> tableColumnInfos = new ArrayList<>();
        List<String> table_name_s = new ArrayList<>();
        try {
            table_name_s = DruidParseQuerySql.parseSqlTableToList(sql);
        } catch (Exception e) {
            log.error("请输入合法的sql! " + sql);
        }
        if (!table_name_s.isEmpty()) {
            table_name_s.forEach(table_name -> {
                Map<String, Object> map = new HashMap<>();
                map.put("table_name", table_name);
                map.put("column_info", DataTableUtil.getColumnByTableName(Dbo.db(), table_name));
                tableColumnInfos.add(map);
            });
        }
        return tableColumnInfos;
    }
}
