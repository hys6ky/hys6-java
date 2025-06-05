package hyren.serv6.m.util.dbConf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.ConnWay;
import fd.ng.db.conf.DbinfoProperties;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.m.util.dbConf.bean.DbConfBean;
import hyren.serv6.m.util.dbConf.bean.JDBCBean;
import hyren.serv6.m.util.dbConf.storagelayer.StorageTypeKeyV2;
import hyren.serv6.m.vo.DatabaseSetVo;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "WangZhengcheng", createdate = "2019/10/28 14:26")
@Slf4j
public class ConnectionTool {

    @Method(desc = "", logicStep = "")
    @Param(name = "dbConfigBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DatabaseWrapper getDBWrapper(DbConfBean dbConfBean) {
        JDBCBean bean = new JDBCBean();
        bean.setDatabase_drive(dbConfBean.getDatabase_drive());
        bean.setJdbc_url(dbConfBean.getJdbc_url());
        bean.setUser_name(dbConfBean.getUser_name());
        bean.setDatabase_pad(dbConfBean.getDatabase_pad());
        bean.setDatabase_type(dbConfBean.getDatabase_type());
        bean.setDatabase_name(dbConfBean.getDatabase_name());
        if (dbConfBean.getStoreConfMap() != null) {
            String fetch_size = dbConfBean.getStoreConfMap().get(StorageTypeKeyV2.fetch_size);
            if (!StringUtil.isEmpty(fetch_size)) {
                bean.setFetch_size(Integer.parseInt(fetch_size));
            }
            String maxPoolSize = dbConfBean.getStoreConfMap().get(StorageTypeKeyV2.maxPoolSize);
            if (!StringUtil.isEmpty(maxPoolSize)) {
                bean.setMaxPoolSize(Integer.parseInt(maxPoolSize));
            }
            String minPoolSize = dbConfBean.getStoreConfMap().get(StorageTypeKeyV2.minPoolSize);
            if (!StringUtil.isEmpty(minPoolSize)) {
                bean.setMinPoolSize(Integer.parseInt(minPoolSize));
            }
        }
        return getDBWrapper(bean);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DatabaseWrapper getDBWrapper(DatabaseSetVo database_set) {
        JDBCBean jdbcBean = new JDBCBean();
        DataStoreLayer database_set1 = new DataStoreLayer();
        jdbcBean.setDatabase_drive(database_set.getDatabase_drive());
        jdbcBean.setJdbc_url(database_set.getJdbc_url());
        jdbcBean.setUser_name(database_set.getUser_name());
        jdbcBean.setDatabase_pad(database_set.getDatabase_pad());
        jdbcBean.setDatabase_type(database_set.getDatabase_type());
        jdbcBean.setDatabase_name(database_set.getDatabase_name());
        jdbcBean.setFetch_size(database_set.getFetch_size());
        return getDBWrapper(jdbcBean);
    }

    public static DatabaseWrapper getDBWrapper(JDBCBean bean) {
        Map<String, String> dbConfig = new HashMap<>();
        dbConfig.put(StorageTypeKeyV2.database_driver, bean.getDatabase_drive());
        dbConfig.put(StorageTypeKeyV2.jdbc_url, bean.getJdbc_url());
        dbConfig.put(StorageTypeKeyV2.user_name, bean.getUser_name());
        dbConfig.put(StorageTypeKeyV2.database_pwd, bean.getDatabase_pad());
        dbConfig.put(StorageTypeKeyV2.database_type, bean.getDatabase_type());
        dbConfig.put(StorageTypeKeyV2.database_name, bean.getDatabase_name());
        dbConfig.put(StorageTypeKeyV2.fetch_size, String.valueOf(bean.getFetch_size()));
        dbConfig.put(StorageTypeKeyV2.maxPoolSize, String.valueOf(bean.getMaxPoolSize()));
        dbConfig.put(StorageTypeKeyV2.minPoolSize, String.valueOf(bean.getMinPoolSize()));
        return getDBWrapper(dbConfig);
    }

    public static DatabaseWrapper getDBWrapper(List<Map<String, Object>> dbConfig) {
        return getDBWrapper(getLayerMap(dbConfig));
    }

    @Method(desc = "", logicStep = "")
    public static DbConfBean getDbConfBean(DatabaseWrapper db, long dsl_id) {
        Map<String, String> dbConfigMap = getLayerMap(getLayerList(db, dsl_id));
        DbConfBean dbConfBean = new DbConfBean();
        dbConfBean.setDatabase_drive(dbConfigMap.get(StorageTypeKeyV2.database_driver));
        dbConfBean.setJdbc_url(dbConfigMap.get(StorageTypeKeyV2.jdbc_url));
        dbConfBean.setUser_name(dbConfigMap.get(StorageTypeKeyV2.user_name));
        dbConfBean.setDatabase_pad(dbConfigMap.get(StorageTypeKeyV2.database_pwd));
        if (Store_type.HIVE.getCode().equals(dbConfigMap.get("store_type")) || Store_type.CARBONDATA.getCode().equals(dbConfigMap.get("store_type"))) {
            dbConfBean.setDatabase_type(Store_type.HIVE.getValue());
        } else if (Store_type.HBASE.getCode().equals(dbConfigMap.get("store_type"))) {
            String increment_engine = dbConfigMap.get(StorageTypeKeyV2.increment_engine);
            if ("hive".equals(increment_engine)) {
                dbConfBean.setDatabase_type(Store_type.HIVE.getValue());
            } else if ("phoenix".equals(increment_engine)) {
                throw new AppSystemException("暂不支持 phoenix 做 hbase 的增量引擎!");
            } else {
                throw new AppSystemException("hbase 的增量引擎不合法!" + increment_engine);
            }
        } else {
            dbConfBean.setDatabase_type(dbConfigMap.get(StorageTypeKeyV2.database_type));
        }
        dbConfBean.setDatabase_name(dbConfigMap.get(StorageTypeKeyV2.database_name));
        dbConfigMap.put(StorageTypeKeyV2.database_type, dbConfBean.getDatabase_type());
        dbConfBean.setStoreConfMap(dbConfigMap);
        return dbConfBean;
    }

    public static DatabaseWrapper getDBWrapper(Map<String, String> dbConfig) {
        log.info("获取到存储层传递的连接的配置为：===============================" + JsonUtil.toJson(dbConfig));
        DbinfoProperties.Dbinfo dbInfo = new DbinfoProperties.Dbinfo();
        dbInfo.setName(DbinfoProperties.DEFAULT_DBNAME);
        dbInfo.setDriver(dbConfig.get(StorageTypeKeyV2.database_driver));
        dbInfo.setUrl(dbConfig.get(StorageTypeKeyV2.jdbc_url));
        dbInfo.setUsername(dbConfig.get(StorageTypeKeyV2.user_name));
        dbInfo.setPassword(dbConfig.get(StorageTypeKeyV2.database_pwd));
        dbInfo.setWay(ConnWay.JDBC);
        dbInfo.setAutoCommit(true);
        dbInfo.setDataBaseName((dbConfig.get(StorageTypeKeyV2.database_name)));
        if (dbInfo.getWay() == ConnWay.POOL) {
            String maxPoolSize = dbConfig.get(StorageTypeKeyV2.maxPoolSize);
            if (maxPoolSize != null) {
                int i_maxPoolSize = Integer.parseInt(maxPoolSize);
                if (i_maxPoolSize != 0) {
                    dbInfo.setMaxPoolSize(i_maxPoolSize);
                }
            }
            String minPoolSize = dbConfig.get(StorageTypeKeyV2.minPoolSize);
            if (minPoolSize != null) {
                int i_minPoolSize = Integer.parseInt(minPoolSize);
                if (i_minPoolSize != 0) {
                    dbInfo.setMinPoolSize(i_minPoolSize);
                }
            }
        }
        Dbtype dbType = getDbType(dbConfig.get(StorageTypeKeyV2.database_type));
        if (dbType == Dbtype.HIVE) {
            hyren.serv6.m.util.dbConf.IHadoop hive = ClassBase.hadoopInstance();
            hive.setHiveConf(dbConfig);
            dbInfo.setAutoCommit(false);
        }
        String fetch_size = dbConfig.get(StorageTypeKeyV2.fetch_size);
        if (fetch_size != null) {
            int i_fetch_size = Integer.parseInt(fetch_size);
            if (i_fetch_size != 0) {
                dbInfo.setFetch_size(i_fetch_size);
                if (dbType == Dbtype.POSTGRESQL) {
                    dbInfo.setAutoCommit(false);
                }
            }
        }
        if (!StringUtil.isEmpty(dbConfig.get(StorageTypeKeyV2.database_name))) {
            dbInfo.setDataBaseName(dbConfig.get(StorageTypeKeyV2.database_name));
        }
        dbInfo.setDbtype(dbType);
        dbInfo.setShow_conn_time(true);
        dbInfo.setShow_sql(true);
        log.info("用来创建DB连接的参数为：==============" + dbInfo);
        DatabaseWrapper db = new DatabaseWrapper.Builder().dbconf(dbInfo).create();
        if (db.getDbtype() == Dbtype.TERADATA && db.getDatabaseName() != null) {
            db.execute("DATABASE " + db.getDatabaseName().toUpperCase());
        }
        if (db.getDbtype() == Dbtype.HIVE && db.getDatabaseName() != null) {
            db.execute("USE " + db.getDatabaseName().toUpperCase());
        }
        if (db.getDbtype() == Dbtype.KINGBASE && db.getDatabaseName() != null) {
            log.info("人大金仓开始切换模式到: " + db.getDatabaseName().toUpperCase());
            db.execute("set search_path=" + db.getDatabaseName().toUpperCase());
        }
        return db;
    }

    @Method(desc = "", logicStep = "")
    public static DatabaseWrapper getDBWrapper(DatabaseWrapper db, long dsl_id) {
        DbConfBean dbConfBean = getDbConfBean(db, dsl_id);
        Map<String, String> storeConfMap = dbConfBean.getStoreConfMap();
        return getDBWrapper(storeConfMap);
    }

    @Method(desc = "", logicStep = "")
    public static List<Map<String, Object>> getLayerList(DatabaseWrapper db, long dsl_id) {
        return SqlOperator.queryList(db, "select t1.*,store_type from " + DataStoreLayerAttr.TableName + " t1 " + "left join " + DataStoreLayer.TableName + " t2 on t1.dsl_id = t2.dsl_id where t1.dsl_id = ?", dsl_id);
    }

    @Method(desc = "", logicStep = "")
    public static Map<String, String> getLayerMap(DatabaseWrapper db, long dsl_id) {
        return getLayerMap(getLayerList(db, dsl_id));
    }

    @Method(desc = "", logicStep = "")
    private static Map<String, String> getLayerMap(List<Map<String, Object>> dbConfig) {
        Map<String, String> dbConfigMap = new HashMap<>();
        for (Map<String, Object> dbMap : dbConfig) {
            String key = dbMap.get("storage_property_key").toString();
            String val = dbMap.get("storage_property_val").toString();
            dbConfigMap.put(key, val);
        }
        dbConfigMap.put("store_type", dbConfig.get(0).get("store_type").toString());
        return dbConfigMap;
    }

    public static Dbtype getDbType(String database_type) {
        return DataBaseType.getDatabase(database_type).getDbtype();
    }
}
