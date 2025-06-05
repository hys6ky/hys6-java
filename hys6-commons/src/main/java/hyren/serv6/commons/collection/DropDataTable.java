package hyren.serv6.commons.collection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.hadoop.i.IHadoop;
import java.util.List;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/5/22 0022 下午 03:18")
public class DropDataTable {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    public static void dropTableByDataLayer(String tableName, DatabaseWrapper db) {
        List<LayerBean> layerBeans = ProcessingData.getLayerByTable(tableName, db);
        layerBeans.forEach(layerBean -> dropTableByDataLayer(db, layerBean, "", tableName));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static void dropTableByDataLayer(DatabaseWrapper db, long dsl_id, String tableSpace, String tableName) {
        LayerBean layerBean = SqlOperator.queryOneObject(db, LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
        layerBean.setLayerAttr(ConnectionTool.getLayerMap(db, layerBean.getDsl_id()));
        dropTableByDataLayer(db, layerBean, "", tableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static void dropTableByDataLayer(DatabaseWrapper db, LayerBean layerBean, String tableSpace, String tableName) {
        try (DatabaseWrapper dbDataConn = ConnectionTool.getDBWrapper(db, layerBean.getDsl_id())) {
            Store_type store_type = Store_type.ofEnumByCode(layerBean.getStore_type());
            String dropTableSQL;
            if (store_type == Store_type.DATABASE) {
                if (dbDataConn.getDbtype() == Dbtype.DB2V1 || dbDataConn.getDbtype() == Dbtype.DB2V2) {
                    if (StringUtil.isBlank(tableSpace)) {
                        dropTableSQL = "DROP " + tableName;
                    } else {
                        dropTableSQL = "DROP " + tableSpace + "." + tableName;
                    }
                } else if (dbDataConn.getDbtype() == Dbtype.TERADATA) {
                    if (StringUtil.isBlank(tableSpace)) {
                        dropTableSQL = "DROP TABLE " + tableName;
                    } else {
                        dropTableSQL = "DROP TABLE " + tableSpace + "." + tableName;
                    }
                } else {
                    if (StringUtil.isBlank(tableSpace)) {
                        dropTableSQL = "DROP TABLE " + tableName;
                    } else {
                        dropTableSQL = "DROP TABLE " + tableSpace + "." + tableName;
                    }
                }
                int execute = SqlOperator.execute(dbDataConn, dropTableSQL);
                if (execute != 0) {
                    throw new BusinessException("修改关系型数据库表失败!");
                }
                SqlOperator.commitTransaction(dbDataConn);
            } else if (store_type == Store_type.HIVE) {
                if (StringUtil.isBlank(tableSpace)) {
                    dropTableSQL = "DROP TABLE " + tableName;
                } else {
                    dropTableSQL = "DROP TABLE " + tableSpace + "." + tableName;
                }
                int execute = SqlOperator.execute(dbDataConn, dropTableSQL);
                if (execute != 0) {
                    throw new BusinessException("删除Hive存储类型的数据表失败!");
                }
            } else if (store_type == Store_type.HBASE) {
                IHadoop hadoop = ClassBase.hadoopInstance();
                hadoop.hbaseDropTable(tableName, layerBean);
            } else if (store_type == Store_type.SOLR) {
                throw new BusinessException("删除 SOLR 层表配置暂未实现!!");
            } else if (store_type == Store_type.ElasticSearch) {
                throw new BusinessException("删除 ElasticSearch 层表配置暂未实现!!");
            } else if (store_type == Store_type.MONGODB) {
                throw new BusinessException("删除 MONGODB 层表配置暂未实现!!");
            } else {
                throw new BusinessException("获取删除存储层表SQL时,未找到匹配的存储层类型!");
            }
        }
    }
}
