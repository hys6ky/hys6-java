package hyren.serv6.commons.collection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.hadoop.i.IHadoop;
import hyren.serv6.commons.utils.constant.Constant;
import java.util.ArrayList;
import java.util.List;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/5/22 0022 下午 03:49")
public class RenameDataTable {

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "operation_type", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static List<String> renameTableByDataLayer(DatabaseWrapper db, String operation_type, String tableSpace, String tableName) {
        List<String> dsl_id_s = new ArrayList<>();
        List<LayerBean> tableLayers = ProcessingData.getLayerByTable(tableName, db);
        tableLayers.forEach(tableLayer -> {
            dsl_id_s.add(tableLayer.getDsl_id().toString());
            renameTableByDataLayer(db, tableLayer, operation_type, tableSpace, tableName);
        });
        return dsl_id_s;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "operation_type", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static void renameTableByDataLayer(DatabaseWrapper db, long dsl_id, String operation_type, String tableSpace, String tableName) {
        LayerBean layerBean = SqlOperator.queryOneObject(db, LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
        layerBean.setLayerAttr(ConnectionTool.getLayerMap(db, layerBean.getDsl_id()));
        renameTableByDataLayer(db, layerBean, operation_type, tableSpace, tableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static void renameTableByDataLayer(DatabaseWrapper db, LayerBean layerBean, String operation_type, String tableSpace, String tableName) {
        renameTableByDataLayer(db, layerBean, operation_type, tableSpace, tableName, "");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "operation_type", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static void renameTableByDataLayer(DatabaseWrapper db, LayerBean layerBean, String operation_type, String tableSpace, String srcTableName, String destTableName) {
        String invalid_table_name = Constant.DQC_INVALID_TABLE + srcTableName;
        if (operation_type.equals(Constant.DM_SET_INVALID_TABLE)) {
            alterTableName(db, layerBean, tableSpace, srcTableName, invalid_table_name);
        } else if (operation_type.equals(Constant.DM_RESTORE_TABLE)) {
            alterTableName(db, layerBean, tableSpace, invalid_table_name, srcTableName);
        } else if (operation_type.equalsIgnoreCase(Constant.CUSTOMIZE)) {
            alterTableName(db, layerBean, tableSpace, srcTableName, destTableName);
        } else {
            throw new BusinessException("未知的重命名表操作类型! see@{remove:删除,restore:恢复,customize:自定义表名}");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "tableSpace", desc = "", range = "")
    @Param(name = "old_table_name", desc = "", range = "")
    @Param(name = "new_table_name", desc = "", range = "")
    private static void alterTableName(DatabaseWrapper db, LayerBean layerBean, String tableSpace, String old_table_name, String new_table_name) {
        String alterTableNameSQL;
        Store_type store_type = Store_type.ofEnumByCode(layerBean.getStore_type());
        if (store_type == Store_type.DATABASE) {
            try (DatabaseWrapper dbDataConn = ConnectionTool.getDBWrapper(db, layerBean.getDsl_id())) {
                alterTableNameSQL = dbDataConn.getDbtype().ofRenameSql(old_table_name, new_table_name, dbDataConn);
                if (StringUtil.isBlank(alterTableNameSQL)) {
                    throw new BusinessException("修改关系型数据库的数据表名称的SQL为空!");
                }
                SqlOperator.execute(dbDataConn, alterTableNameSQL);
                SqlOperator.commitTransaction(dbDataConn);
            }
        } else if (store_type == Store_type.HIVE) {
            try (DatabaseWrapper dbDataConn = ConnectionTool.getDBWrapper(db, layerBean.getDsl_id())) {
                if (StringUtil.isBlank(tableSpace)) {
                    alterTableNameSQL = "ALTER TABLE " + old_table_name + " RENAME TO " + new_table_name;
                } else {
                    alterTableNameSQL = "ALTER TABLE " + tableSpace + "." + old_table_name + " RENAME TO " + tableSpace + "." + new_table_name;
                }
                if (StringUtil.isBlank(alterTableNameSQL)) {
                    throw new BusinessException("修改HIVE类型的数据表名称的SQL为空!");
                }
                int execute = SqlOperator.execute(dbDataConn, alterTableNameSQL);
                if (execute != 0) {
                    throw new BusinessException("修改HIVE类型的数据表名称的SQL,执行失败!");
                }
            }
        } else if (store_type == Store_type.HBASE) {
            IHadoop hadoop = ClassBase.hadoopInstance();
            hadoop.hbaserenameTable(old_table_name, new_table_name, layerBean);
        } else if (store_type == Store_type.SOLR) {
            throw new BusinessException("重命名 SOLR 类型表配置暂未实现!!");
        } else if (store_type == Store_type.ElasticSearch) {
            throw new BusinessException("重命名 ElasticSearch 类型表配置暂未实现!!");
        } else if (store_type == Store_type.MONGODB) {
            throw new BusinessException("重命名 MONGODB 类型表配置暂未实现!!");
        } else {
            throw new BusinessException("重命名为无效表时,未找到匹配的存储层类型!");
        }
    }
}
