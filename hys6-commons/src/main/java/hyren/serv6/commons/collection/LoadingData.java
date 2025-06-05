package hyren.serv6.commons.collection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.collection.bean.LayerTypeBean;
import hyren.serv6.commons.collection.bean.LoadingDataBean;
import hyren.serv6.base.exception.BusinessException;
import java.util.Map;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/4/22 0022 上午 11:25")
public class LoadingData {

    private LoadingDataBean ldbbean;

    public LoadingData(LoadingDataBean ldbean) {
        ldbbean = ldbean;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public long intoDataLayer(String sql, DatabaseWrapper db) {
        return new LoadingData(ldbbean).intoDataLayer(sql, db, null);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "intoLayerBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public long intoDataLayer(String sql, DatabaseWrapper db, LayerBean intoLayerBean) {
        long dsl_id = 0;
        LayerTypeBean ltb = ProcessingData.getAllTableIsLayer(sql, db);
        LayerBean layerBean = ltb.getLayerBean();
        String ofSql = ProcessingData.getdStoreReg(sql, db);
        LayerTypeBean.ConnType connType = ltb.getConnType();
        if (LayerTypeBean.ConnType.oneJdbc == connType) {
            dsl_id = layerBean.getDsl_id();
            try (DatabaseWrapper dbDataConn = ConnectionTool.getDBWrapper(db, dsl_id)) {
                if (dbDataConn.getDbtype() == Dbtype.DB2V1 || dbDataConn.getDbtype() == Dbtype.DB2V2) {
                    SqlOperator.execute(dbDataConn, "create table " + ldbbean.getTableName() + " AS ( SELECT * FROM (" + ofSql + ") as hyren_dqc_temp ) definition only");
                    SqlOperator.execute(dbDataConn, "insert into " + ldbbean.getTableName() + " ( SELECT * FROM ( " + ofSql + ") as hyren_dqc_temp )");
                    dbDataConn.commit();
                } else if (dbDataConn.getDbtype() == Dbtype.TERADATA) {
                    SqlOperator.execute(dbDataConn, "create table " + ldbbean.getTableName() + " AS (" + ofSql + ") WITH DATA");
                    dbDataConn.commit();
                } else if (dbDataConn.getDbtype() == Dbtype.HIVE) {
                    SqlOperator.execute(dbDataConn, "create table " + ldbbean.getTableName() + " AS " + ofSql);
                } else if (dbDataConn.getDbtype() == Dbtype.ORACLE) {
                    SqlOperator.execute(dbDataConn, "create table " + ldbbean.getTableName() + " AS " + ofSql);
                    dbDataConn.commit();
                } else if (dbDataConn.getDbtype() == Dbtype.POSTGRESQL) {
                    SqlOperator.execute(dbDataConn, "create table " + ldbbean.getTableName() + " AS " + ofSql);
                    dbDataConn.commit();
                } else {
                    throw new BusinessException(dbDataConn.getDbtype() + " 类型数据库加载数据暂未实现!");
                }
            }
        } else if (LayerTypeBean.ConnType.oneOther == connType) {
            new ProcessingData() {

                @Override
                public void dealLine(Map<String, Object> map) {
                    if (Store_type.HIVE == Store_type.ofEnumByCode(layerBean.getStore_type())) {
                        throw new BusinessException("同一层 HIVE 插入数据暂未实现!");
                    } else if (Store_type.HBASE == Store_type.ofEnumByCode(layerBean.getStore_type())) {
                        throw new BusinessException("同一层 HBASE 插入数据暂未实现!");
                    } else if (Store_type.SOLR == Store_type.ofEnumByCode(layerBean.getStore_type())) {
                        throw new BusinessException("同一层 SOLR 插入数据暂未实现!");
                    } else if (Store_type.ElasticSearch == Store_type.ofEnumByCode(layerBean.getStore_type())) {
                        throw new BusinessException("同一层 ElasticSearch 插入数据暂未实现!");
                    } else if (Store_type.MONGODB == Store_type.ofEnumByCode(layerBean.getStore_type())) {
                        throw new BusinessException("同一层 MONGODB 插入数据暂未实现!");
                    }
                }
            }.getDataLayer(sql, db);
            dsl_id = layerBean.getDsl_id();
        } else if (LayerTypeBean.ConnType.moreJdbc == connType) {
            throw new BusinessException("所有表使用JDBC,且不是同一个jdbc插入数据暂未实现!");
        } else if (LayerTypeBean.ConnType.moreOther == connType) {
            throw new BusinessException("所有表多个存储层，且不在同一个存储层jdbc插入数据暂未实现!!");
        }
        return dsl_id;
    }
}
