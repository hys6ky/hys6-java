package hyren.serv6.commons.collection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.i.IHadoop;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/7/2 0002 下午 03:41")
@Slf4j
public class CreateDataTable {

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    public static void createDataTableByStorageLayer(DatabaseWrapper db, long dsl_id, DqTableInfo dqTableInfo, List<DqTableColumn> dqTableColumns) {
        LayerBean layerBean = SqlOperator.queryOneObject(db, LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
        createDataTableByStorageLayer(db, layerBean, dqTableInfo, dqTableColumns);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    public static void createDataTableByStorageLayer(DatabaseWrapper db, LayerBean layerBean, DqTableInfo dqTableInfo, List<DqTableColumn> dqTableColumns) {
        Store_type store_type = Store_type.ofEnumByCode(layerBean.getStore_type());
        if (store_type == Store_type.DATABASE) {
            createDatabaseTable(db, layerBean, dqTableInfo, dqTableColumns);
        } else if (store_type == Store_type.HIVE) {
            createHiveTable(db, layerBean, dqTableInfo, dqTableColumns);
        } else if (store_type == Store_type.HBASE) {
            throw new BusinessException("创建 HBase 类型存储层数表，暂未实现!");
        } else if (store_type == Store_type.SOLR) {
            throw new BusinessException("创建 SOLR 类型存储层数表，暂未实现!");
        } else if (store_type == Store_type.ElasticSearch) {
            throw new BusinessException("创建 ElasticSearch 类型存储层数表，暂未实现!");
        } else if (store_type == Store_type.MONGODB) {
            throw new BusinessException("创建 MONGODB 类型存储层数表，暂未实现!");
        } else {
            throw new BusinessException("创建存储层数表时,未找到匹配的存储层类型!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    private static void createDatabaseTable(DatabaseWrapper db, LayerBean layerBean, DqTableInfo dqTableInfo, List<DqTableColumn> dqTableColumns) {
        String table_space = dqTableInfo.getTable_space();
        String table_name = dqTableInfo.getTable_name();
        DatabaseWrapper dbDataConn = null;
        try {
            dbDataConn = ConnectionTool.getDBWrapper(db, layerBean.getDsl_id());
            StringBuilder createTableSQL = new StringBuilder();
            if (StringUtil.isNotBlank(table_space)) {
                int i = dbDataConn.execute("CREATE SCHEMA IF NOT EXISTS " + table_space);
                if (i != 0) {
                    throw new BusinessException("创建表空间失败! table_space: " + table_space);
                }
            }
            if (dbDataConn.getDbtype() == Dbtype.ORACLE) {
                if (table_name.length() > 30) {
                    throw new BusinessException("oracle数据库下表名长度不能超过30位! table_name: " + table_name);
                }
            }
            tableIsExistsStorageLayer(dbDataConn, table_space, table_name);
            if (dbDataConn.getDbtype() == Dbtype.ORACLE || dbDataConn.getDbtype() == Dbtype.TERADATA || dbDataConn.getDbtype() == Dbtype.SQLSERVER) {
                createTableSQL.append("CREATE TABLE ");
            } else {
                createTableSQL.append("CREATE TABLE IF NOT EXISTS");
            }
            if (StringUtil.isNotBlank(table_space)) {
                createTableSQL.append(" ").append(table_space).append(".");
            }
            createTableSQL.append(" ").append(table_name);
            createTableSQL.append(" (");
            List<String> pk_column_s = new ArrayList<>();
            for (DqTableColumn dqTableColumn : dqTableColumns) {
                List<Map<String, Object>> dcol_info_s = SqlOperator.queryList(db, "SELECT * FROM " + DcolRelationStore.TableName + " dcol_rs" + " JOIN " + DataStoreLayerAdded.TableName + " dsl_add ON dcol_rs" + ".dslad_id=dsl_add.dslad_id " + " WHERE col_id=?", dqTableColumn.getField_id());
                for (Map<String, Object> dcol_info : dcol_info_s) {
                    StoreLayerAdded storeLayerAdded = StoreLayerAdded.ofEnumByCode(dcol_info.get("dsla_storelayer").toString());
                    if (storeLayerAdded == StoreLayerAdded.ZhuJian) {
                        pk_column_s.add(dqTableColumn.getColumn_name());
                    }
                }
                String table_column = dqTableColumn.getColumn_name();
                String column_type = dqTableColumn.getColumn_type();
                String column_length = dqTableColumn.getColumn_length();
                createTableSQL.append(table_column).append(Constant.SPACE).append(column_type);
                if (!StringUtil.isEmpty(column_length) && !column_type.equals("int") && !column_type.equals("boolean")) {
                    createTableSQL.append("(").append(column_length).append(")");
                }
                IsFlag is_null = IsFlag.ofEnumByCode(dqTableColumn.getIs_null());
                if (is_null == IsFlag.Shi) {
                    createTableSQL.append(Constant.SPACE).append("NULL");
                } else if (is_null == IsFlag.Fou) {
                    createTableSQL.append(Constant.SPACE).append("NOT NULL");
                } else {
                    throw new BusinessException("字段: column_name=" + table_column + " 的是否标记信息不合法!");
                }
                createTableSQL.append(",");
            }
            if (!pk_column_s.isEmpty()) {
                createTableSQL.append("CONSTRAINT").append(Constant.SPACE);
                createTableSQL.append(table_name).append("_PK").append(Constant.SPACE);
                createTableSQL.append("PRIMARY KEY(").append(String.join(",", pk_column_s)).append(")");
                createTableSQL.append(",");
            }
            createTableSQL.deleteCharAt(createTableSQL.length() - 1);
            createTableSQL.append(")");
            String execute_sql = String.valueOf(createTableSQL);
            log.info("执行关系型数据库创建语句,SQL内容：" + execute_sql);
            int i = dbDataConn.ExecDDL(execute_sql);
            if (i != 0) {
                log.error("指定关系型数据库存储层创建表失败! table_name: " + table_name);
                throw new BusinessException("表已经存在! table_name: " + table_name);
            }
            dbDataConn.commit();
            log.info("指定关系型数据库存储层创建表成功! table_name: " + table_name);
        } catch (Exception e) {
            if (null != dbDataConn) {
                dbDataConn.rollback();
                log.info("关系型数据库创建表时发生异常,回滚此次存储层的db操作!");
            }
            e.printStackTrace();
            throw new BusinessException("创建存储层数表发生异常!" + e.getMessage());
        } finally {
            if (null != dbDataConn) {
                dbDataConn.close();
                log.info("关闭存储层db连接成功!");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    private static void createHiveTable(DatabaseWrapper db, LayerBean layerBean, DqTableInfo dqTableInfo, List<DqTableColumn> dqTableColumns) {
        String table_space = dqTableInfo.getTable_space();
        String table_name = dqTableInfo.getTable_name();
        String table_ch_name = dqTableInfo.getCh_name();
        DatabaseWrapper dbDataConn = null;
        try {
            dbDataConn = ConnectionTool.getDBWrapper(db, layerBean.getDsl_id());
            StringBuilder createTableSQL = new StringBuilder();
            if (StringUtil.isNotBlank(table_space)) {
                int i = dbDataConn.execute("CREATE SCHEMA IF NOT EXISTS " + table_space);
                if (i != 0) {
                    throw new BusinessException("创建表空间失败! table_space: " + table_space);
                }
            }
            tableIsExistsStorageLayer(dbDataConn, table_space, table_name);
            createTableSQL.append("CREATE ");
            IsFlag is_external = IsFlag.ofEnumByCode("0");
            if (is_external == IsFlag.Shi) {
                createTableSQL.append(" EXTERNAL ");
            }
            createTableSQL.append(" TABLE IF NOT EXISTS");
            if (StringUtil.isNotBlank(table_space)) {
                createTableSQL.append(" ").append(table_space).append(".");
            }
            createTableSQL.append(" ").append(table_name);
            createTableSQL.append(" (");
            Map<String, String> partition_column_map = new HashMap<>();
            for (DqTableColumn dqTableColumn : dqTableColumns) {
                String table_column = dqTableColumn.getColumn_name();
                String table_ch_column = dqTableColumn.getField_ch_name();
                String column_type = dqTableColumn.getColumn_type();
                String column_length = dqTableColumn.getColumn_length();
                List<Map<String, Object>> dcol_info_s = SqlOperator.queryList(db, "SELECT * FROM " + DcolRelationStore.TableName + " dcol_rs" + " JOIN " + DataStoreLayerAdded.TableName + " dsl_add ON dcol_rs" + ".dslad_id=dsl_add.dslad_id " + " WHERE col_id=?", dqTableColumn.getField_id());
                for (Map<String, Object> dcol_info : dcol_info_s) {
                    StoreLayerAdded storeLayerAdded = StoreLayerAdded.ofEnumByCode(dcol_info.get("dsla_storelayer").toString());
                    if (storeLayerAdded == StoreLayerAdded.FenQuLie) {
                        if (column_type.equalsIgnoreCase("CHAR") || column_type.equalsIgnoreCase("VARCHAR")) {
                            column_type = column_type + "(" + column_length + ")";
                        }
                        column_type = column_type + " COMMENT '" + table_ch_column + "'";
                        partition_column_map.put(table_column, column_type);
                    }
                }
                if (!partition_column_map.containsKey(table_column)) {
                    createTableSQL.append(table_column).append(Constant.SPACE).append(column_type);
                    if (column_type.equalsIgnoreCase("CHAR") || column_type.equalsIgnoreCase("VARCHAR")) {
                        createTableSQL.append("(").append(column_length).append(")");
                    }
                    createTableSQL.append(" COMMENT '").append(table_ch_column).append("'");
                    createTableSQL.append(",");
                }
            }
            createTableSQL.deleteCharAt(createTableSQL.length() - 1);
            createTableSQL.append(')');
            createTableSQL.append(" COMMENT '").append(table_ch_name).append("'");
            if (!partition_column_map.isEmpty()) {
                createTableSQL.append(" PARTITIONED BY ").append("(");
                for (Map.Entry<String, String> entry : partition_column_map.entrySet()) {
                    String patition_name = entry.getKey();
                    String patition_type = entry.getValue();
                    createTableSQL.append(patition_name).append(Constant.SPACE).append(patition_type).append(',');
                }
                createTableSQL.deleteCharAt(createTableSQL.length() - 1);
                createTableSQL.append(')');
            }
            FileFormat fileFormat = FileFormat.ofEnumByCode(FileFormat.FeiDingChang.getCode());
            if (fileFormat == FileFormat.SEQUENCEFILE || fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC) {
                String hive_stored_as_type;
                if (fileFormat == FileFormat.PARQUET) {
                    hive_stored_as_type = "parquet";
                } else if (fileFormat == FileFormat.ORC) {
                    hive_stored_as_type = "orc";
                } else {
                    hive_stored_as_type = "sequencefile";
                }
                createTableSQL.append(" stored as ").append(hive_stored_as_type);
            } else if (fileFormat == FileFormat.FeiDingChang) {
                String column_separator = "|";
                createTableSQL.append(" ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH  " + "SERDEPROPERTIES (\"field.delim\"=\"").append(column_separator).append("\") stored as textfile");
            } else if (fileFormat == FileFormat.CSV) {
                createTableSQL.append(" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' stored as TEXTFILE");
            } else if (fileFormat == FileFormat.DingChang) {
                throw new BusinessException("创建Hive类型表,暂不支持定长类型");
            } else {
                throw new BusinessException("未找到匹配的存储类型! " + fileFormat);
            }
            if (is_external == IsFlag.Shi) {
                String storage_path = "/hrds/hll/test/text";
                if (StringUtil.isNotBlank(storage_path)) {
                    createTableSQL.append(" location ").append("'").append(storage_path).append("'");
                }
            }
            String execute_sql = String.valueOf(createTableSQL);
            log.info("执行Hive创建语句,SQL内容：" + execute_sql);
            int i = dbDataConn.ExecDDL(execute_sql);
            if (i != 0) {
                log.error("指定Hive数据库存储层创建表失败! table_name: " + table_name);
                throw new BusinessException("表已经存在! table_name: " + table_name);
            }
            log.info("指定Hive存储层创建表成功! table_name: " + table_name);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("创建Hive存储层数表发生异常!" + e.getMessage());
        } finally {
            if (null != dbDataConn) {
                dbDataConn.close();
                log.info("关闭Hive存储层db连接成功!");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    private static void createHBaseTable(DatabaseWrapper db, LayerBean layerBean, DqTableInfo dqTableInfo, List<DqTableColumn> dqTableColumns) {
        String name_space = dqTableInfo.getTable_space();
        String hbase_table_name = dqTableInfo.getTable_name();
        IHadoop hadoop = ClassBase.hadoopInstance();
        hadoop.createHBaseTable(name_space, hbase_table_name, layerBean);
    }

    @Method(desc = "", logicStep = "")
    private static void tableIsExistsStorageLayer(DatabaseWrapper dbDataConn, String tableSpace, String table_name) {
        boolean isExists;
        try {
            if (StringUtil.isNotBlank(tableSpace)) {
                isExists = dbDataConn.isExistTable(tableSpace + "." + table_name);
            } else {
                isExists = dbDataConn.isExistTable(table_name);
            }
            if (isExists) {
                throw new BusinessException("待创建的表在存储层中已经存在! table_name: " + table_name);
            }
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }
}
