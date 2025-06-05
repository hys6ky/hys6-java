package hyren.serv6.r.util;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DfTableColumn;
import hyren.serv6.base.entity.DqTableInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTable {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    public static void createDataTableByStorageLayer(DatabaseWrapper dbDataConn, LayerBean layerBean, DqTableInfo dqTableInfo, List<DfTableColumn> dfTableColumns) {
        createDataTableByDataLayer(dbDataConn, layerBean, dqTableInfo, dfTableColumns);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    public static void createDataTableByDataLayer(DatabaseWrapper dbDataConn, LayerBean layerBean, DqTableInfo dqTableInfo, List<DfTableColumn> dfTableColumns) {
        Store_type store_type = Store_type.ofEnumByCode(layerBean.getStore_type());
        if (store_type == Store_type.DATABASE) {
            createDatabaseTable(dbDataConn, dqTableInfo, dfTableColumns);
        } else if (store_type == Store_type.HIVE) {
            throw new BusinessException("创建 Hive 类型存储层数表，暂未实现!");
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
    private static void createDatabaseTable(DatabaseWrapper dbDataConn, DqTableInfo dqTableInfo, List<DfTableColumn> dfTableColumns) {
        String table_space = dqTableInfo.getTable_space();
        String table_name = dqTableInfo.getTable_name();
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
        for (DfTableColumn dfTableColumn : dfTableColumns) {
            if (dfTableColumn.getIs_primarykey().equals("1")) {
                pk_column_s.add(dfTableColumn.getCol_name());
            }
            String table_column = dfTableColumn.getCol_name();
            String column_type = dfTableColumn.getCol_type();
            createTableSQL.append(table_column).append(Constant.SPACE).append(column_type);
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
        logger.info("执行关系型数据库创建语句,SQL内容：" + execute_sql);
        int i = dbDataConn.ExecDDL(execute_sql);
        if (i != 0) {
            logger.error("指定关系型数据库存储层创建表失败! table_name: " + table_name);
            throw new BusinessException("表已经存在! table_name: " + table_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Param(name = "dqTableInfo", desc = "", range = "")
    @Param(name = "dqTableColumns", desc = "", range = "")
    private static void createHiveTable(DatabaseWrapper db, LayerBean layerBean, DqTableInfo dqTableInfo, List<DfTableColumn> dfTableColumns) {
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
            for (DfTableColumn dfTableColumn : dfTableColumns) {
                String table_column = dfTableColumn.getCol_name();
                String table_ch_column = dfTableColumn.getCol_ch_name();
                String column_type = dfTableColumn.getCol_type();
                if (!partition_column_map.containsKey(table_column)) {
                    createTableSQL.append(table_column).append(Constant.SPACE).append(column_type);
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
            logger.info("执行Hive创建语句,SQL内容：" + execute_sql);
            int i = dbDataConn.ExecDDL(execute_sql);
            if (i != 0) {
                logger.error("指定Hive数据库存储层创建表失败! table_name: " + table_name);
                throw new BusinessException("表已经存在! table_name: " + table_name);
            }
            logger.info("指定Hive存储层创建表成功! table_name: " + table_name);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("创建Hive存储层数表发生异常!" + e.getMessage());
        } finally {
            if (null != dbDataConn) {
                dbDataConn.close();
                logger.info("关闭Hive存储层db连接成功!");
            }
        }
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
            logger.info(e.getMessage());
        }
    }
}
