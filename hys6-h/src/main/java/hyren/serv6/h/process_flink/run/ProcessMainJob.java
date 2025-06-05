package hyren.serv6.h.process_flink.run;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.DataTypes.UnresolvedField;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.TopicSource;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.entity.SdmTopicInfo;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.h.process_flink.bean.FlinkColumn;
import hyren.serv6.h.process_flink.bean.FlinkDataTypesUtil;
import hyren.serv6.h.process_flink.bean.FlinkJDBCConnectParams;
import hyren.serv6.h.process_flink.bean.FlinkSqlUtil;
import hyren.serv6.h.process_flink.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process_flink.utils.ProcessTableConfBeanUtil;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;

@Slf4j
@SpringBootApplication
public class ProcessMainJob {

    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder(ProcessMainJob.class).web(WebApplicationType.NONE).run(args);
        if (args == null || args.length < 3) {
            log.info("请按照规定的格式传入参数,必须参数不能为空");
            log.info("必须参数: 参数1: 加工模型表id;参数2: 加工作业表id;参数3: 作业名;");
            System.exit(-1);
        }
        run(args[0], args[1], args[2]);
    }

    public static void run(String moduleTableId, String jobTableId, String jobName) throws Exception {
        run(moduleTableId, jobTableId, jobName, "");
    }

    public static void run(String moduleTableId, String jobTableId, String jobName, String sqlParams) throws Exception {
        log.info("--参数 moduleTableId : {}", moduleTableId);
        log.info("--参数 jobTableId    : {}", jobTableId);
        log.info("--参数 jobName       : {}", jobName);
        log.info("--参数 sqlParams     : {}", sqlParams);
        ProcessJobTableConfBean processJobTableConfBean = ProcessTableConfBeanUtil.getProcessTableConfBean(moduleTableId, jobTableId, jobName, sqlParams);
        createTable(processJobTableConfBean);
        runKafka(processJobTableConfBean);
    }

    private static void createTable(ProcessJobTableConfBean processJobTableConfBean) throws Exception {
        if (processJobTableConfBean.getPrimaryKeyInfos() == null || processJobTableConfBean.getPrimaryKeyInfos().size() <= 0) {
            throw new BusinessException("模型表必须有主键");
        }
        Map<String, String> attrMap = processJobTableConfBean.getDataStoreLayerAttrs().stream().collect(Collectors.toMap(DataStoreLayerAttr::getStorage_property_key, DataStoreLayerAttr::getStorage_property_val));
        try (DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(attrMap)) {
            StringJoiner column_type_sql_Joiner = new StringJoiner(",");
            for (DmJobTableFieldInfo field : processJobTableConfBean.getDmJobTableFieldInfos()) {
                String sql = field.getJobtab_field_en_name() + " " + field.getJobtab_field_type();
                String fieldLength = field.getJobtab_field_length();
                if (StringUtil.isNotBlank(fieldLength)) {
                    sql = sql + "(" + fieldLength + ")";
                }
                column_type_sql_Joiner.add(sql);
            }
            String createTableColumnTypes = column_type_sql_Joiner.toString();
            StringBuilder createTableSql = new StringBuilder();
            createTableSql.append("CREATE TABLE IF NOT EXISTS ");
            createTableSql.append(processJobTableConfBean.getDmModuleTable().getModule_table_en_name());
            createTableSql.append("(");
            createTableSql.append(createTableColumnTypes);
            createTableSql.append(",CONSTRAINT " + processJobTableConfBean.getDmModuleTable().getModule_table_en_name() + "_pk PRIMARY KEY (" + processJobTableConfBean.getPrimaryKeyInfos().stream().collect(Collectors.joining(",")) + ")");
            createTableSql.append(")");
            String sql = createTableSql.toString();
            dbWrapper.ExecDDL(sql);
        } catch (Exception e) {
            throw new Exception("创建模型表失败：", e);
        }
    }

    private static void runKafka(ProcessJobTableConfBean processJobTableConfBean) {
        String sql = processJobTableConfBean.getCompleteSql();
        String jobNameParam = processJobTableConfBean.getJobNameParam();
        String moduleTableName = processJobTableConfBean.getDmModuleTable().getModule_table_en_name();
        FlinkJDBCConnectParams moduleKafkaJdbc = new FlinkJDBCConnectParams();
        List<FlinkColumn> moduleColumns = null;
        Map<String, FlinkJDBCConnectParams> flinkTableJdbcs = new HashMap<String, FlinkJDBCConnectParams>();
        Map<String, SdmTopicInfo> flinkKafkaTopic = processJobTableConfBean.getTopicBeansByTableMap();
        Map<String, List<FlinkColumn>> flinkTableColumns = new HashMap<String, List<FlinkColumn>>();
        Map<String, List<FlinkColumn>> flinkKafkaColumns = new HashMap<String, List<FlinkColumn>>();
        List<DataStoreLayerAttr> dataStoreLayerAttrs = processJobTableConfBean.getDataStoreLayerAttrs();
        for (DataStoreLayerAttr attr : dataStoreLayerAttrs) {
            if ("jdbc_url".equals(attr.getStorage_property_key())) {
                moduleKafkaJdbc.setUrl(attr.getStorage_property_val());
            } else if ("user_name".equals(attr.getStorage_property_key())) {
                moduleKafkaJdbc.setUsername(attr.getStorage_property_val());
            } else if ("database_pwd".equals(attr.getStorage_property_key())) {
                moduleKafkaJdbc.setPwd(attr.getStorage_property_val());
            }
        }
        moduleKafkaJdbc.setTableName(processJobTableConfBean.getDmModuleTable().getModule_table_en_name());
        moduleColumns = processJobTableConfBean.getDmJobTableFieldInfos().stream().map(column -> {
            String columnName = column.getJobtab_field_en_name();
            DataType dataType = FlinkDataTypesUtil.createDataType(column.getJobtab_field_type());
            boolean isPrimary = processJobTableConfBean.getPrimaryKeyInfos().indexOf(column.getJobtab_field_en_name()) >= 0;
            FlinkColumn c = new FlinkColumn(columnName, dataType, isPrimary);
            return c;
        }).collect(Collectors.toList());
        Map<String, List<LayerBean>> layerBeansByTableMap = processJobTableConfBean.getLayerBeansByTableMap();
        for (Map.Entry<String, List<LayerBean>> entry : layerBeansByTableMap.entrySet()) {
            FlinkJDBCConnectParams params = new FlinkJDBCConnectParams();
            String tableName = entry.getKey();
            List<LayerBean> layerBeans = entry.getValue();
            LayerBean layerBean = layerBeans.get(0);
            Map<String, String> layerAttr = layerBean.getLayerAttr();
            for (Map.Entry<String, String> attr : layerAttr.entrySet()) {
                if ("jdbc_url".equals(attr.getKey())) {
                    params.setUrl(attr.getValue());
                } else if ("user_name".equals(attr.getKey())) {
                    params.setUsername(attr.getValue());
                } else if ("database_pwd".equals(attr.getKey())) {
                    params.setPwd(attr.getValue());
                }
            }
            params.setTableName(tableName);
            flinkTableJdbcs.put(tableName, params);
            List<Map<String, Object>> columnByTableName = DataTableUtil.getColumnByTableName(tableName);
            List<FlinkColumn> column = columnByTableName.stream().map(co -> {
                return new FlinkColumn(co.get("column_name").toString(), FlinkDataTypesUtil.createDataType(co.get("data_type").toString()), Boolean.parseBoolean(co.get("is_primary_key").toString()));
            }).collect(Collectors.toList());
            flinkTableColumns.put(tableName, column);
        }
        if (processJobTableConfBean.getTopicColumnMap() != null && processJobTableConfBean.getTopicColumnMap().size() > 0) {
            for (Entry<String, List<TableColumn>> entry : processJobTableConfBean.getTopicColumnMap().entrySet()) {
                List<TableColumn> tableColumns = entry.getValue();
                List<FlinkColumn> topicColumns = tableColumns.stream().map(c -> new FlinkColumn(c.getColumn_name(), FlinkDataTypesUtil.createDataType(c.getColumn_type()), IsFlag.Shi == IsFlag.ofEnumByCode(c.getIs_primary_key()))).collect(Collectors.toList());
                flinkKafkaColumns.put(entry.getKey(), topicColumns);
            }
        }
        runKafka(sql, moduleTableName, moduleKafkaJdbc, moduleColumns, flinkTableJdbcs, flinkKafkaTopic, flinkTableColumns, flinkKafkaColumns, jobNameParam);
    }

    private static void runKafka(String sql, String moduleTableName, FlinkJDBCConnectParams moduleJdbc, List<FlinkColumn> moduleColumns, Map<String, FlinkJDBCConnectParams> flinkTableJdbcs, Map<String, SdmTopicInfo> flinkKafkaTopic, Map<String, List<FlinkColumn>> flinkTableColumns, Map<String, List<FlinkColumn>> flinkKafkaColumns, String jobNameParam) {
        List<String> tableNames = flinkTableJdbcs.entrySet().stream().map(table -> table.getKey()).collect(Collectors.toList());
        List<String> topicNames = flinkKafkaTopic.entrySet().stream().map(topic -> topic.getKey()).collect(Collectors.toList());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer", "DROP");
        for (String topic : topicNames) {
            SdmTopicInfo sdmTopicInfo = flinkKafkaTopic.get(topic);
            if (TopicSource.Default == TopicSource.ofEnumByCode(sdmTopicInfo.getTopic_source())) {
                List<FlinkColumn> columns = flinkKafkaColumns.get(topic);
                Builder schemaBuilder = Schema.newBuilder();
                for (FlinkColumn column : columns) {
                    schemaBuilder.column(column.getName(), column.getType());
                }
                TableDescriptor tableDescriptor = TableDescriptor.forConnector("kafka").format(FormatDescriptor.forFormat("json").option(JsonFormatOptions.IGNORE_PARSE_ERRORS, true).build()).schema(schemaBuilder.build()).option("topic", sdmTopicInfo.getSdm_top_name()).option("properties.bootstrap.servers", sdmTopicInfo.getSdm_zk_host()).option("scan.startup.mode", "latest-offset").build();
                tEnv.createTemporaryTable(topic, tableDescriptor);
            } else if (TopicSource.CDC == TopicSource.ofEnumByCode(sdmTopicInfo.getTopic_source())) {
                List<FlinkColumn> columns = flinkKafkaColumns.get(topic);
                List<Field> fields = new ArrayList<DataTypes.Field>();
                Builder cdcSchemaBuilder = Schema.newBuilder();
                for (FlinkColumn column : columns) {
                    fields.add(DataTypes.FIELD(column.getName(), column.getType()));
                }
                cdcSchemaBuilder.column("after", DataTypes.ROW(fields)).column("before", DataTypes.ROW()).column("source", DataTypes.ROW()).column("op", DataTypes.STRING()).column("ts_ms", DataTypes.BIGINT()).column("transaction", DataTypes.ROW());
                TableDescriptor cdcTableDescriptor = TableDescriptor.forConnector("kafka").format(FormatDescriptor.forFormat("json").option(JsonFormatOptions.IGNORE_PARSE_ERRORS, true).build()).schema(cdcSchemaBuilder.build()).option("topic", sdmTopicInfo.getSdm_top_name()).option("properties.bootstrap.servers", sdmTopicInfo.getSdm_bstp_serv()).option("scan.startup.mode", "latest-offset").build();
                tEnv.createTemporaryTable(topic, cdcTableDescriptor);
                for (FlinkColumn column : columns) {
                    sql = StringUtil.replace(sql, topic + "." + column.getName(), topic + ".after." + column.getName());
                }
            }
        }
        for (String table : tableNames) {
            FlinkJDBCConnectParams jdbc = flinkTableJdbcs.get(table);
            List<FlinkColumn> columns = flinkTableColumns.get(table);
            Builder schemaBuilder = Schema.newBuilder();
            for (FlinkColumn column : columns) {
                if (column.isPrimary()) {
                    schemaBuilder.column(column.getName(), column.getType().notNull()).primaryKey(column.getName());
                } else {
                    schemaBuilder.column(column.getName(), column.getType());
                }
            }
            TableDescriptor tableDescriptor = TableDescriptor.forConnector("jdbc").schema(schemaBuilder.build()).option("url", jdbc.getUrl()).option("table-name", jdbc.getTableName()).option("username", jdbc.getUsername()).option("password", jdbc.getPwd()).build();
            tEnv.createTemporaryTable(table, tableDescriptor);
        }
        Builder schemaBuilder = Schema.newBuilder();
        for (FlinkColumn field : moduleColumns) {
            if (field.isPrimary()) {
                schemaBuilder.column(field.getName(), field.getType().notNull()).primaryKey(field.getName());
            } else {
                schemaBuilder.column(field.getName(), field.getType());
            }
        }
        TableDescriptor tableDescriptor = TableDescriptor.forConnector("jdbc").schema(schemaBuilder.build()).option("url", moduleJdbc.getUrl()).option("table-name", moduleJdbc.getTableName()).option("username", moduleJdbc.getUsername()).option("password", moduleJdbc.getPwd()).build();
        tEnv.createTemporaryTable(moduleTableName, tableDescriptor);
        sql = FlinkSqlUtil.addDefaultValueColumn(sql, Constant._HYREN_JOB_NAME, jobNameParam);
        for (String topic : topicNames) {
            try {
                String tableName = FlinkSqlUtil.getSimpleTableName(sql, topic);
                sql = FlinkSqlUtil.addWhere(sql, "(" + tableName + ".op='c' or " + tableName + ".op='u')");
            } catch (JSQLParserException e) {
                e.printStackTrace();
            }
        }
        tEnv.sqlQuery(sql).execute().print();
        sql = "insert into " + moduleTableName + " select * from ( " + sql + ") ";
        tEnv.executeSql(sql);
        try {
            env.execute("flinkJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
