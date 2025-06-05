package hyren.serv6.agent.run.flink.producer;

import static org.junit.Assert.assertThrows;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.servlet.http.HttpUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.connectors.db2.Db2Source;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import antlr.StringUtils;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.run.flink.FlinkErrorParams;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.entity.TableCdcJobInfo;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.constant.DataBaseType;
import lombok.extern.slf4j.Slf4j;
import scala.annotation.meta.param;

@Slf4j
public class FlinkServiceImpl {

    private static String Config_F = "/home/moyv/Documents/temp";

    private static String FILE_SUFFIX = "_p";

    public void createListener(StreamExecutionEnvironment env, FlinkProducerParams params, String sourceName, String sinkName) {
        SinkFunction<String> sink = this.generateRichSink(params);
        Properties properties = new Properties();
        switch(params.getDatabase_type()) {
            case MYSQL:
                properties.setProperty("useSSL", "false");
                properties.setProperty("createDatabaseIfNotExist", "true");
                Source<String, ?, ?> source = MySqlSource.<String>builder().hostname(params.getDatabase_ip()).port(params.getDatabase_port()).databaseList(params.getDatabase_name()).tableList(this.getTableNames(params)).username(params.getDatabase_username()).password(params.getDatabase_password()).deserializer(new JsonDebeziumDeserializationSchema()).jdbcProperties(properties).build();
                DataStreamSource<String> fromSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName);
                fromSource.addSink(sink).name(sinkName);
                return;
            case Postgresql:
                DebeziumSourceFunction<String> postgresSourceFunction = PostgreSQLSource.<String>builder().hostname(params.getDatabase_ip()).port(params.getDatabase_port()).database(params.getDatabase_name()).schemaList("public").tableList(this.getTableNames(params)).username(params.getDatabase_username()).password(params.getDatabase_password()).deserializer(new JsonDebeziumDeserializationSchema()).debeziumProperties(properties).decodingPluginName("pgoutput").build();
                env.addSource(postgresSourceFunction).addSink(sink).name(sinkName);
                return;
            case SqlServer:
                DebeziumSourceFunction<String> sqlserverSourceFunction = SqlServerSource.<String>builder().hostname(params.getDatabase_ip()).port(params.getDatabase_port()).database(params.getDatabase_name()).tableList(this.getTableNames(params)).username(params.getDatabase_username()).password(params.getDatabase_password()).deserializer(new JsonDebeziumDeserializationSchema()).build();
                env.addSource(sqlserverSourceFunction).addSink(sink).name(sinkName);
                return;
            case DB2:
                SourceFunction<String> db2Source = Db2Source.<String>builder().hostname(params.getDatabase_ip()).port(params.getDatabase_port()).database(params.getDatabase_name()).tableList(this.getTableNames(params)).username(params.getDatabase_username()).password(params.getDatabase_password()).deserializer(new JsonDebeziumDeserializationSchema()).build();
                env.addSource(db2Source).addSink(sink);
                return;
            case ORACLE9I:
            case Oracle10:
                SourceFunction<String> oracleSourceFunction = OracleSource.<String>builder().hostname(params.getDatabase_ip()).port(params.getDatabase_port()).database(params.getDatabase_name()).schemaList(params.getDatabase_schema()).tableList(this.getTableNames(params)).username(params.getDatabase_username()).password(params.getDatabase_password()).deserializer(new JsonDebeziumDeserializationSchema()).build();
                env.enableCheckpointing(1000);
                env.addSource(oracleSourceFunction).addSink(sink).name(sinkName);
                return;
            default:
                break;
        }
    }

    private String[] getTableNames(FlinkProducerParams params) {
        DataBaseType database_type = params.getDatabase_type();
        List<FlinkCDCTable> tables = params.getTables();
        if (database_type == DataBaseType.MYSQL) {
            String database_name = params.getDatabase_name();
            String[] tableNames = tables.stream().map(table -> database_name + "." + table.getTable_name()).toArray(String[]::new);
            return tableNames;
        } else if (database_type == DataBaseType.Postgresql) {
            String schema = StringUtil.isEmpty(params.getDatabase_schema()) ? "public" : params.getDatabase_schema();
            String[] tableNames = tables.stream().map(table -> schema + "." + table.getTable_name()).toArray(String[]::new);
            return tableNames;
        } else if (database_type == DataBaseType.SqlServer) {
            String schema = StringUtil.isEmpty(params.getDatabase_schema()) ? "dbo" : params.getDatabase_schema();
            String[] tableNames = tables.stream().map(table -> schema + "." + table.getTable_name()).toArray(String[]::new);
            return tableNames;
        } else if (database_type == DataBaseType.DB2) {
            String database_name = params.getDatabase_name();
            String[] tableNames = tables.stream().map(table -> database_name + "." + table.getTable_name()).toArray(String[]::new);
            return tableNames;
        } else if (database_type == DataBaseType.ORACLE9I || database_type == DataBaseType.Oracle10) {
            String database_name = params.getDatabase_name();
            String[] tableNames = tables.stream().map(table -> database_name + "." + table.getTable_name()).toArray(String[]::new);
            return tableNames;
        }
        throw new RuntimeException("不支持此类型数据库，无法获取其全表名：" + database_type);
    }

    public StreamExecutionEnvironment generateEnvironment(FlinkProducerParams params) throws URISyntaxException {
        String lastJobId = params.getLastJobId();
        Configuration config = new Configuration();
        if (StringUtil.isEmpty(params.getCheckpoint_uri())) {
            String urserDir = "file://" + System.getProperty("user.dir") + File.separatorChar + "checkpoint";
            params.setCheckpoint_uri(urserDir);
        }
        log.info("当前检查点：" + params.getCheckpoint_uri());
        URI uri = new URI(params.getCheckpoint_uri());
        if (!StringUtil.isEmpty(lastJobId)) {
            File uriFile = new File(new URI(uri.toString() + File.separator + params.getLastJobId()));
            if (uriFile.isDirectory()) {
                File[] files = uriFile.listFiles();
                long maxN = 0;
                File metaFile = null;
                for (File f : files) {
                    String str = "chk-";
                    if (f.getName().indexOf(str) == 0) {
                        long n = Long.parseLong(f.getName().substring(f.getName().indexOf(str) + str.length()));
                        if (n > maxN) {
                            maxN = n;
                            metaFile = f;
                        }
                    }
                }
                if (metaFile != null) {
                    config.setString(SavepointConfigOptions.SAVEPOINT_PATH, metaFile.toURI().toString());
                }
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(uri);
        return env;
    }

    public RichSinkFunctionKafka generateRichSink(FlinkProducerParams params) {
        return new RichSinkFunctionKafka(params);
    }

    public FlinkProducerParams getFlinkParams(String seed) {
        String paramsStr = FileUtil.readFile2String(new File(Config_F + File.separator + seed + FILE_SUFFIX));
        FlinkProducerParams params = JsonUtil.toObject(paramsStr, FlinkProducerParams.class);
        return params;
    }

    public FlinkProducerParams getFlinkParams(Long taskId, String tableNames) {
        String url = AgentActionUtil.getServerUrl(AgentActionUtil.CDC_COLLECT_GETPARAM);
        HttpClient client = new HttpClient().addData("taskId", taskId).addData("tableNames", tableNames);
        HttpClient.ResponseValue resValue = client.post(url);
        log.info("远程请求：" + url + ":" + client.bodyData);
        ActionResult actionResult = ActionResult.toActionResult(resValue.getBodyString());
        if (!actionResult.isSuccess()) {
            throw new RuntimeException("远程请求失败：" + actionResult.getMessage());
        }
        FlinkProducerParams params = JsonUtil.toObject(JsonUtil.toJson(actionResult.getData()), FlinkProducerParams.class);
        return params;
    }

    public void setRunState(Long taskId, String tableName, Long pId, String jobId) {
        Boolean isRun = this.updateFlinkCDCInfo(AgentActionUtil.getServerUrl(AgentActionUtil.CDC_COLLECT_RUNSTATE), taskId, tableName, pId, jobId);
    }

    public void setFailedState(Long taskId, String tableName, Long pId, String jobId) {
        this.updateFlinkCDCInfo(AgentActionUtil.getServerUrl(AgentActionUtil.CDC_COLLECT_FAILEDSTATE), taskId, tableName, pId, jobId);
    }

    public Boolean updateFlinkCDCInfo(String url, Long taskId, String tableNames, Long pid, String jobId) {
        log.info("远程请求：" + url);
        String date = DateUtil.getSysDate();
        String time = DateUtil.getSysTime();
        HttpClient.ResponseValue resValue = new HttpClient().addData("taskId", taskId).addData("tableNames", tableNames).addData("pid", pid).addData("jobId", jobId).addData("date", date).addData("time", time).post(url);
        ActionResult actionResult = ActionResult.toActionResult(resValue.getBodyString());
        if (actionResult.isSuccess()) {
            if (Boolean.TRUE.equals(actionResult.getData())) {
                log.info("[flink-cdc] update-flink-info-successed");
                return true;
            }
        } else {
            log.info("[flink-cdc] update-flink-info-failed");
        }
        return false;
    }
}
