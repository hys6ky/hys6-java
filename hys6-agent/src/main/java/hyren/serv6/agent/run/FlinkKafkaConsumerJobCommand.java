package hyren.serv6.agent.run;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import hyren.serv6.agent.run.flink.FlinkErrorParams;
import hyren.serv6.agent.run.flink.consumer.JDBCData;
import hyren.serv6.agent.run.flink.consumer.KafkaConsumerParams;
import hyren.serv6.agent.run.flink.consumer.KafkaServiceImpl;
import hyren.serv6.agent.run.flink.consumer.MessageRunable;
import hyren.serv6.agent.run.flink.consumer.service.JDBCManager;
import hyren.serv6.agent.run.flink.consumer.service.ReadKafkaDataThread;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class FlinkKafkaConsumerJobCommand {

    private static final int threadSize = 5;

    public static void main(String[] args) {
        new SpringApplicationBuilder(FlinkKafkaConsumerJobCommand.class).web(WebApplicationType.NONE).run(args);
        if (args == null || args.length <= 0) {
            log.error("参数有误，请检查");
            log.info(FlinkErrorParams.FLINK_CDC_CONSUMER_MAIN_PARAMS_ERROR.getValue());
            System.exit(0);
        }
        Long pid = getCurrentPID();
        String taskId = args[0];
        String tableName = args[1];
        log.info("--参数 pid          : {}", pid);
        log.info("--参数 taskId       : {}", taskId);
        log.info("--参数 tableName    : {}", tableName);
        KafkaServiceImpl service = new KafkaServiceImpl();
        KafkaConsumerParams params = null;
        try {
            params = service.getKafkaParams(taskId, tableName, pid);
        } catch (RuntimeException e) {
            log.error("获取参数失败：", e);
            log.error(FlinkErrorParams.FLINK_CDC_CONSUMER_MAIN_PARAMS_ERROR.getValue());
            System.exit(0);
        }
        if (params.getJdbcDatas() == null || params.getJdbcDatas().size() <= 0) {
            log.info("未找到 JDBC 信息，停止启动");
            log.info(FlinkErrorParams.FLINK_CDC_CONSUMER_NO_JDBC.getValue());
            System.exit(0);
        }
        try {
            for (JDBCData jdbcData : params.getJdbcDatas()) {
                jdbcData.setDb(JDBCManager.getDb(jdbcData));
            }
        } catch (RuntimeException e) {
            log.error("JDBC 链接异常");
            log.error(FlinkErrorParams.FLINK_CDC_JDBC_ERROR.getValue());
            System.exit(0);
        }
        try {
            JDBCManager.initTable(params.getJdbcDatas(), params.getStorageType());
            Consumer<String, String> consumer = service.generateKafkaConsumer(params);
            log.info("订阅主题：" + params.getTopic());
            consumer.subscribe(Collections.singletonList(params.getTopic()));
            ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
            service.setRunState(taskId, tableName, pid);
            new ReadKafkaDataThread(consumer, executorService, service, params).start();
            log.info(FlinkErrorParams.FLINK_CDC_CONSUMER_STARTED.getValue());
        } catch (Exception e) {
            log.error(null, e);
            log.error(FlinkErrorParams.FLINK_CDC_CONSUMER_ERROR.getValue());
        }
    }

    private static Long getCurrentPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        long pid = Long.parseLong(processName.split("@")[0]);
        return pid;
    }
}
