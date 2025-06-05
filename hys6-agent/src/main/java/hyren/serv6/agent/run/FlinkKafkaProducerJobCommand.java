package hyren.serv6.agent.run;

import java.net.URISyntaxException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import hyren.serv6.agent.run.flink.FlinkErrorParams;
import hyren.serv6.agent.run.flink.producer.FlinkProducerParams;
import hyren.serv6.agent.run.flink.producer.FlinkServiceImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class FlinkKafkaProducerJobCommand {

    public static void main(String[] args) {
        new SpringApplicationBuilder(FlinkKafkaConsumerJobCommand.class).web(WebApplicationType.NONE).run(args);
        if (args == null || args.length <= 0) {
            log.error("参数异常，请检查");
            log.error(FlinkErrorParams.FLINK_CDC_PRODUCER_MAIN_PARAMS_ERROR.getValue());
            System.exit(0);
        }
        Long pid = getCurrentPID();
        Long taskId = Long.parseLong(args[0].toString());
        String tableNames = args[1];
        String[] tableNameArr = tableNames.split(",");
        log.info("--参数 pid          : {}", pid);
        log.info("--参数 taskId       : {}", taskId);
        log.info("--参数 tableNames    : {}", tableNames);
        FlinkServiceImpl service = new FlinkServiceImpl();
        FlinkProducerParams params = null;
        try {
            params = service.getFlinkParams(taskId, tableNames);
        } catch (RuntimeException e) {
            log.error("获取参数失败：", e);
            log.error(FlinkErrorParams.FLINK_CDC_PRODUCER_GET_PARAMS_FAIL.getValue());
            System.exit(0);
        }
        try {
            StreamExecutionEnvironment env = service.generateEnvironment(params);
            service.createListener(env, params, "flink_source", "1");
            JobClient jobClient = env.executeAsync("Flink CDC Example");
            String currentJobId = jobClient.getJobID().toString();
            service.setRunState(taskId, tableNames, pid, currentJobId);
            log.info(FlinkErrorParams.FLINK_CDC_PRODUCER_STARTED.getValue());
        } catch (URISyntaxException e) {
            log.error("检查点地址错误", e);
            log.error(FlinkErrorParams.FLINK_CDC_PRODUCER_CHECKPOINT_URL_IS_ERROR.getValue());
        } catch (Exception e) {
            log.error("执行任务失败", e);
            log.info(FlinkErrorParams.FLINK_CDC_PRODUCER_ERROR.getValue());
        }
    }

    private static Long getCurrentPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        long pid = Long.parseLong(processName.split("@")[0]);
        return pid;
    }
}
