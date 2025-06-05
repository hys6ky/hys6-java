package hyren.serv6.agent.run.flink.consumer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.trogdor.agent.AgentClient;
import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaServiceImpl {

    public Consumer<String, String> generateKafkaConsumer(KafkaConsumerParams params) {
        Properties props = new Properties();
        props.put("bootstrap.servers", params.getKafka_servers());
        props.put("group.id", params.getGroup_id());
        props.put("key.deserializer", params.getKey_deserializer());
        props.put("value.deserializer", params.getValue_deserializer());
        log.info("创建kafka消费者实例");
        log.info(props.toString());
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public KafkaConsumerParams getKafkaParams(String taskId, String tableName, Long pId) {
        String url = AgentActionUtil.getServerUrl(AgentActionUtil.CDC_SYNC_GETPARAM);
        log.info("远程请求：" + url);
        HttpClient.ResponseValue resValue = new HttpClient().addData("taskId", taskId).addData("tableName", tableName).post(url);
        ActionResult actionResult = ActionResult.toActionResult(resValue.getBodyString());
        if (!actionResult.isSuccess()) {
            log.error("远程获取参数失败：" + actionResult.getMessage());
        }
        String json = JsonUtil.toJson(actionResult.getData());
        KafkaConsumerParams param = JsonUtil.toObject(json, new TypeReference<KafkaConsumerParams>() {
        });
        param.setTableName(tableName);
        return param;
    }

    public void setRunState(String taskId, String tableName, Long pId) {
        Boolean isRun = this.updateFlinkCDCInfo(AgentActionUtil.getServerUrl(AgentActionUtil.CDC_SYNC_RUNSTATE), taskId, tableName, pId);
        if (!isRun) {
            throw new RuntimeException("远程更新状态失败，后续无法获取程序运行状态");
        }
    }

    public void setFailedState(String taskId, String tableName, Long pId) {
        this.updateFlinkCDCInfo(AgentActionUtil.getServerUrl(AgentActionUtil.CDC_SYNC_FAILEDSTATE), taskId, tableName, pId);
    }

    public void sign(Long taskId, String tableName) {
        String url = AgentActionUtil.getServerUrl(AgentActionUtil.CDC_ADDDATASTOREREG);
        log.info("远程请求：" + url);
        String date = DateUtil.getSysDate();
        String time = DateUtil.getSysTime();
        HttpClient.ResponseValue resValue = new HttpClient().addData("taskId", taskId).addData("tableName", tableName).addData("date", date).addData("time", time).post(url);
        ActionResult actionResult = ActionResult.toActionResult(resValue.getBodyString());
        if (!Boolean.parseBoolean(actionResult.getData().toString())) {
            log.error("新增数据存储登记信息失败");
        }
        return;
    }

    public Boolean updateFlinkCDCInfo(String url, String taskId, String tableName, Long pid) {
        log.info("远程请求：" + url);
        String date = DateUtil.getSysDate();
        String time = DateUtil.getSysTime();
        HttpClient.ResponseValue resValue = new HttpClient().addData("taskId", taskId).addData("tableNames", tableName).addData("pid", pid).addData("date", date).addData("time", time).post(url);
        ActionResult actionResult = ActionResult.toActionResult(resValue.getBodyString());
        if (actionResult.isSuccess()) {
            long size = Long.parseLong(actionResult.getData().toString());
            if (size > 0) {
                log.info("[flink-cdc] update-flink-info-successed");
                return true;
            }
        } else {
            log.info("[flink-cdc] update-flink-info-failed");
        }
        return false;
    }
}
