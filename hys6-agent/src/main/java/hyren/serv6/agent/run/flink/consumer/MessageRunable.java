package hyren.serv6.agent.run.flink.consumer;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.run.flink.FlinkData;
import hyren.serv6.agent.run.flink.consumer.service.JDBCManager;
import hyren.serv6.base.codes.StorageType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageRunable implements Runnable {

    private static final String POSTGRES_NULL = "NULL::character varying";

    private static boolean isFirstPutIn = true;

    private KafkaServiceImpl service;

    private ConsumerRecord<String, String> record;

    private List<JDBCData> jdbcDatas;

    private StorageType storageType;

    private Long taskId;

    private String tableName;

    public MessageRunable(KafkaServiceImpl service, ConsumerRecord<String, String> record, KafkaConsumerParams params) {
        this.service = service;
        this.record = record;
        this.jdbcDatas = params.getJdbcDatas();
        this.storageType = params.getStorageType();
        this.taskId = params.getTaskId();
        this.tableName = params.getTableName();
    }

    @Override
    public void run() {
        log.info(Thread.currentThread().getName());
        log.info(record.key() + "\t" + record.value());
        FlinkData data = null;
        try {
            data = JsonUtil.toObject(record.value(), FlinkData.class);
            _NULL(data.getAfter());
            _NULL(data.getBefore());
        } catch (Exception e) {
            log.error("数据解析失败。", e);
            return;
        }
        try {
            data.verify();
        } catch (Exception e) {
            log.error("数据验证失败:" + data, e);
            return;
        }
        try {
            int num = 0;
            switch(data.getOperation()) {
                case create:
                    num = JDBCManager.insert(storageType, jdbcDatas, data.getAfter());
                    break;
                case update:
                    num = JDBCManager.update(storageType, jdbcDatas, data.getBefore(), data.getAfter());
                    break;
                case delete:
                    num = JDBCManager.delete(storageType, jdbcDatas, data.getBefore());
                    break;
                case reade:
                    num = JDBCManager.reade(storageType, jdbcDatas, data.getAfter());
                    break;
                default:
                    break;
            }
            firstPutIn(num);
        } catch (Exception e) {
            log.error("数据处理失败", e);
            return;
        }
    }

    public static void _NULL(Map<String, Object> map) {
        if (map != null) {
            for (Map.Entry<String, Object> m : map.entrySet()) {
                if (m.getValue() != null && m.getValue() instanceof String && m.getValue().equals(POSTGRES_NULL)) {
                    m.setValue(null);
                }
            }
        }
    }

    public void firstPutIn(int num) {
        if (isFirstPutIn && num > 0) {
            service.sign(taskId, tableName);
            isFirstPutIn = false;
        }
    }
}
