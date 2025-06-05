package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.script.Invocable;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerRunable implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private KafkaConsumer<String, byte[]> consumer;

    @SuppressWarnings("rawtypes")
    private ConsumerBusinessProcess businessProcess = null;

    private Map<String, Object> jsonParm;

    private Map<String, Object> jsonsdm;

    public long fileCompressStartTime = System.currentTimeMillis();

    private CountDownLatch threadSignal;

    private Boolean AUTO_OFFSET_RESET;

    private Invocable invocable = null;

    private long poll = 1000;

    public static Long endTime = 9999999999999L;

    public KafkaConsumerRunable() {
    }

    public KafkaConsumerRunable(CountDownLatch threadSignal, Map<String, Object> jsonParm, Map<String, Object> jsonsdm, Map<String, Object> jsonStore) {
        this.threadSignal = threadSignal;
        this.jsonParm = jsonParm;
        this.jsonsdm = jsonsdm;
        ConsumerUtil consumerUtil;
        long startTime = Long.parseLong(jsonParm.get("startTime").toString());
        if (startTime > 0) {
            endTime = Long.parseLong(jsonParm.get("endTime").toString());
            consumerUtil = new ConsumerUtil(jsonParm, jsonsdm, jsonStore, startTime);
        } else {
            consumerUtil = new ConsumerUtil(jsonParm, jsonsdm, jsonStore);
        }
        this.consumer = consumerUtil.consumer;
        this.businessProcess = consumerUtil.businessProcess;
        this.AUTO_OFFSET_RESET = Boolean.parseBoolean(jsonParm.get("enable_auto_commit").toString());
        this.poll = Long.parseLong(jsonParm.get("poll").toString());
        this.invocable = consumerUtil.invocable;
    }

    public KafkaConsumerRunable(CountDownLatch threadSignal, Map<String, Object> jsonParm, Map<String, Object> jsonsdm, Map<String, Object> jsonStore, int partition) {
        this.threadSignal = threadSignal;
        this.jsonParm = jsonParm;
        this.jsonsdm = jsonsdm;
        ConsumerUtil consumerUtil;
        long startTime = Long.parseLong(jsonParm.get("startTime").toString());
        if (startTime > 0) {
            endTime = Long.parseLong(jsonParm.get("endTime").toString());
            consumerUtil = new ConsumerUtil(jsonParm, jsonsdm, jsonStore, partition, startTime);
        } else {
            consumerUtil = new ConsumerUtil(jsonParm, jsonsdm, jsonStore, partition);
        }
        this.consumer = consumerUtil.consumer;
        this.businessProcess = consumerUtil.businessProcess;
        this.AUTO_OFFSET_RESET = Boolean.parseBoolean(jsonParm.get("enable_auto_commit").toString());
        this.poll = Long.parseLong(jsonParm.get("poll").toString());
        this.invocable = consumerUtil.invocable;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        try {
            Thread.sleep(2000);
            logger.info("开始消费Kafka数据！！！");
            int num;
            while (ConsumerSelector.flag) {
                ConsumerRecords<String, byte[]> records = consumer.poll(poll);
                logger.info("单次拉取数据条数：" + records.count());
                for (TopicPartition tp : records.partitions()) {
                    List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(tp);
                    if (!partitionRecords.isEmpty()) {
                        if (invocable == null) {
                            num = businessProcess.processOrder(partitionRecords, tp.partition(), jsonParm);
                        } else {
                            num = (int) invocable.invokeFunction("consumerFunction", partitionRecords, tp.partition());
                        }
                        if (AUTO_OFFSET_RESET) {
                            long lastOffset = partitionRecords.get(partitionRecords.size() - num - 1).offset();
                            consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(lastOffset + 1)));
                        }
                    }
                }
            }
            if (businessProcess.toString().contains("FileOperator")) {
                JsonProps jsonProps = new JsonProps();
                Map<String, Object> jsonCompress = jsonProps.jsonProperties("compress.properties");
                if ("0".equals(jsonCompress.get("iscompress").toString())) {
                    Map<String, Object> jsonFile = JsonUtil.toObject(JsonUtil.toJson(jsonsdm.get("params")), new TypeReference<Map<String, Object>>() {
                    });
                    String path = jsonFile.get("filePath").toString() + File.separator + jsonFile.get("fileName").toString();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
                    String pathRename = path + "-" + sdf.format(fileCompressStartTime) + ".dat";
                    File file = new File(path);
                    if (file.exists()) {
                        File fileRename = new File(pathRename);
                        if (!file.renameTo(fileRename)) {
                            throw new BusinessException("重命名文件: " + file.getAbsolutePath() + " 为 " + fileRename.getAbsolutePath() + "失败!");
                        }
                        FileGZIP.gZip(fileRename, jsonFile.get("filePath").toString(), jsonCompress.get("compressFileName").toString());
                    }
                }
            }
        } catch (Exception e) {
            logger.error(logger, e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
            stop();
            Thread.currentThread().interrupt();
            threadSignal.countDown();
        }
    }

    public void stop() {
        ConsumerSelector.flag = false;
    }
}
