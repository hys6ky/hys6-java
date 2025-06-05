package hyren.serv6.stream.agent.producer.run;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.stream.agent.producer.avro.file.dir.DirListenerReader;
import hyren.serv6.stream.agent.producer.avro.file.bigFile.SplitFileToKafka;
import hyren.serv6.stream.agent.producer.avro.rest.ServerToProducer;
import hyren.serv6.stream.agent.producer.string.file.bigFile.SplitFileToKafkaString;
import hyren.serv6.stream.agent.producer.string.file.dirString.DirListenerReaderString;
import hyren.serv6.stream.agent.producer.string.file.file.FileReadAllroundString;
import hyren.serv6.stream.agent.producer.string.rest.ServerToProducerString;
import hyren.serv6.stream.agent.util.FileReadAllround;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@SpringBootApplication
public class StreamProducerJobCommand {

    private static final Logger logger = LogManager.getLogger();

    private static String address;

    private static String port;

    @Value("${management.server.address}")
    public void setAddress(String address) {
        StreamProducerJobCommand.address = address;
    }

    @Value("${management.server.port}")
    public void setPort(String port) {
        StreamProducerJobCommand.port = port;
    }

    public static void main(String[] args) throws InterruptedException {
        new SpringApplicationBuilder(StreamProducerJobCommand.class).web(WebApplicationType.NONE).run(args);
        String jobId = args[0];
        ActionResult resVal = getSendDataByJobId(jobId);
        Map<String, Object> json = JsonUtil.toObject(resVal.getData().toString(), new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> jsonConf = (Map<String, Object>) json.get("sdm_receive_conf");
        String ra_file_path = null;
        if (null != jsonConf.get("ra_file_path")) {
            ra_file_path = jsonConf.get("ra_file_path").toString();
        }
        if (StringUtil.isEmpty(ra_file_path)) {
            Map<String, Object> kafka_params = (Map<String, Object>) json.get("kafka_params");
            String serializerType = null;
            if (null != kafka_params.get("value_serializer")) {
                serializerType = kafka_params.get("value_serializer").toString();
            }
            try {
                if ("Avro".equals(serializerType)) {
                    ServerToProducer instance = ServerToProducer.getInstance();
                    instance.server(json);
                } else {
                    ServerToProducerString instance = ServerToProducerString.getInstance();
                    instance.server(json);
                }
            } catch (Exception e) {
                logger.info(logger, e);
            }
        } else {
            if (ra_file_path.contains("#{TXDATE}")) {
                ra_file_path = ra_file_path.replace("#{TXDATE}", DateUtil.getSysDate());
                ((Map<String, Object>) json.get("sdm_receive_conf")).put("ra_file_path", ra_file_path);
            }
            CountDownLatch countDownLatch = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            long startTime = System.currentTimeMillis();
            logger.info("beginTime：" + startTime);
            Map<String, Object> params = (Map<String, Object>) json.get("kafka_params");
            String serializerType = null;
            if (null != params.get("value_serializer")) {
                serializerType = params.get("value_serializer").toString();
            }
            String readType = null;
            if (null != jsonConf.get("file_readtype")) {
                readType = jsonConf.get("file_readtype").toString();
            }
            if (IsFlag.Shi == IsFlag.ofEnumByCode(readType)) {
                if ("Avro".equals(serializerType)) {
                    SplitFileToKafka splitFileToKafka = new SplitFileToKafka(jobId, json, countDownLatch);
                    executor.execute(splitFileToKafka);
                } else {
                    SplitFileToKafkaString splitFileToKafka = new SplitFileToKafkaString(jobId, json, countDownLatch);
                    executor.execute(splitFileToKafka);
                }
            } else {
                String fileType = null;
                if (null != jsonConf.get("monitor_type")) {
                    fileType = jsonConf.get("monitor_type").toString();
                }
                if ("Avro".equals(serializerType)) {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(fileType)) {
                        FileReadAllround fileReadAllround = new FileReadAllround(jobId, json, countDownLatch);
                        executor.submit(fileReadAllround);
                    } else if (IsFlag.Shi == IsFlag.ofEnumByCode(fileType)) {
                        DirListenerReader dirListenerReader = new DirListenerReader(jobId, json, countDownLatch);
                        executor.execute(dirListenerReader);
                    }
                } else {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(fileType)) {
                        FileReadAllroundString fileReadAllroundString = new FileReadAllroundString(jobId, json, countDownLatch);
                        executor.execute(fileReadAllroundString);
                    } else if (IsFlag.Shi == IsFlag.ofEnumByCode(fileType)) {
                        DirListenerReaderString dirListenerReaderString = new DirListenerReaderString(jobId, json, countDownLatch);
                        executor.execute(dirListenerReaderString);
                    }
                }
            }
            countDownLatch.await();
            System.exit(0);
        }
    }

    public static ActionResult getSendDataByJobId(String jobId) {
        if (jobId == null) {
            throw new BusinessException("向Agent发送信息，模糊查询表信息时agentId不能为空");
        }
        String url = "http://" + address + ":" + port + "/B/dataCollectionO/sdmcollecttask/wenbenliutask/getSendDataByJobId";
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("sdm_receive_id", jobId).post(url);
        } catch (Exception e) {
            throw new BusinessException("服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (ar.isSuccess()) {
            return ar;
        }
        logger.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
        throw new BusinessException("根据输入的字符查询表失败，详情请查看日志");
    }
}
