package hyren.serv6.stream.agent.realtimecollection.filecontentstream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.ExecuteWay;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.stream.agent.producer.avro.file.bigFile.SplitFileToKafka;
import hyren.serv6.stream.agent.producer.avro.file.dir.DirListenerReader;
import hyren.serv6.stream.agent.producer.string.file.dirString.DirListenerReaderString;
import hyren.serv6.stream.agent.producer.avro.file.file.FileReadAllRound;
import hyren.serv6.stream.agent.producer.string.file.bigFile.SplitFileToKafkaString;
import hyren.serv6.stream.agent.producer.string.file.file.FileReadAllroundString;
import hyren.serv6.stream.agent.realtimecollection.util.WriterParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.concurrent.*;

@Api(tags = "")
@RestController
@RequestMapping("/filecontentstream")
@Configuration
public class FileContentStreamInfoController {

    private static final Logger logger = LogManager.getLogger(FileContentStreamInfoController.class);

    public static final ConcurrentMap<String, ExecutorService> mapExec = new ConcurrentHashMap<>();

    public static final ConcurrentMap<String, MapDBHelper> mapMapDb = new ConcurrentHashMap<>();

    public static final ConcurrentMap<String, Thread> mapJob = new ConcurrentHashMap<>();

    public static final ConcurrentMap<String, ExecutorService> mapExecS = new ConcurrentHashMap<>();

    public final static ConcurrentMap<String, Boolean> mapStop = new ConcurrentHashMap<>();

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sendMsg", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/execute")
    public void execute(String sendMsg) throws InterruptedException {
        Map<String, Object> messParams = JsonUtil.toObject(sendMsg, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> jsonParam = (Map<String, Object>) messParams.get("sdm_receive_conf");
        String service_id = null;
        if (null != jsonParam.get("sdm_receive_id")) {
            service_id = jsonParam.get("sdm_receive_id").toString();
        }
        String run_way = null;
        if (null != jsonParam.get("run_way")) {
            run_way = jsonParam.get("run_way").toString();
        }
        if (ExecuteWay.AnShiQiDong == ExecuteWay.ofEnumByCode(run_way)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Map<String, Object> params = (Map<String, Object>) messParams.get("kafka_params");
            String serializerType = null;
            if (null != jsonParam.get("file_readtype")) {
                serializerType = params.get("value_serializer").toString();
            }
            String readType = null;
            if (null != jsonParam.get("file_readtype")) {
                readType = jsonParam.get("file_readtype").toString();
            }
            if (IsFlag.Shi == IsFlag.ofEnumByCode(readType)) {
                WriterParam writerParam = new WriterParam();
                writerParam.writeProducerParam(service_id, messParams.toString());
                if ("Avro".equals(serializerType)) {
                    SplitFileToKafka splitFileToKafka = new SplitFileToKafka(service_id, messParams, null);
                    executor.execute(splitFileToKafka);
                } else {
                    SplitFileToKafkaString splitFileToKafka = new SplitFileToKafkaString(service_id, messParams, null);
                    executor.execute(splitFileToKafka);
                }
            } else {
                String fileType = null;
                if (null != jsonParam.get("monitor_type")) {
                    fileType = jsonParam.get("monitor_type").toString();
                }
                if ("Avro".equals(serializerType)) {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(fileType)) {
                        FileReadAllRound fileReadAllround = new FileReadAllRound(service_id, messParams, null);
                        executor.submit(fileReadAllround);
                    } else if (IsFlag.Shi == IsFlag.ofEnumByCode(fileType)) {
                        DirListenerReader dirListenerReader = new DirListenerReader(service_id, messParams, null);
                        executor.execute(dirListenerReader);
                    }
                } else {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(fileType)) {
                        FileReadAllroundString fileReadAllroundString = new FileReadAllroundString(service_id, messParams, null);
                        executor.execute(fileReadAllroundString);
                    } else if (IsFlag.Shi == IsFlag.ofEnumByCode(fileType)) {
                        DirListenerReaderString dirListenerReaderString = new DirListenerReaderString(service_id, messParams, null);
                        executor.execute(dirListenerReaderString);
                    }
                }
            }
            if (mapExecS.get(service_id) != null) {
                ExecutorService executorService = mapExecS.get(service_id);
                executorService.shutdown();
                executorService.awaitTermination(1, TimeUnit.HOURS);
            }
            mapExecS.put(service_id, executor);
        }
    }
}
