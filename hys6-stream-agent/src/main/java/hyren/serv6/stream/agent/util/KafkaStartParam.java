package hyren.serv6.stream.agent.util;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.ExecuteWay;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.stream.agent.producer.avro.file.bigFile.SplitFileToKafka;
import hyren.serv6.stream.agent.producer.avro.file.dir.DirListenerReader;
import hyren.serv6.stream.agent.producer.string.file.dirString.DirListenerReaderString;
import hyren.serv6.stream.agent.producer.string.file.bigFile.SplitFileToKafkaString;
import hyren.serv6.stream.agent.producer.string.file.file.FileReadAllroundString;
import hyren.serv6.stream.agent.realtimecollection.util.WriterParam;
import java.util.Map;
import java.util.concurrent.*;

public class KafkaStartParam extends TranBase {

    public static final String SUCCEED = "{\"msgCode\":\"00\",\"msgContent\":\"处理成功\"}";

    public static volatile ConcurrentMap<String, ExecutorService> mapExec = new ConcurrentHashMap<>();

    public static volatile ConcurrentMap<String, Thread> mapJob = new ConcurrentHashMap<>();

    public static volatile ConcurrentMap<String, ExecutorService> mapExecS = new ConcurrentHashMap<>();

    @Override
    public String transact(String bitHead, String headMsg, String component, String jobKey, String msg) throws Exception {
        Map<String, Object> json = JsonUtil.toObject(msg, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> jsonConf = (Map<String, Object>) json.get("sdm_receive_conf");
        String jobId = null;
        if (null != jsonConf.get("sdm_receive_id")) {
            jobId = jsonConf.get("sdm_receive_id").toString();
        }
        String run_way = null;
        if (null != jsonConf.get("run_way")) {
            run_way = jsonConf.get("run_way").toString();
        }
        if (ExecuteWay.AnShiQiDong == ExecuteWay.ofEnumByCode(run_way)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
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
                WriterParam writerParm = new WriterParam();
                String heads = bitHead + headMsg + component + jobKey;
                json.put("jobHead", heads);
                writerParm.writeProducerParam(jobId, json.toString());
                if ("Avro".equals(serializerType)) {
                    SplitFileToKafka splitFileToKafka = new SplitFileToKafka(jobId, json, null);
                    executor.execute(splitFileToKafka);
                } else {
                    SplitFileToKafkaString splitFileToKafka = new SplitFileToKafkaString(jobId, json, null);
                    executor.execute(splitFileToKafka);
                }
            } else {
                String fileType = null;
                if (null != jsonConf.get("monitor_type")) {
                    fileType = jsonConf.get("monitor_type").toString();
                }
                if ("Avro".equals(serializerType)) {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(fileType)) {
                        FileReadAllround fileReadAllround = new FileReadAllround(jobId, json, null);
                        executor.submit(fileReadAllround);
                    } else if (IsFlag.Shi == IsFlag.ofEnumByCode(fileType)) {
                        DirListenerReader dirListenerReader = new DirListenerReader(jobId, json, null);
                        executor.execute(dirListenerReader);
                    }
                } else {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(fileType)) {
                        FileReadAllroundString fileReadAllroundString = new FileReadAllroundString(jobId, json, null);
                        executor.execute(fileReadAllroundString);
                    } else if (IsFlag.Shi == IsFlag.ofEnumByCode(fileType)) {
                        DirListenerReaderString dirListenerReaderString = new DirListenerReaderString(jobId, json, null);
                        executor.execute(dirListenerReaderString);
                    }
                }
            }
            if (mapExecS.get(jobId) != null) {
                ExecutorService executorService = mapExecS.get(jobId);
                executorService.shutdown();
                executorService.awaitTermination(1, TimeUnit.HOURS);
            }
            mapExecS.put(jobId, executor);
        } else {
            WriterParm writerParm = new WriterParm();
            String head = bitHead + headMsg + component + jobKey;
            json.put("jobHead", head);
            writerParm.witeProducerParam(jobId, json.toString());
        }
        return PackUtil.packMsg(SUCCEED);
    }
}
