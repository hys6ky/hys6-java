package hyren.serv6.commons.utils.stream.commond;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.ConsumerCyc;
import hyren.serv6.base.codes.ConsumerType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.utils.stream.BigFileListener;
import hyren.serv6.commons.utils.stream.JsonProps;
import hyren.serv6.commons.utils.stream.KafkaConsumerRunable;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConsumerSelector {

    public static void main(String[] args) {
        if (args == null) {
            log.info("请按照规定的格式传入参数，必须参数不能为空");
            log.info("必须参数：sdm_consume_conf.sdm_consum_id");
            System.exit(-1);
        }
        try {
            String sdm_consum_id = args[0];
            log.info("sdm_consum_id : {}", sdm_consum_id);
            start(sdm_consum_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static volatile boolean flag = true;

    private static ExecutorService executor;

    private static Map<Object, Future<?>> mapListener = new HashMap<>();

    private static CountDownLatch threadSignal;

    public static AtomicInteger count = new AtomicInteger();

    public static CachedSchemaRegistryClient registryClient = null;

    public static void start(String consumerFilePath) throws Exception {
        consumerFilePath = System.getProperty("user.dir") + File.separator + consumerFilePath + ".json";
        JsonProps jsonProps = new JsonProps();
        Map<String, Object> json = jsonProps.jsonProperties(consumerFilePath);
        Map<String, Object> jsonParm = JsonUtil.toObject(JsonUtil.toJson(json.get("param_conf")), new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> jsonsdm = JsonUtil.toObject(JsonUtil.toJson(json.get("consume_conf")), new TypeReference<Map<String, Object>>() {
        });
        if (!Objects.isNull(jsonParm.get("schema.registry.url"))) {
            registryClient = new CachedSchemaRegistryClient(jsonParm.get("schema.registry.url").toString(), 10000);
        }
        String partitionType = jsonsdm.get("con_with_par").toString();
        new BigFileListener(jsonsdm, partitionType);
        String consumerType = ConsumerType.ofEnumByCode(jsonsdm.get("consumer_type").toString()).getValue();
        if ("consumer".equals(consumerType)) {
            if (partitionType.equals(IsFlag.Fou.getCode())) {
                int threadNum = Integer.parseInt(jsonsdm.get("thread_num").toString());
                threadSignal = new CountDownLatch(threadNum);
                executor = Executors.newFixedThreadPool(threadNum);
                Map<String, Object> jsonStore = JsonUtil.toObject(JsonUtil.toJson(jsonsdm.get("params")), new TypeReference<Map<String, Object>>() {
                });
                jsonStore.put("processtype", "kafka");
                for (int i = 0; i < threadNum; i++) {
                    KafkaConsumerRunable consumerThread = new KafkaConsumerRunable(threadSignal, jsonParm, jsonsdm, jsonStore);
                    Future<?> future = executor.submit(consumerThread);
                    mapListener.put(consumerThread, future);
                }
            } else {
                int threadNum = Integer.parseInt(jsonParm.get("partitionCount").toString());
                threadSignal = new CountDownLatch(threadNum);
                executor = Executors.newFixedThreadPool(threadNum);
                for (int i = 0; i < threadNum; i++) {
                    List<Map<String, Object>> params = JsonUtil.toObject(JsonUtil.toJson(jsonsdm.get("params")), new TypeReference<List<Map<String, Object>>>() {
                    });
                    Map<String, Object> jsonStore = params.get(i);
                    jsonStore.put("processtype", "kafka");
                    KafkaConsumerRunable consumerThread = new KafkaConsumerRunable(threadSignal, jsonParm, jsonsdm, jsonStore, i);
                    Future<?> future = executor.submit(consumerThread);
                    mapListener.put(consumerThread, future);
                }
            }
        } else {
            log.error("请确认消费模式是否正确，当前支持原生consumer和streams消费模式！！！");
        }
        ConsumerCyc consumerCyc = ConsumerCyc.ofEnumByCode(jsonsdm.get("consum_thread_cycle").toString());
        if (ConsumerCyc.WuXianQi == consumerCyc) {
            while (true) {
                if (!flag) {
                    try {
                        threadSignal.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.exit(0);
                } else {
                    listener(mapListener, executor);
                }
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else if (ConsumerCyc.AnShiJianJieShu == consumerCyc) {
            String end_type = jsonsdm.get("end_type").toString();
            long endTime = 0;
            if ("1".equals(end_type)) {
                endTime = Long.parseLong(jsonsdm.get("deadline").toString());
            } else if ("2".equals(end_type)) {
                long startTime = System.currentTimeMillis();
                endTime = startTime + Integer.parseInt(jsonsdm.get("run_time_long").toString());
            }
            while (true) {
                if (!flag) {
                    try {
                        threadSignal.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    log.error("退出进程！！！");
                    System.exit(0);
                } else {
                    listener(mapListener, executor);
                    if (System.currentTimeMillis() > endTime) {
                        flag = false;
                        try {
                            threadSignal.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        log.error("退出进程！！！");
                        System.exit(0);
                    }
                }
            }
        } else if (ConsumerCyc.AnShuJuLiangJieShu == consumerCyc) {
            String data_volume = jsonsdm.get("data_volume").toString();
            while (true) {
                if (!flag) {
                    try {
                        threadSignal.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    log.error("退出进程！！！");
                    System.exit(0);
                } else {
                    listener(mapListener, executor);
                    if (count.get() >= Integer.parseInt(data_volume)) {
                        flag = false;
                        try {
                            threadSignal.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        log.error("退出进程！！！");
                        System.exit(0);
                    }
                }
            }
        } else {
            throw new Exception("错误的消费线程周期!");
        }
    }

    public static void listener(Map<Object, Future<?>> mapListener, ExecutorService executor) {
        for (Object object : mapListener.keySet()) {
            Future<?> future = mapListener.get(object);
            if (future.isDone()) {
                future = executor.submit((Runnable) object);
                mapListener.put(object, future);
            }
        }
    }

    public void threadEnd(Map<Future<?>, String> map) {
        while (map.containsValue("start")) {
            for (Future<?> future : map.keySet()) {
                if (future.isDone() && map.get(future).equals("start")) {
                    map.put(future, "end");
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
