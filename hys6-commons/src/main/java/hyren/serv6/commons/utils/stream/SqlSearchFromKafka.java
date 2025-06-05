package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.SdmTopicInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.stream.bean.HostsDomain;
import hyren.serv6.commons.utils.stream.bean.KafkaSqlDomain;
import hyren.serv6.commons.utils.stream.bean.PartitionsDomain;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.Tuple2;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SqlSearchFromKafka {

    private Properties getAdminClientProperties() {
        Properties properties = new Properties();
        List<String> kafkaBrokersReturnList = getKafkaBrokersReturnList();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokersReturnList);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 300000);
        return properties;
    }

    private final static String BROKER_IDS_PATH = "/brokers/ids";

    public List<String> getKafkaBrokersByZkHost(String zkHost) {
        ZKPoolUtils zkPoolUtils = new ZKPoolUtils();
        ZkClient zkClientSerializer = zkPoolUtils.getZkClientSerializerByHost(zkHost);
        List<String> brokers = new ArrayList<>();
        if (zkClientSerializer.exists(BROKER_IDS_PATH)) {
            List<String> children = zkClientSerializer.getChildren(BROKER_IDS_PATH);
            for (String child : children) {
                System.out.println("child = " + child);
                Object o = zkClientSerializer.readData(BROKER_IDS_PATH + "/" + child);
                String s = JsonUtil.toJson(o);
                String s1 = s.replaceAll("\\\\", "");
                String substring = s1.substring(1, s1.length() - 1);
                Map<String, Object> map = JsonUtil.toObject(substring, new TypeReference<Map<String, Object>>() {
                });
                String host = map.get("host").toString();
                String port = map.get("port").toString();
                brokers.add(host + ":" + port);
            }
        }
        zkClientSerializer.close();
        return brokers;
    }

    public List<String> getKafkaBrokersReturnList() {
        ZKPoolUtils zkPoolUtils = new ZKPoolUtils();
        ZkClient zkClientSerializer = zkPoolUtils.getZkClientSerializer();
        List<String> brokers = new ArrayList<>();
        if (zkClientSerializer.exists(BROKER_IDS_PATH)) {
            List<String> children = zkClientSerializer.getChildren(BROKER_IDS_PATH);
            for (String child : children) {
                System.out.println("child = " + child);
                Object o = zkClientSerializer.readData(BROKER_IDS_PATH + "/" + child);
                String s = JsonUtil.toJson(o);
                String s1 = s.replaceAll("\\\\", "");
                String substring = s1.substring(1, s1.length() - 1);
                Map<String, Object> map = JsonUtil.toObject(substring, new TypeReference<Map<String, Object>>() {
                });
                String host = map.get("host").toString();
                String port = map.get("port").toString();
                brokers.add(host + ":" + port);
            }
        }
        zkClientSerializer.close();
        return brokers;
    }

    public List<Map<String, Object>> getAllBrokersInfo() {
        ZKPoolUtils zkPoolUtils = new ZKPoolUtils();
        ZkClient zkClientSerializer = zkPoolUtils.getZkClientSerializer();
        List<Map<String, Object>> brokers = new ArrayList<>();
        if (zkClientSerializer.exists(BROKER_IDS_PATH)) {
            List<String> children = zkClientSerializer.getChildren(BROKER_IDS_PATH);
            for (String child : children) {
                System.out.println("child = " + child);
                Object o = zkClientSerializer.readData(BROKER_IDS_PATH + "/" + child);
                String s = JsonUtil.toJson(o);
                String s1 = s.replaceAll("\\\\", "");
                String substring = s1.substring(1, s1.length() - 1);
                Map<String, Object> map = JsonUtil.toObject(substring, new TypeReference<Map<String, Object>>() {
                });
                brokers.add(map);
            }
        }
        zkClientSerializer.close();
        return brokers;
    }

    public String execute(String sql) {
        Map<String, Object> status = new HashMap<>();
        try {
            KafkaSqlDomain kafkaSql = parseSql(sql);
            log.info("KafkaSqlParser - SQL[" + kafkaSql.getSql() + "]");
            if (kafkaSql.isStatus()) {
                if (!hasTopic(kafkaSql)) {
                    status.put("error", true);
                    status.put("msg", "ERROR - Topic[" + kafkaSql.getTableName() + "] not exist.");
                } else {
                    long start = System.currentTimeMillis();
                    List<List<Map<String, Object>>> dataSets = KafkaConsumerAdapter.executor(kafkaSql);
                    String results = JSqlUtils.query(kafkaSql.getSchema(), kafkaSql.getTableName(), dataSets, kafkaSql.getMetaSql());
                    long end = System.currentTimeMillis();
                    status.put("error", false);
                    status.put("msg", results);
                    status.put("status", "Finished by [" + (end - start) / 1000.0 + "s].");
                }
            } else {
                status.put("error", true);
                status.put("msg", "ERROR - SQL[" + kafkaSql.getSql() + "] has error,please start with select.");
            }
        } catch (Exception e) {
            status.put("error", true);
            status.put("msg", e.getMessage());
            e.printStackTrace();
            log.error("Execute sql to query kafka topic has error,msg is " + e.getMessage());
            throw new BusinessException(e.getMessage());
        }
        return JsonUtil.toJson(status);
    }

    public KafkaSqlDomain parseSql(String sql) {
        return segments(prepare(sql));
    }

    private String prepare(String sql) {
        sql = sql.trim();
        sql = sql.replaceAll("\\s+", " ");
        return sql;
    }

    private KafkaSqlDomain segments(String sql) {
        KafkaSqlDomain kafkaSql = new KafkaSqlDomain();
        kafkaSql.setMetaSql(sql);
        sql = sql.toLowerCase();
        kafkaSql.setSql(sql);
        if (sql.contains("where")) {
            String group = "";
            String str = sql.split("where")[1];
            str = str.replace(" ", "");
            String regEx = "offset.{0,2}[0-9]+";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(str);
            while (m.find()) {
                group = m.group();
            }
            Pattern pp = Pattern.compile("[0-9.]+");
            Matcher mm = pp.matcher(group);
            while (mm.find()) {
                group = mm.group();
            }
            kafkaSql.setOffsetSize(group);
        }
        if (sql.contains("and")) {
            sql = sql.split("and")[0];
        } else if (sql.contains("group by")) {
            sql = sql.split("group")[0];
        } else if (sql.contains("limit")) {
            sql = sql.split("limit")[0];
        }
        kafkaSql.getSchema().put("partition", "integer");
        kafkaSql.getSchema().put("offset", "bigint");
        if (!sql.startsWith("select")) {
            kafkaSql.setStatus(false);
            return kafkaSql;
        } else {
            kafkaSql.setStatus(true);
            Matcher matcher = Pattern.compile("select\\s.+from\\s(.+)where\\s(.+)").matcher(sql);
            String[] split = kafkaSql.getMetaSql().split(" ");
            String table_name = "";
            for (int i = 0; i < split.length; i++) {
                if ("from".equalsIgnoreCase(split[i].trim())) {
                    table_name = split[i + 1].trim();
                }
            }
            kafkaSql.setTableName(table_name.replace("\"", ""));
            if (matcher.find()) {
                if (matcher.group(2).trim().startsWith("\"partition\"")) {
                    String[] columns = matcher.group(2).trim().split("in")[1].replace("(", "").replace(")", "").trim().split(",");
                    for (String column : columns) {
                        try {
                            kafkaSql.getPartition().add(Integer.parseInt(column));
                        } catch (Exception e) {
                            log.error("Parse parition[" + column + "] has error,msg is " + e.getMessage());
                        }
                    }
                }
            }
            kafkaSql.setSeeds(getBrokers());
            Result messResult = KafkaMonitorManager.getSdm_mess_infoByTopic(kafkaSql.getTableName());
            if (kafkaSql.getPartition().size() == 0) {
                Result result2 = KafkaMonitorManager.getPartitionByTopic(kafkaSql.getTableName());
                Integer partitionNum = result2.getInteger(0, "sdm_partition");
                for (int i = 0; i < partitionNum; i++) {
                    kafkaSql.getPartition().add(i);
                }
            }
            if (!messResult.isEmpty()) {
                if (StringUtil.isEmpty(messResult.getString(0, "msgtype")) && IsFlag.Fou == IsFlag.ofEnumByCode(messResult.getString(0, "is_data_partition")) && IsFlag.Fou == IsFlag.ofEnumByCode(messResult.getString(0, "is_obj"))) {
                    kafkaSql.getSchema().put("line", "varchar");
                } else {
                    for (int i = 0; i < messResult.getRowCount(); i++) {
                        String column_name = messResult.getString(i, "sdm_var_name_en");
                        kafkaSql.getSchema().put(column_name, "varchar");
                    }
                }
            } else {
                kafkaSql.getSchema().put("line", "varchar");
            }
            kafkaSql.setResult(messResult);
        }
        return kafkaSql;
    }

    private List<HostsDomain> getBrokers() {
        List<HostsDomain> targets = new ArrayList<>();
        List<Map<String, Object>> brokers = getAllBrokersInfo();
        for (Map<String, Object> broker : brokers) {
            HostsDomain host = new HostsDomain();
            host.setHost(broker.get("host").toString());
            host.setPort(Integer.parseInt(broker.get("port").toString()));
            targets.add(host);
        }
        return targets;
    }

    private boolean hasTopic(KafkaSqlDomain kafkaSql) {
        String topics = getAllPartitions();
        List<Map<String, Object>> topicDataSets = JsonUtil.toObject(topics, new TypeReference<List<Map<String, Object>>>() {
        });
        for (Object object : topicDataSets) {
            Map<String, Object> topicDataSet = (Map<String, Object>) object;
            if (kafkaSql.getMetaSql().contains(topicDataSet.get("topic").toString())) {
                kafkaSql.setTopic(topicDataSet.get("topic").toString());
                return true;
            }
        }
        return false;
    }

    @Autowired
    ZKPoolUtils zkPool;

    public String getAllPartitions() {
        ZkClient zkc = zkPool.getZkClientSerializer();
        List<PartitionsDomain> targets = new ArrayList<>();
        Properties adminClientProperties = getAdminClientProperties();
        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            ListTopicsResult topicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> topicsFuture = topicsResult.names();
            Set<String> topicSet = null;
            try {
                topicSet = topicsFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            List<SdmTopicInfo> listTopicsResult = getAllTopic();
            int id = 0;
            for (String topic : topicSet) {
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(configResource));
                Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
                Config topicConfig = configResourceConfigMap.get(configResource);
                PartitionsDomain partition = new PartitionsDomain();
                partition.setId(++id);
                for (SdmTopicInfo sdmTopicInfo : listTopicsResult) {
                    if (sdmTopicInfo.getSdm_top_name().equals(topic)) {
                        String ctime = sdmTopicInfo.getCreate_date() + sdmTopicInfo.getCreate_time();
                        SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                        SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            Date date = inputDateFormat.parse(ctime);
                            String formattedDate = outputDateFormat.format(date);
                            partition.setCreated(formattedDate);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                partition.setTopic(topic);
                partition.setPartitionNumbers(topicConfig.entries().size());
                Set<String> partitions = new HashSet<>();
                for (ConfigEntry entry : topicConfig.entries()) {
                    partitions.add(entry.name());
                }
                partition.setPartitions(partitions);
                targets.add(partition);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        ZKPoolUtils.closeZkSerializer(zkc);
        targets.sort((arg0, arg1) -> {
            try {
                long hits0 = CalendarUtils.convertDate2UnixTime(arg0.getCreated());
                long hits1 = CalendarUtils.convertDate2UnixTime(arg1.getCreated());
                return Long.compare(hits1, hits0);
            } catch (Exception e) {
                log.error("Convert date to unix time has error,msg is " + e.getMessage());
                return 0;
            }
        });
        return JsonUtil.toJson(targets);
    }

    public String getKafkaBrokerServer() {
        return parseBrokerServer();
    }

    private String parseBrokerServer() {
        StringBuilder brokerServer = new StringBuilder();
        List<Map<String, Object>> brokers = getAllBrokersInfo();
        for (Map<String, Object> broker : brokers) {
            brokerServer.append(broker.get("host").toString()).append(":").append(broker.get("port").toString()).append(",");
        }
        return brokerServer.substring(0, brokerServer.length() - 1);
    }

    public List<SdmTopicInfo> getAllTopic() {
        List<SdmTopicInfo> sdmTopicInfo = Dbo.queryList(SdmTopicInfo.class, "select * from " + SdmTopicInfo.TableName);
        return sdmTopicInfo;
    }
}
