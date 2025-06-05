package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaMonitorManager {

    private final static String BROKER_IDS_PATH = "/brokers/ids";

    @Autowired
    ZKPoolUtils zkPoolUtils;

    public List<Map<String, Object>> getAllBrokerInfo() {
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

    public void close(AdminClient adminClient) {
        adminClient.close();
    }

    public String parseBrokerServer() {
        StringBuilder brokerServer = new StringBuilder();
        List<Map<String, Object>> allBrokersInfo = getAllBrokerInfo();
        String str = null;
        if (!allBrokersInfo.isEmpty()) {
            for (Map<String, Object> brokersdomain : allBrokersInfo) {
                brokerServer.append(brokersdomain.get("host")).append(":").append(brokersdomain.get("port")).append(",");
                str = brokerServer.substring(0, brokerServer.length() - 1);
            }
        }
        return str;
    }

    public static Result getSdm_mess_infoByTopic(String topic) {
        return Dbo.queryResult("SELECT T5.msgtype, T3.format, T4.sdm_var_name_en, T4.sdm_var_type, T4.num, T4.sdm_is_send, " + " (CASE WHEN T5.is_data_partition IS NULL THEN '1' ELSE T5.is_data_partition END ) AS is_data_partition, " + " (CASE WHEN T5.is_obj IS NULL THEN '1' ELSE T5.is_obj END ) AS is_obj FROM sdm_mess_info T4 " + " RIGHT JOIN(SELECT T2.sdm_param_value AS format, T1.sdm_receive_id FROM " + " (SELECT * FROM sdm_rec_param WHERE sdm_param_key = 'topic' AND sdm_param_value = ? LIMIT 1) T1 " + " LEFT JOIN(SELECT * FROM sdm_rec_param WHERE sdm_param_key = 'value_serializer')T2 ON T1.sdm_receive_id = T2.sdm_receive_id " + " ) T3 ON T4.sdm_receive_id = T3.sdm_receive_id LEFT JOIN sdm_receive_conf T5 ON T4.sdm_receive_id = T5.sdm_receive_id " + " WHERE T4.sdm_is_send = ? ORDER BY T4.num ASC ", topic, IsFlag.Shi.getCode());
    }

    public static Result getPartitionByTopic(String topic) {
        if (StringUtil.isBlank(topic)) {
            throw new BusinessException("topic名称不能为空!");
        }
        try {
            return Dbo.queryResult("SELECT sdm_partition FROM sdm_topic_info WHERE sdm_top_name = ?", topic);
        } catch (Exception e) {
            if (e instanceof BusinessException)
                throw (BusinessException) e;
            else
                throw new AppSystemException(e);
        }
    }
}
