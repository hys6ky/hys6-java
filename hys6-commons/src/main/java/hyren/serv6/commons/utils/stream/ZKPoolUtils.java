package hyren.serv6.commons.utils.stream;

import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import kafka.utils.ZKStringSerializer$;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.stereotype.Component;

@Slf4j
public final class ZKPoolUtils {

    public static void closeZkSerializer(ZkClient adminClient) {
        adminClient.close();
    }

    public synchronized ZkClient getZkClientSerializer() {
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(PropertyParaValue.getString("kafka_zk_address", "hyshf@beyondsoft.com"), Integer.MAX_VALUE, 300000, ZKStringSerializer$.MODULE$);
            return zkClient;
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(" zkClient connect timeout . please recheck... ");
        }
    }

    public ZkClient getZkClientSerializerByHost(String zkHost) {
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkHost, Integer.MAX_VALUE, 300000, ZKStringSerializer$.MODULE$);
            return zkClient;
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException(" zkClient connect timeout . please recheck... ");
        }
    }
}
