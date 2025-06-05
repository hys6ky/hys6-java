package hyren.serv6.commons.jobUtil.task;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import fd.ng.core.utils.DateUtil;
import hyren.serv6.base.exception.AppSystemException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HazelcastHelper {

    private final HazelcastInstance hazelcastInstance;

    private volatile static HazelcastHelper INSTANCE;

    private HazelcastHelper(HazelcastConfigBean bean) {
        Config config = new Config();
        config.getGroupConfig().setName("HazelcastHelper-C");
        config.getGroupConfig().setPassword("HazelcastHelper-C");
        NetworkConfig netConfig = config.getNetworkConfig();
        netConfig.getJoin().getTcpIpConfig().setEnabled(true);
        netConfig.getJoin().getMulticastConfig().setEnabled(false);
        netConfig.getJoin().getAwsConfig().setEnabled(false);
        netConfig.setPort(bean.getAutoIncrementPort());
        netConfig.setPortAutoIncrement(true);
        netConfig.setPortCount(bean.getPortCount());
        String localAddress = bean.getLocalAddress();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(localAddress);
        this.hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        log.info("---------- Hazelcas构建成功 ----------");
    }

    public static HazelcastHelper getInstance(HazelcastConfigBean bean) {
        if (INSTANCE == null) {
            synchronized (HazelcastHelper.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HazelcastHelper(bean);
                }
            }
        }
        return INSTANCE;
    }

    public void deleteByKey(String... keys) {
        for (String key : keys) {
            IQueue<Object> queue = hazelcastInstance.getQueue(key);
            queue.clear();
        }
    }

    public void rpush(String key, String content) {
        log.info(key + ":\t" + content + "\t追加数据==============================================" + DateUtil.getDateTime());
        try {
            hazelcastInstance.getQueue(key).put(content);
        } catch (Exception ex) {
            throw new AppSystemException("将数据追加到hazelcast缓存中" + key + "队列失败", ex);
        }
    }

    public long llen(String key) {
        try {
            return hazelcastInstance.getQueue(key).size();
        } catch (Exception ex) {
            throw new AppSystemException("获取hazelcast缓存中" + key + "队列的大小失败", ex);
        }
    }

    public String lpop(String key) {
        log.info(key + ": \t" + "弹出数据==============================================" + DateUtil.getDateTime());
        try {
            return (String) hazelcastInstance.getQueue(key).take();
        } catch (Exception ex) {
            throw new AppSystemException("获取hazelcast缓存中" + key + "队列中最靠前的数据失败", ex);
        }
    }

    public void close() {
        hazelcastInstance.shutdown();
        log.info("---------- 关闭hazelcastInstance分布式缓存成功 ----------");
    }
}
