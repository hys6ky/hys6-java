package hyren.serv6.trigger.constans;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.HazelcastConfigBean;
import hyren.serv6.commons.utils.yaml.ConfFileLoader;
import hyren.serv6.commons.utils.yaml.YamlFactory;
import hyren.serv6.commons.utils.yaml.YamlMap;

public class TriggerConfigure {

    private static final YamlMap trigger;

    static {
        try {
            trigger = YamlFactory.yaml2Map("trigger.conf");
        } catch (Exception var2) {
            throw new AppSystemException("无法加载trigger配置文件");
        }
    }

    public static HazelcastConfigBean getHazelcastConfig() {
        if (!trigger.exist(HazelcastConfigBean.CONFNAME)) {
            throw new AppSystemException("无法从trigger配置文件中加载属性" + HazelcastConfigBean.CONFNAME);
        }
        YamlMap hazelcast = trigger.getMap(HazelcastConfigBean.CONFNAME);
        HazelcastConfigBean bean = new HazelcastConfigBean();
        bean.setLocalAddress(hazelcast.getString("localAddress"));
        bean.setAutoIncrementPort(hazelcast.getInt("autoIncrementPort"));
        bean.setPortCount(hazelcast.getInt("portCount"));
        return bean;
    }
}
