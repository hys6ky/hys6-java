package hyren.serv6.control.constans;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.HazelcastConfigBean;
import hyren.serv6.commons.utils.yaml.YamlFactory;
import hyren.serv6.commons.utils.yaml.YamlMap;

public class ControlConfigure {

    private static final YamlMap control;

    static {
        try {
            control = YamlFactory.yaml2Map("control.conf");
        } catch (Exception var2) {
            throw new AppSystemException("无法加载control配置文件");
        }
    }

    public static HazelcastConfigBean getHazelcastConfig() {
        if (!control.exist(HazelcastConfigBean.CONFNAME)) {
            throw new AppSystemException("无法从trigger配置文件中加载属性" + HazelcastConfigBean.CONFNAME);
        }
        YamlMap hazelcast = control.getMap(HazelcastConfigBean.CONFNAME);
        HazelcastConfigBean bean = new HazelcastConfigBean();
        bean.setLocalAddress(hazelcast.getString("localAddress"));
        bean.setAutoIncrementPort(hazelcast.getInt("autoIncrementPort"));
        bean.setPortCount(hazelcast.getInt("portCount"));
        return bean;
    }

    public static class NotifyConfig {

        private static final String CONFNAME = "notify";

        public static final Boolean isNeedSendSMS;

        public static final String smsAccountName;

        public static final String smsAccountPasswd;

        public static final String cmHostIp;

        public static final Integer cmHostPort;

        public static final String wsHostIp;

        public static final Integer wsHostPort;

        public static final String phoneNumber;

        public static final String bizType;

        static {
            if (!control.exist(CONFNAME)) {
                throw new AppSystemException("无法从control配置文件中加载属性" + CONFNAME);
            }
            YamlMap notify = control.getMap(CONFNAME);
            isNeedSendSMS = notify.getBool("isNeedSendSMS");
            smsAccountName = notify.getString("smsAccountName");
            smsAccountPasswd = notify.getString("smsAccountPasswd");
            cmHostIp = notify.getString("cmHostIp");
            cmHostPort = notify.getInt("cmHostPort");
            wsHostIp = notify.getString("wsHostIp");
            wsHostPort = notify.getInt("wsHostPort");
            phoneNumber = notify.getString("phoneNumber");
            bizType = notify.getString("bizType");
        }
    }
}
