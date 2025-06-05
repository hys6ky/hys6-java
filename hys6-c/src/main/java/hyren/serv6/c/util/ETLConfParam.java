package hyren.serv6.c.util;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import java.util.HashMap;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2020/3/20 16:05")
public class ETLConfParam {

    public static final String CONTROL_CONF_NAME = "control.conf";

    public static final String TRIGGER_CONF_NAME = "trigger.conf";

    public static final String APPLICATION_CONF_NAME = "application.yml";

    public static final String CONTROL_LOG4J2_CONF_NAME = "log4j2.xml";

    public static final String TRIGGER_LOG4J2_CONF_NAME = "log4j2.xml";

    public static final String CONTROL_APPINFO = "control_appinfo.conf";

    public static final String TRIGGER_APPINFO = "trigger_appinfo.conf";

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getControlApplicationParam() {
        Map<String, Object> appInfoMap = new HashMap<>();
        appInfoMap.put("basePackage", "hrds");
        appInfoMap.put("projectId", "Cont");
        return appInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getTriggerApplicationParam() {
        Map<String, Object> appInfoMap = new HashMap<>();
        appInfoMap.put("basePackage", "hrds");
        appInfoMap.put("projectId", "Trig");
        return appInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getControlAppInfoConfParam() {
        Map<String, Object> appInfoMap = new HashMap<>();
        appInfoMap.put("basePackage", "hrds");
        appInfoMap.put("projectId", "Cont");
        return appInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getTriggerAppInfoConfParam() {
        Map<String, Object> appInfoMap = new HashMap<>();
        appInfoMap.put("basePackage", "hrds");
        appInfoMap.put("projectId", "Trig");
        return appInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getControlConfParam(String etl_serv_ip) {
        Map<String, Object> controlMap = new HashMap<>();
        Map<String, Object> notifyParam = setNotifyParam();
        controlMap.put("notify", notifyParam);
        controlMap.put("hazelcast", setHazelcastParam(etl_serv_ip));
        return controlMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getTriggerConfParam(String etl_serv_ip) {
        Map<String, Object> triggerMap = new HashMap<>();
        triggerMap.put("hazelcast", setHazelcastParam(etl_serv_ip));
        return triggerMap;
    }

    public static Map<String, Object> setHazelcastParam(String etl_serv_ip) {
        Map<String, Object> map = new HashMap<>();
        map.put("localAddress", etl_serv_ip);
        map.put("autoIncrementPort", 5701);
        map.put("portCount", 100);
        return map;
    }

    public static Map<String, Object> setNotifyParam() {
        Map<String, Object> map = new HashMap<>();
        map.put("isNeedSendSMS", true);
        map.put("smsAccountName", 1);
        map.put("smsAccountPasswd", 1);
        map.put("cmHostIp", 1);
        map.put("cmHostPort", 1);
        map.put("wsHostIp", 1);
        map.put("wsHostPort", 1);
        map.put("phoneNumber", 1);
        map.put("bizType", 1);
        return map;
    }
}
