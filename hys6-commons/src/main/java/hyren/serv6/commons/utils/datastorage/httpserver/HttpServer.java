package hyren.serv6.commons.utils.datastorage.httpserver;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.config.httpconfig.HttpServerConf;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-02-17 14:23")
public class HttpServer {

    public static final String HTTP_NAME = "httpserver";

    public static final String HTTP_CONF_NAME = "httpserver.conf";

    private static final String HYREN_NAME = "hyren_main";

    private static final String WEB_CONTEXT = "/main";

    private static final String ACTION_PATTERN = "/receive/*";

    private static final String AGENT_CONTEXT = "/agent";

    public static HttpYaml getHttpServerDefaultConf(String webContext, String actionPattern, String agentHost, String agentport) {
        HttpYaml defaultConf = new HttpYaml();
        defaultConf.setHost(agentHost);
        defaultConf.setPort(Integer.parseInt(agentport));
        defaultConf.setWebContext(StringUtil.isNotBlank(webContext) ? webContext : AGENT_CONTEXT);
        defaultConf.setActionPattern(StringUtil.isNotBlank(actionPattern) ? actionPattern : ACTION_PATTERN);
        return defaultConf;
    }

    public static HttpYaml getHttpserverHyrenMainConf(String receiveHost, int receivePort, String receiveContext, String receivePattern) {
        HttpYaml hyrenMainConf = new HttpYaml();
        hyrenMainConf.setName(HYREN_NAME);
        hyrenMainConf.setHost(receiveHost);
        hyrenMainConf.setPort(receivePort);
        hyrenMainConf.setWebContext(receiveContext);
        hyrenMainConf.setActionPattern(receivePattern);
        return hyrenMainConf;
    }
}
