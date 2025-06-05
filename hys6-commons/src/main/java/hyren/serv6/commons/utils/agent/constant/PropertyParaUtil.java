package hyren.serv6.commons.utils.agent.constant;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.commons.utils.yaml.ConfFileLoader;
import hyren.serv6.commons.utils.yaml.YamlFactory;
import hyren.serv6.commons.utils.yaml.YamlMap;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/8 14:30")
public class PropertyParaUtil {

    private static final YamlMap paramMap;

    static {
        YamlMap rootConfig = YamlFactory.yaml2Map(ConfFileLoader.getConfFile("sysparam"));
        paramMap = rootConfig.getArray("param").getMap(0);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "key", desc = "", range = "")
    @Param(name = "defaultValue", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getString(String key, String defaultValue) {
        if (paramMap != null) {
            if (paramMap.containsKey(key)) {
                return paramMap.getString(key);
            }
        }
        return defaultValue;
    }
}
