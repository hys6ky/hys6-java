package hyren.serv6.commons.utils.yaml;

import fd.ng.core.utils.StringUtil;
import hyren.daos.base.exception.internal.FrameworkRuntimeException;

public class ConfFileLoader {

    public static String getConfFile(String filename) {
        if (StringUtil.isEmpty(filename))
            throw new FrameworkRuntimeException("conf filename must not null");
        return "hyrenconf/" + filename + ".conf";
    }
}
