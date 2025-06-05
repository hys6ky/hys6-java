package hyren.serv6.commons.key;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import hyren.serv6.base.key.PrimayKeyGener;

@DocClass(desc = "", author = "dhw", createdate = "2023-02-07 17:41:11")
public class DemandCodeUtils {

    public static String getDemandCode() {
        return "XQ" + DateUtil.getSysDate() + DateUtil.getSysTime() + PrimayKeyGener.getOperId();
    }
}
