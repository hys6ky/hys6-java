package hyren.serv6.f.source;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-04-13 15:04")
public class CheckParam {

    @Method(desc = "", logicStep = "")
    @Param(name = "errorMsg", desc = "", range = "")
    @Param(name = "columnData", desc = "", range = "")
    @Param(name = "errorParam", desc = "", range = "")
    public static void checkData(String errorMsg, String columnData, Object... errorParam) {
        if (columnData == null || StringUtil.isBlank(columnData))
            throwErrorMsg(String.format(errorMsg, errorParam));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "errorMsg", desc = "", range = "", example = "")
    @Param(name = "errorParam", desc = "", range = "")
    public static void throwErrorMsg(String errorMsg, Object... errorParam) {
        throw new BusinessException(String.format(errorMsg, errorParam));
    }
}
