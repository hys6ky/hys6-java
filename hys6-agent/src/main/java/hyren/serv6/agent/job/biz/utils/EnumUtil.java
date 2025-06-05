package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.commons.enumtools.EnumConstantInterface;

@DocClass(desc = "", author = "WangZhengcheng")
public class EnumUtil {

    public static <T extends EnumConstantInterface> T getEnumByCode(Class<T> enumClass, int code) {
        for (T each : enumClass.getEnumConstants()) {
            if (each.getCode() == code) {
                return each;
            }
        }
        return null;
    }
}
