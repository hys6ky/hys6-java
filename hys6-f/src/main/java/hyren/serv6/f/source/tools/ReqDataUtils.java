package hyren.serv6.f.source.tools;

import hyren.serv6.base.exception.BusinessException;
import org.apache.commons.lang3.StringUtils;
import java.util.Map;
import java.util.Objects;

public class ReqDataUtils {

    public static long getLongData(Map<String, Object> req, String dataName) {
        long l = 0;
        try {
            l = Long.parseLong(req.get(dataName).toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req data:" + dataName + "format failed.");
        }
        return l;
    }

    public static String getStringData(Map<String, Object> req, String dataName) {
        String s = StringUtils.EMPTY;
        try {
            s = Objects.isNull(req.get(dataName)) ? s : req.get(dataName).toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req data:" + dataName + "format failed.");
        }
        return s;
    }
}
