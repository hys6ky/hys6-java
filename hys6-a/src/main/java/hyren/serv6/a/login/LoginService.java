package hyren.serv6.a.login;

import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;

@Service("loginService")
public class LoginService {

    public String getHyrenHost() {
        return PropertyParaValue.getString("hyren_host", "127.0.0.1");
    }

    public String getSysName() {
        return PropertyParaValue.getString("sys_Name", "海云数服");
    }

    public Map<String, Object> getWaterMark() {
        Map<String, Object> waterMark = new HashMap<>();
        waterMark.put("watermark", PropertyParaValue.getString("watermark", "海云数服"));
        waterMark.put("is_watermark", PropertyParaValue.getString("is_watermark", IsFlag.Fou.getCode()));
        waterMark.put("showMonitor", PropertyParaValue.getBoolean("showMonitor", false));
        return waterMark;
    }
}
