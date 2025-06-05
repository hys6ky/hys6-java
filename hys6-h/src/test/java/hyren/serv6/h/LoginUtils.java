package hyren.serv6.h;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;

public class LoginUtils {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String LOGIN_PATH = IP_PORT + "/hyren-gateway/login";

    private static final String USER_ID = "2001";

    private static final String PASSWORD = "1";

    public static String login() {
        System.out.println("登录");
        BufferedReader in = null;
        String token = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("user_id", USER_ID);
            params.put("password", PASSWORD);
            String response = HttpUtils.sendPost(LOGIN_PATH, params, null, null);
            Map<?, ?> map = JsonUtil.toObjectSafety(response.toString(), Map.class).get();
            if (map != null && map.get("code").equals(999)) {
                Object object = map.get("data");
                if (object instanceof Map) {
                    Map<?, ?> m = (Map<?, ?>) object;
                    if (m != null && m.get("accessToken") != null) {
                        token = m.get("accessToken").toString();
                        System.out.println("登录成功，token：" + token);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (StringUtil.isEmpty(token)) {
                System.err.println("登录失败，退出程序");
                System.exit(0);
            }
        }
        return token;
    }
}
