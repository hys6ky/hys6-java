package hyren.serv6.h.websqlquery;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.h.HttpUtils;
import hyren.serv6.h.LoginUtils;

public class WebsqlqueryTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String GET_ALL_TABLE_NAME_BY_PLATFORM_PATH = IP_PORT + "/H/websqlquery/getAllTableNameByPlatform";

    private static final String GET_TABLE_COLUMN_INFO_BY_SQL_PATH = IP_PORT + "/H/websqlquery/getTableColumnInfoBySql";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void getAllTableNameByPlatformTest() {
        System.out.println("TEST: 获取平台登记的所有表信息");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_ALL_TABLE_NAME_BY_PLATFORM_PATH, map, null, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控历史批量信息 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t获取平台登记的所有表信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取平台登记的所有表信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取平台登记的所有表信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取平台登记的所有表信息");
    }

    @Test
    public void getTableColumnInfoBySqlTest() {
        System.out.println("TEST: 获取平台登记的所有表信息");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_TABLE_COLUMN_INFO_BY_SQL_PATH, map, null, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控历史批量信息 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t获取平台登记的所有表信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取平台登记的所有表信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取平台登记的所有表信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取平台登记的所有表信息");
    }
}
