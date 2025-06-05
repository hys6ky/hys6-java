package hyren.serv6.h.datastore;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.h.HttpUtils;
import hyren.serv6.h.LoginUtils;

public class DatastoreTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String SEARCH_DATA_STORE_BY_ID_PATH = IP_PORT + "/H/datastore/searchDataStoreById";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void searchDataStoreByIdTest() {
        System.out.println("TEST: 根据权限数据存储层配置ID关联查询数据存储层信息");
        Map<String, String> map = new HashMap<>();
        map.put("dsl_id", "764796914716639232");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_DATA_STORE_BY_ID_PATH, map, null, token);
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
                    System.err.println("\t根据权限数据存储层配置ID关联查询数据存储层信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据权限数据存储层配置ID关联查询数据存储层信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据权限数据存储层配置ID关联查询数据存储层信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据权限数据存储层配置ID关联查询数据存储层信息");
    }
}
