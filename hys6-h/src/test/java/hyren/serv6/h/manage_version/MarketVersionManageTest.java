package hyren.serv6.h.manage_version;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.h.HttpUtils;
import hyren.serv6.h.LoginUtils;
import hyren.serv6.h.manage_version.dto.GetDataTableMappingInfosDTO;

public class MarketVersionManageTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String GET_MARKET_VER_MANAGE_TREE_DATA_PATH = IP_PORT + "/H/manage/version/getMarketVerManageTreeData";

    private static final String GET_DATA_TABLE_MAPPING_INFOS_PATH = IP_PORT + "/H/manage/version/getDataTableMappingInfos";

    private static final String GET_DATA_TABLE_STRUCTURE_INFOS_COLUMU_PATH = IP_PORT + "/H/manage/version/getDataTableStructureInfos_columu";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void getMarketVerManageTreeDataTest() {
        System.out.println("TEST: 获取版本管理树数据");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_MARKET_VER_MANAGE_TREE_DATA_PATH, map, null, token);
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
                    System.err.println("\t获取版本管理树数据 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取版本管理树数据 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取版本管理树数据 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取版本管理树数据");
    }

    @Test
    public void getDataTableMappingInfosTest() {
        System.out.println("TEST: 获取集市数据表Mapping的版本信息列表");
        Map<String, String> map = new HashMap<>();
        GetDataTableMappingInfosDTO dto = new GetDataTableMappingInfosDTO(111L, Arrays.asList("1", "2", "3"));
        try {
            String responseStr = HttpUtils.sendPost(GET_DATA_TABLE_MAPPING_INFOS_PATH, map, dto, token);
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
                    System.err.println("\t获取集市数据表Mapping的版本信息列表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取集市数据表Mapping的版本信息列表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取集市数据表Mapping的版本信息列表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取集市数据表Mapping的版本信息列表");
    }

    @Test
    public void getDataTableStructureInfos_columuTest() {
        System.out.println("TEST: 获取集市数据表结构的版本信息列表");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_DATA_TABLE_STRUCTURE_INFOS_COLUMU_PATH, map, null, token);
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
                    System.err.println("\t获取集市数据表结构的版本信息列表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取集市数据表结构的版本信息列表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取集市数据表结构的版本信息列表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取集市数据表结构的版本信息列表");
    }
}
