package hyren.serv6.c.syslevelintervention;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.c.HttpUtils;
import hyren.serv6.c.LoginUtils;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;

@Api(tags = "")
@RestController
@RequestMapping("syslevelintervention")
@Slf4j
public class SysLevelInterventionTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String SYS_LEVEL_INTERVENTION_OPERATE_PATH = IP_PORT + "/C/syslevelintervention/sysLevelInterventionOperate";

    private static final String SEARCH_SYSTEM_BATCH_CONDITIONS_PATH = IP_PORT + "/C/syslevelintervention/searchSystemBatchConditions";

    private static final String SEARCH_SYS_LEVER_HIS_INTERVENTION_BY_PAGE_PATH = IP_PORT + "/C/syslevelintervention/searchSysLeverHisInterventionByPage";

    private static final String SEARCH_SYS_LEVEL_CURR_INTERVENTION_INFO_PATH = IP_PORT + "/C/syslevelintervention/searchSysLevelCurrInterventionInfo";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void sysLevelInterventionOperateTest() {
        System.out.println("TEST: 系统级干预操作");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_hand_type", "SO");
        map.put("curr_bath_date", null);
        try {
            String responseStr = HttpUtils.sendPost(SYS_LEVEL_INTERVENTION_OPERATE_PATH, map, token);
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
                    System.err.println("\t系统级干预操作 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t系统级干预操作 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t系统级干预操作 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 系统级干预操作");
    }

    @Test
    public void searchSystemBatchConditionsTest() {
        System.out.println("TEST: 查询系统级干预系统批量情况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_SYSTEM_BATCH_CONDITIONS_PATH, map, token);
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
                    System.err.println("\t查询系统级干预系统批量情况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询系统级干预系统批量情况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询系统级干预系统批量情况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询系统级干预系统批量情况");
    }

    @Test
    public void searchSysLeverHisInterventionByPageTest() {
        System.out.println("TEST: 分页查询系统级历史干预情况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("currPage", null);
        map.put("pageSize", null);
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_SYS_LEVER_HIS_INTERVENTION_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询系统级历史干预情况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询系统级历史干预情况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询系统级历史干预情况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询系统级历史干预情况");
    }

    @Test
    public void searchSysLevelCurrInterventionInfoTest() {
        System.out.println("TEST: 查询系统级当前干预情况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_SYS_LEVEL_CURR_INTERVENTION_INFO_PATH, map, token);
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
                    System.err.println("\t查询系统级当前干预情况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询系统级当前干预情况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询系统级当前干预情况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询系统级当前干预情况");
    }
}
