package hyren.serv6.c.joblevelintervention;

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
@RequestMapping("joblevelintervention")
@Slf4j
public class JobLevelInterventionTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String BATCH_JOB_LEVEL_INTERVENTION_OPERATE_PATH = IP_PORT + "/C/joblevelintervention/batchJobLevelInterventionOperate";

    private static final String JOB_LEVEL_INTERVENTION_OPERATE_PATH = IP_PORT + "/C/joblevelintervention/jobLevelInterventionOperate";

    private static final String SEARCH_JOB_LEVEL_INTERVENTION_PATH = IP_PORT + "/C/joblevelintervention/searchJobLevelIntervention";

    private static final String SEARCH_JOB_LEVEL_CURR_INTERVENTION_BY_PAGE_PATH = IP_PORT + "/C/joblevelintervention/searchJobLevelCurrInterventionByPage";

    private static final String SEARCH_JOB_LEVER_HIS_INTERVENTION_BY_PAGE_PATH = IP_PORT + "/C/joblevelintervention/searchJobLeverHisInterventionByPage";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void batchJobLevelInterventionOperateTest() {
        System.out.println("TEST: 作业批量干预");
        Map<String, String> map = new HashMap<>();
        map.put("jobHandBeans", "[{\"etl_job\":\"Y02\",\"etl_sys_cd\":\"Y02\",\"etl_hand_type\":\"JJ\",\"curr_bath_date\":\"20230605 105811\"}]");
        map.put("job_priority", null);
        try {
            String responseStr = HttpUtils.sendPost(BATCH_JOB_LEVEL_INTERVENTION_OPERATE_PATH, map, token);
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
                    System.err.println("\t作业批量干预 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t作业批量干预 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t作业批量干预 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 作业批量干预");
    }

    @Test
    public void jobLevelInterventionOperateTest() {
        System.out.println("TEST: 作业级干预操作");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("etl_hand_type", "JJ");
        map.put("curr_bath_date", "20220605 110000");
        map.put("job_priority", null);
        try {
            String responseStr = HttpUtils.sendPost(JOB_LEVEL_INTERVENTION_OPERATE_PATH, map, token);
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
                    System.err.println("\t作业级干预操作 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t作业级干预操作 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t作业级干预操作 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 作业级干预操作");
    }

    @Test
    public void searchJobLevelInterventionTest() {
        System.out.println("TEST: 查询作业级干预作业情况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("sub_sys_desc", null);
        map.put("job_status", null);
        map.put("currPage", null);
        map.put("pageSize", null);
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_JOB_LEVEL_INTERVENTION_PATH, map, token);
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
                    System.err.println("\t查询作业级干预作业情况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询作业级干预作业情况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询作业级干预作业情况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询作业级干预作业情况");
    }

    @Test
    public void searchJobLeverHisInterventionByPageTest() {
        System.out.println("TEST: 分页查询作业级历史干预情况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("currPage", null);
        map.put("pageSize", null);
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_JOB_LEVER_HIS_INTERVENTION_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询作业级历史干预情况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询作业级历史干预情况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询作业级历史干预情况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询作业级历史干预情况");
    }

    @Test
    public void searchJobLevelCurrInterventionByPageTest() {
        System.out.println("TEST: 查询作业级当前干预情况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("currPage", null);
        map.put("pageSize", null);
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_JOB_LEVEL_CURR_INTERVENTION_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t查询作业级当前干预情况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询作业级当前干预情况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询作业级当前干预情况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询作业级当前干预情况");
    }
}
