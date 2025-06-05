package hyren.serv6.c.etlsys;

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
@RequestMapping("etlsys")
@Slf4j
public class EtlSysTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String DOWNLOAD_CONTROL_OR_TRIGGER_LOG_PATH = IP_PORT + "/C/etlsys/downloadControlOrTriggerLog";

    private static final String DEPLOY_ETL_JOB_SCHEDULE_PROJECT_PATH = IP_PORT + "/C/etlsys/deployEtlJobScheduleProject";

    private static final String READ_CONTROL_OR_TRIGGER_LOG_PATH = IP_PORT + "/C/etlsys/readControlOrTriggerLog";

    private static final String SAVE_ETL_SYS_DEP_PATH = IP_PORT + "/C/etlsys/saveEtlSysDep";

    private static final String SEARCH_ETL_SYS_BY_ID_PATH = IP_PORT + "/C/etlsys/searchEtlSysById";

    private static final String START_TRIGGER_PATH = IP_PORT + "/C/etlsys/startTrigger";

    private static final String STOP_ETL_PROJECT_PATH = IP_PORT + "/C/etlsys/stopEtlProject";

    private static final String GET_ETL_SYS_DEP_BY_ID_PATH = IP_PORT + "/C/etlsys/getEtlSysDepById";

    private static final String SEARCH_ETL_SYS_PATH = IP_PORT + "/C/etlsys/searchEtlSys";

    private static final String SEARCH_TABLE_PATH = IP_PORT + "/C/etlsys/searchTable";

    private static final String START_CONTROL_PATH = IP_PORT + "/C/etlsys/startControl";

    private static final String UPDATE_ETL_SYS_PATH = IP_PORT + "/C/etlsys/updateEtlSys";

    private static final String DELETE_ETL_PROJECT_PATH = IP_PORT + "/C/etlsys/deleteEtlProject";

    private static final String ADD_ETL_SYS_PATH = IP_PORT + "/C/etlsys/addEtlSys";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void downloadControlOrTriggerLogTest() {
        System.out.println("TEST: 下载Control或Trigger日志");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", null);
        map.put("curr_bath_date", null);
        map.put("isControl", null);
        System.out.println("下载暂时不做");
        System.out.println("TEST-E: 下载Control或Trigger日志");
    }

    @Test
    public void readControlOrTriggerLogTest() {
        System.out.println("TEST: 读取Control或Trigger日志信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("readNum", "5");
        map.put("isControl", "0");
        try {
            String responseStr = HttpUtils.sendPost(READ_CONTROL_OR_TRIGGER_LOG_PATH, map, token);
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
                    System.err.println("\t读取Control或Trigger日志信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t读取Control或Trigger日志信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t读取Control或Trigger日志信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 读取Control或Trigger日志信息");
    }

    @Test
    public void deployEtlJobScheduleProjectTest() {
        System.out.println("TEST: 部署作业调度工程");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_serv_ip", null);
        map.put("serv_file_path", null);
        map.put("user_name", null);
        map.put("user_pwd", null);
        map.put("isCustomize", "0");
        System.out.println("无有效数据无法测试");
        System.out.println("TEST-E: 部署作业调度工程");
    }

    @Test
    public void addEtlSysTest() {
        System.out.println("TEST: 新增保存作业调度工程信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("etl_sys_name", "采集");
        map.put("comments", "comments-ql");
        map.put("pre_etl_sys_cds", "Y02,A123");
        map.put("status", "T");
        try {
            String responseStr = HttpUtils.sendPost(ADD_ETL_SYS_PATH, map, token);
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
                    System.err.println("\t新增保存作业调度工程信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存作业调度工程信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存作业调度工程信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存作业调度工程信息");
    }

    @Test
    public void saveEtlSysDepTest() {
        System.out.println("TEST: 保存工程依赖信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql2");
        map.put("status", "T");
        map.put("pre_etl_sys_cds", "Y02,A123");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_SYS_DEP_PATH, map, token);
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
                    System.err.println("\t保存工程依赖信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存工程依赖信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存工程依赖信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存工程依赖信息");
    }

    @Test
    public void deleteEtlProjectTest() {
        System.out.println("TEST: 删除作业调度工程");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql2");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_PROJECT_PATH, map, token);
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
                    System.err.println("\t删除作业调度工程 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除作业调度工程 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除作业调度工程 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除作业调度工程");
    }

    @Test
    public void searchEtlSysByIdTest() {
        System.out.println("TEST: 根据工程编号查询作业调度工程信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_SYS_BY_ID_PATH, map, token);
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
                    System.err.println("\t根据工程编号查询作业调度工程信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据工程编号查询作业调度工程信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据工程编号查询作业调度工程信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据工程编号查询作业调度工程信息");
    }

    @Test
    public void stopEtlProjectTest() {
        System.out.println("TEST: 停止工程信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(STOP_ETL_PROJECT_PATH, map, token);
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
                    System.err.println("\t停止工程信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t停止工程信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t停止工程信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 停止工程信息");
    }

    @Test
    public void getEtlSysDepByIdTest() {
        System.out.println("TEST: 根据工程编号查询工程依赖信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        try {
            String responseStr = HttpUtils.sendPost(GET_ETL_SYS_DEP_BY_ID_PATH, map, token);
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
                    System.err.println("\t根据工程编号查询工程依赖信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据工程编号查询工程依赖信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据工程编号查询工程依赖信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据工程编号查询工程依赖信息");
    }

    @Test
    public void startTriggerTest() {
        System.out.println("TEST: 启动TRIGGER");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(START_TRIGGER_PATH, map, token);
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
                    System.err.println("\t启动TRIGGER - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t启动TRIGGER - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t启动TRIGGER - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 启动TRIGGER");
    }

    @Test
    public void searchEtlSysTest() {
        System.out.println("TEST: 查询作业调度工程信息");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_SYS_PATH, map, token);
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
                    System.err.println("\t查询作业调度工程信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询作业调度工程信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询作业调度工程信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询作业调度工程信息");
    }

    @Test
    public void updateEtlSysTest() {
        System.out.println("TEST: 更新保存作业调度工程");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_sys_name", "采集ql");
        map.put("comments", "comments-ql");
        map.put("status", "T");
        map.put("pre_etl_sys_cds", "Y02,A123");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_SYS_PATH, map, token);
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
                    System.err.println("\t更新保存作业调度工程 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新保存作业调度工程 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新保存作业调度工程 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新保存作业调度工程");
    }

    @Test
    public void startControlTest() {
        System.out.println("TEST: 启动CONTROL");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("isResumeRun", "1");
        map.put("isAutoShift", "1");
        map.put("curr_bath_date", "20230602 180000");
        try {
            String responseStr = HttpUtils.sendPost(START_CONTROL_PATH, map, token);
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
                    System.err.println("\t启动CONTROL - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t启动CONTROL - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t启动CONTROL - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 启动CONTROL");
    }

    @Test
    public void searchTableTest() {
        System.out.println("TEST: 根据表名查询采集表信息");
        Map<String, String> map = new HashMap<>();
        map.put("tableName", "time");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_TABLE_PATH, map, token);
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
                    System.err.println("\t根据表名查询采集表信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据表名查询采集表信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据表名查询采集表信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据表名查询采集表信息");
    }
}
