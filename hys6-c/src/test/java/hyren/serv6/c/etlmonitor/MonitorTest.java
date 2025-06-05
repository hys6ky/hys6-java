package hyren.serv6.c.etlmonitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.c.HttpUtils;
import hyren.serv6.c.LoginUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonitorTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String DOWN_HISTORY_JOB_LOG_PATH = IP_PORT + "/C/monitor/downHistoryJobLog";

    private static final String MONITOR_ALL_PROJECT_CHARTS_DATA_PATH = IP_PORT + "/C/monitor/monitorAllProjectChartsData";

    private static final String MONITOR_BATCH_ETL_JOB_DEPENDENCY_INFO_PATH = IP_PORT + "/C/monitor/monitorBatchEtlJobDependencyInfo";

    private static final String MONITOR_CURRENT_BATCH_INFO_PATH = IP_PORT + "/C/monitor/monitorCurrentBatchInfo";

    private static final String MONITOR_CURRENT_BATCH_INFO_BY_TASK_PATH = IP_PORT + "/C/monitor/monitorCurrentBatchInfoByTask";

    private static final String GET_JOB_CONSUME_TIME_SUM_PATH = IP_PORT + "/C/monitor/getJobConsumeTimeSum";

    private static final String GET_PROJECT_CONSUME_TIME_SUM_PATH = IP_PORT + "/C/monitor/getProjectConsumeTimeSum";

    private static final String GET_TASK_CONSUME_TIME_SUM_PATH = IP_PORT + "/C/monitor/getTaskConsumeTimeSum";

    private static final String GENERATE_EXCEL_PATH = IP_PORT + "/C/monitor/generateExcel";

    private static final String DOWNLOAD_FILE_PATH = IP_PORT + "/C/monitor/downloadFile";

    private static final String READ_HISTORY_JOB_LOG_INFO_PATH = IP_PORT + "/C/monitor/readHistoryJobLogInfo";

    private static final String MONITOR_JOB_DEPENDENCY_INFO_PATH = IP_PORT + "/C/monitor/monitorJobDependencyInfo";

    private static final String MONITOR_SYSTEM_RESOURCE_INFO_PATH = IP_PORT + "/C/monitor/monitorSystemResourceInfo";

    private static final String MONITOR_HISTORY_JOB_INFO_PATH = IP_PORT + "/C/monitor/monitorHistoryJobInfo";

    private static final String MONITOR_HISTORY_BATCH_INFO_PATH = IP_PORT + "/C/monitor/monitorHistoryBatchInfo";

    private static final String MONITOR_CURR_JOB_INFO_PATH = IP_PORT + "/C/monitor/monitorCurrJobInfo";

    private String token;

    @Before
    public void login() {
        if (StringUtil.isEmpty(this.token)) {
            this.token = LoginUtils.login();
        }
    }

    @Test
    public void monitorCurrentBatchInfoTest() {
        System.out.println("TEST: 监控当前批量情况(系统运行状态）");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_CURRENT_BATCH_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控当前批量情况(系统运行状态） - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控当前批量情况(系统运行状态） - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控当前批量情况(系统运行状态） - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控当前批量情况(系统运行状态） - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控当前批量情况(系统运行状态）");
    }

    @Test
    public void downHistoryJobLogTest() {
        System.out.println("TEST: 下载日志文件到本地");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("curr_bath_date", null);
        System.out.println("暂时不做");
        System.out.println("TEST-E: 下载日志文件到本地");
    }

    @Test
    public void monitorAllProjectChartsDataTest() {
        System.out.println("TEST: 监控所有项目图表数据");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_ALL_PROJECT_CHARTS_DATA_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控所有项目图表数据 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控所有项目图表数据 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控所有项目图表数据 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控所有项目图表数据 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控所有项目图表数据");
    }

    @Test
    public void getJobConsumeTimeSumTest() {
        System.out.println("TEST: 作业耗时记总");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("curr_bath_date", "20220828");
        try {
            String responseStr = HttpUtils.sendPost(GET_JOB_CONSUME_TIME_SUM_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t作业耗时记总 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t作业耗时记总 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t作业耗时记总 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t作业耗时记总 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 作业耗时记总");
    }

    @Test
    public void getTaskConsumeTimeSumTest() {
        System.out.println("TEST: 任务耗时记总");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("curr_bath_date", "20220828");
        try {
            String responseStr = HttpUtils.sendPost(GET_TASK_CONSUME_TIME_SUM_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t任务耗时记总 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t任务耗时记总 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t任务耗时记总 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t任务耗时记总 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 任务耗时记总");
    }

    @Test
    public void getProjectConsumeTimeSumTest() {
        System.out.println("TEST: 工程耗时记总");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("curr_bath_date", "20220828");
        try {
            String responseStr = HttpUtils.sendPost(GET_PROJECT_CONSUME_TIME_SUM_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t工程耗时记总 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t工程耗时记总 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t工程耗时记总 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t工程耗时记总 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 工程耗时记总");
    }

    @Test
    public void monitorBatchEtlJobDependencyInfoTest() {
        System.out.println("TEST: 监控依赖作业(全作业搜索)");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_BATCH_ETL_JOB_DEPENDENCY_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控依赖作业(全作业搜索) - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控依赖作业(全作业搜索) - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控依赖作业(全作业搜索) - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控依赖作业(全作业搜索) - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控依赖作业(全作业搜索)");
    }

    @Test
    public void generateExcelTest() {
        System.out.println("TEST: 导出excel表格");
        Map<String, String> map = new HashMap<>();
        map.put("projectData", "[{\"sub_sys_cd\":\"y02\",\"jobNum\":\"1\",\"taskConsumeAveTime\":\"585\",\"sub_sys_desc\":\"02\",\"taskConsumeTime\":\"585\",\"curr_end_time\":\"20220829093753\",\"curr_st_time\":\"20220829092808\"}]s");
        map.put("taskData", "[{\"sub_sys_cd\":\"y02\",\"jobNum\":\"1\",\"taskConsumeAveTime\":\"585\",\"sub_sys_desc\":\"02\",\"taskConsumeTime\":\"585\",\"curr_end_time\":\"20220829093753\",\"curr_st_time\":\"20220829092808\"}]s");
        map.put("jobData", "[{\"etl_job_desc\":\"YYY_Y008_测试_Test1_PARQUET\",\"curr_end_time\":\"20220829\",\"jobTime\":\"0\",\"curr_st_time\":\"20220829093753\",\"etl_job\":\"Y003_Y003_Test1_PARQUET\"}]");
        try {
            String responseStr = HttpUtils.sendPost(GENERATE_EXCEL_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t导出excel表格 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t导出excel表格 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t导出excel表格 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t导出excel表格 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 导出excel表格");
    }

    @Test
    public void downloadFileTest() {
        System.out.println("TEST: 下载文件");
        Map<String, String> map = new HashMap<>();
        map.put("fileName", "bachWorkTime.xlsx");
        System.out.println("暂时跳过");
        System.out.println("TEST-E: 下载文件");
    }

    @Test
    public void readHistoryJobLogInfoTest() {
        System.out.println("TEST: 查看作业日志信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("readNum", null);
        try {
            String responseStr = HttpUtils.sendPost(READ_HISTORY_JOB_LOG_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t查看作业日志信息 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t查看作业日志信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查看作业日志信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查看作业日志信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查看作业日志信息");
    }

    @Test
    public void monitorHistoryJobInfoTest() {
        System.out.println("TEST: 监控历史作业信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("start_date", null);
        map.put("end_date", null);
        map.put("isHistoryBatch", null);
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_HISTORY_JOB_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控历史作业信息 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控历史作业信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控历史作业信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控历史作业信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控历史作业信息");
    }

    @Test
    public void monitorCurrJobInfoTest() {
        System.out.println("TEST: 监控当前作业信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_CURR_JOB_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控当前作业信息 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控当前作业信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控当前作业信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控当前作业信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控当前作业信息");
    }

    @Test
    public void monitorJobDependencyInfoTest() {
        System.out.println("TEST: 监控作业依赖信息（单作业搜索）");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_JOB_DEPENDENCY_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控作业依赖信息（单作业搜索） - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控作业依赖信息（单作业搜索） - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控作业依赖信息（单作业搜索） - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控作业依赖信息（单作业搜索） - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控作业依赖信息（单作业搜索）");
    }

    @Test
    public void monitorCurrentBatchInfoByTaskTest() {
        System.out.println("TEST: 监控当前批量情况(根据任务查询作业运行状态）");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_CURRENT_BATCH_INFO_BY_TASK_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控当前批量情况(根据任务查询作业运行状态） - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控当前批量情况(根据任务查询作业运行状态） - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控当前批量情况(根据任务查询作业运行状态） - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控当前批量情况(根据任务查询作业运行状态） - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控当前批量情况(根据任务查询作业运行状态）");
    }

    @Test
    public void monitorSystemResourceInfoTest() {
        System.out.println("TEST: 监控系统资源状况");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_SYSTEM_RESOURCE_INFO_PATH, map, token);
            Map<?, ?> response = JsonUtil.toObjectSafety(responseStr, Map.class).get();
            if (response != null) {
                if (response.get("code") != null) {
                    Integer code = (Integer) response.get("code");
                    if (code.equals(999)) {
                        Object data = response.get("data");
                        System.out.println("--Success");
                        System.out.println("\t" + data);
                    } else {
                        System.err.println("\t监控系统资源状况 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t监控系统资源状况 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控系统资源状况 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控系统资源状况 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控系统资源状况");
    }

    @Test
    public void monitorHistoryBatchInfoTest() {
        System.out.println("TEST: 监控历史批量信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("curr_bath_date", "20220828");
        try {
            String responseStr = HttpUtils.sendPost(MONITOR_HISTORY_BATCH_INFO_PATH, map, token);
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
                    System.err.println("\t监控历史批量信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t监控历史批量信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t监控历史批量信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 监控历史批量信息");
    }
}
