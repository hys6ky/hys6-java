package hyren.serv6.c.jobconfig;

import java.util.HashMap;
import java.util.Map;
import hyren.serv6.commons.utils.constant.Constant;
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
@RequestMapping("jobconfig")
@Slf4j
public class JobConfigTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String UPDATE_ETL_JOB_DEF_PATH = IP_PORT + "/C/jobconfig/updateEtlJobDef";

    private static final String UPDATE_ETL_PARA_PATH = IP_PORT + "/C/jobconfig/updateEtlPara";

    private static final String UPDATE_ETL_SUB_SYS_PATH = IP_PORT + "/C/jobconfig/updateEtlSubSys";

    private static final String SEARCH_ETL_JOB_PATH = IP_PORT + "/C/jobconfig/searchEtlJob";

    private static final String SEARCH_ETL_SUB_SYS_PATH = IP_PORT + "/C/jobconfig/searchEtlSubSys";

    private static final String SAVE_ETL_PARA_PATH = IP_PORT + "/C/jobconfig/saveEtlPara";

    private static final String SAVE_ETL_SUB_SYS_PATH = IP_PORT + "/C/jobconfig/saveEtlSubSys";

    private static final String SAVE_ETL_RESOURCE_PATH = IP_PORT + "/C/jobconfig/saveEtlResource";

    private static final String DELETE_ETL_SUB_SYS_PATH = IP_PORT + "/C/jobconfig/deleteEtlSubSys";

    private static final String GENERATE_EXCEL_PATH = IP_PORT + "/C/jobconfig/generateExcel";

    private static final String SAVE_ETL_JOB_DEF_PATH = IP_PORT + "/C/jobconfig/saveEtlJobDef";

    private static final String SAVE_ETL_JOB_TEMP_PATH = IP_PORT + "/C/jobconfig/saveEtlJobTemp";

    private static final String DELETE_ETL_JOB_DEF_PATH = IP_PORT + "/C/jobconfig/deleteEtlJobDef";

    private static final String DELETE_ETL_PARA_PATH = IP_PORT + "/C/jobconfig/deleteEtlPara";

    private static final String SAVE_ETL_JOB_RESOURCE_RELA_PATH = IP_PORT + "/C/jobconfig/saveEtlJobResourceRela";

    private static final String BATCH_DELETE_ETL_DEPENDENCY_PATH = IP_PORT + "/C/jobconfig/batchDeleteEtlDependency";

    private static final String DELETE_ETL_JOB_RESOURCE_RELA_PATH = IP_PORT + "/C/jobconfig/deleteEtlJobResourceRela";

    private static final String BATCH_SAVE_ETL_DEPENDENCY_PATH = IP_PORT + "/C/jobconfig/batchSaveEtlDependency";

    private static final String DELETE_ETL_RESOURCE_PATH = IP_PORT + "/C/jobconfig/deleteEtlResource";

    private static final String SAVE_ETL_DEPENDENCY_PATH = IP_PORT + "/C/jobconfig/saveEtlDependency";

    private static final String DELETE_ETL_DEPENDENCY_PATH = IP_PORT + "/C/jobconfig/deleteEtlDependency";

    private static final String SAVE_ETL_ERROR_RESOURCE_PATH = IP_PORT + "/C/jobconfig/saveEtlErrorResource";

    private static final String UPDATE_ETL_DEPENDENCY_PATH = IP_PORT + "/C/jobconfig/updateEtlDependency";

    private static final String SEARCH_JOB_DEPENDENCY_PATH = IP_PORT + "/C/jobconfig/searchJobDependency";

    private static final String SEARCH_ETL_DEPENDENCY_BY_PAGE_PATH = IP_PORT + "/C/jobconfig/searchEtlDependencyByPage";

    private static final String SEARCH_ETL_SUB_SYS_BY_PAGE_PATH = IP_PORT + "/C/jobconfig/searchEtlSubSysByPage";

    private static final String SAVE_ETL_JOB_RESOURCE_PATH = IP_PORT + "/C/jobconfig/saveEtlJobResource";

    private static final String SEARCH_ETL_JOB_TEMP_AND_PARAM_PATH = IP_PORT + "/C/jobconfig/searchEtlJobTempAndParam";

    private static final String SEARCH_ETL_PARA_BY_PAGE_PATH = IP_PORT + "/C/jobconfig/searchEtlParaByPage";

    private static final String SEARCH_ETL_RESOURCE_BY_PAGE_PATH = IP_PORT + "/C/jobconfig/searchEtlResourceByPage";

    private static final String UPDATE_ETL_RESOURCE_PATH = IP_PORT + "/C/jobconfig/updateEtlResource";

    private static final String UPDATE_ETL_JOB_RESOURCE_RELA_PATH = IP_PORT + "/C/jobconfig/updateEtlJobResourceRela";

    private static final String SEARCH_ETL_JOB_DEF_BY_PAGE_PATH = IP_PORT + "/C/jobconfig/searchEtlJobDefByPage";

    private static final String SEARCH_ETL_RESOURCE_TYPE_PATH = IP_PORT + "/C/jobconfig/searchEtlResourceType";

    private static final String SEARCH_ETL_ERROR_RESOURCE_PATH = IP_PORT + "/C/jobconfig/searchEtlErrorResource";

    private static final String SEARCH_ETL_JOB_TEMPLATE_PATH = IP_PORT + "/C/jobconfig/searchEtlJobTemplate";

    private static final String SEARCH_ETL_JOB_RESOURCE_RELA_BY_PAGE_PATH = IP_PORT + "/C/jobconfig/searchEtlJobResourceRelaByPage";

    private static final String SEARCH_ETL_JOB_DEF_BY_ID_PATH = IP_PORT + "/C/jobconfig/searchEtlJobDefById";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void batchSaveEtlDependencyTest() {
        System.out.println("TEST: 批量新增保存作业依赖");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("pre_etl_sys_cd", "Y02");
        map.put("status", "T");
        map.put("sub_sys_cd", "y02");
        map.put("pre_sub_sys_cd", "y02");
        try {
            String responseStr = HttpUtils.sendPost(BATCH_SAVE_ETL_DEPENDENCY_PATH, map, token);
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
                    System.err.println("\t批量新增保存作业依赖 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t批量新增保存作业依赖 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t批量新增保存作业依赖 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 批量新增保存作业依赖");
    }

    @Test
    public void batchDeleteEtlDependencyTest() {
        System.out.println("TEST: 批量删除作业依赖");
        Map<String, String> map = new HashMap<>();
        map.put("etlDependencies", "[{\"etl_sys_cd\" : \"Y02\",\"etl_job\" : \"cs01_C01_agent_info_feidingzhang\",\"pre_etl_sys_cd\" : \"Y02\",\"pre_etl_job\" : \"Y003_Y003_Test1_PARQUET\",\"status\" : \"T\"}]");
        try {
            String responseStr = HttpUtils.sendPost(BATCH_DELETE_ETL_DEPENDENCY_PATH, map, token);
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
                    System.err.println("\t批量删除作业依赖 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t批量删除作业依赖 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t批量删除作业依赖 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 批量删除作业依赖");
    }

    @Test
    public void updateEtlJobResourceRelaTest() {
        System.out.println("TEST: 更新资源分配信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("resource_type", "normalDefType");
        map.put("resource_req", "1");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_JOB_RESOURCE_RELA_PATH, map, token);
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
                    System.err.println("\t更新资源分配信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新资源分配信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新资源分配信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新资源分配信息");
    }

    @Test
    public void searchEtlJobTempAndParamTest() {
        System.out.println("TEST: 关联查询作业模板表和作业模板参数表获取作业模板信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_temp_id", "123");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_JOB_TEMP_AND_PARAM_PATH, map, token);
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
                    System.err.println("\t关联查询作业模板表和作业模板参数表获取作业模板信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t关联查询作业模板表和作业模板参数表获取作业模板信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t关联查询作业模板表和作业模板参数表获取作业模板信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 关联查询作业模板表和作业模板参数表获取作业模板信息");
    }

    @Test
    public void searchEtlResourceByPageTest() {
        System.out.println("TEST: 分页查询etl资源定义信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("resource_type", "normalDefType");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_RESOURCE_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询etl资源定义信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询etl资源定义信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询etl资源定义信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询etl资源定义信息");
    }

    @Test
    public void searchEtlResourceTypeTest() {
        System.out.println("TEST: 查询资源类型");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_RESOURCE_TYPE_PATH, map, token);
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
                    System.err.println("\t查询资源类型 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询资源类型 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询资源类型 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询资源类型");
    }

    @Test
    public void searchJobDependencyTest() {
        System.out.println("TEST: 查询作业依赖的条数");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_JOB_DEPENDENCY_PATH, map, token);
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
                    System.err.println("\t查询作业依赖的条数 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询作业依赖的条数 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询作业依赖的条数 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询作业依赖的条数");
    }

    @Test
    public void updateEtlResourceTest() {
        System.out.println("TEST: 更新资源信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("resource_type", "Thrift");
        map.put("resource_max", "10");
        map.put("resource_name", "Y");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_RESOURCE_PATH, map, token);
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
                    System.err.println("\t更新资源信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新资源信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新资源信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新资源信息");
    }

    @Test
    public void searchEtlSubSysByPageTest() {
        System.out.println("TEST: 分页查询作业调度某工程任务信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("pre_sub_sys_cd", "y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_SUB_SYS_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询作业调度某工程任务信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询作业调度某工程任务信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询作业调度某工程任务信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询作业调度某工程任务信息");
    }

    @Test
    public void updateEtlDependencyTest() {
        System.out.println("TEST: 更新保存作业依赖");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("pre_etl_sys_cd", "Y02");
        map.put("pre_etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("oldEtlJob", "cs01_C01_agent_info_feidingzhang");
        map.put("oldPreEtlJob", "Y003_Y003_Test1_PARQUET");
        map.put("status", "T");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_DEPENDENCY_PATH, map, token);
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
                    System.err.println("\t更新保存作业依赖 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新保存作业依赖 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新保存作业依赖 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新保存作业依赖");
    }

    @Test
    public void saveEtlJobResourceTest() {
        System.out.println("TEST: 保存作业资源关系");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("pro_type", "normalDefType");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_JOB_RESOURCE_PATH, map, token);
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
                    System.err.println("\t保存作业资源关系 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存作业资源关系 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存作业资源关系 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存作业资源关系");
    }

    @Test
    public void searchEtlParaByPageTest() {
        System.out.println("TEST: 分页查询作业调度系统参数信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_PARA_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询作业调度系统参数信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询作业调度系统参数信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询作业调度系统参数信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询作业调度系统参数信息");
    }

    @Test
    public void deleteEtlResourceTest() {
        System.out.println("TEST: 删除作业资源定义");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("resource_type", "normalDefType");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_RESOURCE_PATH, map, token);
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
                    System.err.println("\t删除作业资源定义 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除作业资源定义 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除作业资源定义 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除作业资源定义");
    }

    @Test
    public void searchEtlJobResourceRelaByPageTest() {
        System.out.println("TEST: 分页查询作业资源分配信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("pageType", null);
        map.put("resource_type", "normalDefType");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_JOB_RESOURCE_RELA_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询作业资源分配信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询作业资源分配信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询作业资源分配信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询作业资源分配信息");
    }

    @Test
    public void saveEtlErrorResourceTest() {
        System.out.println("TEST: 新增错误作业重提机制配置信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "cs");
        map.put("start_number", "0");
        map.put("start_interval", "11");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_ERROR_RESOURCE_PATH, map, token);
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
                    System.err.println("\t新增错误作业重提机制配置信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增错误作业重提机制配置信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增错误作业重提机制配置信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增错误作业重提机制配置信息");
    }

    @Test
    public void searchEtlDependencyByPageTest() {
        System.out.println("TEST: 分页查询作业依赖信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("pre_etl_job", "Y003_Y003_Test1_PARQUET");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_DEPENDENCY_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询作业依赖信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询作业依赖信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询作业依赖信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询作业依赖信息");
    }

    @Test
    public void searchEtlJobDefByPageTest() {
        System.out.println("TEST: 分页查询作业定义信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("pro_type", "SHELL");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("pro_name", Constant.COLLECT_JOB_COMMAND);
        map.put("sub_sys_cd", "y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_JOB_DEF_BY_PAGE_PATH, map, token);
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
                    System.err.println("\t分页查询作业定义信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t分页查询作业定义信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t分页查询作业定义信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 分页查询作业定义信息");
    }

    @Test
    public void deleteEtlJobResourceRelaTest() {
        System.out.println("TEST: 根据工程编号，作业名称删除Etl作业资源关系");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_JOB_RESOURCE_RELA_PATH, map, token);
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
                    System.err.println("\t根据工程编号，作业名称删除Etl作业资源关系 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据工程编号，作业名称删除Etl作业资源关系 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据工程编号，作业名称删除Etl作业资源关系 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据工程编号，作业名称删除Etl作业资源关系");
    }

    @Test
    public void searchEtlJobTemplateTest() {
        System.out.println("TEST: 获取该工程下素有作业模板信息");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_JOB_TEMPLATE_PATH, map, token);
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
                    System.err.println("\t获取该工程下素有作业模板信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取该工程下素有作业模板信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取该工程下素有作业模板信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取该工程下素有作业模板信息");
    }

    @Test
    public void deleteEtlDependencyTest() {
        System.out.println("TEST: 删除作业依赖");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        map.put("pre_etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("pre_etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_DEPENDENCY_PATH, map, token);
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
                    System.err.println("\t删除作业依赖 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除作业依赖 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除作业依赖 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除作业依赖");
    }

    @Test
    public void searchEtlJobDefByIdTest() {
        System.out.println("TEST: 根据工程编号、作业名称查询作业定义信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "cs01_C01_agent_info_feidingzhang");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_JOB_DEF_BY_ID_PATH, map, token);
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
                    System.err.println("\t根据工程编号、作业名称查询作业定义信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据工程编号、作业名称查询作业定义信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据工程编号、作业名称查询作业定义信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据工程编号、作业名称查询作业定义信息");
    }

    @Test
    public void saveEtlJobResourceRelaTest() {
        System.out.println("TEST: 新增保存资源分配信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET-ql");
        map.put("resource_type", "normalDefType");
        map.put("resource_req", "1");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_JOB_RESOURCE_RELA_PATH, map, token);
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
                    System.err.println("\t新增保存资源分配信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存资源分配信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存资源分配信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存资源分配信息");
    }

    @Test
    public void saveEtlDependencyTest() {
        System.out.println("TEST: 新增保存作业依赖");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("pre_etl_sys_cd", "Y02ql");
        map.put("pre_etl_job", "Y02");
        map.put("status", "T");
        map.put("main_serv_sync", null);
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_DEPENDENCY_PATH, map, token);
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
                    System.err.println("\t新增保存作业依赖 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存作业依赖 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存作业依赖 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存作业依赖");
    }

    @Test
    public void searchEtlErrorResourceTest() {
        System.out.println("TEST: 查询错误作业重提机制配置信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_ERROR_RESOURCE_PATH, map, token);
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
                    System.err.println("\t查询错误作业重提机制配置信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询错误作业重提机制配置信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询错误作业重提机制配置信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询错误作业重提机制配置信息");
    }

    @Test
    public void deleteEtlJobDefTest() {
        System.out.println("TEST: 删除Etl作业定义信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("etl_job", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_JOB_DEF_PATH, map, token);
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
                    System.err.println("\t删除Etl作业定义信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除Etl作业定义信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除Etl作业定义信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除Etl作业定义信息");
    }

    @Test
    public void deleteEtlSubSysTest() {
        System.out.println("TEST: 根据工程编号，任务编号删除任务信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("sub_sys_cd", "y02");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_SUB_SYS_PATH, map, token);
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
                    System.err.println("\t根据工程编号，任务编号删除任务信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据工程编号，任务编号删除任务信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据工程编号，任务编号删除任务信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据工程编号，任务编号删除任务信息");
    }

    @Test
    public void generateExcelTest() {
        System.out.println("TEST: 生成Excel表");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("tableName", "etl_sub_sys_list");
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
                        System.err.println("\t监控历史批量信息 - 其它错误，信息:");
                        System.err.println("\t" + responseStr);
                    }
                } else {
                    System.err.println("\t生成Excel表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t生成Excel表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t生成Excel表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 生成Excel表");
    }

    @Test
    public void saveEtlJobDefTest() {
        System.out.println("TEST: 新增保存作业信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("sub_sys_cd", "y02");
        map.put("etl_job", "Y02");
        map.put("etl_job_desc", "ceshi_ql");
        map.put("pro_type", "SHELL");
        map.put("pro_dic", "/home/hyshf/HRSDATA/agent_deploy_dir/hrsagent/sjk_5551/.bin\\");
        map.put("pro_name", Constant.COLLECT_JOB_COMMAND);
        map.put("pro_para", "1062049903959609344@agent_info@4@#{txdate}@1");
        map.put("log_dic", "!{HYLOG}");
        map.put("disp_freq", "D");
        map.put("disp_offset", "0");
        map.put("disp_type", "Z");
        map.put("disp_time", "14:42:23");
        map.put("job_priority", "0");
        map.put("job_eff_flag", "Y");
        map.put("today_disp", "Y");
        map.put("success_job", null);
        map.put("fail_job", null);
        map.put("job_datasource", "01");
        map.put("comments", null);
        map.put("pre_etl_job", "Y003_Y003_Test1_PARQUET");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_JOB_DEF_PATH, map, token);
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
                    System.err.println("\t新增保存作业信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存作业信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存作业信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存作业信息");
    }

    @Test
    public void deleteEtlParaTest() {
        System.out.println("TEST: 删除作业系统参数");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("para_cd", "!HYLOG");
        try {
            String responseStr = HttpUtils.sendPost(DELETE_ETL_PARA_PATH, map, token);
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
                    System.err.println("\t删除作业系统参数 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除作业系统参数 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除作业系统参数 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除作业系统参数");
    }

    @Test
    public void searchEtlJobTest() {
        System.out.println("TEST: 模糊查询作业名称信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_JOB_PATH, map, token);
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
                    System.err.println("\t模糊查询作业名称信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t模糊查询作业名称信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t模糊查询作业名称信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 模糊查询作业名称信息");
    }

    @Test
    public void saveEtlSubSysTest() {
        System.out.println("TEST: 新增保存任务");
        Map<String, String> map = new HashMap<>();
        map.put("sub_sys_cd", "y02");
        map.put("etl_sys_cd", "Y02ql");
        map.put("sub_sys_desc", "ql");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_SUB_SYS_PATH, map, token);
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
                    System.err.println("\t新增保存任务 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存任务 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存任务 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存任务");
    }

    @Test
    public void updateEtlParaTest() {
        System.out.println("TEST: 更新保存作业系统参数");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("para_cd", "!HYLOG");
        map.put("para_val", "/home/hyshf/HRSDATA/agent_deploy_dir/hrsagent/Y007_7415/running/");
        map.put("para_type", "url");
        map.put("para_desc", "123");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_PARA_PATH, map, token);
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
                    System.err.println("\t更新保存作业系统参数 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新保存作业系统参数 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新保存作业系统参数 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新保存作业系统参数");
    }

    @Test
    public void saveEtlResourceTest() {
        System.out.println("TEST: 新增保存etl资源定义信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("resource_name", "Y02ql");
        map.put("resource_type", "normalDefType");
        map.put("resource_max", "10");
        map.put("resource_used", "0");
        map.put("main_serv_sync", "Y");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_RESOURCE_PATH, map, token);
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
                    System.err.println("\t新增保存etl资源定义信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存etl资源定义信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存etl资源定义信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存etl资源定义信息");
    }

    @Test
    public void saveEtlParaTest() {
        System.out.println("TEST: 新增保存作业系统参数");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("para_cd", "!HYLOG");
        map.put("para_val", "/home/hyshf/HRSDATA/agent_deploy_dir/hrsagent/Y007_7415/running/");
        map.put("para_type", "url");
        map.put("para_desc", "123");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_PARA_PATH, map, token);
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
                    System.err.println("\t新增保存作业系统参数 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增保存作业系统参数 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增保存作业系统参数 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增保存作业系统参数");
    }

    @Test
    public void updateEtlJobDefTest() {
        System.out.println("TEST: 更新作业定义信息并返回更新后的最新的作业信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        map.put("sub_sys_cd", "y02");
        map.put("etl_job", "Y02");
        map.put("etl_job_desc", "ceshi_ql");
        map.put("pro_type", "SHELL");
        map.put("pro_dic", "/home/hyshf/HRSDATA/agent_deploy_dir/hrsagent/sjk_5551/.bin\\");
        map.put("pro_name", Constant.COLLECT_JOB_COMMAND);
        map.put("pro_para", "1062049903959609344@agent_info@4@#{txdate}@1");
        map.put("log_dic", "!{HYLOG}");
        map.put("disp_freq", "D");
        map.put("disp_offset", "0");
        map.put("disp_type", "Z");
        map.put("disp_time", "14:42:23");
        map.put("job_priority", "0");
        map.put("job_eff_flag", "Y");
        map.put("today_disp", null);
        map.put("success_job", null);
        map.put("fail_job", null);
        map.put("job_datasource", "01");
        map.put("comments", null);
        map.put("pre_etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("old_disp_freq", "D");
        map.put("old_pre_etl_job", "Y003_Y003_Test1_PARQUET");
        map.put("old_dispatch_type", "Z");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_JOB_DEF_PATH, map, token);
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
                    System.err.println("\t更新作业定义信息并返回更新后的最新的作业信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新作业定义信息并返回更新后的最新的作业信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新作业定义信息并返回更新后的最新的作业信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新作业定义信息并返回更新后的最新的作业信息");
    }

    @Test
    public void saveEtlJobTempTest() {
        System.out.println("TEST: 保存作业模板信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02ql");
        map.put("sub_sys_cd", "y02");
        map.put("etl_job", "Y02");
        map.put("etl_temp_id", "100");
        map.put("etl_job_temp_para", "abc");
        map.put("job_datasource", "01");
        try {
            String responseStr = HttpUtils.sendPost(SAVE_ETL_JOB_TEMP_PATH, map, token);
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
                    System.err.println("\t保存作业模板信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存作业模板信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存作业模板信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存作业模板信息");
    }

    @Test
    public void searchEtlSubSysTest() {
        System.out.println("TEST: 查询任务信息");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "Y02");
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_ETL_SUB_SYS_PATH, map, token);
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
                    System.err.println("\t查询任务信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询任务信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询任务信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询任务信息");
    }

    @Test
    public void updateEtlSubSysTest() {
        System.out.println("TEST: 更新保存任务");
        Map<String, String> map = new HashMap<>();
        map.put("sub_sys_cd", "y02");
        map.put("etl_sys_cd", "Y02ql");
        map.put("sub_sys_desc", "ql");
        map.put("comments", "123");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_ETL_SUB_SYS_PATH, map, token);
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
                    System.err.println("\t更新保存任务 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新保存任务 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新保存任务 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新保存任务");
    }
}
