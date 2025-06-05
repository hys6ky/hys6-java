package hyren.serv6.h.market;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.h.HttpUtils;
import hyren.serv6.h.LoginUtils;

public class MarketTest {

    private static final String IP_PORT = "http://localhost:20001";

    private static final String DOWNLOAD_FILE_PATH = "E:/qianglei/localtemp/";

    private static final String GET_DM_CATEGORY_NODE_INFO_PATH = IP_PORT + "/H/market/getDmCategoryNodeInfo";

    private static final String GET_DM_DATA_TABLE_BY_DM_CATEGORY_PATH = IP_PORT + "/H/market/getDmDataTableByDmCategory";

    private static final String GET_DM_CATEGORY_FOR_DM_DATA_TABLE_PATH = IP_PORT + "/H/market/getDmCategoryForDmDataTable";

    private static final String GET_DM_CATEGORY_NODE_INFO_BY_ID_AND_NAME_PATH = IP_PORT + "/H/market/getDmCategoryNodeInfoByIdAndName";

    private static final String GET_ALL_DATATABLE__EN__NAME_PATH = IP_PORT + "/H/market/getAllDatatable_En_Name";

    private static final String GET_DM_CATEGORY_TREE_DATA_PATH = IP_PORT + "/H/market/getDmCategoryTreeData";

    private static final String GET_FROM_COLUMN_LIST_PATH = IP_PORT + "/H/market/getFromColumnList";

    private static final String GET_COLUMN_FROM_DATABASE_PATH = IP_PORT + "/H/market/getColumnFromDatabase";

    private static final String GET_DM_CATEGORY_INFO_PATH = IP_PORT + "/H/market/getDmCategoryInfo";

    private static final String DELETE_DM_CATEGORY_PATH = IP_PORT + "/H/market/deleteDmCategory";

    private static final String ADD_D_F_INFO_PATH = IP_PORT + "/H/market/addDFInfo";

    private static final String CHECK_ORACLE_PATH = IP_PORT + "/H/market/checkOracle";

    private static final String ADD_MARKET_PATH = IP_PORT + "/H/market/addMarket";

    private static final String ADD_D_M_DATA_TABLE_PATH = IP_PORT + "/H/market/addDMDataTable";

    private static final String DELETE_D_M_DATA_TABLE_PATH = IP_PORT + "/H/market/deleteDMDataTable";

    private static final String GENERATE_MART_JOB_TO_ETL_PATH = IP_PORT + "/H/market/generateMartJobToEtl";

    private static final String DELETE_PRE_POLYMERIZATION_PATH = IP_PORT + "/H/market/deletePrePolymerization";

    private static final String DELETE_IMPORT_FILE_PATH_PATH = IP_PORT + "/H/market/deleteImportFilePath";

    private static final String UPLOAD_EXCEL_FILE_PATH = IP_PORT + "/H/market/uploadExcelFile";

    private static final String UPLOAD_FILE_PATH = IP_PORT + "/H/market/uploadFile";

    private static final String SORT_HBAE_PATH = IP_PORT + "/H/market/sortHbae";

    private static final String SEARCH_DATA_STORE_PATH = IP_PORT + "/H/market/searchDataStore";

    private static final String DOWNLOAD_EXCEL_PATH = IP_PORT + "/H/market/downloadExcel";

    private static final String UPDATE_DM_CATEGORY_NAME_PATH = IP_PORT + "/H/market/updateDmCategoryName";

    private static final String UPDATE_D_M_DATA_TABLE_PATH = IP_PORT + "/H/market/updateDMDataTable";

    private static final String GET_ALL_DSL_IN_MART_PATH = IP_PORT + "/H/market/getAllDslInMart";

    private static final String GET_TABLE_NAME_PATH = IP_PORT + "/H/market/getTableName";

    private static final String GET_IF_HBASE_PATH = IP_PORT + "/H/market/getIfHbase";

    private static final String GET_ALL_FIELD__TYPE_PATH = IP_PORT + "/H/market/getAllField_Type";

    private static final String DELETE_MART_PATH = IP_PORT + "/H/market/deleteMart";

    private static final String QUERY_ALL_ETL_SYS_PATH = IP_PORT + "/H/market/queryAllEtlSys";

    private static final String IS_DATA_TABLE_EXIST_PATH = IP_PORT + "/H/market/isDataTableExist";

    private static final String SAVE_PRE_JOB_PATH = IP_PORT + "/H/market/savePreJob";

    private static final String GET_QUERY_SQL_PATH = IP_PORT + "/H/market/getQuerySql";

    private static final String EXCUT_MART_JOB_PATH = IP_PORT + "/H/market/excutMartJob";

    private static final String GET_TREE_DATA_INFO_PATH = IP_PORT + "/H/market/getTreeDataInfo";

    private static final String SAVE_DM_CATEGORY_PATH = IP_PORT + "/H/market/saveDmCategory";

    private static final String GET_MARKET_INFO_PATH = IP_PORT + "/H/market/getMarketInfo";

    private static final String SAVE_AFTER_JOB_PATH = IP_PORT + "/H/market/saveAfterJob";

    private static final String GET_SPARK_SQL_GRAM_PATH = IP_PORT + "/H/market/getSparkSqlGram";

    private static final String GET_COLUMN_MORE_PATH = IP_PORT + "/H/market/getColumnMore";

    private static final String GETDMINFO_PATH = IP_PORT + "/H/market/getdminfo";

    private static final String GET_IMPORT_REVIEW_DATA_PATH = IP_PORT + "/H/market/getImportReviewData";

    private static final String GET_PRE_AND_AFTER_JOB_PATH = IP_PORT + "/H/market/getPreAndAfterJob";

    private static final String GET_IF_RELATION_DATABASE2_PATH = IP_PORT + "/H/market/getIfRelationDatabase2";

    private static final String GET_RELATION_BY_TABLE_NAMES_PATH = IP_PORT + "/H/market/getRelationByTableNames";

    private static final String GET_TABLE_ID_FROM_SAME_NAME_TABLE_ID_PATH = IP_PORT + "/H/market/getTableIdFromSameNameTableId";

    private static final String GET_TABLE_TOP5_IN_DSL_PATH = IP_PORT + "/H/market/getTableTop5InDsl";

    private static final String QUERY_D_M_DATA_TABLE_BY_DATA_TABLE_ID_PATH = IP_PORT + "/H/market/queryDMDataTableByDataTableId";

    private static final String QUERY_ALL_COLUMN_ON_TABLE_NAME_PATH = IP_PORT + "/H/market/queryAllColumnOnTableName";

    private static final String SAVE_PRE_POLYMERIZATION_PATH = IP_PORT + "/H/market/savePrePolymerization";

    private static final String QUERY_D_M_DATA_TABLE_BY_DATA_MART_I_D_PATH = IP_PORT + "/H/market/queryDMDataTableByDataMartID";

    private static final String QUERY_ETL_TASK_BY_ETL_SYS_PATH = IP_PORT + "/H/market/queryEtlTaskByEtlSys";

    private static final String SEARCH_DATA_STORE_BY_FUZZY_QUERY_PATH = IP_PORT + "/H/market/searchDataStoreByFuzzyQuery";

    private static final String QUERY_TABLE_NAME_IF_REPEAT_PATH = IP_PORT + "/H/market/queryTableNameIfRepeat";

    private static final String PRE_POLYMERIZATION_PATH = IP_PORT + "/H/market/prePolymerization";

    private static final String EXPORT_MAPPING_EXCEL_PATH = IP_PORT + "/H/market/exportMappingExcel";

    private String token;

    @Before
    public void login() {
        this.token = LoginUtils.login();
    }

    @Test
    public void getDmCategoryNodeInfoTest() {
        System.out.println("TEST: 获取所有分类节点信息");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_DM_CATEGORY_NODE_INFO_PATH, map, null, token);
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
                    System.err.println("\t获取所有分类节点信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取所有分类节点信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取所有分类节点信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取所有分类节点信息");
    }

    @Test
    public void getDmDataTableByDmCategoryTest() {
        System.out.println("TEST: 根据加工分类获取数据表信息");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        map.put("category_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_DM_DATA_TABLE_BY_DM_CATEGORY_PATH, map, null, token);
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
                    System.err.println("\t根据加工分类获取数据表信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据加工分类获取数据表信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据加工分类获取数据表信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据加工分类获取数据表信息");
    }

    @Test
    public void getDmCategoryForDmDataTableTest() {
        System.out.println("TEST: 获取数据表所有分类信息集合");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", "961314446716727296");
        try {
            String responseStr = HttpUtils.sendPost(GET_DM_CATEGORY_FOR_DM_DATA_TABLE_PATH, map, null, token);
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
                    System.err.println("\t获取数据表所有分类信息集合 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取数据表所有分类信息集合 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取数据表所有分类信息集合 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取数据表所有分类信息集合");
    }

    @Test
    public void getDmCategoryNodeInfoByIdAndNameTest() {
        System.out.println("TEST: 根据分类ID，分类名称获取分类信息");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        map.put("category_name", null);
        map.put("category_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_DM_CATEGORY_NODE_INFO_BY_ID_AND_NAME_PATH, map, null, token);
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
                    System.err.println("\t根据分类ID，分类名称获取分类信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据分类ID，分类名称获取分类信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据分类ID，分类名称获取分类信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据分类ID，分类名称获取分类信息");
    }

    @Test
    public void getAllDatatable_En_NameTest() {
        System.out.println("TEST: 根据用户所属的部门查询所有加工表");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_ALL_DATATABLE__EN__NAME_PATH, map, null, token);
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
                    System.err.println("\t根据用户所属的部门查询所有加工表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据用户所属的部门查询所有加工表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据用户所属的部门查询所有加工表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据用户所属的部门查询所有加工表");
    }

    @Test
    public void getDmCategoryTreeDataTest() {
        System.out.println("TEST: 获取加工分类树数据");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_DM_CATEGORY_TREE_DATA_PATH, map, null, token);
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
                    System.err.println("\t获取加工分类树数据 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取加工分类树数据 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取加工分类树数据 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取加工分类树数据");
    }

    @Test
    public void getFromColumnListTest() {
        System.out.println("TEST: 回显新增加工页面2中记录所有来源字段");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_FROM_COLUMN_LIST_PATH, map, null, token);
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
                    System.err.println("\t回显新增加工页面2中记录所有来源字段 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t回显新增加工页面2中记录所有来源字段 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t回显新增加工页面2中记录所有来源字段 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 回显新增加工页面2中记录所有来源字段");
    }

    @Test
    public void getColumnFromDatabaseTest() {
        System.out.println("TEST: 回显新增加工页面2中记录在数据库中的字段信息");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_COLUMN_FROM_DATABASE_PATH, map, null, token);
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
                    System.err.println("\t回显新增加工页面2中记录在数据库中的字段信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t回显新增加工页面2中记录在数据库中的字段信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t回显新增加工页面2中记录在数据库中的字段信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 回显新增加工页面2中记录在数据库中的字段信息");
    }

    @Test
    public void getDmCategoryInfoTest() {
        System.out.println("TEST: 根据数据加工id查询加工分类信息");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_DM_CATEGORY_INFO_PATH, map, null, token);
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
                    System.err.println("\t根据数据加工id查询加工分类信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据数据加工id查询加工分类信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据数据加工id查询加工分类信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据数据加工id查询加工分类信息");
    }

    @Test
    public void deleteDmCategoryTest() {
        System.out.println("TEST: 根据加工分类id删除加工分类");
        Map<String, String> map = new HashMap<>();
        map.put("category_id", null);
        try {
            String responseStr = HttpUtils.sendPost(DELETE_DM_CATEGORY_PATH, map, null, token);
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
                    System.err.println("\t根据加工分类id删除加工分类 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据加工分类id删除加工分类 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据加工分类id删除加工分类 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据加工分类id删除加工分类");
    }

    @Test
    public void addDFInfoTest() {
        System.out.println("TEST: 保存新增加工2的数据");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = "";
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
                    System.err.println("\t保存新增加工2的数据 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存新增加工2的数据 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存新增加工2的数据 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存新增加工2的数据");
    }

    @Test
    public void checkOracleTest() {
        System.out.println("TEST: 新增页面判断选择的当前存储类型是否为oracle,且判断表名是否过长");
        Map<String, String> map = new HashMap<>();
        map.put("dsl_id", "764796914716639232");
        map.put("datatable_en_name", "hll_test01_psql_zl_01");
        try {
            String responseStr = HttpUtils.sendPost(CHECK_ORACLE_PATH, map, null, token);
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
                    System.err.println("\t新增页面判断选择的当前存储类型是否为oracle,且判断表名是否过长 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增页面判断选择的当前存储类型是否为oracle,且判断表名是否过长 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增页面判断选择的当前存储类型是否为oracle,且判断表名是否过长 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增页面判断选择的当前存储类型是否为oracle,且判断表名是否过长");
    }

    @Test
    public void addMarketTest() {
        System.out.println("TEST: 新增加工工程");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(ADD_MARKET_PATH, map, null, token);
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
                    System.err.println("\t新增加工工程 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t新增加工工程 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t新增加工工程 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 新增加工工程");
    }

    @Test
    public void addDMDataTableTest() {
        System.out.println("TEST: 保存加工添加表页面1的信息，新增加工表");
        Map<String, String> map = new HashMap<>();
        map.put("dm_datatable", null);
        map.put("dsl_id", null);
        try {
            String responseStr = HttpUtils.sendPost(ADD_D_M_DATA_TABLE_PATH, map, null, token);
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
                    System.err.println("\t保存加工添加表页面1的信息，新增加工表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存加工添加表页面1的信息，新增加工表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存加工添加表页面1的信息，新增加工表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存加工添加表页面1的信息，新增加工表");
    }

    @Test
    public void deleteDMDataTableTest() {
        System.out.println("TEST: 删除加工表及其相关的所有信息");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        try {
            String responseStr = HttpUtils.sendPost(DELETE_D_M_DATA_TABLE_PATH, map, null, token);
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
                    System.err.println("\t删除加工表及其相关的所有信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除加工表及其相关的所有信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除加工表及其相关的所有信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除加工表及其相关的所有信息");
    }

    @Test
    public void generateMartJobToEtlTest() {
        System.out.println("TEST: 生成加工表到作业调度");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "962004026247901184");
        map.put("etl_sys_cd", "CS01");
        map.put("sub_sys_cd", null);
        try {
            String responseStr = HttpUtils.sendPost(GENERATE_MART_JOB_TO_ETL_PATH, map, null, token);
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
                    System.err.println("\t生成加工表到作业调度 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t生成加工表到作业调度 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t生成加工表到作业调度 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 生成加工表到作业调度");
    }

    @Test
    public void deletePrePolymerizationTest() {
        System.out.println("TEST: 删除预聚合SQL");
        Map<String, String> map = new HashMap<>();
        map.put("agg_id", null);
        try {
            String responseStr = HttpUtils.sendPost(DELETE_PRE_POLYMERIZATION_PATH, map, null, token);
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
                    System.err.println("\t删除预聚合SQL - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除预聚合SQL - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除预聚合SQL - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除预聚合SQL");
    }

    @Test
    public void deleteImportFilePathTest() {
        System.out.println("TEST: 删除加工导入审核上传文件数据");
        Map<String, String> map = new HashMap<>();
        map.put("file_path", null);
        try {
            String responseStr = HttpUtils.sendPost(DELETE_IMPORT_FILE_PATH_PATH, map, null, token);
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
                    System.err.println("\t删除加工导入审核上传文件数据 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除加工导入审核上传文件数据 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除加工导入审核上传文件数据 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除加工导入审核上传文件数据");
    }

    @Test
    public void uploadExcelFileTest() {
        System.out.println("TEST: 上传Excel文件");
        Map<String, String> map = new HashMap<>();
        map.put("file", null);
        map.put("data_mart_id", null);
        try {
            String responseStr = HttpUtils.sendPost(UPLOAD_EXCEL_FILE_PATH, map, null, token);
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
                    System.err.println("\t上传Excel文件 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t上传Excel文件 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t上传Excel文件 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 上传Excel文件");
    }

    @Test
    public void uploadFileTest() {
        System.out.println("TEST: 上传加工工程");
        Map<String, String> map = new HashMap<>();
        map.put("file_path", null);
        try {
            String responseStr = HttpUtils.sendPost(UPLOAD_FILE_PATH, map, null, token);
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
                    System.err.println("\t上传加工工程 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t上传加工工程 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t上传加工工程 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 上传加工工程");
    }

    @Test
    public void sortHbaeTest() {
        System.out.println("TEST: 回显hbase的rowkey排序");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        map.put("hbasesort", null);
        try {
            String responseStr = HttpUtils.sendPost(SORT_HBAE_PATH, map, null, token);
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
                    System.err.println("\t回显hbase的rowkey排序 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t回显hbase的rowkey排序 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t回显hbase的rowkey排序 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 回显hbase的rowkey排序");
    }

    @Test
    public void searchDataStoreTest() {
        System.out.println("TEST: 加工查询存储配置表");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_DATA_STORE_PATH, map, null, token);
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
                    System.err.println("\t加工查询存储配置表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t加工查询存储配置表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t加工查询存储配置表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 加工查询存储配置表");
    }

    @Test
    public void downloadExcelTest() {
        System.out.println("TEST: 下载加工数据表excel模板");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(DOWNLOAD_EXCEL_PATH, map, null, token);
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
                    System.err.println("\t下载加工数据表excel模板 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t下载加工数据表excel模板 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t下载加工数据表excel模板 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 下载加工数据表excel模板");
    }

    @Test
    public void updateDmCategoryNameTest() {
        System.out.println("TEST: 更新加工分类名称");
        Map<String, String> map = new HashMap<>();
        map.put("category_id", null);
        map.put("category_name", null);
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_DM_CATEGORY_NAME_PATH, map, null, token);
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
                    System.err.println("\t更新加工分类名称 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t更新加工分类名称 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t更新加工分类名称 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 更新加工分类名称");
    }

    @Test
    public void updateDMDataTableTest() {
        System.out.println("TEST: 编辑更新加工添加表页面1的信息，更新加工表");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        map.put("data_mart_id", "961314446716727296");
        map.put("datatable_cn_name", "hll_test01_psql_zl_01");
        map.put("datatable_en_name", "hll_test01_psql_zl_01");
        map.put("datatable_desc", "hll_test01_psql_zl_01");
        map.put("datatable_create_date", "20220408");
        map.put("datatable_create_time", "105746");
        map.put("datatable_due_date", "	9999-12-31");
        map.put("ddlc_date", "20220408");
        map.put("ddlc_time", "105928");
        map.put("datac_date", "20220408");
        map.put("datac_time", "105943");
        map.put("datatable_lifecycle", "1");
        map.put("soruce_size", "0");
        map.put("etl_date", "20220402");
        map.put("sql_engine", "1");
        map.put("storage_type", "4");
        map.put("table_storage", "0");
        map.put("remark", "");
        map.put("pre_partition", "");
        map.put("repeat_flag", "0");
        map.put("category_id", "961314446842556416");
        map.put("dsl_id", "764796914716639232");
        try {
            String responseStr = HttpUtils.sendPost(UPDATE_D_M_DATA_TABLE_PATH, map, null, token);
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
                    System.err.println("\t编辑更新加工添加表页面1的信息，更新加工表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t编辑更新加工添加表页面1的信息，更新加工表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t编辑更新加工添加表页面1的信息，更新加工表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 编辑更新加工添加表页面1的信息，更新加工表");
    }

    @Test
    public void getAllDslInMartTest() {
        System.out.println("TEST: 获取加工所有用到的存储层");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_ALL_DSL_IN_MART_PATH, map, null, token);
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
                    System.err.println("\t获取加工所有用到的存储层 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取加工所有用到的存储层 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取加工所有用到的存储层 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取加工所有用到的存储层");
    }

    @Test
    public void getTableNameTest() {
        System.out.println("TEST: 根据表主键查询表名");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_TABLE_NAME_PATH, map, null, token);
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
                    System.err.println("\t根据表主键查询表名 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据表主键查询表名 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据表主键查询表名 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据表主键查询表名");
    }

    @Test
    public void getIfHbaseTest() {
        System.out.println("TEST: 根据加工表ID，判断是否是进入Hbase的目的地");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_IF_HBASE_PATH, map, null, token);
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
                    System.err.println("\t根据加工表ID，判断是否是进入Hbase的目的地 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据加工表ID，判断是否是进入Hbase的目的地 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据加工表ID，判断是否是进入Hbase的目的地 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据加工表ID，判断是否是进入Hbase的目的地");
    }

    @Test
    public void getAllField_TypeTest() {
        System.out.println("TEST: 根据加工表ID,获取字段类型的所有类型");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_ALL_FIELD__TYPE_PATH, map, null, token);
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
                    System.err.println("\t根据加工表ID,获取字段类型的所有类型 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据加工表ID,获取字段类型的所有类型 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据加工表ID,获取字段类型的所有类型 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据加工表ID,获取字段类型的所有类型");
    }

    @Test
    public void deleteMartTest() {
        System.out.println("TEST: 删除加工工程");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        try {
            String responseStr = HttpUtils.sendPost(DELETE_MART_PATH, map, null, token);
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
                    System.err.println("\t删除加工工程 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t删除加工工程 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t删除加工工程 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 删除加工工程");
    }

    @Test
    public void queryAllEtlSysTest() {
        System.out.println("TEST: 查询所有作业调度工程");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(QUERY_ALL_ETL_SYS_PATH, map, null, token);
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
                    System.err.println("\t查询所有作业调度工程 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询所有作业调度工程 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询所有作业调度工程 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询所有作业调度工程");
    }

    @Test
    public void isDataTableExistTest() {
        System.out.println("TEST: 判断表是否存在");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_en_name", null);
        try {
            String responseStr = HttpUtils.sendPost(IS_DATA_TABLE_EXIST_PATH, map, null, token);
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
                    System.err.println("\t判断表是否存在 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t判断表是否存在 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t判断表是否存在 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 判断表是否存在");
    }

    @Test
    public void savePreJobTest() {
        System.out.println("TEST: 保存前置作业");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        map.put("pre_work", null);
        try {
            String responseStr = HttpUtils.sendPost(SAVE_PRE_JOB_PATH, map, null, token);
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
                    System.err.println("\t保存前置作业 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存前置作业 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存前置作业 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存前置作业");
    }

    @Test
    public void getQuerySqlTest() {
        System.out.println("TEST: 根据加工表ID,获取SQL回显");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_QUERY_SQL_PATH, map, null, token);
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
                    System.err.println("\t根据加工表ID,获取SQL回显 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据加工表ID,获取SQL回显 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据加工表ID,获取SQL回显 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据加工表ID,获取SQL回显");
    }

    @Test
    public void excutMartJobTest() {
        System.out.println("TEST: 执行加工作业");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "1117832377800880128");
        map.put("date", "20230619");
        map.put("parameter", null);
        try {
            String responseStr = HttpUtils.sendPost(EXCUT_MART_JOB_PATH, map, null, token);
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
                    System.err.println("\t执行加工作业 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t执行加工作业 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t执行加工作业 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 执行加工作业");
    }

    @Test
    public void getTreeDataInfoTest() {
        System.out.println("TEST: 获取树的数据信息");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_TREE_DATA_INFO_PATH, map, null, token);
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
                    System.err.println("\t获取树的数据信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取树的数据信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取树的数据信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取树的数据信息");
    }

    @Test
    public void saveDmCategoryTest() {
        System.out.println("TEST: 保存加工分类");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(SAVE_DM_CATEGORY_PATH, map, null, token);
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
                    System.err.println("\t保存加工分类 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存加工分类 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存加工分类 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存加工分类");
    }

    @Test
    public void getMarketInfoTest() {
        System.out.println("TEST: 获取登录用户数据加工首页信息");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_MARKET_INFO_PATH, map, null, token);
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
                    System.err.println("\t获取登录用户数据加工首页信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取登录用户数据加工首页信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取登录用户数据加工首页信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取登录用户数据加工首页信息");
    }

    @Test
    public void saveAfterJobTest() {
        System.out.println("TEST: 保存后置作业");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        map.put("post_work", null);
        try {
            String responseStr = HttpUtils.sendPost(SAVE_AFTER_JOB_PATH, map, null, token);
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
                    System.err.println("\t保存后置作业 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t保存后置作业 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t保存后置作业 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 保存后置作业");
    }

    @Test
    public void getSparkSqlGramTest() {
        System.out.println("TEST: 获取加工函数映射可用的函数");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_SPARK_SQL_GRAM_PATH, map, null, token);
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
                    System.err.println("\t获取加工函数映射可用的函数 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取加工函数映射可用的函数 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取加工函数映射可用的函数 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取加工函数映射可用的函数");
    }

    @Test
    public void getColumnMoreTest() {
        System.out.println("TEST: 根据数据表ID,获取数据库类型，获取选中数据库的附加属性字段");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_COLUMN_MORE_PATH, map, null, token);
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
                    System.err.println("\t根据数据表ID,获取数据库类型，获取选中数据库的附加属性字段 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据数据表ID,获取数据库类型，获取选中数据库的附加属性字段 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据数据表ID,获取数据库类型，获取选中数据库的附加属性字段 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据数据表ID,获取数据库类型，获取选中数据库的附加属性字段");
    }

    @Test
    public void getdminfoTest() {
        System.out.println("TEST: 获取加工工程的具体信息");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GETDMINFO_PATH, map, null, token);
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
                    System.err.println("\t获取加工工程的具体信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取加工工程的具体信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取加工工程的具体信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取加工工程的具体信息");
    }

    @Test
    public void getImportReviewDataTest() {
        System.out.println("TEST: 获取加工导入审核数据");
        Map<String, String> map = new HashMap<>();
        map.put("file_path", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_IMPORT_REVIEW_DATA_PATH, map, null, token);
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
                    System.err.println("\t获取加工导入审核数据 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取加工导入审核数据 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取加工导入审核数据 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取加工导入审核数据");
    }

    @Test
    public void getPreAndAfterJobTest() {
        System.out.println("TEST: 前后置处理SQL回显");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_PRE_AND_AFTER_JOB_PATH, map, null, token);
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
                    System.err.println("\t前后置处理SQL回显 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t前后置处理SQL回显 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t前后置处理SQL回显 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 前后置处理SQL回显");
    }

    @Test
    public void getIfRelationDatabase2Test() {
        System.out.println("TEST: 根据加工表主键查询当前存储层是否是关系性数据库");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        try {
            String responseStr = HttpUtils.sendPost(GET_IF_RELATION_DATABASE2_PATH, map, null, token);
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
                    System.err.println("\t根据加工表主键查询当前存储层是否是关系性数据库 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据加工表主键查询当前存储层是否是关系性数据库 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据加工表主键查询当前存储层是否是关系性数据库 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据加工表主键查询当前存储层是否是关系性数据库");
    }

    @Test
    public void getRelationByTableNamesTest() {
        System.out.println("TEST: 根据表名获取表外键关联信息");
        Map<String, String> map = new HashMap<>();
        map.put("tableNames", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_RELATION_BY_TABLE_NAMES_PATH, map, null, token);
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
                    System.err.println("\t根据表名获取表外键关联信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据表名获取表外键关联信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据表名获取表外键关联信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据表名获取表外键关联信息");
    }

    @Test
    public void getTableIdFromSameNameTableIdTest() {
        System.out.println("TEST: 查询与当前datatable_id拥有相同datatable_en_name的另外一组datatable_id");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        try {
            String responseStr = HttpUtils.sendPost(GET_TABLE_ID_FROM_SAME_NAME_TABLE_ID_PATH, map, null, token);
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
                    System.err.println("\t查询与当前datatable_id拥有相同datatable_en_name的另外一组datatable_id - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询与当前datatable_id拥有相同datatable_en_name的另外一组datatable_id - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询与当前datatable_id拥有相同datatable_en_name的另外一组datatable_id - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询与当前datatable_id拥有相同datatable_en_name的另外一组datatable_id");
    }

    @Test
    public void getTableTop5InDslTest() {
        System.out.println("TEST: 获取各个存储层中表大小的前五名");
        Map<String, String> map = new HashMap<>();
        try {
            String responseStr = HttpUtils.sendPost(GET_TABLE_TOP5_IN_DSL_PATH, map, null, token);
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
                    System.err.println("\t获取各个存储层中表大小的前五名 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取各个存储层中表大小的前五名 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取各个存储层中表大小的前五名 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取各个存储层中表大小的前五名");
    }

    @Test
    public void queryDMDataTableByDataTableIdTest() {
        System.out.println("TEST: 加工页面1回显");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "1117832281529020416");
        try {
            String responseStr = HttpUtils.sendPost(QUERY_D_M_DATA_TABLE_BY_DATA_TABLE_ID_PATH, map, null, token);
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
                    System.err.println("\t加工页面1回显 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t加工页面1回显 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t加工页面1回显 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 加工页面1回显");
    }

    @Test
    public void queryAllColumnOnTableNameTest() {
        System.out.println("TEST: 树上的展示根据表名,返回源表名和全表字段名");
        Map<String, String> map = new HashMap<>();
        map.put("source", null);
        map.put("id", null);
        try {
            String responseStr = HttpUtils.sendPost(QUERY_ALL_COLUMN_ON_TABLE_NAME_PATH, map, null, token);
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
                    System.err.println("\t树上的展示根据表名,返回源表名和全表字段名 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t树上的展示根据表名,返回源表名和全表字段名 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t树上的展示根据表名,返回源表名和全表字段名 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 树上的展示根据表名,返回源表名和全表字段名");
    }

    @Test
    public void savePrePolymerizationTest() {
        System.out.println("TEST: 获取预聚合SQL数据信息");
        Map<String, String> map = new HashMap<>();
        map.put("preaggregate", null);
        try {
            String responseStr = HttpUtils.sendPost(SAVE_PRE_POLYMERIZATION_PATH, map, null, token);
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
                    System.err.println("\t获取预聚合SQL数据信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取预聚合SQL数据信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取预聚合SQL数据信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取预聚合SQL数据信息");
    }

    @Test
    public void queryDMDataTableByDataMartIDTest() {
        System.out.println("TEST: 获取登录用户查询数据加工工程下的所有加工表");
        Map<String, String> map = new HashMap<>();
        map.put("data_mart_id", "961314446716727296");
        try {
            String responseStr = HttpUtils.sendPost(QUERY_D_M_DATA_TABLE_BY_DATA_MART_I_D_PATH, map, null, token);
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
                    System.err.println("\t获取登录用户查询数据加工工程下的所有加工表 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取登录用户查询数据加工工程下的所有加工表 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取登录用户查询数据加工工程下的所有加工表 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取登录用户查询数据加工工程下的所有加工表");
    }

    @Test
    public void queryEtlTaskByEtlSysTest() {
        System.out.println("TEST: 查询作业调度工程下的所有任务");
        Map<String, String> map = new HashMap<>();
        map.put("etl_sys_cd", "CS01");
        try {
            String responseStr = HttpUtils.sendPost(QUERY_ETL_TASK_BY_ETL_SYS_PATH, map, null, token);
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
                    System.err.println("\t查询作业调度工程下的所有任务 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t查询作业调度工程下的所有任务 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t查询作业调度工程下的所有任务 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 查询作业调度工程下的所有任务");
    }

    @Test
    public void searchDataStoreByFuzzyQueryTest() {
        System.out.println("TEST: 加工查询存储配置表（模糊查询）");
        Map<String, String> map = new HashMap<>();
        map.put("fuzzyqueryitem", null);
        try {
            String responseStr = HttpUtils.sendPost(SEARCH_DATA_STORE_BY_FUZZY_QUERY_PATH, map, null, token);
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
                    System.err.println("\t加工查询存储配置表（模糊查询） - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t加工查询存储配置表（模糊查询） - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t加工查询存储配置表（模糊查询） - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 加工查询存储配置表（模糊查询）");
    }

    @Test
    public void queryTableNameIfRepeatTest() {
        System.out.println("TEST: 根据数据加工表英文名 检查表名是否重复");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_en_name", null);
        map.put("datatable_id", null);
        try {
            String responseStr = HttpUtils.sendPost(QUERY_TABLE_NAME_IF_REPEAT_PATH, map, null, token);
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
                    System.err.println("\t根据数据加工表英文名 检查表名是否重复 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t根据数据加工表英文名 检查表名是否重复 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t根据数据加工表英文名 检查表名是否重复 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 根据数据加工表英文名 检查表名是否重复");
    }

    @Test
    public void prePolymerizationTest() {
        System.out.println("TEST: 获取预聚合SQL数据信息");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", null);
        try {
            String responseStr = HttpUtils.sendPost(PRE_POLYMERIZATION_PATH, map, null, token);
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
                    System.err.println("\t获取预聚合SQL数据信息 - 失败，信息:");
                    System.err.println("\t" + responseStr);
                }
            } else {
                System.err.println("\t获取预聚合SQL数据信息 - 失败：请检查url");
            }
        } catch (Exception e) {
            System.err.println("\t获取预聚合SQL数据信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 获取预聚合SQL数据信息");
    }

    @Test
    public void exportMappingExcelTest() {
        System.out.println("TEST: 控制响应头下载加工表的excel信息");
        Map<String, String> map = new HashMap<>();
        map.put("datatable_id", "961942930786906112");
        map.put("tablename", "hll_test01_psql_zl_01");
        try {
            String path = HttpUtils.download(EXPORT_MAPPING_EXCEL_PATH, map, null, token, DOWNLOAD_FILE_PATH);
            System.out.println("文件地址：" + path);
        } catch (Exception e) {
            System.err.println("\t控制响应头下载加工表的excel信息 - 失败:");
            System.err.println("\t" + e.getMessage());
        }
        System.out.println("TEST-E: 控制响应头下载加工表的excel信息");
    }
}
