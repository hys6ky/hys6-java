package hyren.serv6.f.source.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.f.dataRegister.bean.StoreConnectionBean;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class SendMsgUtil {

    private static final String MESSAGE = "message";

    private static final String CODE = "code";

    private static final String MSG = "msg";

    private static final String DATA = "data";

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "databaseInfo", desc = "", range = "")
    @Param(name = "inputString", desc = "", range = "")
    @Param(name = "methodName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String searchTableName(Long agentId, Long userId, Map<String, Object> databaseInfo, String inputString, String methodName) {
        if (agentId == null) {
            throw new BusinessException("向Agent发送信息，模糊查询表信息时agentId不能为空");
        }
        if (userId == null) {
            throw new BusinessException("向Agent发送信息，模糊查询表信息时userId不能为空");
        }
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("向Agent发送信息，模糊查询表信息时，请指定数据库连接信息");
        }
        if (StringUtil.isBlank(inputString)) {
            throw new BusinessException("向Agent发送信息，模糊查询表信息时，请指定模糊查询字段");
        }
        if (StringUtil.isBlank(methodName)) {
            throw new BusinessException("向Agent发送信息，模糊查询表信息时，methodName不能为空");
        }
        String url = AgentActionUtil.getUrl(agentId, userId, methodName);
        log.debug("准备建立连接，请求的URL为" + url);
        StoreConnectionBean legalParam = SendMsgUtil.setStoreConnectionBean(Long.parseLong(databaseInfo.get("dsl_id").toString()));
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("database_name", legalParam.getDatabase_name()).addData("database_pad", legalParam.getDatabase_pwd()).addData("user_name", legalParam.getUser_name()).addData("database_drive", legalParam.getDatabase_driver()).addData("jdbc_url", legalParam.getJdbc_url()).addData("database_type", legalParam.getDatabase_type()).addData("plane_url", databaseInfo.get("plane_url") == null ? "" : (String) databaseInfo.get("plane_url")).addData("db_agent", databaseInfo.get("db_agent") == null ? "" : (String) databaseInfo.get("db_agent")).addData("fetch_size", databaseInfo.get("fetch_size") == null ? "" : String.valueOf(databaseInfo.get("fetch_size"))).addData("search", inputString).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (ar.isSuccess()) {
            String msg = PackUtil.unpackMsg((String) ar.getData()).get(MSG);
            log.debug(">>>>>>>>>>>>>>>>>>>>>>>>返回消息为：" + msg);
            return msg;
        }
        log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
        throw new BusinessException("根据输入的字符查询表失败，详情请查看日志");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "databaseInfo", desc = "", range = "")
    @Param(name = "methodName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getAllTableName(Long agentId, Long userId, Map<String, Object> databaseInfo, String methodName) {
        if (agentId == null) {
            throw new BusinessException("向Agent发送信息，获取目标数据库所有表时，agentId不能为空");
        }
        if (userId == null) {
            throw new BusinessException("向Agent发送信息，获取目标数据库所有表时，userId不能为空");
        }
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("向Agent发送信息，获取目标数据库所有表时，请指定数据库连接信息");
        }
        if (StringUtil.isBlank(methodName)) {
            throw new BusinessException("向Agent发送信息，获取目标数据库所有表时，methodName不能为空");
        }
        String url = AgentActionUtil.getUrl(agentId, userId, methodName);
        log.debug("准备建立连接，请求的URL为" + url);
        StoreConnectionBean legalParam = SendMsgUtil.setStoreConnectionBean(Long.parseLong(databaseInfo.get("dsl_id").toString()));
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("database_name", legalParam.getDatabase_name()).addData("database_pad", legalParam.getDatabase_pwd()).addData("user_name", legalParam.getUser_name()).addData("database_drive", legalParam.getDatabase_driver()).addData("jdbc_url", legalParam.getJdbc_url()).addData("database_type", legalParam.getDatabase_type()).addData("db_agent", databaseInfo.get("db_agent") == null ? "" : (String) databaseInfo.get("db_agent")).addData("plane_url", databaseInfo.get("plane_url") == null ? "" : (String) databaseInfo.get("plane_url")).addData("fetch_size", databaseInfo.get("fetch_size") == null ? "" : String.valueOf(databaseInfo.get("fetch_size"))).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (ar.isSuccess()) {
            String msg = PackUtil.unpackMsg(ar.getData().toString()).get(MSG);
            log.debug(">>>>>>>>>>>>>>>>>>>>>>>>返回消息为：" + msg);
            return msg;
        }
        log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
        throw new BusinessException(ar.getMessage());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "databaseInfo", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "methodName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getColInfoByTbName(Long agentId, Long userId, Map<String, Object> databaseInfo, String tableName, String methodName) {
        if (agentId == null) {
            throw new BusinessException("向Agent发送信息，根据表名查询表字段信息时，agentId不能为空");
        }
        if (userId == null) {
            throw new BusinessException("向Agent发送信息，根据表名查询表字段信息时，userId不能为空");
        }
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("向Agent发送信息，根据表名查询表字段信息时，请指定数据库连接信息");
        }
        if (StringUtil.isBlank(tableName)) {
            throw new BusinessException("向Agent发送信息，根据表名查询表字段信息时，请填写表名");
        }
        if (StringUtil.isBlank(methodName)) {
            throw new BusinessException("向Agent发送信息，根据表名查询表字段信息时，methodName不能为空");
        }
        String url = AgentActionUtil.getUrl(agentId, userId, methodName);
        log.debug("准备建立连接，请求的URL为" + url);
        StoreConnectionBean legalParam = SendMsgUtil.setStoreConnectionBean(Long.parseLong(databaseInfo.get("dsl_id").toString()));
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("database_name", legalParam.getDatabase_name()).addData("database_pad", legalParam.getDatabase_pwd()).addData("user_name", legalParam.getUser_name()).addData("database_drive", legalParam.getDatabase_driver()).addData("jdbc_url", legalParam.getJdbc_url()).addData("database_type", legalParam.getDatabase_type()).addData("db_agent", databaseInfo.get("db_agent") == null ? "" : (String) databaseInfo.get("db_agent")).addData("plane_url", databaseInfo.get("plane_url") == null ? "" : (String) databaseInfo.get("plane_url")).addData("tableName", tableName).addData("fetch_size", databaseInfo.get("fetch_size") == null ? "" : String.valueOf(databaseInfo.get("fetch_size"))).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (ar.isSuccess()) {
            String msg = PackUtil.unpackMsg((String) ar.getData()).get(MSG);
            log.debug(">>>>>>>>>>>>>>>>>>>>>>>>返回消息为：" + msg);
            return msg;
        }
        log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
        throw new BusinessException("根据表名获取该表的字段信息失败，详情请查看日志");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "databaseInfo", desc = "", range = "")
    @Param(name = "custSQL", desc = "", range = "")
    @Param(name = "methodName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getCustColumn(Long agentId, Long userId, Map<String, Object> databaseInfo, String custSQL, String methodName) {
        if (agentId == null) {
            throw new BusinessException("向Agent发送信息，根据自定义抽取SQL获取该表的字段信息，agentId不能为空");
        }
        if (userId == null) {
            throw new BusinessException("向Agent发送信息，根据自定义抽取SQL获取该表的字段信息，userId不能为空");
        }
        if (databaseInfo.isEmpty()) {
            throw new BusinessException("向Agent发送信息，根据自定义抽取SQL获取该表的字段信息，请指定数据库连接信息");
        }
        if (StringUtil.isBlank(custSQL)) {
            throw new BusinessException("向Agent发送信息，根据自定义抽取SQL获取该表的字段信息，自定义抽取SQL不能为空");
        }
        if (StringUtil.isBlank(methodName)) {
            throw new BusinessException("向Agent发送信息，根据自定义抽取SQL获取该表的字段信息，methodName不能为空");
        }
        StoreConnectionBean legalParam = SendMsgUtil.setStoreConnectionBean(Long.parseLong(databaseInfo.get("dsl_id").toString()));
        String url = AgentActionUtil.getUrl(agentId, userId, methodName);
        log.debug("准备建立连接，请求的URL为" + url);
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("database_name", legalParam.getDatabase_name()).addData("database_pad", legalParam.getDatabase_pwd()).addData("user_name", legalParam.getUser_name()).addData("database_drive", legalParam.getDatabase_driver()).addData("jdbc_url", legalParam.getJdbc_url()).addData("database_type", legalParam.getDatabase_type()).addData("db_agent", databaseInfo.get("db_agent") == null ? "" : (String) databaseInfo.get("db_agent")).addData("plane_url", databaseInfo.get("plane_url") == null ? "" : (String) databaseInfo.get("plane_url")).addData("fetch_size", databaseInfo.get("fetch_size") == null ? "" : String.valueOf(databaseInfo.get("fetch_size"))).addData("custSQL", custSQL).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (ar.isSuccess()) {
            String msg = PackUtil.unpackMsg((String) ar.getData()).get(MSG);
            log.debug(">>>>>>>>>>>>>>>>>>>>>>>>返回消息为：" + msg);
            return msg;
        }
        log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
        throw new BusinessException("根据自定义抽取SQL获取该表的字段信息失败，详情请查看日志");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Param(name = "etlDate", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "is_download", desc = "", range = "", nullable = true, valueIfNull = "false")
    @Param(name = "methodName", desc = "", range = "")
    @Param(name = "sqlParam", range = "", desc = "", nullable = true, valueIfNull = "")
    public static Object sendDBCollectTaskInfo(Long colSetId, Long agentId, Long userId, String taskInfo, String methodName, String etlDate, String is_download, String sqlParam) {
        if (agentId == null) {
            throw new BusinessException("向Agent发送数据库采集任务信息，agentId不能为空");
        }
        if (userId == null) {
            throw new BusinessException("向Agent发送数据库采集任务信息，userId不能为空");
        }
        if (StringUtil.isBlank(taskInfo)) {
            throw new BusinessException("向Agent发送数据库采集任务信息，任务信息不能为空");
        }
        if (StringUtil.isBlank(methodName)) {
            throw new BusinessException("向Agent发送数据库采集任务信息时，methodName不能为空");
        }
        log.info("=============是否更新任务为配置完成: {}=============", is_download);
        if (Boolean.parseBoolean(is_download)) {
            DboExecute.updatesOrThrow("此次采集任务配置完成,更新状态失败", "UPDATE " + DatabaseSet.TableName + " SET is_sendok = ? WHERE database_id = ?", IsFlag.Shi.getCode(), colSetId);
            Dbo.db().commit();
        }
        String url = AgentActionUtil.getUrl(agentId, userId, methodName);
        log.debug("准备建立连接，请求的URL为" + url);
        log.info("跑批日期==========================" + etlDate);
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("etlDate", StringUtil.isBlank(etlDate) ? "" : etlDate).addData("taskInfo", PackUtil.packMsg(taskInfo)).addData("sqlParam", StringUtil.isBlank(sqlParam) ? "" : sqlParam).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (!ar.isSuccess()) {
            Map<String, String> exeMap = JsonUtil.toObject(JsonUtil.toJson(ar.getData()), new TypeReference<Map<String, String>>() {
            });
            log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + exeMap.get("exMessage"));
            throw new BusinessException(String.format("Agent采集任务失败：%s", exeMap.get("exMessage")));
        } else {
            if (!"true".equals(is_download)) {
                if (!"执行成功".equals(ar.getData())) {
                    throw new BusinessException(ar.getData().toString());
                }
            }
        }
        return ar.getData();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Param(name = "etlDate", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "is_download", desc = "", range = "", nullable = true, valueIfNull = "false")
    @Param(name = "methodName", desc = "", range = "")
    @Param(name = "sqlParam", range = "", desc = "", nullable = true, valueIfNull = "")
    public static Object sendObjectCollectTaskInfo(Long odc_id, Long agentId, Long userId, String taskInfo, String methodName, String etlDate) {
        if (agentId == null) {
            throw new BusinessException("向Agent发送数据库采集任务信息，agentId不能为空");
        }
        if (userId == null) {
            throw new BusinessException("向Agent发送数据库采集任务信息，userId不能为空");
        }
        if (StringUtil.isBlank(taskInfo)) {
            throw new BusinessException("向Agent发送数据库采集任务信息，任务信息不能为空");
        }
        if (StringUtil.isBlank(methodName)) {
            throw new BusinessException("向Agent发送数据库采集任务信息时，methodName不能为空");
        }
        DboExecute.updatesOrThrow("此次采集任务配置完成,更新状态失败", "UPDATE " + ObjectCollect.TableName + " SET is_sendok = ? WHERE odc_id = ?", IsFlag.Shi.getCode(), odc_id);
        Dbo.db().commit();
        String url = AgentActionUtil.getUrl(agentId, userId, methodName);
        log.debug("准备建立连接，请求的URL为" + url);
        log.info("跑批日期==========================" + etlDate);
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("etlDate", StringUtil.isBlank(etlDate) ? "" : etlDate).addData("taskInfo", PackUtil.packMsg(taskInfo)).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (!ar.isSuccess()) {
            log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        } else {
            if (!"执行成功".equals(ar.getData())) {
                throw new BusinessException(ar.getData().toString());
            }
        }
        return ar.getData();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseInfo", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DatabaseSet getLegalParam(Map<String, Object> databaseInfo) {
        DatabaseSet databaseSet = new DatabaseSet();
        databaseSet.setDb_agent(databaseInfo.get("db_agent") == null ? "" : (String) databaseInfo.get("db_agent"));
        databaseSet.setPlane_url(databaseInfo.get("plane_url") == null ? "" : (String) databaseInfo.get("plane_url"));
        databaseSet.setFetch_size(databaseInfo.get("fetch_size") == null ? "" : String.valueOf(databaseInfo.get("fetch_size")));
        databaseSet.setDsl_id(databaseInfo.get("dsl_id") == null ? "" : String.valueOf(databaseInfo.get("dsl_id")));
        return databaseSet;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "file_path", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<ObjectCollectTask> getDictionaryTableInfo(long agent_id, String file_path, long user_id) {
        String url = AgentActionUtil.getUrl(agent_id, user_id, AgentActionUtil.GETDICTABLE);
        String bodyString = null;
        try {
            bodyString = new HttpClient().addData("file_path", file_path).post(url).getBodyString();
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        return JsonUtil.toObject(getRespMsg(bodyString, url), new TypeReference<List<ObjectCollectTask>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "file_path", desc = "", range = "")
    @Param(name = "data_date", desc = "", range = "")
    @Param(name = "file_suffix", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<ObjectCollectTask> getFirstLineData(long agent_id, String file_path, String data_date, String file_suffix, long user_id) {
        String url = AgentActionUtil.getUrl(agent_id, user_id, AgentActionUtil.GETFIRSTLINEDATA);
        Validator.notEmpty(data_date, "没有数据字典时数据日期不能为空");
        String bodyString = null;
        try {
            bodyString = new HttpClient().addData("file_suffix", file_suffix).addData("data_date", data_date).addData("file_path", file_path).post(url).getBodyString();
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        return JsonUtil.toObject(getRespMsg(bodyString, url), new TypeReference<List<ObjectCollectTask>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "file_path", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, List<ObjectCollectStruct>> getAllDicColumns(long agent_id, String file_path, long user_id) {
        String url = AgentActionUtil.getUrl(agent_id, user_id, AgentActionUtil.GETALLDICCOLUMNS);
        String bodyString = null;
        try {
            bodyString = new HttpClient().addData("file_path", file_path).post(url).getBodyString();
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        return JsonUtil.toObject(getRespMsg(bodyString, url), new TypeReference<Map<String, List<ObjectCollectStruct>>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "file_path", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, List<ObjectHandleType>> getAllHandleType(long agent_id, String file_path, long user_id) {
        String url = AgentActionUtil.getUrl(agent_id, user_id, AgentActionUtil.GETALLHANDLETYPE);
        String bodyString = null;
        try {
            bodyString = new HttpClient().addData("file_path", file_path).post(url).getBodyString();
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        return JsonUtil.toObject(getRespMsg(bodyString, url), new TypeReference<Map<String, List<ObjectHandleType>>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "bodyString", desc = "", range = "")
    @Param(name = "url", desc = "", range = "")
    @Return(desc = "", range = "")
    private static String getRespMsg(String bodyString, String url) {
        Map<String, Object> result = JsonUtil.toObject(bodyString, new TypeReference<Map<String, Object>>() {
        });
        if (Integer.parseInt(result.get(CODE).toString()) != 999) {
            throw new BusinessException("与agent交互失败，详情请查看agent日志:" + result.get(MESSAGE));
        }
        return PackUtil.unpackMsg(result.get(DATA).toString()).get(MSG);
    }

    public static void startSingleJob(long database_id, String table_name, String collect_type, String etlDate, String file_type, String sqlParam, long agentId, long userId) {
        AgentDownInfo agent_down_info = Dbo.queryOneObject(AgentDownInfo.class, "SELECT * FROM " + "agent_down_info t1 join agent_info t2 on t1.agent_ip = t2.agent_ip and t1.agent_port=" + "t2.agent_port where  t2.agent_id= ? and t2.user_id = ?", agentId, userId).orElseThrow(() -> new BusinessException("根据Agent_id:" + agentId + "查询不到部署信息"));
        String url = AgentActionUtil.getUrl(agentId, userId, AgentActionUtil.SINGLEJOB);
        log.info("agent_down_info" + JsonUtil.toJson(agent_down_info));
        log.info("准备建立连接，请求的URL为" + url);
        log.info("跑批日期==========================" + etlDate);
        HttpClient.ResponseValue resVal = null;
        try {
            resVal = new HttpClient().addData("database_id", database_id).addData("table_name", table_name).addData("collect_type", collect_type).addData("etl_date", StringUtil.isBlank(etlDate) ? "" : etlDate).addData("file_type", file_type).addData("sql_para", StringUtil.isBlank(sqlParam) ? "" : sqlParam).addData("agent_down_info", JsonUtil.toJson(agent_down_info)).post(url);
        } catch (Exception e) {
            throw new BusinessException("agent 服务端连接超时,请刷新重试！");
        }
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (!ar.isSuccess()) {
            log.error(">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage());
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        }
    }

    public static StoreConnectionBean setStoreConnectionBean(Long dslId) {
        Result connectionResult = getDBConnectionDerils(dslId);
        StoreConnectionBean storeConnectionBean = new StoreConnectionBean();
        if (!connectionResult.isEmpty()) {
            for (Map<String, Object> map : connectionResult.toList()) {
                if (map.get("storage_property_key").equals("database_type")) {
                    storeConnectionBean.setDatabase_type(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("database_driver")) {
                    storeConnectionBean.setDatabase_driver(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("user_name")) {
                    storeConnectionBean.setUser_name(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("database_pwd")) {
                    storeConnectionBean.setDatabase_pwd(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("database_name")) {
                    storeConnectionBean.setDatabase_name(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("jdbc_url")) {
                    storeConnectionBean.setJdbc_url(map.get("storage_property_val").toString());
                }
            }
        }
        return storeConnectionBean;
    }

    private static Result getDBConnectionDerils(Long dslId) {
        return Dbo.queryResult(" select t1.storage_property_key, t1.storage_property_val,t2.store_type " + " FROM " + DataStoreLayerAttr.TableName + " t1 JOIN " + DataStoreLayer.TableName + " t2 ON t1.dsl_id = t2.dsl_id  WHERE t1.dsl_id = ?", dslId);
    }
}
