package hyren.serv6.g.serviceuser.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import eu.bitwalker.useragentutils.Version;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentStatus;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.RequestUtil;
import hyren.serv6.commons.utils.agentmonitor.AgentMonitorUtil;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.commons.utils.fileutil.FileOperations;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.g.bean.*;
import hyren.serv6.g.commons.FileDownload;
import hyren.serv6.g.commons.LocalFile;
import hyren.serv6.g.enumerate.StateType;
import hyren.serv6.g.init.InterfaceManager;
import hyren.serv6.g.serviceuser.common.InterfaceCommon;
import hyren.serv6.g.serviceuser.query.QueryByRowkey;
import hyren.serv6.g.serviceuser.req.DashboardDataReq;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Service
public class ServiceInterfaceUserImplService {

    private static final Logger logger = LogManager.getLogger();

    private static final String isRecordInterfaceLog = PropertyParaValue.getString("isRecordInterfaceLog", "1");

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "user_password", desc = "", range = "")
    @Return(desc = "", range = "")
    public ActionResult getToken(Long user_id, String user_password) {
        return InterfaceCommon.getTokenById(Dbo.db(), user_id, user_password);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "singleTable", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult generalQuery(SingleTable singleTable, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        actionResult = InterfaceCommon.checkType(singleTable.getDataType(), singleTable.getOutType(), singleTable.getAsynType(), singleTable.getBackurl(), singleTable.getFilepath(), singleTable.getFilename());
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        actionResult = InterfaceCommon.checkTable(Dbo.db(), userByToken.getUser_id(), singleTable);
        actionResult = InterfaceCommon.operateInterfaceByType(singleTable.getDataType(), singleTable.getOutType(), singleTable.getAsynType(), singleTable.getBackurl(), singleTable.getFilepath(), singleTable.getFilename(), actionResult);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult tableUsePermissions(CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        actionResult = StateType.getActionResult(StateType.NORMAL);
        actionResult.setData(InterfaceManager.getTableList(userByToken.getUser_id()));
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "", nullable = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult tableStructureQuery(String tableName, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        if (StringUtil.isBlank(tableName)) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
        }
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        if (InterfaceManager.existsTable(Dbo.db(), userByToken.getUser_id(), tableName)) {
            QueryInterfaceInfo userTableInfo = InterfaceManager.getUserTableInfo(Dbo.db(), userByToken.getUser_id(), tableName);
            String type = userTableInfo.getTable_blsystem();
            Map<String, Object> res = new HashMap<>();
            res.put("table_type", type);
            List<Map<String, Object>> columns = DataTableUtil.getColumnByTableName(Dbo.db(), tableName);
            res.put("field", columns);
            actionResult = StateType.getActionResult(StateType.NORMAL);
            actionResult.setData(res);
        } else {
            actionResult = StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "", nullable = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult tableSearchGetJson(String tableName, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        if (StringUtil.isBlank(tableName)) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
        }
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        if (!InterfaceManager.existsTable(Dbo.db(), userByToken.getUser_id(), tableName)) {
            return StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
        }
        List<Map<String, Object>> columns = DataTableUtil.getColumnInfoByTableName(Dbo.db(), tableName);
        if (columns == null) {
            return StateType.getActionResult(StateType.STORAGELAYER_NOT_EXIST_BY_TABLE);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        actionResult = StateType.getActionResult(StateType.NORMAL);
        actionResult.setData(columns);
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "id", desc = "", range = "")
    @Param(name = "file_name", desc = "", range = "")
    public void unstructuredFileDownloadApi(String id, String file_name) {
        try (OutputStream out = ContextDataHolder.getResponse().getOutputStream()) {
            ContextDataHolder.getResponse().reset();
            if (RequestUtil.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + file_name);
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + file_name);
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            byte[] bye = FileOperations.getFileBytesFromAvro(id);
            if (bye == null) {
                throw new BusinessException("文件已不存在! fileName=" + file_name);
            }
            out.write(bye);
            out.flush();
        } catch (IOException e) {
            throw new AppSystemException("文件下载失败! fileName=" + file_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sqlSearch", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult sqlInterfaceSearch(SqlSearch sqlSearch, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        actionResult = InterfaceCommon.checkType(sqlSearch.getDataType(), sqlSearch.getOutType(), sqlSearch.getAsynType(), sqlSearch.getBackurl(), sqlSearch.getFilepath(), sqlSearch.getFilename());
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        if (StringUtil.isBlank(sqlSearch.getSql())) {
            return StateType.getActionResult(StateType.SQL_IS_INCORRECT);
        }
        List<String> tableList = DruidParseQuerySql.parseSqlTableToList(sqlSearch.getSql());
        List<String> columnList = new ArrayList<>();
        for (String table : tableList) {
            actionResult = InterfaceCommon.verifyTable(Dbo.db(), userByToken.getUser_id(), table);
            if (StateType.NORMAL != StateType.ofEnumByCode(actionResult.getCode())) {
                return actionResult;
            }
            QueryInterfaceInfo userTableInfo = InterfaceManager.getUserTableInfo(Dbo.db(), userByToken.getUser_id(), table);
            columnList = StringUtil.split(userTableInfo.getTable_en_column().toLowerCase(), Constant.METAINFOSPLIT);
        }
        String sqlNew = sqlSearch.getSql().trim();
        if (!CommonVariables.AUTHORITY.contains(String.valueOf(userByToken.getUser_id()))) {
            DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(sqlSearch.getSql());
            List<String> sqlColumnList = druidParseQuerySql.parseSelectOriginalField();
            if (!columnList.isEmpty()) {
                if (!sqlColumnList.contains(null)) {
                    for (String col : sqlColumnList) {
                        if (col.contains(".")) {
                            col = col.substring(col.indexOf(".") + 1).toLowerCase();
                        } else {
                            col = col.toLowerCase();
                        }
                        if (InterfaceCommon.columnIsExist(col, columnList)) {
                            actionResult = StateType.getActionResult(StateType.NO_COLUMN_USE_PERMISSIONS);
                            actionResult.setData("请求错误,查询列名" + col + "没有使用权限");
                            return actionResult;
                        }
                    }
                } else {
                    sqlNew = sqlNew.replace("*", String.join(",", columnList));
                }
            }
        }
        if (sqlNew.endsWith(";")) {
            sqlNew = sqlNew.substring(0, sqlNew.length() - 1);
        }
        actionResult = InterfaceCommon.getSqlData(Dbo.db(), sqlSearch.getOutType(), sqlSearch.getDataType(), sqlNew, userByToken.getUser_id(), null);
        actionResult = InterfaceCommon.operateInterfaceByType(sqlSearch.getDataType(), sqlSearch.getOutType(), sqlSearch.getAsynType(), sqlSearch.getBackurl(), sqlSearch.getFilepath(), sqlSearch.getFilename(), actionResult);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileAttribute", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult fileAttributeSearch(FileAttribute fileAttribute, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        int num_start = 0, num_count = 10, fileSizeStart = 0, fileSizeEnd = 0;
        String num = fileAttribute.getNum();
        if (StringUtil.isNotBlank(num)) {
            if (num.contains(",")) {
                List<String> numList = StringUtil.split(num, ",");
                num_start = Integer.parseInt(numList.get(0));
                num_count = Integer.parseInt(numList.get(1));
            } else {
                num_count = Integer.parseInt(num);
            }
        }
        String fileSize = fileAttribute.getFilesize();
        if (StringUtil.isNotBlank(fileSize)) {
            if (fileSize.contains(",")) {
                List<String> fileSizeList = StringUtil.split(fileSize, ",");
                try {
                    fileSizeStart = Integer.parseInt(fileSizeList.get(0));
                    fileSizeEnd = Integer.parseInt(fileSizeList.get(1));
                } catch (NumberFormatException e) {
                    actionResult = StateType.getActionResult(StateType.EXCEPTION);
                    actionResult.setData("输入的文件大小不合法请确认");
                    return actionResult;
                }
            } else {
                try {
                    fileSizeStart = Integer.parseInt(fileSize);
                } catch (NumberFormatException e) {
                    actionResult = StateType.getActionResult(StateType.EXCEPTION);
                    actionResult.setData("输入的文件大小不合法请确认" + e.getMessage());
                    return actionResult;
                }
            }
        }
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("SELECT source_path,file_suffix,file_id,storage_time,storage_date,original_update_date," + " original_update_time,file_md5,original_name,file_size,file_avro_path,file_avro_block," + " sfa.collect_set_id,sfa.source_id,sfa.agent_id,fcs_name,datasource_name,agent_name FROM  " + DataSource.TableName + "  ds JOIN agent_info ai ON ds.SOURCE_ID = ai.SOURCE_ID" + " JOIN " + FileCollectSet.TableName + " fcs ON fcs.agent_id = ai.agent_id" + " JOIN " + SourceFileAttribute.TableName + " sfa ON sfa.SOURCE_ID = ds.SOURCE_ID" + " and  sfa.AGENT_ID = ai.AGENT_ID and sfa.collect_set_id = fcs.FCS_ID " + " where collect_type = ? ");
        assembler.addParam(AgentType.WenJianXiTong.name());
        assembler.addLikeParam("original_name", fileAttribute.getFilename());
        List<Object> sourceIdList = SqlOperator.queryOneColumnList(Dbo.db(), "select source_id from data_source ");
        assembler.addORParam("sfa.source_id", sourceIdList.toArray());
        if (StringUtil.isNotBlank(fileSize)) {
            assembler.addSql(" and file_size >=").addParam(fileSizeStart);
            assembler.addSql(" and ile_size <=").addParam(fileSizeEnd);
        }
        if (StringUtil.isNotBlank(fileAttribute.getFilesuffix())) {
            String[] split = fileAttribute.getFilesuffix().split(",");
            assembler.addORParam("file_suffix", split);
        }
        String[] filepath = fileAttribute.getFilepath();
        if (filepath != null && filepath.length > 0) {
            assembler.addORParam("source_path", filepath);
        }
        Long[] fcs_id = fileAttribute.getFcs_id();
        if (fcs_id != null && fcs_id.length > 0) {
            assembler.addORParam("fcs_id", fcs_id);
        }
        assembler.addSql(" and storage_date=?").addParam(fileAttribute.getStoragedate());
        assembler.addSql(" and file_md5=?").addParam(fileAttribute.getFileMD5());
        assembler.addLikeParam("datasource_name", fileAttribute.getDs_name());
        assembler.addLikeParam("agent_name", fileAttribute.getAgent_name());
        assembler.addLikeParam("fcs_name", fileAttribute.getFcs_name());
        Long[] dep_id = fileAttribute.getDep_id();
        if (dep_id != null && dep_id.length > 0) {
            assembler.addSql(" and  exists (select source_id from " + SourceRelationDep.TableName + " dep where dep.SOURCE_ID = ds.SOURCE_ID ").addORParam("dep_id", dep_id).addSql(" ) ");
        }
        assembler.addSql("limit " + num_count + " offset " + num_start);
        List<Map<String, Object>> fileAttrList = SqlOperator.queryList(Dbo.db(), assembler.sql(), assembler.params());
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        actionResult = StateType.getActionResult(StateType.NORMAL);
        actionResult.setData(fileAttrList.isEmpty() ? new ArrayList<>() : fileAttribute);
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Param(name = "agent_name", desc = "", range = "", nullable = true)
    @Param(name = "startTime", desc = "", range = "", nullable = true, valueIfNull = "0")
    @Param(name = "endTime", desc = "", range = "", nullable = true, valueIfNull = "0")
    public ActionResult computerResourceInfo(CheckParam checkParam, String agent_name, long startTime, long endTime) {
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        Validator.notBlank(agent_name, "数据库名称不能为空");
        List<AgentDownInfo> agent_down_infos = Dbo.queryList(AgentDownInfo.class, "SELECT t1.agent_ip,t1.agent_port,t2.agent_context,t2.agent_pattern FROM " + AgentInfo.TableName + " t1 JOIN " + AgentDownInfo.TableName + " t2 ON t1.agent_ip = t2.agent_ip AND t1.agent_port = t2.agent_port WHERE t1.agent_status = ? " + " AND t1.user_id = ? AND t1.agent_name = ? GROUP BY t2.agent_name,t1.agent_ip,t1.agent_port,t2.agent_context,t2.agent_pattern", AgentStatus.YiLianJie.getCode(), checkParam.getUser_id(), agent_name);
        if (agent_down_infos.size() == 0) {
            actionResult = StateType.getActionResult(StateType.AGENT_ERROR);
            actionResult.setData("Agent名称: " + agent_name + " ,为获取到相关信息");
            return actionResult;
        }
        List<Map<String, Object>> listData = new ArrayList<Map<String, Object>>();
        agent_down_infos.forEach(agent_down_info -> {
            ActionResult computerInfo = getComputerInfo(agent_down_info, startTime, endTime);
            Map<String, Object> object = JsonUtil.toObject(JsonUtil.toJson(agent_down_info), new TypeReference<Map<String, Object>>() {
            });
            object.put("agent_name", agent_name);
            if (computerInfo.getData() != null && computerInfo.getCode() != 500) {
                List<Object> resourceData = JsonUtil.toObject(computerInfo.getData().toString(), new TypeReference<List<Object>>() {
                });
                object.put("resource", resourceData);
                object.put("status", true);
            } else {
                object.put("status", false);
            }
            listData.add(object);
        });
        actionResult.setData(listData);
        return actionResult;
    }

    private ActionResult getComputerInfo(AgentDownInfo agent_down_info, long startTime, long endTime) {
        if (!AgentMonitorUtil.isPortOccupied(agent_down_info.getAgent_ip(), agent_down_info.getAgent_port())) {
            ActionResult failureActionResult = ActionResult.failure();
            failureActionResult.setData(String.format(StateType.COMMUNICATION_ERROR.getMessage(), agent_down_info.getAgent_ip(), agent_down_info.getAgent_port()));
            return failureActionResult;
        }
        ActionResult actionResult = ActionResult.failure();
        if (StringUtil.isBlank(agent_down_info.getAgent_context())) {
            actionResult.setData("agent_context 信息不能为空");
        }
        if (StringUtil.isBlank(agent_down_info.getAgent_pattern())) {
            actionResult.setData("agent_pattern 信息不能为空");
        }
        actionResult = AgentMonitorUtil.agentResourceInfo(agent_down_info, startTime, endTime);
        if (actionResult.isSuccess()) {
            Object msg = actionResult.getData();
            logger.debug(">>>>>>>>>>>>>>>>>>>>>>>>返回消息为：" + msg);
            actionResult.setData(msg);
            return actionResult;
        }
        actionResult.setData(String.format(StateType.COMMUNICATION_ERROR.getMessage(), agent_down_info.getAgent_ip(), agent_down_info.getAgent_port()));
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "uuid", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult uuidDownload(String uuid, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        FileDownload fileDownload = new FileDownload();
        try {
            if (uuid != null) {
                QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
                HttpServletResponse response = fileDownload.downLoadFile(uuid, userByToken.getUser_id());
                if (response.getStatus() < 300) {
                    if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
                        long response_time = System.currentTimeMillis() - start;
                        insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
                    }
                    return null;
                } else {
                    actionResult = StateType.getActionResult(StateType.EXCEPTION);
                    actionResult.setData("下载失败");
                    return actionResult;
                }
            } else {
                return StateType.getActionResult(StateType.UUID_NOT_NULL);
            }
        } catch (Exception e) {
            logger.error(e);
            actionResult = StateType.getActionResult(StateType.EXCEPTION);
            actionResult.setData("下载失败" + e.getMessage());
            return actionResult;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult showReleaseDashboard(CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        String dashboard_id = new String(Base64.getDecoder().decode(checkParam.getInterface_code()));
        AutoDashboardInfo auto_dashboard_info = new AutoDashboardInfo();
        auto_dashboard_info.setDashboard_id(dashboard_id);
        try {
            DashboardDataReq dashboardInfo = getDashboardInfoById(auto_dashboard_info.getDashboard_id());
            actionResult = StateType.getActionResult(StateType.NORMAL);
            actionResult.setData(dashboardInfo);
        } catch (Exception e) {
            actionResult = StateType.getActionResult(StateType.EXCEPTION);
            actionResult.setData("查询仪表盘信息失败：" + e.getMessage());
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dashboard_id", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DashboardDataReq getDashboardInfoById(long dashboard_id) {
        Map<String, Object> dashboardInfo = SqlOperator.queryOneObject(Dbo.db(), "SELECT * FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_id=?", dashboard_id);
        if (dashboardInfo != null && !dashboardInfo.isEmpty()) {
            DashboardDataReq dataReq = new DashboardDataReq();
            dataReq.setAutoDashboardInfo(dashboardInfo);
            String dashboardWidget = dashboardInfo.get("dashboard_widget").toString();
            if (dashboardWidget != null && !dashboardWidget.isEmpty()) {
                List<Object> widgets = JsonUtil.toObject(dashboardWidget, new TypeReference<List<Object>>() {
                });
                dataReq.setWidget(widgets);
            }
            return dataReq;
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "rowKeySearch", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult rowKeySearch(RowKeySearch rowKeySearch, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        actionResult = InterfaceCommon.checkType(rowKeySearch.getDataType(), rowKeySearch.getOutType(), rowKeySearch.getAsynType(), rowKeySearch.getBackurl(), rowKeySearch.getFilepath(), rowKeySearch.getFilename());
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        List<LayerBean> hbaseLayerList = InterfaceCommon.getLayerBeans(rowKeySearch.getEn_table(), Store_type.HBASE);
        if (hbaseLayerList.isEmpty()) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXIST_ON_HBASE_STOREAGE);
        }
        LayerBean layerBean = hbaseLayerList.get(0);
        Map<String, String> layerAttr = layerBean.getLayerAttr();
        actionResult = // fixme HLL
        QueryByRowkey.// fixme HLL
        query(// fixme HLL
        rowKeySearch.getEn_table(), rowKeySearch.getRowkey(), rowKeySearch.getEn_column(), rowKeySearch.getGet_version(), layerBean.getDsl_name(), layerAttr.get(StorageTypeKey.platform), layerAttr.get(StorageTypeKey.prncipal_name), layerAttr.get(StorageTypeKey.hadoop_user_name), userByToken.getUser_id(), Dbo.db());
        if (StateType.NORMAL != StateType.ofEnumByCode(actionResult.getCode())) {
            return actionResult;
        }
        LocalFile.dealDataByType(Dbo.db(), actionResult, rowKeySearch.getDataType(), rowKeySearch.getOutType(), userByToken.getUser_id());
        LocalFile.dealDataByAsynType(rowKeySearch.getOutType(), rowKeySearch.getAsynType(), rowKeySearch.getBackurl(), rowKeySearch.getFilepath(), rowKeySearch.getFilename(), actionResult);
        ObjectMapper objectMapper = new ObjectMapper();
        JavaType javaType = objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
        Map<String, Object> map;
        try {
            map = objectMapper.readValue(JsonUtil.toJson(actionResult.getData()), javaType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        map.put("enTable", rowKeySearch.getEn_table());
        actionResult.setData(map);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "solrSearch", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    public ActionResult solrSearch(SolrSearch solrSearch, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        if (StringUtil.isBlank(solrSearch.getTableName())) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
        }
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        actionResult = InterfaceCommon.checkType(solrSearch.getDataType(), solrSearch.getOutType(), solrSearch.getAsynType(), solrSearch.getBackurl(), solrSearch.getFilepath(), solrSearch.getFilename());
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        if (!InterfaceManager.existsTable(Dbo.db(), userByToken.getUser_id(), solrSearch.getTableName())) {
            return StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
        }
        try {
            actionResult = InterfaceCommon.solrSearch(solrSearch);
            actionResult = LocalFile.dealDataByType(Dbo.db(), actionResult, solrSearch.getDataType(), solrSearch.getOutType(), userByToken.getUser_id());
            LocalFile.dealDataByAsynType(solrSearch.getOutType(), solrSearch.getAsynType(), solrSearch.getBackurl(), solrSearch.getFilepath(), solrSearch.getFilename(), actionResult);
        } catch (Exception e) {
            actionResult = StateType.getActionResult(StateType.EXCEPTION);
            if (e instanceof BusinessException) {
                actionResult.setData(e.getMessage());
            }
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "hbaseSolr", desc = "", range = "", isBean = true)
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult hbaseSolrQuery(HbaseSolr hbaseSolr, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        if (StringUtil.isBlank(hbaseSolr.getTableName())) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
        }
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        actionResult = InterfaceCommon.checkType(hbaseSolr.getDataType(), hbaseSolr.getOutType(), hbaseSolr.getAsynType(), hbaseSolr.getBackurl(), hbaseSolr.getFilepath(), hbaseSolr.getFilename());
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        if (!InterfaceManager.existsTable(Dbo.db(), userByToken.getUser_id(), hbaseSolr.getTableName())) {
            return StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
        }
        QueryInterfaceInfo userTableInfo = InterfaceManager.getUserTableInfo(Dbo.db(), userByToken.getUser_id(), hbaseSolr.getTableName());
        String selectColumn = hbaseSolr.getSelectColumn();
        if (StringUtil.isBlank(selectColumn)) {
            selectColumn = userTableInfo.getTable_en_column();
        }
        if (StringUtil.isBlank(hbaseSolr.getWhereColumn())) {
            return StateType.getActionResult(StateType.CONDITION_ERROR);
        }
        List<LayerBean> hbaseLayerList = InterfaceCommon.getLayerBeans(hbaseSolr.getTableName(), Store_type.HBASE);
        if (hbaseLayerList.isEmpty()) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXIST_ON_HBASE_STOREAGE);
        }
        LayerBean layerBean = hbaseLayerList.get(0);
        Map<String, String> layerAttr = layerBean.getLayerAttr();
        actionResult = InterfaceCommon.getHbaseSolrQuery(hbaseSolr.getTableName(), hbaseSolr.getWhereColumn(), selectColumn, hbaseSolr.getStart(), hbaseSolr.getNum(), userTableInfo.getTable_en_column(), userTableInfo.getTable_type_name(), layerBean.getDsl_name(), layerAttr.get(StorageTypeKey.platform), layerAttr.get(StorageTypeKey.prncipal_name), layerAttr.get(StorageTypeKey.hadoop_user_name));
        actionResult = InterfaceCommon.operateInterfaceByType(hbaseSolr.getDataType(), hbaseSolr.getOutType(), hbaseSolr.getAsynType(), hbaseSolr.getBackurl(), hbaseSolr.getFilepath(), hbaseSolr.getFilename(), actionResult);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fullTextSearchBean", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult fullTextSearchApi(FullTextSearchBean fullTextSearchBean, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        HttpServletRequest request = RequestUtil.getRequest();
        Map<String, String> customizeRequestKV = new HashMap<>();
        List<String> field_name_s = Dbo.queryOneColumnList("select field_name from " + SolrDataRelation.TableName);
        field_name_s.forEach(field_name -> {
            String fieldNameValue = request.getParameter(field_name);
            if (StringUtil.isNotBlank(fieldNameValue)) {
                customizeRequestKV.put(field_name, fieldNameValue);
            }
        });
        actionResult = InterfaceCommon.getFullTextSearchResult(fullTextSearchBean, customizeRequestKV, request, field_name_s);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableData", desc = "", range = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public ActionResult singleTableDataDelete(TableData tableData, CheckParam checkParam) {
        long start = System.currentTimeMillis();
        String request_stime = DateUtil.getDateTime();
        if (StringUtil.isBlank(tableData.getTableName())) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
        }
        ActionResult actionResult = InterfaceCommon.checkTokenAndInterface(Dbo.db(), checkParam);
        if (StateType.ofEnumByCode(actionResult.getCode()) != StateType.NORMAL) {
            return actionResult;
        }
        QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(actionResult.getData().toString());
        if (!InterfaceManager.existsTable(Dbo.db(), userByToken.getUser_id(), tableData.getTableName())) {
            return StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
        }
        actionResult = InterfaceCommon.deleteTableDataByTableName(Dbo.db(), tableData, userByToken.getUser_id());
        if (IsFlag.Shi == IsFlag.ofEnumByCode(isRecordInterfaceLog)) {
            long response_time = System.currentTimeMillis() - start;
            insertInterfaceUseLog(checkParam.getUrl(), request_stime, response_time, userByToken, actionResult.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "url", desc = "", range = "")
    @Param(name = "interface_use_log", desc = "", range = "", isBean = true)
    @Param(name = "userByToken", desc = "", range = "", isBean = true)
    @Param(name = "request_state", desc = "", range = "")
    @Param(name = "start", desc = "", range = "")
    private void insertInterfaceUseLog(String url, String request_stime, long response_time, QueryInterfaceInfo userByToken, String request_state) {
        QueryInterfaceInfo interfaceUseInfo = InterfaceManager.getInterfaceUseInfo(userByToken.getUser_id(), url);
        InterfaceUseLog interface_use_log = new InterfaceUseLog();
        interface_use_log.setResponse_time(response_time);
        interface_use_log.setRequest_stime(request_stime);
        interface_use_log.setRequest_etime(DateUtil.getDateTime());
        interface_use_log.setUser_id(userByToken.getUser_id());
        interface_use_log.setUser_name(userByToken.getUser_name());
        interface_use_log.setInterface_use_id(Long.parseLong(interfaceUseInfo.getInterface_use_id()));
        interface_use_log.setInterface_name(interfaceUseInfo.getInterface_name());
        interface_use_log.setRequest_state(request_state);
        String header = RequestUtil.getRequest().getHeader("User-Agent");
        String headerStr = header.substring(0, header.indexOf('/')).toUpperCase();
        interface_use_log.setRequest_type(headerStr);
        UserAgent userAgent = UserAgent.parseUserAgentString(header);
        Browser browser = userAgent.getBrowser();
        if ("DOWNLOAD".equalsIgnoreCase(browser.toString())) {
            interface_use_log.setBrowser_type(headerStr);
        } else {
            interface_use_log.setBrowser_type(browser.toString());
        }
        Version browserVersion = userAgent.getBrowserVersion();
        interface_use_log.setBrowser_version(browserVersion == null ? header : browserVersion.toString());
        OperatingSystem operatingSystem = userAgent.getOperatingSystem();
        if ("UNKNOWN".equalsIgnoreCase(operatingSystem.toString())) {
            interface_use_log.setSystem_type(headerStr);
        } else {
            interface_use_log.setSystem_type(operatingSystem.toString());
        }
        String method = RequestUtil.getRequest().getMethod();
        interface_use_log.setRequest_mode(method);
        String remoteAddr = RequestUtil.getRequest().getRemoteAddr();
        interface_use_log.setRemoteaddr(remoteAddr);
        String protocol = RequestUtil.getRequest().getProtocol();
        interface_use_log.setProtocol(protocol);
        interface_use_log.setLog_id(PrimayKeyGener.getNextId());
        interface_use_log.add(Dbo.db());
    }
}
