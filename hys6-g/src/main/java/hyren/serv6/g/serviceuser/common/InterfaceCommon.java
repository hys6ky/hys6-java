package hyren.serv6.g.serviceuser.common;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.InterfaceFileInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.hadoop.hbaseindexer.bean.HbaseSolrField;
import hyren.serv6.commons.hadoop.hbaseindexer.type.TypeFieldNameMapper;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.http.RestTemplateSingleton;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.g.bean.*;
import hyren.serv6.g.commons.LocalFile;
import hyren.serv6.g.enumerate.AsynType;
import hyren.serv6.g.enumerate.DataType;
import hyren.serv6.g.enumerate.OutType;
import hyren.serv6.g.enumerate.StateType;
import hyren.serv6.g.init.InterfaceManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/9 17:34")
@Slf4j
public class InterfaceCommon {

    private static final Logger logger = LogManager.getLogger();

    private static final List<String> notCheckFunction = new ArrayList<>();

    private static final String HYREN_SEPER = "<|>";

    private static ActionResult actionResult = ActionResult.success();

    static {
        notCheckFunction.add("count(*)");
        notCheckFunction.add("count(1)");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "user_password", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult getTokenById(DatabaseWrapper db, Long user_id, String user_password) {
        QueryInterfaceInfo queryInterfaceInfo = InterfaceManager.getUserTokenInfo(db, user_id);
        if (null == queryInterfaceInfo) {
            return StateType.getActionResult(StateType.NOT_REST_USER);
        }
        if (!user_password.equals(queryInterfaceInfo.getUser_password())) {
            return StateType.getActionResult(StateType.UNAUTHORIZED);
        }
        Map<String, Object> tokenMap = new HashMap<>();
        tokenMap.put("token", queryInterfaceInfo.getToken());
        tokenMap.put("expires_in", 7200);
        tokenMap.put("use_valid_date", queryInterfaceInfo.getUse_valid_date());
        ActionResult actionResult = StateType.getActionResult(StateType.NORMAL);
        actionResult.setData(tokenMap);
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "checkParam", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public static ActionResult checkTokenAndInterface(DatabaseWrapper db, CheckParam checkParam) {
        String token = checkParam.getToken();
        ActionResult actionResult;
        if (StringUtil.isBlank(token)) {
            if (checkParam.getUser_id() == null || StringUtil.isBlank(checkParam.getUser_password())) {
                actionResult = StateType.getActionResult(StateType.ARGUMENT_ERROR);
                actionResult.setData("token值为空时，user_id与user_password不能为空");
                return actionResult;
            }
            actionResult = getTokenById(db, checkParam.getUser_id(), checkParam.getUser_password());
            if (StateType.NORMAL != StateType.ofEnumByCode(actionResult.getCode())) {
                return actionResult;
            }
            Map<String, Object> message = JsonUtil.toObject(JsonUtil.toJson(actionResult.getData()), new TypeReference<Map<String, Object>>() {
            });
            token = message.get("token").toString();
        }
        if (InterfaceManager.existsToken(db, token)) {
            QueryInterfaceInfo userByToken = InterfaceManager.getUserByToken(token);
            actionResult = interfaceInfoCheck(db, userByToken.getUser_id(), checkParam.getUrl(), checkParam.getInterface_code());
            actionResult.setData(token);
            return actionResult;
        }
        return StateType.getActionResult(StateType.TOKEN_ERROR);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Param(name = "url", desc = "", range = "")
    @Param(name = "interface_code", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static ActionResult interfaceInfoCheck(DatabaseWrapper db, Long user_id, String url, String interface_code) {
        if (StringUtil.isBlank(url)) {
            return StateType.getActionResult(StateType.URL_NOT_EXIST);
        }
        if (InterfaceManager.existsInterface(db, user_id, url)) {
            QueryInterfaceInfo queryInterfaceInfo = InterfaceManager.getInterfaceUseInfo(user_id, url);
            if (StringUtil.isNotBlank(interface_code)) {
                if (!InterfaceManager.existsReportGraphic(user_id, interface_code)) {
                    ActionResult actionResult = StateType.getActionResult(StateType.REPORT_CODE_ERROR);
                    actionResult.setData("报表( " + interface_code + " )编码不正确");
                    return actionResult;
                }
            }
            if (InterfaceState.JinYong == InterfaceState.ofEnumByCode(queryInterfaceInfo.getUse_state())) {
                return StateType.getActionResult(StateType.INTERFACE_STATE_ERROR);
            }
            int num = queryInterfaceInfo.getStart_use_date().compareTo(DateUtil.getSysDate());
            if (num > 0) {
                return StateType.getActionResult(StateType.START_DATE_ERROR);
            }
            num = queryInterfaceInfo.getUse_valid_date().compareTo(DateUtil.getSysDate());
            if (num < 0) {
                return StateType.getActionResult(StateType.EFFECTIVE_DATE_ERROR);
            }
            return StateType.getActionResult(StateType.NORMAL);
        }
        return StateType.getActionResult(StateType.NO_INTERFACE_USE_PERMISSIONS);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "responseMap", desc = "", range = "")
    @Param(name = "outType", desc = "", range = "")
    @Param(name = "asynType", desc = "", range = "")
    @Param(name = "backUrl", desc = "", range = "", nullable = true)
    @Param(name = "fileName", desc = "", range = "", nullable = true)
    @Param(name = "filepath", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static ActionResult checkType(String dataType, String outType, String asynType, String backUrl, String filepath, String fileName) {
        if (!DataType.isDataType(dataType)) {
            return StateType.getActionResult(StateType.DATA_TYPE_ERROR);
        }
        if (!OutType.isOutType(outType)) {
            return StateType.getActionResult(StateType.OUT_TYPE_ERROR);
        }
        if (OutType.FILE == OutType.ofEnumByCode(outType)) {
            if (!AsynType.isAsynType(asynType)) {
                return StateType.getActionResult(StateType.ASYNTYPE_ERROR);
            }
            if (AsynType.ASYNCALLBACK == AsynType.ofEnumByCode(asynType)) {
                if (StringUtil.isBlank(backUrl)) {
                    return StateType.getActionResult(StateType.CALBACK_URL_ERROR);
                }
            }
            if (AsynType.ASYNPOLLING == AsynType.ofEnumByCode(asynType)) {
                if (StringUtil.isBlank(fileName)) {
                    return StateType.getActionResult(StateType.FILENAME_ERROR);
                }
                if (StringUtil.isBlank(filepath)) {
                    return StateType.getActionResult(StateType.FILEPARH_ERROR);
                }
            }
        }
        return StateType.getActionResult(StateType.NORMAL);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult verifyTable(DatabaseWrapper db, Long user_id, String table_name) {
        if (StringUtil.isBlank(table_name)) {
            return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
        }
        if (!InterfaceManager.existsTable(db, user_id, table_name)) {
            return StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
        }
        return StateType.getActionResult(StateType.NORMAL);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "col", desc = "", range = "")
    @Param(name = "columns", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean columnIsExist(String col, List<String> columns) {
        if (notCheckFunction.contains(col.toLowerCase())) {
            return false;
        }
        for (String column : columns) {
            if (column.trim().equalsIgnoreCase(col.trim())) {
                return false;
            }
        }
        return true;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "outType", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Param(name = "sqlSb", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult getSqlData(DatabaseWrapper db, String outType, String dataType, String sqlSb, Long user_id, Integer num) {
        List<Object> streamJson = new ArrayList<>();
        List<String> streamCsv = new ArrayList<>();
        List<String> streamCsvData = new ArrayList<>();
        String uuid = UUID.randomUUID().toString();
        try {
            if (OutType.STREAM == OutType.ofEnumByCode(outType)) {
                if (num == null) {
                    num = 100;
                }
                new ProcessingData() {

                    long lineCounter = 0;

                    @Override
                    public void dealLine(Map<String, Object> map) {
                        lineCounter++;
                        StringBuffer sbVal = new StringBuffer();
                        dealWithStream(map, sbVal, dataType, streamCsv, streamCsvData, streamJson, lineCounter);
                    }
                }.getPageDataLayer(sqlSb, db, 1, num);
            } else {
                File createFile = LocalFile.createFile(uuid, dataType);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(createFile))) {
                    if (num == null) {
                        getProcessingData(dataType, createFile, writer).getDataLayer(sqlSb, db);
                    } else {
                        getProcessingData(dataType, createFile, writer).getPageDataLayer(sqlSb, db, 1, num);
                    }
                } catch (Exception e) {
                    ActionResult actionResult = StateType.getActionResult(StateType.EXCEPTION);
                    actionResult.setData("写文件异常：" + e.getMessage());
                }
            }
        } catch (Exception e) {
            ActionResult actionResult = StateType.getActionResult(StateType.EXCEPTION);
            actionResult.setData(e.getMessage());
            return actionResult;
        }
        if (StateType.NORMAL != StateType.ofEnumByCode(actionResult.getCode())) {
            return actionResult;
        }
        if (OutType.STREAM == OutType.ofEnumByCode(outType)) {
            if (DataType.csv == DataType.ofEnumByCode(dataType)) {
                Map<String, Object> map = new HashMap<>();
                map.put("column", streamCsv);
                map.put("data", streamCsvData);
                actionResult = StateType.getActionResult(StateType.NORMAL);
                actionResult.setData(JsonUtil.toJson(map));
            } else {
                actionResult = StateType.getActionResult(StateType.NORMAL);
                actionResult.setData(streamJson);
            }
        } else {
            if (InterfaceCommon.saveFileInfo(db, user_id, uuid, dataType, outType, CommonVariables.RESTFILEPATH) != 1) {
                actionResult = StateType.getActionResult(StateType.EXCEPTION);
                actionResult.setData("保存接口文件信息失败");
            }
            Map<String, Object> uuidMap = new HashMap<>();
            uuidMap.put("dataType", dataType);
            uuidMap.put("outType", outType);
            uuidMap.put("uuid", uuid);
            actionResult = StateType.getActionResult(StateType.NORMAL);
            actionResult.setData(uuidMap);
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "map", desc = "", range = "")
    @Param(name = "sbCol", desc = "", range = "")
    @Param(name = "sbVal", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Param(name = "streamCsv", desc = "", range = "")
    @Param(name = "streamCsvData", desc = "", range = "")
    @Param(name = "streamJson", desc = "", range = "")
    private static void dealWithStream(Map<String, Object> map, StringBuffer sbVal, String dataType, List<String> streamCsv, List<String> streamCsvData, List<Object> streamJson, long lineCounter) {
        if (DataType.csv == DataType.ofEnumByCode(dataType)) {
            map.forEach((k, v) -> {
                if (lineCounter == 1) {
                    streamCsv.add(k);
                }
                sbVal.append(v).append(",");
            });
            streamCsvData.add(sbVal.deleteCharAt(sbVal.length() - 1).toString());
            actionResult = StateType.getActionResult(StateType.NORMAL);
        } else if (DataType.json == DataType.ofEnumByCode(dataType)) {
            streamJson.add(map);
            actionResult = StateType.getActionResult(StateType.NORMAL);
        } else {
            actionResult = StateType.getActionResult(StateType.DATA_TYPE_ERROR);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "uuid", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Param(name = "outType", desc = "", range = "")
    @Param(name = "path", desc = "", range = "")
    @Return(desc = "", range = "")
    public static int saveFileInfo(DatabaseWrapper db, Long user_id, String uuid, String dataType, String outType, String path) {
        InterfaceFileInfo file_info = new InterfaceFileInfo();
        file_info.setFile_id(uuid);
        file_info.setFile_path(path);
        file_info.setData_output(outType);
        file_info.setUser_id(user_id);
        file_info.setData_class(dataType);
        int num = file_info.add(db);
        db.commit();
        return num;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "responseMap", desc = "", range = "")
    @Param(name = "backurl", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult checkBackUrl(ActionResult actionResult, String backurl) {
        String response = new HttpClient().addData("backurl", backurl).addData("message", JsonUtil.toJson(actionResult)).post(backurl, new RestTemplateSingleton().getRestTemplate()).getBodyString();
        try {
            Optional<ActionResult> optionalActionResult = JsonUtil.toObjectSafety(response, ActionResult.class);
            actionResult = optionalActionResult.orElseGet(() -> StateType.getActionResult(StateType.CALBACK_URL_ERROR));
        } catch (Exception e) {
            actionResult = StateType.getActionResult(StateType.CALBACK_URL_ERROR);
            actionResult.setData(e.getMessage());
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "responseMap", desc = "", range = "")
    @Param(name = "filepath", desc = "", range = "")
    @Param(name = "filename", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult createFile(ActionResult actionResult, String filepath, String filename) {
        BufferedWriter writer = null;
        try {
            File file = new File(filepath);
            if (!file.exists() && !file.isDirectory()) {
                if (!file.mkdirs()) {
                    return StateType.getActionResult(StateType.CREATE_DIRECTOR_ERROR);
                }
            }
            filepath = filepath + File.separator + filename;
            File writeFile = new File(filepath);
            if (!writeFile.exists()) {
                if (!writeFile.createNewFile()) {
                    return StateType.getActionResult(StateType.CREATE_FILE_ERROR);
                }
            }
            writer = new BufferedWriter(new FileWriter(writeFile));
            writer.write(actionResult.toString());
            writer.flush();
            return actionResult;
        } catch (IOException e) {
            logger.error(e);
            actionResult = StateType.getActionResult(StateType.SIGNAL_FILE_ERROR);
            actionResult.setData(e.getMessage());
            return actionResult;
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.info(e);
                }
            }
        }
    }

    private static ProcessingData getProcessingData(String dataType, File createFile, BufferedWriter writer) {
        return new ProcessingData() {

            long lineCounter = 0;

            @Override
            public void dealLine(Map<String, Object> map) throws IOException {
                lineCounter++;
                StringBuffer sbCol = new StringBuffer();
                StringBuffer sbVal = new StringBuffer();
                dealWithFile(map, sbCol, sbVal, dataType, createFile, lineCounter, writer);
            }
        };
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "map", desc = "", range = "")
    @Param(name = "sbCol", desc = "", range = "")
    @Param(name = "sbVal", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Param(name = "writer", desc = "", range = "")
    private static void dealWithFile(Map<String, Object> map, StringBuffer sbCol, StringBuffer sbVal, String dataType, File createFile, long lineCounter, BufferedWriter writer) throws IOException {
        if (DataType.csv == DataType.ofEnumByCode(dataType)) {
            map.forEach((k, v) -> {
                sbCol.append(k).append(",");
                sbVal.append(v).append(",");
            });
            if (lineCounter == 1) {
                writer.write(sbCol.deleteCharAt(sbCol.length() - 1).toString());
                writer.newLine();
            }
            if (lineCounter % CommonVariables.DB_BATCH_ROW == 0) {
                logger.info("已经处理了 ：" + lineCounter + " 行数据！");
                writer.flush();
            }
            writer.write(sbVal.deleteCharAt(sbVal.length() - 1).toString());
            writer.newLine();
            actionResult = StateType.getActionResult(StateType.NORMAL);
        } else if (DataType.json == DataType.ofEnumByCode(dataType)) {
            if (lineCounter % CommonVariables.DB_BATCH_ROW == 0) {
                writer.flush();
            }
            writer.write(JsonUtil.toJson(map));
            writer.newLine();
            actionResult = StateType.getActionResult(StateType.NORMAL);
        } else {
            actionResult = StateType.getActionResult(StateType.EXCEPTION);
            actionResult.setData("输出数据类型有误,不知道什么文件");
        }
        writer.flush();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataType", desc = "", range = "")
    @Param(name = "outType", desc = "", range = "")
    @Param(name = "asynType", desc = "", range = "")
    @Param(name = "actionResult", desc = "", range = "")
    @Param(name = "filePath", desc = "", range = "")
    @Param(name = "fileName", desc = "", range = "")
    @Param(name = "backUrl", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult operateInterfaceByType(String dataType, String outType, String asynType, String backUrl, String filePath, String fileName, ActionResult actionResult) {
        if (DataType.csv == DataType.ofEnumByCode(dataType) && OutType.STREAM == OutType.ofEnumByCode(outType) && StateType.NORMAL == StateType.ofEnumByCode(actionResult.getCode())) {
            try {
                Map<String, Object> message = JsonUtil.toObject(actionResult.getData().toString(), new TypeReference<Map<String, Object>>() {
                });
                List<String> dataList = JsonUtil.toObject(message.get("data").toString(), new TypeReference<List<String>>() {
                });
                List<String> columnList = new ArrayList<>();
                Object o = message.get("column");
                if (o != null) {
                    if (o instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = (List<Object>) o;
                        columnList = list.stream().map(Object::toString).collect(Collectors.toList());
                    } else {
                        columnList = JsonUtil.toObject(o.toString(), new TypeReference<List<String>>() {
                        });
                    }
                }
                String data = String.join(System.lineSeparator(), dataList);
                String column = String.join(",", columnList);
                actionResult = StateType.getActionResult(StateType.NORMAL);
                actionResult.setData(column + System.lineSeparator() + data);
                return actionResult;
            } catch (Exception e) {
                return StateType.getActionResult(StateType.JSONCONVERSION_EXCEPTION);
            }
        }
        if (DataType.json == DataType.ofEnumByCode(dataType) && OutType.STREAM == OutType.ofEnumByCode(outType) && StateType.NORMAL == StateType.ofEnumByCode(InterfaceCommon.actionResult.getCode())) {
            return actionResult;
        }
        if (AsynType.SYNCHRONIZE == AsynType.ofEnumByCode(asynType)) {
            return actionResult;
        }
        if (AsynType.ASYNCALLBACK == AsynType.ofEnumByCode(asynType)) {
            actionResult = checkBackUrl(actionResult, backUrl);
        }
        if (AsynType.ASYNPOLLING == AsynType.ofEnumByCode(asynType)) {
            actionResult = createFile(actionResult, filePath, fileName);
        }
        return actionResult;
    }

    public static List<LayerBean> getLayerBeans(String tableName, Store_type store_type) {
        List<LayerBean> layerByTableList = ProcessingData.getLayerByTable(tableName, Dbo.db());
        return layerByTableList.stream().filter(layerBean -> store_type == Store_type.ofEnumByCode(layerBean.getStore_type())).collect(Collectors.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "singleTable", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult checkTable(DatabaseWrapper db, Long user_id, SingleTable singleTable) {
        try {
            if (StringUtil.isBlank(singleTable.getTableName())) {
                return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
            }
            if (!InterfaceManager.existsTable(db, user_id, singleTable.getTableName())) {
                return StateType.getActionResult(StateType.NO_USR_PERMISSIONS);
            }
            String table_en_column = InterfaceManager.getUserTableInfo(Dbo.db(), user_id, singleTable.getTableName()).getTable_en_column();
            return checkColumn(db, singleTable, table_en_column, user_id);
        } catch (Exception e) {
            ActionResult actionResult = StateType.getActionResult(StateType.EXCEPTION);
            if (e instanceof BusinessException) {
                actionResult.setData(e.getMessage());
                return actionResult;
            }
            return actionResult;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "singleTable", desc = "", range = "")
    @Param(name = "table_en_column", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult checkColumn(DatabaseWrapper db, SingleTable singleTable, String table_en_column, Long user_id) {
        Integer num = singleTable.getNum();
        if (num == null) {
            num = 10;
        }
        List<String> columns = StringUtil.split(table_en_column.toLowerCase(), Constant.METAINFOSPLIT);
        String selectColumn = singleTable.getSelectColumn();
        if (StringUtil.isNotBlank(selectColumn)) {
            ActionResult userColumn = checkColumnsIsExist(selectColumn, user_id, columns);
            if (userColumn != null)
                return userColumn;
        } else if (StringUtil.isNotBlank(table_en_column.toLowerCase())) {
            selectColumn = String.join(",", columns).toLowerCase();
        } else {
            selectColumn = " * ";
        }
        String whereColumn = singleTable.getWhereColumn();
        String condition = "";
        if (StringUtil.isNotBlank(whereColumn)) {
            ActionResult sqlSelectCondition = getSqlSelectCondition(columns, whereColumn);
            if (StateType.NORMAL != StateType.ofEnumByCode(sqlSelectCondition.getCode())) {
                return sqlSelectCondition;
            }
            condition = sqlSelectCondition.getData().toString();
        }
        String sqlSb = "SELECT " + selectColumn + " FROM " + singleTable.getTableName() + condition;
        DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(sqlSb);
        String newSql = druidParseQuerySql.GetNewSql(sqlSb);
        return getSqlData(db, singleTable.getOutType(), singleTable.getDataType(), newSql, user_id, num);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "selectColumn", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "columns", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult checkColumnsIsExist(String selectColumn, Long user_id, List<String> columns) {
        if (StringUtil.isNotBlank(selectColumn)) {
            if (!CommonVariables.AUTHORITY.contains(String.valueOf(user_id))) {
                List<String> userColumns = StringUtil.split(selectColumn, ",");
                if (columns != null && !columns.isEmpty()) {
                    for (String userColumn : userColumns) {
                        if (columnIsExist(userColumn.toLowerCase(), columns)) {
                            ActionResult actionResult = StateType.getActionResult(StateType.NO_COLUMN_USE_PERMISSIONS);
                            actionResult.setData("请求错误,查询列名" + userColumn + "没有使用权限");
                            return actionResult;
                        }
                    }
                }
            }
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columns", desc = "", range = "")
    @Param(name = "whereColumn", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult getSqlSelectCondition(List<String> columns, String whereColumn) {
        String condition;
        String[] cols = whereColumn.split(",");
        StringBuilder whereSb = new StringBuilder();
        whereSb.append(" where ");
        for (String col : cols) {
            String[] col_name;
            String symbol;
            if (col.contains(">=")) {
                col_name = col.split(">=");
                symbol = ">=";
            } else if (col.contains("<=")) {
                col_name = col.split("<=");
                symbol = "<=";
            } else if (col.contains(">")) {
                col_name = col.split(">");
                symbol = ">";
            } else if (col.contains("<")) {
                col_name = col.split("<");
                symbol = "<";
            } else if (col.contains("!=")) {
                col_name = col.split("!=");
                symbol = "!=";
            } else if (col.contains("=")) {
                col_name = col.split("=");
                symbol = "=";
            } else {
                ActionResult actionResult = StateType.getActionResult(StateType.CONDITION_ERROR);
                actionResult.setData("请求错误,条件符号错误,暂不支持");
                return actionResult;
            }
            if (col_name.length == 2) {
                String colName = col_name[0];
                String colVal = col_name[1];
                if (columnIsExist(colName.toLowerCase(), columns)) {
                    ActionResult actionResult = StateType.getActionResult(StateType.NO_COLUMN_USE_PERMISSIONS);
                    actionResult.setData("请求错误,条件列名" + colName + "没有使用权限");
                    return actionResult;
                }
                whereSb.append(colName).append(symbol).append("'").append(colVal).append("'").append(" and ");
            }
        }
        condition = whereSb.substring(0, whereSb.toString().lastIndexOf("and"));
        ActionResult actionResult = StateType.getActionResult(StateType.NORMAL);
        actionResult.setData(condition);
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableData", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "responseMap", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult deleteTableDataByTableName(DatabaseWrapper db, TableData tableData, Long user_id) {
        List<LayerBean> tableLayerList = ProcessingData.getLayerByTable(tableData.getTableName(), db);
        if (tableLayerList.isEmpty()) {
            return StateType.getActionResult(StateType.STORAGELAYER_NOT_EXIST_BY_TABLE);
        }
        for (LayerBean layerBean : tableLayerList) {
            try (DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(db, layerBean.getDsl_id())) {
                String store_type = layerBean.getStore_type();
                if (Store_type.DATABASE == Store_type.ofEnumByCode(store_type)) {
                    String table_en_column = InterfaceManager.getUserTableInfo(Dbo.db(), user_id, tableData.getTableName()).getTable_en_column();
                    List<String> columns = StringUtil.split(table_en_column.toLowerCase(), Constant.METAINFOSPLIT);
                    String whereColumn = tableData.getWhereColumn();
                    String condition = "";
                    if (StringUtil.isNotBlank(whereColumn)) {
                        actionResult = InterfaceCommon.getSqlSelectCondition(columns, whereColumn);
                        if (StateType.NORMAL != StateType.ofEnumByCode(actionResult.getCode())) {
                            return actionResult;
                        }
                        condition = actionResult.getData().toString();
                    }
                    String deleteSql = "delete from " + tableData.getTableName() + condition;
                    SqlOperator.execute(dbWrapper, deleteSql);
                    SqlOperator.commitTransaction(dbWrapper);
                } else if (Store_type.ofEnumByCode(store_type) == Store_type.HBASE) {
                    return deleteHbaseData(tableData.getTableName(), tableData.getRowKeys());
                } else {
                    return StateType.getActionResult(StateType.STORE_TYPE_NOT_EXIST);
                }
            } catch (Exception e) {
                return StateType.getActionResult(StateType.DELETE_TABLE_DATA_FAILED);
            }
        }
        return StateType.getActionResult(StateType.NORMAL);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "rowKeys", desc = "", range = "")
    @Return(desc = "", range = "")
    private static ActionResult deleteHbaseData(String tableName, String[] rowKeys) {
        try {
            List<String> rowkeyList = ClassBase.HbaseInstance().deleteHBaseData(tableName, rowKeys);
            if (rowkeyList == null) {
                return StateType.getActionResult(StateType.TABLE_NOT_EXISTENT);
            } else {
                if (!rowkeyList.isEmpty()) {
                    return StateType.getActionResult(StateType.TABLE_DATA_NOT_EXIST_BY_ROWKEY);
                }
            }
        } catch (Exception e) {
            ActionResult actionResult = StateType.getActionResult(StateType.EXCEPTION);
            actionResult.setData(e.getMessage());
            return actionResult;
        }
        return StateType.getActionResult(StateType.NORMAL);
    }

    public static ActionResult getFullTextSearchResult(FullTextSearchBean fullTextSearchBean, Map<String, String> customizeRequestKV, HttpServletRequest request, List<String> field_name_s) {
        String num = fullTextSearchBean.getNum();
        int startnum, limitnum;
        if (StringUtil.isBlank(num)) {
            startnum = 0;
            limitnum = 10;
        } else {
            if (num.contains(",")) {
                try {
                    List<String> splits = StringUtil.split(num, ",");
                    startnum = Integer.parseInt(splits.get(0));
                    limitnum = Integer.parseInt(splits.get(1));
                } catch (Exception e) {
                    throw new BusinessException("参数 num: " + num + " 输入不合法! see { 1,9 }");
                }
            } else {
                try {
                    limitnum = Integer.parseInt(num);
                    startnum = 0;
                } catch (Exception e) {
                    throw new BusinessException("参数 num: " + num + " 输入不合法! see { 9 }");
                }
            }
        }
        IsFlag isAccurateQuery = IsFlag.ofEnumByCode(fullTextSearchBean.getIsAccurateQuery());
        Map<String, Object> search_rs_map = queryFromSolr(fullTextSearchBean, customizeRequestKV, startnum, limitnum, request, isAccurateQuery, field_name_s);
        ActionResult actionResult = StateType.getActionResult(StateType.NORMAL);
        if (search_rs_map.isEmpty()) {
            actionResult.setData(new HashMap<>());
        } else {
            actionResult.setData(search_rs_map);
        }
        return actionResult;
    }

    public static Map<String, Object> queryFromSolr(FullTextSearchBean fullTextSearchBean, Map<String, String> customizeRequestKV, int start, int pageSize, HttpServletRequest request, IsFlag isAccurateQuery, List<String> file_names) {
        Map<String, String> params = new HashMap<>();
        params.put("fq", "tf-collect_type:\"" + AgentType.WenJianXiTong.getCode() + "\"");
        String id = fullTextSearchBean.getId();
        StringBuilder sb = new StringBuilder();
        if (StringUtil.isNotBlank(id) && !file_names.contains("id")) {
            sb.append("id:").append("\"").append(id).append("\"");
        }
        String file_name = fullTextSearchBean.getFilename();
        if (StringUtil.isNotEmpty(file_name) && !file_names.contains("filename")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-file_name:").append("\"").append(file_name).append("\"");
        }
        String filesuffix = fullTextSearchBean.getFilesuffix();
        if (StringUtil.isNotEmpty(filesuffix) && !file_names.contains("filesuffix")) {
            String[] ffix = filesuffix.split(",");
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-file_suffix:");
            sb.append("(");
            for (int i = 0; i < ffix.length; i++) {
                if (i != ffix.length - 1) {
                    sb.append("\"").append(ffix[i]).append("\"").append(" OR ");
                } else {
                    sb.append(ffix[i]);
                }
            }
            sb.append(")");
        }
        String filepath = fullTextSearchBean.getFilepath();
        if (!StringUtil.isEmpty(filepath) && !file_names.contains("filepath")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            String[] fileA = filepath.split(",");
            sb.append("tf-file_scr_path:");
            sb.append("(");
            for (int i = 0; i < fileA.length; i++) {
                if (i != fileA.length - 1) {
                    sb.append("\"").append(fileA[i]).append("\"").append(" OR ");
                } else {
                    sb.append("\"").append(fileA[i]).append("\"");
                }
            }
            sb.append(")");
        }
        String filemd5 = fullTextSearchBean.getFilemd5();
        if (!StringUtil.isEmpty(filemd5) && !file_names.contains("filemd5")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-file_md5:").append("\"").append(filemd5).append("\"");
        }
        String agent_name = fullTextSearchBean.getAgent_name();
        if (!StringUtil.isEmpty(agent_name) && !file_names.contains("agent_name")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-agent_name:").append("\"").append(agent_name).append("\"");
        }
        String ds_name = fullTextSearchBean.getDs_name();
        if (!StringUtil.isEmpty(ds_name) && !file_names.contains("ds_name")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-ds_name:").append("\"").append(ds_name).append("\"");
        }
        String fcs_name = fullTextSearchBean.getFcs_name();
        if (!StringUtil.isEmpty(fcs_name) && !file_names.contains("fcs_name")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            if (fcs_name.contains(","))
                fcs_name = "(" + fcs_name.replace(",", " OR ") + ")";
            sb.append("tf-fcs_name:").append(fcs_name);
        }
        String fcs_id = fullTextSearchBean.getFcs_id();
        if (!StringUtil.isEmpty(fcs_id) && !file_names.contains("fcs_id")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            if (fcs_id.contains(","))
                fcs_id = "(" + fcs_id.replace(",", " OR ") + ")";
            sb.append("tf-fcs_id:").append(fcs_id);
        }
        String filesize = fullTextSearchBean.getFilesize();
        if (!StringUtil.isEmpty(filesize) && !file_names.contains("filesize")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-file_size:").append(filesize);
        }
        String dep_id = fullTextSearchBean.getDep_id();
        if (!StringUtil.isEmpty(dep_id) && !file_names.contains("dep_id")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            if (dep_id.contains(","))
                dep_id = "(" + dep_id.replace(",", " OR ") + ")";
            sb.append("tf-dep_id:").append(dep_id);
        }
        String storagedate = fullTextSearchBean.getStoragedate();
        if (!StringUtil.isEmpty(storagedate) && !file_names.contains("storagedate")) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            sb.append("tf-storage_date:").append(storagedate);
        }
        Set<String> set = customizeRequestKV.keySet();
        for (String requestKey : set) {
            String requestvalue = customizeRequestKV.get(requestKey);
            if (!StringUtil.isEmpty(requestvalue)) {
                if (sb.length() != 0) {
                    sb.append(" AND ");
                }
                if (requestvalue.contains(HYREN_SEPER)) {
                    sb.append(Constant.SOLR_DATA_ASSOCIATION_PREFIX).append(requestKey).append(":").append("[").append(requestvalue.replace(HYREN_SEPER, " TO ").replace("-", "")).append("]");
                } else {
                    sb.append(Constant.SOLR_DATA_ASSOCIATION_PREFIX).append(requestKey).append(":").append("\"").append(requestvalue).append("\"");
                }
            }
        }
        String query = fullTextSearchBean.getQuery();
        if (!StringUtil.isEmpty(query)) {
            if (sb.length() != 0) {
                sb.append(" AND ");
            }
            if (isAccurateQuery == IsFlag.Fou) {
                query = getParticipleQuery(query.trim());
                query = "(" + query + ")";
            } else {
                query = "\"" + query.trim() + "\"";
            }
            sb.append("tf-file_text:").append(query);
        }
        params.put("q", sb.toString());
        List<String> columnList = new ArrayList<>();
        columnList.add("file_name");
        columnList.add("file_suffix");
        columnList.add("file_scr_path");
        columnList.add("file_md5");
        columnList.add("agent_name");
        columnList.add("ds_name");
        columnList.add("fcs_name");
        columnList.add("fcs_id");
        columnList.add("file_size");
        columnList.add("dep_id");
        columnList.add("storage_date");
        columnList.add("file_text");
        String hyren_sort = fullTextSearchBean.getHyren_sort();
        if (!StringUtil.isEmpty(hyren_sort)) {
            String[] sorts = hyren_sort.split(",");
            for (String sort : sorts) {
                String singleSortColumn = sort.split(" ")[0];
                if (!columnList.contains(singleSortColumn)) {
                    hyren_sort = hyren_sort.replace(singleSortColumn, Constant.SOLR_DATA_ASSOCIATION_PREFIX + singleSortColumn);
                    logger.info(singleSortColumn + " 自定义字段");
                } else {
                    hyren_sort = hyren_sort.replace(singleSortColumn, "tf-" + singleSortColumn);
                    logger.info(singleSortColumn + " 非自定义字段");
                }
            }
            if (!StringUtil.isEmpty(query)) {
                hyren_sort = "score desc," + hyren_sort;
            }
            logger.info("========hyren_sort========" + hyren_sort);
            params.put("sort", hyren_sort);
        }
        List<Map<String, Object>> search_rs = new ArrayList<>();
        List<String> analysis = new ArrayList<>();
        try (ISolrOperator os = SolrFactory.getSolrOperatorInstance()) {
            logger.info("===========params=========" + params);
            search_rs = os.querySolrPlus(params, start, pageSize, IsFlag.Fou);
            if (!StringUtil.isEmpty(query)) {
                analysis = Arrays.asList(query.substring(1, query.length() - 1).split("\" OR \""));
            }
        } catch (Exception e) {
            logger.error("Failed to queryFromSolr...", e);
        }
        String requestUrl = request.getRequestURL().toString();
        String action = requestUrl.substring(0, requestUrl.lastIndexOf('/') + 1);
        List<Map<String, Object>> arr = new ArrayList<>();
        for (Map<String, Object> rs : search_rs) {
            if (AgentType.WenJianXiTong.getCode().equals(rs.get("collect_type"))) {
                String file_id = rs.get("id").toString();
                String original_name = rs.get("file_name").toString();
                rs.put("downloadpath", action + "unstructuredFileDownloadApi?id=" + file_id + "&file_name=" + original_name);
            }
            arr.add(rs);
        }
        Map<String, Object> fullTextSearchMap = new HashMap<>();
        fullTextSearchMap.put("search_rs", arr);
        fullTextSearchMap.put("analysis", analysis);
        return fullTextSearchMap;
    }

    private static String getParticipleQuery(String query) {
        List<String> participleList = Arrays.asList(query.split(Constant.SPACE));
        StringBuilder queryPlus = new StringBuilder();
        for (int i = 0; i < participleList.size(); i++) {
            String s = participleList.get(i);
            if (StringUtil.isNotBlank(s)) {
                queryPlus.append("*").append(s).append("*");
                if (i != participleList.size() - 1) {
                    queryPlus.append(Constant.SPACE).append("OR").append(Constant.SPACE);
                }
            }
        }
        log.info("finalQueryPlus: " + queryPlus);
        return queryPlus.toString();
    }

    public static ActionResult solrSearch(SolrSearch solrSearch) {
        ActionResult actionResult;
        int limitNum = 10;
        int startNum = 0;
        if (StringUtil.isNotBlank(solrSearch.getNum())) {
            try {
                if (solrSearch.getNum().contains(",")) {
                    List<String> split = StringUtil.split(solrSearch.getNum(), ",");
                    startNum = Integer.parseInt(split.get(0));
                    limitNum = Integer.parseInt(split.get(1));
                } else {
                    limitNum = Integer.parseInt(solrSearch.getNum());
                }
            } catch (Exception e) {
                actionResult = StateType.getActionResult(StateType.ARGUMENT_ERROR);
                actionResult.setData(e.getMessage());
            }
        }
        Map<String, Object> layerMap = DataTableUtil.getDataLayerByTableName(Dbo.db(), solrSearch.getTableName());
        Object data_layer = layerMap.get("data_layer");
        if (data_layer == null) {
            actionResult = StateType.getActionResult(StateType.ARGUMENT_ERROR);
            actionResult.setData("获取表数据层失败");
            return actionResult;
        }
        SolrParam solrParam;
        if (DataSourceType.DML == DataSourceType.ofEnumByCode(data_layer.toString())) {
            solrParam = getSolrParam(solrSearch.getTableName().toUpperCase());
        } else if (DataSourceType.DCL == DataSourceType.ofEnumByCode(data_layer.toString())) {
            List<LayerBean> layerBeans = getLayerBeans(solrSearch.getTableName(), Store_type.SOLR);
            if (layerBeans.isEmpty()) {
                return StateType.getActionResult(StateType.TABLE_NOT_EXIST_ON_SOLR_STOREAGE);
            }
            String collection = layerBeans.get(0).getLayerAttr().get("collection");
            solrParam = getSolrParam(collection);
        } else {
            return StateType.getActionResult(StateType.ONLY_SUPPORT_TABLE_TO_SOLR);
        }
        SolrQuery solrQuery = getSolrQuery(solrSearch.getTableName(), solrSearch.getColumns(), solrSearch.getColumn(), data_layer.toString());
        List<Map<String, Object>> list = searchSolrBySolrQuery(solrParam, solrQuery, startNum, limitNum, IsFlag.Fou);
        actionResult = StateType.getActionResult(StateType.NORMAL);
        actionResult.setData(list);
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "solrParams", desc = "", range = "")
    @Param(name = "solrParam", desc = "", range = "")
    @Param(name = "start", desc = "", range = "")
    @Param(name = "rows", desc = "", range = "")
    @Param(name = "flag", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> searchSolrBySolrQuery(SolrParam solrParam, SolrQuery solrQuery, int start, int rows, IsFlag is_return_file_text) {
        List<Map<String, Object>> solrDocList = new ArrayList<>();
        try (ISolrOperator iSolrOperator = SolrFactory.getSolrOperatorInstance(solrParam)) {
            if (0 != rows) {
                solrQuery.setStart(start);
                solrQuery.setRows(rows);
            }
            solrQuery.setIncludeScore(true);
            SolrClient solrClient = iSolrOperator.getSolrClient();
            QueryResponse queryResponse = solrClient.query(solrQuery);
            SolrDocumentList solrDocumentList = queryResponse.getResults();
            log.debug("检索到的所有记录数: " + queryResponse.getResults().getNumFound() + " 条");
            for (SolrDocument singleDoc : solrDocumentList) {
                Map<String, Object> resMap = new HashMap<>();
                resMap.put("score", singleDoc.getFieldValue("score"));
                String sub_field;
                for (String fieldName : singleDoc.getFieldNames()) {
                    if (!fieldName.equals("_version_") && !fieldName.equals("score")) {
                        if (is_return_file_text == IsFlag.Fou && fieldName.equals("tf-file_text")) {
                            continue;
                        }
                        if (!fieldName.equals("id") && !fieldName.equals("table-name")) {
                            sub_field = fieldName.substring(3).trim();
                        } else {
                            sub_field = fieldName;
                        }
                        resMap.put(sub_field, singleDoc.getFieldValue(fieldName));
                    }
                }
                solrDocList.add(resMap);
            }
        } catch (Exception e) {
            log.error("获取solr检索结果失败!");
            throw new BusinessException("获取solr检索结果失败:" + e.getMessage());
        }
        return solrDocList;
    }

    public static ActionResult getHbaseSolrQuery(String table_name, String whereColumn, String selectColumn, Integer start, Integer num, String table_column_name, String tableTypeJsonStr, String dsl_name, String platform, String prncipal_name, String hadoop_user_name) {
        try {
            List<String> columns = StringUtil.split(table_column_name.toLowerCase(), Constant.METAINFOSPLIT);
            StringBuilder filter = new StringBuilder();
            List<String> tableTypeList = StringUtil.split(tableTypeJsonStr, Constant.METAINFOSPLIT);
            if (StringUtil.isNotBlank(whereColumn)) {
                String[] cols = whereColumn.split(",");
                for (String col : cols) {
                    List<String> col_name;
                    if (col.contains(">=")) {
                        col_name = StringUtil.split(col, ">=");
                    } else if (col.contains("<=")) {
                        col_name = StringUtil.split(col, "<=");
                    } else if (col.contains(">")) {
                        col_name = StringUtil.split(col, ">");
                    } else if (col.contains("<")) {
                        col_name = StringUtil.split(col, "<");
                    } else if (col.contains("!=")) {
                        col_name = StringUtil.split(col, "!=");
                    } else if (col.contains("=")) {
                        col_name = StringUtil.split(col, "=");
                    } else {
                        return StateType.getActionResult(StateType.CONDITION_ERROR);
                    }
                    if (col_name.size() == 2) {
                        String colName = col_name.get(0).trim();
                        String colVal = col_name.get(1);
                        if (!columns.contains(colName.toLowerCase())) {
                            ActionResult actionResult = StateType.getActionResult(StateType.NO_COLUMN_USE_PERMISSIONS);
                            actionResult.setData("列" + colName + "没有使用权限");
                            return actionResult;
                        }
                        setSolrFieldNames(filter, tableTypeList, colName, colVal);
                    }
                }
            } else {
                return StateType.getActionResult(StateType.CONDITION_ERROR);
            }
            String query = filter.substring(0, filter.lastIndexOf(" AND "));
            logger.info("query:" + query);
            Map<String, String> param = new HashMap<>();
            param.put("q", query);
            param.put("fl", "id");
            logger.info("############solr查询参数############");
            Map<String, Object> layerMap = DataTableUtil.getDataLayerByTableName(Dbo.db(), table_name);
            Object data_layer = layerMap.get("data_layer");
            if (data_layer == null) {
                actionResult = StateType.getActionResult(StateType.ARGUMENT_ERROR);
                actionResult.setData("获取表数据层失败");
                return actionResult;
            }
            SolrParam solrParam;
            if (DataSourceType.DML == DataSourceType.ofEnumByCode(data_layer.toString())) {
                solrParam = getSolrParam(table_name.toUpperCase());
            } else if (DataSourceType.DCL == DataSourceType.ofEnumByCode(data_layer.toString())) {
                List<LayerBean> layerBeans = getLayerBeans(table_name, Store_type.SOLR);
                if (layerBeans.isEmpty()) {
                    return StateType.getActionResult(StateType.TABLE_NOT_EXIST_ON_SOLR_STOREAGE);
                }
                String collection = layerBeans.get(0).getLayerAttr().get("collection");
                solrParam = getSolrParam(collection);
            } else {
                return StateType.getActionResult(StateType.ONLY_SUPPORT_TABLE_TO_SOLR);
            }
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.set("q", param.get("q"));
            solrQuery.set("fl", param.get("id"));
            List<Map<String, Object>> result = searchSolrBySolrQuery(solrParam, solrQuery, start == null ? 0 : start, num == null ? 10 : num, IsFlag.Fou);
            List<String> rowkeyList = new ArrayList<>();
            for (Map<String, Object> obj : result) {
                String rowkey = StringUtil.replace(obj.get("id").toString(), table_name + "@", "");
                logger.info("rowkey: " + rowkey);
                rowkeyList.add(rowkey);
            }
            List<Map<String, Object>> js = ClassBase.HbaseInstance().queryByRowkey(table_name, dsl_name, platform, prncipal_name, hadoop_user_name, selectColumn, rowkeyList);
            ActionResult actionResult = StateType.getActionResult(StateType.NORMAL);
            actionResult.setData(js);
            return actionResult;
        } catch (Exception e) {
            logger.error(e);
            ActionResult actionResult = StateType.getActionResult(StateType.EXCEPTION);
            if (e instanceof BusinessException) {
                actionResult.setData(e.getMessage());
                return actionResult;
            }
            return actionResult;
        }
    }

    private static void setSolrFieldNames(StringBuilder filter, List<String> tableTypeList, String colName, String colVal) {
        for (String tableType : tableTypeList) {
            Map<String, Object> tableTypeJson = JsonUtil.toObject(tableType, new TypeReference<Map<String, Object>>() {
            });
            String column_name = (String) tableTypeJson.get("column_name");
            if (colName.equals(column_name)) {
                String solrFieldName = solrFieldName(colName, (String) tableTypeJson.get("data_type"));
                filter.append(solrFieldName).append(":").append(colVal).append(" AND ");
            }
        }
    }

    private static String solrFieldName(String hbaseFieldName, String type) {
        if (type == null) {
            logger.info("column " + hbaseFieldName + "'type is null, so using STRING");
            type = "STRING";
        }
        HbaseSolrField hsf = new HbaseSolrField();
        hsf.setHbaseColumnName(hbaseFieldName);
        hsf.setType(type);
        new TypeFieldNameMapper().map(hsf);
        return hsf.getSolrColumnName();
    }

    public static SolrParam getSolrParam(String collection) {
        SolrParam solrParam = new SolrParam();
        String solrZkUrl = PropertyParaValue.getString("zkHost", "cdh063:2181,cdh064:2181,cdh065:2181/solr");
        solrParam.setCollection(collection);
        solrParam.setSolrZkUrl(solrZkUrl);
        return solrParam;
    }

    private static SolrQuery getSolrQuery(String tableName, String columns, String column, String data_layer) {
        SolrQuery solrQuery = new SolrQuery();
        String prefix;
        if (DataSourceType.DML == DataSourceType.ofEnumByCode(data_layer)) {
            prefix = "F-";
        } else if (DataSourceType.DCL == DataSourceType.ofEnumByCode(data_layer)) {
            solrQuery.set("q", "table-name:" + "\"" + tableName + "\"");
            prefix = "tf-";
        } else {
            throw new BusinessException(StateType.ONLY_SUPPORT_TABLE_TO_SOLR.getMessage());
        }
        if (StringUtil.isNotBlank(columns)) {
            StringBuilder sb = new StringBuilder();
            List<String> columnList = StringUtil.split(columns.toUpperCase(), ",");
            for (String col : columnList) {
                sb.append(prefix).append(col).append(",");
            }
            solrQuery.set("fl", sb.deleteCharAt(sb.length() - 1).toString());
        }
        List<String> colList = StringUtil.split(column, ",");
        List<String> cols = StringUtil.split(colList.get(0), "=");
        if (colList.size() == 1) {
            if (DataSourceType.DML == DataSourceType.ofEnumByCode(data_layer)) {
                solrQuery.set("q", prefix + cols.get(0) + ":" + cols.get(1));
            }
            solrQuery.set("fq", prefix + cols.get(0) + ":" + cols.get(1));
            solrQuery.set("sort", prefix + cols.get(0) + " asc");
        } else {
            StringBuilder query = new StringBuilder();
            StringBuilder sort = new StringBuilder();
            for (int i = 0; i < colList.size(); i++) {
                query.append(prefix).append(cols.get(0)).append(":").append(cols.get(1)).append(" AND ");
                sort.append(prefix).append(cols.get(0)).append(",");
            }
            if (DataSourceType.DML == DataSourceType.ofEnumByCode(data_layer)) {
                solrQuery.set("q", query.deleteCharAt(query.length() - 5).toString());
            }
            solrQuery.set("fq", query.deleteCharAt(query.length() - 5).toString());
            solrQuery.set("sort", sort.deleteCharAt(sort.length() - 1) + " asc");
        }
        logger.info("======solr查询参数========" + solrQuery);
        return solrQuery;
    }
}
