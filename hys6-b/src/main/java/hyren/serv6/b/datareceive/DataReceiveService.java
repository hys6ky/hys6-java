package hyren.serv6.b.datareceive;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.datadistribution.bean.DistributeJobBean;
import hyren.serv6.b.datareceive.req.BatchAnals;
import hyren.serv6.b.datareceive.req.MenuTree;
import hyren.serv6.b.datareceive.req.ReqTaskInfo;
import hyren.serv6.b.datareceive.req.TreeNode;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DataReceiveService {

    private static final Integer RESOURCE_MAX = 5;

    public String obtainSampleData(ReqTaskInfo taskInfo) {
        DrTask Task = taskInfo.getDrTask();
        checkTask(Task);
        List<DrParamsDef> paramsDefs = taskInfo.getDrParamsDefList();
        String urlParamValue = taskInfo.getParamValues();
        List<DrParamsDef> drParamsDefList = replaceUrlParam(urlParamValue, paramsDefs);
        try {
            Object data = dataReceive(Task, drParamsDefList);
            return JsonUtil.toJson(data);
        } catch (Exception e) {
            throw new BusinessException("获取样例数据失败！" + e);
        }
    }

    public Long saveTask(ReqTaskInfo taskInfo) {
        DrTask task = taskInfo.getDrTask();
        checkTask(task);
        List<Map<String, Object>> list = SqlOperator.queryList(Dbo.db(), " SELECT * FROM dr_task WHERE dr_task_name=?", task.getDr_task_name());
        if (!list.isEmpty() && list != null) {
            throw new BusinessException("任务名称重复！");
        }
        task.setDr_task_id(PrimayKeyGener.getNextId());
        task.setCreated_by(UserUtil.getUserId());
        task.setCreated_date(DateUtil.getSysDate());
        task.setCreated_time(DateUtil.getSysTime());
        task.setUpdate_by(UserUtil.getUserId());
        task.setUpdated_date(DateUtil.getSysDate());
        task.setUpdated_time(DateUtil.getSysTime());
        task.add(Dbo.db());
        List<DrParamsDef> paramsDefs = taskInfo.getDrParamsDefList();
        if (paramsDefs != null && !paramsDefs.isEmpty()) {
            paramsDefs.forEach(param -> {
                param.setParam_id(PrimayKeyGener.getNextId());
                param.setDr_task_id(task.getDr_task_id());
                param.add(Dbo.db());
            });
        }
        DrFileDef fileDef = taskInfo.getFileDef();
        checkFileDef(fileDef);
        String fileName = fileDef.getDr_file_name();
        List<Map<String, Object>> fileDefList = SqlOperator.queryList(Dbo.db(), "SELECT * FROM dr_file_def WHERE dr_file_name =? ", fileName);
        if (fileDefList != null && !fileDefList.isEmpty()) {
            throw new BusinessException("定义文件名重复");
        }
        fileDef.setDr_file_id(PrimayKeyGener.getNextId());
        fileDef.setDr_task_id(task.getDr_task_id());
        fileDef.add(Dbo.db());
        List<DrAnalysis> drAnalysisList = taskInfo.getDrAnalyses();
        if (drAnalysisList == null || drAnalysisList.isEmpty()) {
            throw new BusinessException("未填写数据解析信息");
        }
        drAnalysisList.forEach(analysis -> {
            analysis.setDr_anal_id(PrimayKeyGener.getNextId());
            analysis.setDr_task_id(task.getDr_task_id());
            analysis.add(Dbo.db());
        });
        return task.getDr_task_id();
    }

    public Map<String, Object> queryReceiveTask(Integer pageNum, Integer pageSize, Long userId) {
        Page page = new DefaultPageImpl(pageNum, pageSize);
        String querySQL = "SELECT t.*,f.dr_file_id,f.dr_file_name,f.dr_plane_url FROM " + "dr_task t JOIN dr_file_def f ON t.dr_task_id = f.dr_task_id WHERE " + "t.created_by = ?";
        List<Map<String, Object>> ReceiveTaskList = SqlOperator.queryPagedList(Dbo.db(), page, querySQL, userId);
        ReceiveTaskList.forEach(task -> {
            String dr_task_id = task.get("dr_task_id").toString();
            List<Map<String, Object>> ParamList = SqlOperator.queryList(Dbo.db(), "SELECT * FROM dr_params_def WHERE dr_task_id=?", Long.parseLong(dr_task_id));
            task.put("dr_params", ParamList);
        });
        Map map = new HashMap();
        map.put("ReceiveTaskList", ReceiveTaskList);
        map.put("total", page.getTotalSize());
        log.info("打印结果数据: {}", map);
        return map;
    }

    public Map<String, Object> queryTaskAndFileById(Long dr_task_id) {
        Map<String, Object> map = new HashMap<String, Object>();
        String taskSql = "select * from " + DrTask.TableName + " where dr_task_id = ?";
        List<Map<String, Object>> taskList = SqlOperator.queryList(Dbo.db(), taskSql, dr_task_id);
        if (taskList == null || taskList.size() != 1) {
            throw new BusinessException("查询不到任务信息");
        }
        Map<String, Object> taskInfo = taskList.get(0);
        String analysisSql = "select * from " + DrAnalysis.TableName + " where dr_task_id = ?";
        List<Map<String, Object>> analysisList = SqlOperator.queryList(Dbo.db(), analysisSql, dr_task_id);
        taskInfo.put("analysisList", analysisList);
        String paramsSql = "select * from " + DrParamsDef.TableName + " where dr_task_id=?";
        List<Map<String, Object>> paramList = SqlOperator.queryList(Dbo.db(), paramsSql, dr_task_id);
        taskInfo.put("paramList", paramList);
        String fileSql = "select * from " + DrFileDef.TableName + " where dr_task_id = ?";
        Optional<DrFileDef> drFileDef = SqlOperator.queryOneObject(Dbo.db(), DrFileDef.class, fileSql, dr_task_id);
        if (drFileDef.isPresent()) {
            map.put("fileInfo", drFileDef.get());
        }
        map.put("taskInfo", taskInfo);
        return map;
    }

    public Long updateReceiveTask(ReqTaskInfo taskInfo) {
        DrTask drTask = taskInfo.getDrTask();
        checkTask(drTask);
        List<Map<String, Object>> list = SqlOperator.queryList(Dbo.db(), "select * from dr_task where dr_task_id=?", drTask.getDr_task_id());
        if (list == null || list.isEmpty()) {
            throw new BusinessException("该任务不存在");
        }
        if (list.size() != 1) {
            throw new BusinessException("该任务存在多条记录,请检查");
        }
        drTask.setUpdate_by(UserUtil.getUserId());
        drTask.setUpdated_date(DateUtil.getSysDate());
        drTask.setUpdated_time(DateUtil.getSysTime());
        drTask.update(Dbo.db());
        List<DrParamsDef> drParamsDefList = taskInfo.getDrParamsDefList();
        List<Map<String, Object>> allParams = SqlOperator.queryList(Dbo.db(), "select * from " + DrParamsDef.TableName + " where dr_task_id = ?", drTask.getDr_task_id());
        if (allParams != null && !allParams.isEmpty()) {
            allParams.forEach(map -> Dbo.execute("delete from dr_params_def where param_id = ?", Long.parseLong(map.get("param_id").toString())));
        }
        if (drParamsDefList != null && !drParamsDefList.isEmpty()) {
            for (DrParamsDef drParamsDef : drParamsDefList) {
                if (StringUtil.isBlank(drParamsDef.getParam_key()) || StringUtil.isBlank(drParamsDef.getParams_value())) {
                    throw new BusinessException("please check fields  is not null...");
                }
            }
            Set<String> set = new HashSet<String>();
            for (DrParamsDef drParamsDef : drParamsDefList) {
                set.add(drParamsDef.getParam_key());
            }
            if (set.size() != drParamsDefList.size()) {
                throw new BusinessException("fields is repeat, please change ...");
            }
            drParamsDefList.forEach(paramDef -> {
                if (paramDef.getParam_id() == null || paramDef.getParam_id().equals(0L)) {
                    paramDef.setParam_id(PrimayKeyGener.getNextId());
                }
                paramDef.setDr_task_id(drTask.getDr_task_id());
                paramDef.add(Dbo.db());
            });
        }
        DrFileDef fileDef = taskInfo.getFileDef();
        checkFileDef(fileDef);
        Long file_id = fileDef.getDr_file_id();
        List<Map<String, Object>> fileDefList = SqlOperator.queryList(Dbo.db(), "SELECT * FROM dr_file_def WHERE dr_file_id =? ", file_id);
        if (fileDefList == null || fileDefList.size() != 1) {
            throw new BusinessException("文件信息不存在或存在多条");
        }
        fileDef.update(Dbo.db());
        List<DrAnalysis> drAnalysisList = taskInfo.getDrAnalyses();
        if (drAnalysisList == null || drAnalysisList.isEmpty()) {
            throw new BusinessException("未填写数据解析信息");
        }
        for (DrAnalysis drAnalysis : drAnalysisList) {
            if (StringUtil.isBlank(drAnalysis.getDr_anal()) || StringUtil.isBlank(drAnalysis.getDr_anal_name())) {
                throw new BusinessException("please check fields  is not null...");
            }
        }
        Set<String> set = new HashSet<String>();
        for (DrAnalysis drAnalysis : drAnalysisList) {
            set.add(drAnalysis.getDr_anal_name());
        }
        if (set.size() != drAnalysisList.size()) {
            throw new BusinessException("fields is repeat, please change ...");
        }
        List<Map<String, Object>> analysisList = SqlOperator.queryList(Dbo.db(), "select * from dr_analysis where dr_task_id = ?", drTask.getDr_task_id());
        if (analysisList != null && !analysisList.isEmpty()) {
            analysisList.forEach(analysis -> Dbo.execute("delete from dr_analysis where dr_anal_id = ?", Long.parseLong(analysis.get("dr_anal_id").toString())));
        }
        for (DrAnalysis drAnalysis : drAnalysisList) {
            if (drAnalysis.getDr_anal_id() == null || drAnalysis.getDr_anal_id().equals(0L)) {
                drAnalysis.setDr_anal_id(PrimayKeyGener.getNextId());
            }
            drAnalysis.setDr_task_id(drTask.getDr_task_id());
            drAnalysis.add(Dbo.db());
        }
        return drTask.getDr_task_id();
    }

    public void deleteReceiveTask(Long drTaskId) {
        String deleteParams = "DELETE FROM dr_params_def WHERE dr_task_id=?";
        SqlOperator.execute(Dbo.db(), deleteParams, drTaskId);
        String deleteTask = "DELETE FROM dr_task WHERE dr_task_id=?";
        SqlOperator.execute(Dbo.db(), deleteTask, drTaskId);
        String deleteAnalysis = "DELETE FROM dr_analysis WHERE dr_task_id = ?";
        SqlOperator.execute(Dbo.db(), deleteAnalysis, drTaskId);
        String deleteFile = "DELETE FROM DR_FILE_DEF WHERE dr_task_id=?";
        SqlOperator.execute(Dbo.db(), deleteFile, drTaskId);
    }

    public void unloadAnalData(Long dr_task_id, String curr_bath_date, String drParams) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            List<DrTask> drTaskList = SqlOperator.queryList(db, DrTask.class, "SELECT * FROM " + DrTask.TableName + " WHERE dr_task_id = ?", dr_task_id);
            if (drTaskList == null || drTaskList.isEmpty()) {
                throw new BusinessException("根据任务ID未查询到任务定义信息");
            }
            DrTask taskInfo = drTaskList.get(0);
            List<DrParamsDef> paramsDefList = Dbo.queryList(db, DrParamsDef.class, "SELECT * FROM " + DrParamsDef.TableName + " WHERE dr_task_id = ?", dr_task_id);
            if (paramsDefList != null && !paramsDefList.isEmpty()) {
                paramsDefList = replaceUrlParam(drParams, paramsDefList);
            }
            Object object = dataReceive(taskInfo, paramsDefList);
            log.info("根据URL获取到的数据为：===============================" + JsonUtil.toJson(object));
            List<DrAnalysis> drAnalysisList = SqlOperator.queryList(db, DrAnalysis.class, "select * from " + DrAnalysis.TableName + " where dr_task_id=?", dr_task_id);
            List<Map<String, Object>> resultList = parseObjectByAnalysis(drAnalysisList, object);
            List<DrFileDef> drFileDefs = SqlOperator.queryList(db, DrFileDef.class, "select * from " + DrFileDef.TableName + " where dr_task_id=?", dr_task_id);
            if (drFileDefs == null || drFileDefs.size() != 1) {
                throw new BusinessException("获取文件定义信息有误!");
            }
            DrFileDef fileInfo = drFileDefs.get(0);
            unloadFileData(fileInfo, resultList, curr_bath_date);
        } catch (Exception e) {
            log.error("数据接收失败：" + e);
            throw new AppSystemException("数据接收失败：" + e);
        }
    }

    public void unloadAnalData(List<BatchAnals> batchAnals) {
        if (batchAnals != null && !batchAnals.isEmpty()) {
            for (BatchAnals batchAnal : batchAnals) {
                this.unloadAnalData(batchAnal.getDr_task_id(), batchAnal.getCurr_bath_date(), batchAnal.getDrParams());
            }
        }
    }

    private List<Map<String, Object>> parseObjectByAnalysis(List<DrAnalysis> drAnalysisList, Object object) {
        List<Map<String, Object>> resultMap = new ArrayList<Map<String, Object>>();
        String jsonData = JsonUtil.toJson(object);
        if (object instanceof List) {
            List<Object> objects = JsonUtil.toObject(jsonData, new TypeReference<List<Object>>() {
            });
            for (Object data : objects) {
                Map<String, Object> dataMap = new HashMap<>();
                String jsonItem = JsonUtil.toJson(data);
                if (drAnalysisList != null && !drAnalysisList.isEmpty()) {
                    for (DrAnalysis drAnalysis : drAnalysisList) {
                        String analName = drAnalysis.getDr_anal_name();
                        String anal = drAnalysis.getDr_anal();
                        String[] key = anal.split("\\.");
                        Optional<String> valueOptional = JsonUtil.toObjectByNodeNameSafety(jsonItem, key[key.length - 1], String.class);
                        if (valueOptional.isPresent()) {
                            dataMap.put(analName, valueOptional.get());
                        } else {
                            dataMap.put(analName, "");
                        }
                    }
                }
                resultMap.add(dataMap);
            }
        } else {
            if (drAnalysisList != null && !drAnalysisList.isEmpty()) {
                Map<String, Object> dataMap = new HashMap<>();
                for (DrAnalysis drAnalysis : drAnalysisList) {
                    String analName = drAnalysis.getDr_anal_name();
                    String anal = drAnalysis.getDr_anal();
                    String[] key = anal.split("\\.");
                    Optional<String> valueOptional = JsonUtil.toObjectByNodeNameSafety(jsonData, key[key.length - 1], String.class);
                    if (valueOptional.isPresent()) {
                        dataMap.put(analName, valueOptional.get());
                    } else {
                        dataMap.put(analName, "");
                    }
                }
                resultMap.add(dataMap);
            }
        }
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "category", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCategoryItems(String category) {
        return WebCodesItem.getCategoryItems(category);
    }

    private void unloadFileData(DrFileDef fileDef, List<Map<String, Object>> resultData, String curr_bath_date) {
        String separator = File.separator;
        String plane_url = fileDef.getDr_plane_url() + separator + curr_bath_date + separator + fileDef.getDr_file_id() + separator;
        FileNameUtils.normalize(plane_url);
        String fileName = fileDef.getDr_file_name();
        String suffix = FileUtil.FILE_EXT_CHAR + fileDef.getDr_file_suffix();
        File parentFile = new File(plane_url);
        if (parentFile.exists()) {
            if (!FileUtil.deleteDirectoryFiles(plane_url)) {
                throw new AppSystemException("删除目录失败:" + parentFile.getAbsolutePath());
            }
        } else {
            if (!parentFile.mkdirs()) {
                throw new AppSystemException("创建父目录失败:" + parentFile.getAbsolutePath());
            }
        }
        String fullFilePath = plane_url + fileName + suffix;
        writeAnalysisFile(fileDef, resultData, fullFilePath);
        String signalFile = plane_url + fileName + ".ok";
        if (IsFlag.Shi == IsFlag.ofEnumByCode(fileDef.getDr_is_flag())) {
            FileUtil.createFileIfAbsent(signalFile, "");
        }
    }

    private void writeAnalysisFile(DrFileDef fileDef, List<Map<String, Object>> resultData, String fullFilePath) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(fullFilePath)), DataBaseCode.ofValueByCode(fileDef.getDr_database_code())))) {
            StringBuffer sbCol = new StringBuffer();
            List<StringBuffer> valueList = new ArrayList<>();
            if (resultData != null && !resultData.isEmpty()) {
                long i = 1;
                for (Map<String, Object> resultDatum : resultData) {
                    StringBuffer sbVal = new StringBuffer();
                    Set<Map.Entry<String, Object>> entries = resultDatum.entrySet();
                    for (Map.Entry<String, Object> entry : entries) {
                        if (i == 1) {
                            sbCol.append(entry.getKey()).append(fileDef.getDr_database_separator());
                        }
                        sbVal.append(entry.getValue()).append(fileDef.getDr_database_separator());
                    }
                    valueList.add(sbVal);
                    i++;
                }
            }
            if (IsFlag.ofEnumByCode(fileDef.getIs_header()) == IsFlag.Shi) {
                writeData(bufferedWriter, fileDef, sbCol);
            }
            writeData(bufferedWriter, fileDef, valueList);
            bufferedWriter.flush();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeData(BufferedWriter bufferedWriter, DrFileDef fileDef, StringBuffer sb) throws IOException {
        sb.delete(sb.length() - fileDef.getDr_database_separator().length(), sb.length());
        if (fileDef.getDr_row_separator().equals("\\r\\n") || fileDef.getDr_row_separator().equals("\\r") || fileDef.getDr_row_separator().equals("\\n")) {
            fileDef.setDr_row_separator(System.lineSeparator());
        }
        sb.append(fileDef.getDr_row_separator());
        bufferedWriter.write(sb.toString());
    }

    private void writeData(BufferedWriter bufferedWriter, DrFileDef fileDef, List<StringBuffer> values) throws IOException {
        List<StringBuffer> dataCollect = values.stream().map(value -> {
            return value.delete(value.length() - fileDef.getDr_database_separator().length(), value.length());
        }).collect(Collectors.toList());
        if (fileDef.getDr_row_separator().equals("\\r\\n") || fileDef.getDr_row_separator().equals("\\r") || fileDef.getDr_row_separator().equals("\\n")) {
            fileDef.setDr_row_separator(System.lineSeparator());
        }
        StringJoiner stringJoiner = new StringJoiner(fileDef.getDr_row_separator());
        for (StringBuffer sb : dataCollect) {
            stringJoiner.add(sb);
        }
        ;
        bufferedWriter.write(stringJoiner.toString());
    }

    private List<DrParamsDef> replaceUrlParam(String urlParamValue, List<DrParamsDef> paramsDefs) {
        if (urlParamValue != null && urlParamValue.length() > 0) {
            List<String> split = StringUtil.split(urlParamValue, Constant.SQLDELIMITER);
            for (String str : split) {
                List<String> key_value = StringUtil.split(str, "=");
                String placeHolder = "#{" + key_value.get(0) + "}";
                String value = key_value.get(1);
                for (DrParamsDef drParamsDef : paramsDefs) {
                    if (drParamsDef.getParams_value().equals(placeHolder)) {
                        drParamsDef.setParams_value(value);
                    }
                }
            }
        }
        return paramsDefs;
    }

    private Object dataReceive(DrTask task, List<DrParamsDef> paramsDefs) {
        RequestMethod requestMethod = RequestMethod.ofEnumByCode(task.getDr_request_method());
        if (requestMethod == RequestMethod.POST) {
            try {
                HttpClient httpClient = new HttpClient();
                if (paramsDefs != null && !paramsDefs.isEmpty()) {
                    for (DrParamsDef param : paramsDefs) {
                        httpClient.addData(param.getParam_key(), param.getParams_value());
                    }
                }
                HttpClient.ResponseValue responseValue = httpClient.post(task.getDr_url());
                ActionResult actionResult = ActionResult.toActionResult(responseValue.getBodyString());
                if (!actionResult.isSuccess()) {
                    throw new BusinessException("获取样例数据失败！" + actionResult.getMessage());
                }
                log.info("返回的json数据：{}", actionResult.getData());
                return actionResult.getData();
            } catch (Exception e) {
                throw new BusinessException("获取样例数据失败！" + e);
            }
        } else if (requestMethod == RequestMethod.GET) {
            String url = task.getDr_url();
            if (paramsDefs != null && !paramsDefs.isEmpty()) {
                List<String> paramList = paramsDefs.stream().map(param -> {
                    String paramStr = param.getParam_key() + "=" + param.getParams_value();
                    return paramStr;
                }).collect(Collectors.toList());
                StringJoiner stringJoiner = new StringJoiner("&");
                for (String val : paramList) {
                    stringJoiner.add(val);
                }
                url = url + "?" + stringJoiner.toString();
            }
            try {
                HttpClient.ResponseValue responseValue = new HttpClient().get(url);
                ActionResult ar = ActionResult.toActionResult(responseValue.getBodyString());
                if (!ar.isSuccess()) {
                    throw new BusinessException("获取响应数据失败！" + ar.getMessage());
                }
                log.info("返回的json数据：{}", ar.getData());
                return ar.getData();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new Object();
    }

    private void checkTask(DrTask task) {
        Validator.notBlank(task.getDr_task_name(), "接收数据任务名称不能为空");
        Validator.notBlank(task.getDr_format(), "接收数据输入格式不能为空");
        Validator.notBlank(task.getDr_request_method(), "请求方式不能为空");
        Validator.notBlank(task.getDr_url(), "请求URL不能为空");
    }

    private void checkFileDef(DrFileDef fileDef) {
        Validator.notBlank(fileDef.getDr_file_name(), "文件名称不能为空");
        Validator.notBlank(fileDef.getDr_plane_url(), "数据落地目录不能为空");
        Validator.notBlank(fileDef.getDr_file_suffix(), "文件后缀不能为空");
        Validator.notBlank(fileDef.getDr_row_separator(), "行分隔符不能为空");
        Validator.notBlank(fileDef.getDr_database_separator(), "列分隔符不能为空");
        Validator.notBlank(fileDef.getDr_database_code(), "数据编码格式不能为空");
        Validator.notBlank(fileDef.getDbfile_format(), "文件格式不能为空");
        Validator.notBlank(fileDef.getIs_header(), "文件是否需要表头不能为空");
    }

    private List<Object> parseObject(Object data) {
        List<TreeNode> treeNodeList = parseJsonToTree(data);
        MenuTree menuTree = new MenuTree(treeNodeList);
        List<Object> treeMenu = menuTree.buildTree();
        return treeMenu;
    }

    public List<TreeNode> parseJsonToTree(Object object) {
        List<TreeNode> treeNodes = new ArrayList<TreeNode>();
        if (object instanceof Map) {
            List<TreeNode> treeNodes1 = parseMap(object);
            treeNodes.addAll(treeNodes1);
        } else if (object instanceof List) {
            List<Object> list = JsonUtil.toObject(JsonUtil.toJson(object), new TypeReference<List<Object>>() {
            });
            if (!list.isEmpty() && list != null) {
                Object o = list.get(0);
                if (o instanceof Map) {
                    List<TreeNode> treeNodes1 = parseMap(o);
                    treeNodes.addAll(treeNodes1);
                } else if (o instanceof List) {
                    List<TreeNode> treeNodes1 = parsePList(o);
                    treeNodes.addAll(treeNodes1);
                }
            }
        } else {
            log.info("未知的数据结构，无法解析:{}", object);
        }
        return treeNodes;
    }

    public List<TreeNode> parseMap(Object object) {
        List<TreeNode> treeNodes = new ArrayList<>();
        Map<String, Object> jsonMap = JsonUtil.toObject(JsonUtil.toJson(object), new TypeReference<Map<String, Object>>() {
        });
        Iterator<String> iterator = jsonMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            TreeNode treeNode = new TreeNode();
            treeNode.setId(PrimayKeyGener.getNextId());
            treeNode.setPId(0L);
            treeNode.setLabel(key);
            treeNodes.add(treeNode);
            List<TreeNode> treeNodes1 = parseChildrenNodeMap(treeNode, jsonMap.get(key));
            treeNodes.addAll(treeNodes1);
        }
        return treeNodes;
    }

    public List<TreeNode> parsePList(Object o) {
        List<TreeNode> treeNodes = new ArrayList<TreeNode>();
        if (o instanceof List) {
            List<Object> list = JsonUtil.toObject(JsonUtil.toJson(o), new TypeReference<List<Object>>() {
            });
            if (list != null && !list.isEmpty()) {
                Object obj = list.get(0);
                if (obj instanceof Map) {
                    List<TreeNode> treeNodeList = parseMap(obj);
                    treeNodes.addAll(treeNodeList);
                } else if (obj instanceof List) {
                    List<TreeNode> treeNodeslist = parsePList(obj);
                    treeNodes.addAll(treeNodeslist);
                }
            }
        } else if (o instanceof Map) {
            Map<String, Object> map = JsonUtil.toObject(JsonUtil.toJson(o), new TypeReference<Map<String, Object>>() {
            });
            Iterator<String> iterator = map.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                TreeNode treeNode = new TreeNode();
                treeNode.setId(PrimayKeyGener.getNextId());
                treeNode.setPId(0L);
                treeNode.setLabel(key);
                treeNodes.add(treeNode);
                List<TreeNode> treeNodes1 = parseChildrenNodeMap(treeNode, map.get(key));
                treeNodes.addAll(treeNodes1);
            }
        }
        return treeNodes;
    }

    private List<TreeNode> parseChildrenNodeMap(TreeNode treeNode, Object object) {
        List<TreeNode> nodes = new ArrayList<>();
        if (object instanceof Map) {
            Map<String, Object> map = JsonUtil.toObject(JsonUtil.toJson(object), new TypeReference<Map<String, Object>>() {
            });
            if (map != null && !map.isEmpty()) {
                Iterator<String> iterator = map.keySet().iterator();
                while (iterator.hasNext()) {
                    TreeNode treeNode1 = new TreeNode();
                    String key = iterator.next();
                    treeNode1.setId(PrimayKeyGener.getNextId());
                    treeNode1.setPId(treeNode.getId());
                    treeNode1.setLabel(key);
                    nodes.add(treeNode1);
                    List<TreeNode> treeNodes = parseChildrenNodeMap(treeNode1, map.get(key));
                    nodes.addAll(treeNodes);
                }
            }
        } else if (object instanceof List) {
            List<Object> list = JsonUtil.toObject(JsonUtil.toJson(object), new TypeReference<List<Object>>() {
            });
            if (list != null && !list.isEmpty()) {
                Object obj = list.get(0);
                if (obj instanceof Map) {
                    Map<String, Object> map = JsonUtil.toObject(JsonUtil.toJson(obj), new TypeReference<Map<String, Object>>() {
                    });
                    if (map != null && !map.isEmpty()) {
                        Iterator<String> iterator = map.keySet().iterator();
                        while (iterator.hasNext()) {
                            TreeNode treeNode1 = new TreeNode();
                            String key = iterator.next();
                            treeNode1.setId(PrimayKeyGener.getNextId());
                            treeNode1.setPId(treeNode.getId());
                            treeNode1.setLabel(key);
                            nodes.add(treeNode1);
                            List<TreeNode> treeNodeList = parseChildrenNodeMap(treeNode1, map.get(key));
                            nodes.addAll(treeNodeList);
                        }
                    }
                } else if (obj instanceof List) {
                    List<TreeNode> treeNodes = parseChildrenNodeMap(treeNode, obj);
                    nodes.addAll(treeNodes);
                }
            }
        }
        return nodes;
    }

    @Param(name = "taskIds", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getIsReleaseData(Long[] taskIds, int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT dr_task_id,dr_task_name,dr_format,dr_request_method,dr_url," + "concat(dr_task_name,'_',dr_task_id) AS etl_job,dr_remark FROM " + DrTask.TableName);
        asmSql.addORParam("dr_task_id", taskIds);
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> data_receive = new HashMap<>();
        List<Map<String, Object>> receiveInfo = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        data_receive.put("receiveInfo", receiveInfo);
        data_receive.put("totalSize", page.getTotalSize());
        String path = PropertyParaValue.getString("agentpath", "");
        data_receive.put("sysDir", StringUtil.isBlank(path) ? System.getProperty("user.dir") : new File(path).getParent());
        return data_receive;
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "distributeJobBean", value = "", dataTypeClass = DistributeJobBean.class)
    public void saveDataReceiveJobRelation(List<List<String>> pre_etl_job_ids, List<Map<String, String>> task_ids, List<EtlJobDef> relation) {
        List<List<Long>> preEtJobIdList = new ArrayList<>();
        if (pre_etl_job_ids != null) {
            for (List<String> strings : pre_etl_job_ids) {
                List<Long> ids = new ArrayList<>();
                for (String s : strings) {
                    Map<String, Object> id = Dbo.queryOneObject("select etl_job_id from " + EtlJobDef.TableName + " where etl_job=?", s);
                    if (id.get("etl_job_id") != null) {
                        ids.add(Long.parseLong(id.get("etl_job_id").toString()));
                    }
                }
                preEtJobIdList.add(ids);
            }
        }
        for (EtlJobDef etlJobDef : relation) {
            if (etlJobDef.getEtl_sys_id() == null || etlJobDef.getEtl_sys_id().toString().isEmpty()) {
                etlJobDef.setEtl_sys_id(PrimayKeyGener.getNextId());
            }
            if (etlJobDef.getEtl_job_id() == null || etlJobDef.getEtl_job_id().toString().isEmpty()) {
                etlJobDef.setEtl_job_id(PrimayKeyGener.getNextId());
            }
        }
        saveDataReceiveJob(relation, task_ids, preEtJobIdList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "relation", desc = "", range = "")
    @Param(name = "task_ids", desc = "", range = "")
    @Param(name = "pre_etl_job_ids", desc = "", range = "", nullable = true)
    private void saveDataReceiveJob(List<EtlJobDef> relation, List<Map<String, String>> task_ids, List<List<Long>> pre_etl_job_ids) {
        for (int i = 0; i < relation.size(); i++) {
            EtlJobDef etlJobDef = new EtlJobDef();
            BeanUtil.copyProperties(relation.get(i), etlJobDef);
            etlJobDef.setPro_type(Pro_Type.SHELL.getCode());
            etlJobDef.setPro_name(Constant.RECEIVE_JOB_COMMAND);
            etlJobDef.setJob_datasource(ETLDataSource.Other.getCode());
            etlJobDef.setPro_para(task_ids.get(i).get("task_id") + Constant.ETLPARASEPARATOR + "#{txdate}");
            etlJobDef.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
            etlJobDef.setToday_disp(Today_Dispatch_Flag.YES.getCode());
            etlJobDef.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
            Optional<EtlJobDef> etl_job_def = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_job_id = ? ", relation.get(i).getEtl_job_id());
            if (etl_job_def.isPresent()) {
                EtlJobDef jobDef = etl_job_def.get();
                int job_ret = Dbo.execute("delete from " + EtlJobDef.TableName + " where etl_job_id = ? and etl_sys_id = ?", jobDef.getEtl_job_id(), jobDef.getEtl_sys_id());
                if (job_ret < 1) {
                    throw new BusinessException("删除作业失败!");
                }
                long num = Dbo.queryNumber("select count(1) from " + TakeRelationEtl.TableName + " where etl_job_id = ?", jobDef.getEtl_job_id()).orElseThrow(() -> new BusinessException("sql查询任务作业关系表信息错误"));
                if (num > 0) {
                    int execute = Dbo.execute("delete from " + TakeRelationEtl.TableName + " where etl_job_id = ?", jobDef.getEtl_job_id());
                    if (execute < 1) {
                        throw new BusinessException("删除作业分发数据关系表失败!");
                    }
                }
                long count = Dbo.queryNumber("select count(pre_etl_job_id) from " + EtlDependency.TableName + " where etl_job_id = ? and etl_sys_id = ?", jobDef.getEtl_job_id(), jobDef.getEtl_sys_id()).orElseThrow(() -> new BusinessException("sql查询错误"));
                if (count > 0) {
                    int ret = Dbo.execute("delete from " + EtlDependency.TableName + " where etl_job_id = ? and etl_sys_id = ?", jobDef.getEtl_job_id(), jobDef.getEtl_sys_id());
                    if (ret < 1) {
                        throw new BusinessException("删除上游依赖关系失败!");
                    }
                }
            }
            etlJobDef.add(Dbo.db());
            EtlResource etl_resource = new EtlResource();
            etl_resource.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            etl_resource.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etl_resource.setResource_max(RESOURCE_MAX);
            etl_resource.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            if (Dbo.queryNumber("select count(*) from " + EtlResource.TableName + " where etl_sys_id = ? and resource_type = ?", etl_resource.getEtl_sys_id(), etl_resource.getResource_type()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etl_resource.add(Dbo.db());
            }
            EtlJobResourceRela etlJobResourceRela = new EtlJobResourceRela();
            etlJobResourceRela.setEtl_job_id(etlJobDef.getEtl_job_id());
            etlJobResourceRela.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            etlJobResourceRela.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etlJobResourceRela.setResource_req(1);
            if (Dbo.queryNumber("select count(*) from " + EtlJobResourceRela.TableName + " where etl_job_id = ? and resource_type = ?", etlJobResourceRela.getEtl_job_id(), etlJobResourceRela.getResource_type()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etlJobResourceRela.add(Dbo.db());
            }
            TakeRelationEtl takeRelationEtl = new TakeRelationEtl();
            takeRelationEtl.setTre_id(PrimayKeyGener.getNextId());
            takeRelationEtl.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            takeRelationEtl.setSub_sys_id(etlJobDef.getSub_sys_id());
            takeRelationEtl.setEtl_job_id(etlJobDef.getEtl_job_id());
            takeRelationEtl.setTake_id(task_ids.get(i).get("task_id"));
            takeRelationEtl.setJob_datasource(ETLDataSource.Other.getCode());
            takeRelationEtl.setTake_source_table(DrFileDef.TableName);
            takeRelationEtl.add(Dbo.db());
            EtlDependency etl_dependency = new EtlDependency();
            etl_dependency.setEtl_job_id(etlJobDef.getEtl_job_id());
            etl_dependency.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            etl_dependency.setStatus(Status.TRUE.getCode());
            etl_dependency.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            etl_dependency.setPre_etl_sys_id(etlJobDef.getEtl_sys_id());
            if (Dispatch_Type.DEPENDENCE == Dispatch_Type.ofEnumByCode(etlJobDef.getDisp_type()) && (!CollectionUtils.isEmpty(pre_etl_job_ids))) {
                for (Long pre_etl_job_id : pre_etl_job_ids.get(i)) {
                    etl_dependency.setPre_etl_job_id(pre_etl_job_id);
                    etl_dependency.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dd_ids", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<List<Map<String, Object>>> getJobMsg(Long[] task_ids) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT concat (dr_task_name , '_' ,dr_task_id) AS etl_job,dr_task_id FROM " + DrTask.TableName);
        asmSql.addORParam("dr_task_id", task_ids);
        List<Map<String, Object>> mapList = Dbo.queryList(asmSql.sql(), asmSql.params());
        List<List<Map<String, Object>>> objMapList = new ArrayList<>();
        for (Map<String, Object> map : mapList) {
            String etl_job = map.get("etl_job").toString();
            Map<String, Object> dd_msg = new HashMap<>();
            List<Map<String, Object>> jobList = Dbo.queryList("select * from " + EtlJobDef.TableName + " where etl_job = ?", etl_job);
            Result result = Dbo.queryResult(" select etl_job from " + EtlJobDef.TableName + "  where etl_job_id in (select pre_etl_job_id from " + EtlJobDef.TableName + " t1 join " + EtlDependency.TableName + " t2 on t1.etl_job_id=t2.etl_job_id where etl_job=? ) ", etl_job);
            if (!result.isEmpty()) {
                Map<String, Object> msgJob = new HashMap<>();
                List<String> msgPre = new ArrayList<>();
                for (int i = 0; i < result.getRowCount(); i++) {
                    msgPre.add(i, result.getString(i, "etl_job"));
                }
                msgJob.put("pre_etl_job", msgPre);
                jobList.add(msgJob);
            }
            dd_msg.put("dr_task_id", map.get("dr_task_id").toString());
            jobList.add(dd_msg);
            objMapList.add(jobList);
        }
        return objMapList;
    }
}
