package hyren.serv6.b.batchcollection.semiStructuredAgent.collectfileconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.tools.CommonUtils;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.ObjectCollect;
import hyren.serv6.base.entity.ObjectCollectStruct;
import hyren.serv6.base.entity.ObjectCollectTask;
import hyren.serv6.base.entity.ObjectHandleType;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@DocClass(desc = "", author = "dhw", createdate = "2020/6/10 14:29")
public class CollectFileConfService {

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchObjectCollectTask(long odc_id) {
        CommonUtils.isObjectCollectExist(odc_id);
        ObjectCollect object_collect = getObjectCollect(odc_id);
        object_collect.setOdc_id(odc_id);
        List<ObjectCollectTask> tableInfo = getTableInfo(object_collect);
        Map<String, Object> tableMap = new HashMap<>();
        tableMap.put("is_dictionary", object_collect.getIs_dictionary());
        tableMap.put("tableInfo", tableInfo);
        return tableMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private ObjectCollect getObjectCollect(long odc_id) {
        return Dbo.queryOneObject(ObjectCollect.class, "select * from " + ObjectCollect.TableName + " where odc_id=?", odc_id).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object_collect", desc = "", range = "", isBean = true)
    private List<ObjectCollectTask> getTableInfo(ObjectCollect object_collect) {
        List<ObjectCollectTask> dicTableList = getDictionaryTableInfo(object_collect);
        List<ObjectCollectTask> dataBaseList = getObjectCollectTaskList(object_collect.getOdc_id());
        if (dataBaseList.isEmpty()) {
            addDicTable(object_collect.getOdc_id(), object_collect.getAgent_id(), dicTableList);
            return dicTableList;
        }
        addObjectCollectTask(object_collect.getOdc_id(), object_collect.getAgent_id(), dicTableList);
        dicTableList.addAll(dataBaseList);
        return dicTableList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "dicTableList", desc = "", range = "")
    @Return(desc = "", range = "")
    private void addObjectCollectTask(long odc_id, long agent_id, List<ObjectCollectTask> dicTableList) {
        List<ObjectCollectTask> objCollectTaskList = getObjectCollectTaskList(odc_id);
        List<String> dicTableNameList = getTableName(dicTableList);
        List<String> tableNameList = getTableName(objCollectTaskList);
        List<String> deleteList = tableNameList.stream().filter(item -> !dicTableNameList.contains(item)).collect(Collectors.toList());
        deleteTable(odc_id, deleteList);
        for (ObjectCollectTask object_collect_task : objCollectTaskList) {
            for (ObjectCollectTask object_collect_task2 : dicTableList) {
                if (object_collect_task.getEn_name().equals(object_collect_task2.getEn_name())) {
                    object_collect_task.setFirstline(object_collect_task2.getFirstline());
                    object_collect_task.update(Dbo.db());
                }
            }
        }
        List<String> addList = dicTableNameList.stream().filter(item -> !tableNameList.contains(item)).collect(Collectors.toList());
        dicTableList.removeIf(object_collect_task -> !addList.contains(object_collect_task.getEn_name()));
        addDicTable(odc_id, agent_id, dicTableList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<ObjectCollectTask> getObjectCollectTaskList(long odc_id) {
        return Dbo.queryList(ObjectCollectTask.class, "select * from " + ObjectCollectTask.TableName + " where odc_id =?", odc_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object_collect", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    private List<ObjectCollectTask> getDictionaryTableInfo(ObjectCollect object_collect) {
        if (IsFlag.Shi == IsFlag.ofEnumByCode(object_collect.getIs_dictionary())) {
            return SendMsgUtil.getDictionaryTableInfo(object_collect.getAgent_id(), object_collect.getFile_path(), UserUtil.getUserId());
        } else {
            return SendMsgUtil.getFirstLineData(object_collect.getAgent_id(), object_collect.getFile_path(), object_collect.getData_date(), object_collect.getFile_suffix(), UserUtil.getUserId());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableBeanList", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<String> getTableName(List<ObjectCollectTask> tableBeanList) {
        List<String> tableNameList = new ArrayList<>();
        tableBeanList.forEach(object_collect_task -> tableNameList.add(object_collect_task.getEn_name()));
        return tableNameList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnBeanList", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<String> getColumnName(List<ObjectCollectStruct> columnBeanList) {
        List<String> columnNameList = new ArrayList<>();
        columnBeanList.forEach(object_collect_struct -> columnNameList.add(object_collect_struct.getColumn_name()));
        return columnNameList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object_collect", desc = "", range = "", isBean = true)
    @Param(name = "dicTableList", desc = "", range = "")
    private void addDicTable(long odc_id, long agent_id, List<ObjectCollectTask> dicTableList) {
        if (!dicTableList.isEmpty()) {
            for (ObjectCollectTask object_collect_task : dicTableList) {
                object_collect_task.setOcs_id(PrimayKeyGener.getNextId());
                object_collect_task.setDatabase_code(DataBaseCode.UTF_8.getCode());
                object_collect_task.setUpdatetype(StringUtil.isBlank(object_collect_task.getUpdatetype()) ? UpdateType.DirectUpdate.getCode() : object_collect_task.getUpdatetype());
                object_collect_task.setOdc_id(odc_id);
                object_collect_task.setAgent_id(agent_id);
                object_collect_task.setCollect_data_type(CollectDataType.JSON.getCode());
                object_collect_task.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "deleteNameList", desc = "", range = "")
    private void deleteTable(long odc_id, List<String> deleteNameList) {
        if (!deleteNameList.isEmpty()) {
            SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
            assembler.addSql("select ocs_id from " + ObjectCollectTask.TableName + " t1 join " + ObjectCollect.TableName + " t2 on t1.odc_id=t2.odc_id" + " where t2.odc_id=? ").addParam(odc_id).addORParam("t1.en_name", deleteNameList.toArray());
            List<Long> ocsIdList = Dbo.queryOneColumnList(assembler.sql(), assembler.params());
            for (Long ocs_id : ocsIdList) {
                Dbo.execute("delete from " + ObjectCollectStruct.TableName + " where ocs_id=?", ocs_id);
                Dbo.execute("delete from " + ObjectHandleType.TableName + " where ocs_id =?", ocs_id);
                DboExecute.deletesOrThrow("删除表失败", "delete from " + ObjectCollectTask.TableName + " where ocs_id=?", ocs_id);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "en_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<ObjectCollectStruct> getObjectCollectStruct(long odc_id, long ocs_id, String en_name) {
        CommonUtils.isObjectCollectExist(odc_id);
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        ObjectCollect object_collect = getObjectCollect(odc_id);
        if (IsFlag.Fou == IsFlag.ofEnumByCode(object_collect.getIs_dictionary())) {
            throw new BusinessException("该采集任务的是否数据字典应为是,实际为否，请检查");
        }
        List<ObjectCollectStruct> dicColumnByTable = getDicColumnsByTableName(object_collect, en_name);
        List<ObjectCollectStruct> objectCollectStructList = getObjectCollectStructById(ocs_id);
        if (objectCollectStructList.isEmpty()) {
            addColumns(ocs_id, dicColumnByTable);
            return dicColumnByTable;
        }
        addOrDeleteColumns(ocs_id, dicColumnByTable, objectCollectStructList);
        dicColumnByTable.addAll(objectCollectStructList);
        return dicColumnByTable;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "dicColumnByTable", desc = "", range = "")
    @Param(name = "objectCollectStructList", desc = "", range = "")
    private void addOrDeleteColumns(long ocs_id, List<ObjectCollectStruct> dicColumnByTable, List<ObjectCollectStruct> objectCollectStructList) {
        List<String> dicColumnNameList = getColumnName(dicColumnByTable);
        List<String> columnNameList = getColumnName(objectCollectStructList);
        List<String> deleteList = columnNameList.stream().filter(item -> !dicColumnNameList.contains(item)).collect(Collectors.toList());
        objectCollectStructList.removeIf(object_collect_struct -> deleteList.contains(object_collect_struct.getColumn_name()));
        List<String> addList = dicColumnNameList.stream().filter(item -> !columnNameList.contains(item)).collect(Collectors.toList());
        dicColumnByTable.removeIf(object_collect_struct -> !addList.contains(object_collect_struct.getColumn_name()));
        deleteColumns(ocs_id, deleteList);
        addColumns(ocs_id, dicColumnByTable);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getFirstLineTreeInfo(long odc_id, long ocs_id) {
        CommonUtils.isObjectCollectExist(odc_id);
        String firstLine = getFirstLineData(ocs_id);
        Validator.notBlank(firstLine, "数据字典不存在时第一行数据不能为空");
        return parseFirstLine(firstLine, "");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private String getFirstLineData(long ocs_id) {
        ObjectCollectTask object_collect_task = Dbo.queryOneObject(ObjectCollectTask.class, "select firstline from " + ObjectCollectTask.TableName + " where ocs_id=?", ocs_id).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
        return object_collect_task.getFirstline();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<ObjectCollectStruct> getObjectCollectStructById(long ocs_id) {
        return Dbo.queryList(ObjectCollectStruct.class, "select * from " + ObjectCollectStruct.TableName + " where ocs_id=?", ocs_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object_collect", desc = "", range = "", isBean = true)
    @Param(name = "en_name", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<ObjectCollectStruct> getDicColumnsByTableName(ObjectCollect object_collect, String en_name) {
        Validator.notBlank(object_collect.getFile_path(), "采集文件路径不能为空");
        Map<String, List<ObjectCollectStruct>> allDicColumns = SendMsgUtil.getAllDicColumns(object_collect.getAgent_id(), object_collect.getFile_path(), UserUtil.getUserId());
        if (allDicColumns == null || allDicColumns.isEmpty()) {
            throw new BusinessException("数据字典中未找到表对应列信息");
        }
        if (!allDicColumns.containsKey(en_name)) {
            throw new BusinessException("当前表名" + en_name + "对应的列信息不存在，请检查表名是否正确");
        }
        return allDicColumns.get(en_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private ObjectCollectTask getCollectTask(long ocs_id) {
        return Dbo.queryOneObject(ObjectCollectTask.class, "select * from " + ObjectCollectTask.TableName + " where ocs_id=?", ocs_id).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "addList", desc = "", range = "")
    private void addColumns(long ocs_id, List<ObjectCollectStruct> addList) {
        if (!addList.isEmpty()) {
            addList.forEach(object_collect_struct -> {
                object_collect_struct.setStruct_id(PrimayKeyGener.getNextId());
                if (StringUtil.isBlank(object_collect_struct.getData_desc())) {
                    object_collect_struct.setData_desc(object_collect_struct.getColumn_name());
                }
                object_collect_struct.setOcs_id(ocs_id);
                checkObjectCollectStructParam(object_collect_struct);
                object_collect_struct.add(Dbo.db());
            });
        }
    }

    private void checkObjectCollectStructParam(ObjectCollectStruct objectCollectStruct) {
        try {
            Validator.notNull(objectCollectStruct.getIs_zipper_field(), "object_collect_struct.is_zipper_field is not null.");
            Validator.notNull(objectCollectStruct.getIs_operate(), "object_collect_struct.is_operate is not null.");
            Validator.notNull(objectCollectStruct.getColumn_name(), "object_collect_struct.column_name is not null.");
            Validator.notNull(objectCollectStruct.getOcs_id(), "object_collect_struct.ocs_id is not null.");
            Validator.notNull(objectCollectStruct.getColumnposition(), "object_collect_struct.columnposition is not null.");
            Validator.notNull(objectCollectStruct.getColumn_type(), "object_collect_struct.column_type is not null.");
            Validator.notNull(objectCollectStruct.getStruct_id(), "object_collect_struct.struct_id is not null.");
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "en_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<ObjectHandleType> searchObjectHandleType(long odc_id, String en_name) {
        ObjectCollect object_collect = getObjectCollect(odc_id);
        if (IsFlag.Fou == IsFlag.ofEnumByCode(object_collect.getIs_dictionary())) {
            throw new BusinessException("该采集任务的是否数据字典应为是,实际为否，请检查");
        }
        return getObjectHandleTypeList(object_collect, en_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object_collect", desc = "", range = "", isBean = true)
    @Param(name = "en_name", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<ObjectHandleType> getObjectHandleTypeList(ObjectCollect object_collect, String en_name) {
        Validator.notBlank(object_collect.getFile_path(), "采集文件路径不能为空");
        Validator.notNull(object_collect.getAgent_id(), "agent ID不能为空");
        Map<String, List<ObjectHandleType>> allHandleType = SendMsgUtil.getAllHandleType(object_collect.getAgent_id(), object_collect.getFile_path(), UserUtil.getUserId());
        if (allHandleType == null || allHandleType.isEmpty()) {
            throw new BusinessException("数据字典存在时，处理方式不能为空，请检查数据字典");
        }
        if (!allHandleType.containsKey(en_name)) {
            throw new BusinessException("当前表" + en_name + "对应处理方式不存在，请检查表名是否正确");
        }
        return allHandleType.get(en_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "objectHandleTypes", desc = "", range = "", isBean = true)
    public void saveObjectHandleType(long ocs_id, ObjectHandleType[] objectHandleTypes) {
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        for (ObjectHandleType objectHandleType : objectHandleTypes) {
            if (objectHandleType.getObject_handle_id() != null) {
                try {
                    objectHandleType.update(Dbo.db());
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            } else {
                objectHandleType.setObject_handle_id(PrimayKeyGener.getNextId());
                objectHandleType.setOcs_id(ocs_id);
                objectHandleType.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getObjectHandleType(long ocs_id) {
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        return Dbo.queryResult("select * from " + ObjectHandleType.TableName + " where ocs_id=?", ocs_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "firstLine", desc = "", range = "")
    @Param(name = "location", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> parseFirstLine(String firstLine, String location) {
        List<Map<String, Object>> treeInfo;
        List<String> treeId = StringUtil.split(location, ",");
        try {
            List<Map<String, Object>> parseArray = JsonUtil.toObject(firstLine, new TypeReference<List<Map<String, Object>>>() {
            });
            Object everyObject = parseArray.get(0);
            if (everyObject instanceof List) {
                List<Map<String, Object>> jsonarray = JsonUtil.toObject(JsonUtil.toJson(everyObject), new TypeReference<List<Map<String, Object>>>() {
                });
                everyObject = jsonarray.get(0);
            }
            if (everyObject instanceof Map) {
                Map<String, Object> jsonobject = JsonUtil.toObject(JsonUtil.toJson(everyObject), new TypeReference<Map<String, Object>>() {
                });
                if (StringUtil.isNotBlank(location)) {
                    for (int i = 0; i < treeId.size(); i++) {
                        jsonobject = makeJsonFileToJsonObj(jsonobject, treeId.get(i));
                    }
                }
                treeInfo = getTree(jsonobject, location);
            } else {
                throw new BusinessException("解析json结构错误 jsonArray下面不存在jsonObject");
            }
        } catch (Exception e) {
            try {
                Map<String, Object> parseObject;
                try {
                    parseObject = JsonUtil.toObject(JsonUtil.toJson(firstLine), new TypeReference<Map<String, Object>>() {
                    });
                } catch (Exception e1) {
                    parseObject = JsonUtil.toObject(firstLine, new TypeReference<Map<String, Object>>() {
                    });
                }
                if (StringUtil.isNotBlank(location)) {
                    for (int i = 0; i < treeId.size(); i++) {
                        parseObject = makeJsonFileToJsonObj(parseObject, treeId.get(i));
                    }
                }
                treeInfo = getTree(parseObject, location);
            } catch (Exception e2) {
                throw new BusinessException("既不是jsonArray，也不是jsonObject");
            }
        }
        return treeInfo;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jsonObject", desc = "", range = "")
    @Param(name = "nextKey", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> makeJsonFileToJsonObj(Map<String, Object> jsonObject, String nextKey) {
        Object object = jsonObject.get(nextKey);
        Map<String, Object> jsonobject;
        if (object instanceof List) {
            List<Map<String, Object>> jsonarray = JsonUtil.toObject(JsonUtil.toJson(object), new TypeReference<List<Map<String, Object>>>() {
            });
            object = jsonarray.get(0);
            if (object instanceof Map) {
                jsonobject = (Map<String, Object>) object;
            } else {
                throw new BusinessException("解析json结构错误 jsonArray下面不存在jsonObject");
            }
        } else if (object instanceof Map) {
            jsonobject = (Map<String, Object>) object;
        } else {
            throw new BusinessException("json格式错误，既不是jsonArray也不是jsonObject");
        }
        return jsonobject;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "location", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getObjectCollectTreeInfo(long odc_id, long ocs_id, String location) {
        CommonUtils.isObjectCollectExist(odc_id);
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        String firstLine = getFirstLineData(ocs_id);
        return parseFirstLine(firstLine, location);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jsonObject", desc = "", range = "")
    @Param(name = "keys", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> getTree(Map<String, Object> jsonObject, String keys) {
        if (StringUtil.isBlank(keys)) {
            keys = "";
        } else {
            keys += ",";
        }
        List<Map<String, Object>> array = new ArrayList<>();
        Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();
        int rowcount = 0;
        for (Map.Entry<String, Object> entry : entrySet) {
            Map<String, Object> resultObject = new HashMap<>();
            String key = entry.getKey();
            Object object = jsonObject.get(key);
            boolean isParent;
            isParent = object instanceof Map || object instanceof List;
            resultObject.put("location", keys + key);
            resultObject.put("description", key);
            resultObject.put("id", key);
            resultObject.put("isParent", isParent);
            resultObject.put("name", key);
            resultObject.put("pId", "~" + rowcount);
            resultObject.put("rootName", "~" + rowcount);
            array.add(resultObject);
            rowcount++;
        }
        return array;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "objectCollectStructs", desc = "", range = "", isBean = true)
    public void saveObjectCollectStruct(long odc_id, long ocs_id, ObjectCollectStruct[] objectCollectStructs) {
        CommonUtils.isObjectCollectExist(odc_id);
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        List<ObjectCollectStruct> objectCollectStructList = new ArrayList<>(objectCollectStructs.length);
        Collections.addAll(objectCollectStructList, objectCollectStructs);
        objectCollectStructList = objectCollectStructList.stream().filter(object_collect_struct -> IsFlag.Shi == IsFlag.ofEnumByCode(object_collect_struct.getIs_operate())).collect(Collectors.toList());
        if (objectCollectStructList.size() != 1) {
            throw new BusinessException("操作字段只能为1个，请检查");
        }
        Dbo.execute("delete from " + ObjectCollectStruct.TableName + " where ocs_id=?", ocs_id);
        for (ObjectCollectStruct object_collect_struct : objectCollectStructs) {
            object_collect_struct.setStruct_id(PrimayKeyGener.getNextId());
            object_collect_struct.setData_desc(StringUtil.isBlank(object_collect_struct.getData_desc()) ? object_collect_struct.getColumn_name() : object_collect_struct.getData_desc());
            object_collect_struct.setOcs_id(ocs_id);
            object_collect_struct.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private ObjectCollectTask getObjectCollectTask(long ocs_id) {
        return Dbo.queryOneObject(ObjectCollectTask.class, "select * from " + ObjectCollectTask.TableName + " where ocs_id=?", ocs_id).orElseThrow(() -> new BusinessException("sql查询错误或实体映射失败"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "deleteNameList", desc = "", range = "")
    private void deleteColumns(long ocs_id, List<String> deleteNameList) {
        if (!deleteNameList.isEmpty()) {
            SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
            assembler.addSql("delete from " + ObjectCollectStruct.TableName + " where ocs_id=? ").addParam(ocs_id).addORParam("column_name", deleteNameList.toArray());
            DboExecute.deletesOrThrow(deleteNameList.size(), "删除对象采集结构信息失败", assembler.sql(), assembler.params());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objectCollectTasks", desc = "", range = "", isBean = true)
    private void checkFieldsForSaveObjectCollectTask(ObjectCollectTask[] objectCollectTasks) {
        for (int i = 0; i < objectCollectTasks.length; i++) {
            Validator.notBlank(objectCollectTasks[i].getEn_name(), "第" + (i + 1) + "行表" + objectCollectTasks[i].getEn_name() + "英文名为空，请检查");
            Validator.notBlank(objectCollectTasks[i].getZh_name(), "第" + (i + 1) + "行表" + objectCollectTasks[i].getZh_name() + "中文名为空，请检查");
            try {
                UpdateType.ofEnumByCode(objectCollectTasks[i].getUpdatetype());
            } catch (Exception e) {
                throw new BusinessException("第" + (i + 1) + "行表" + objectCollectTasks[i].getEn_name() + "更新方式不合法，" + e.getMessage());
            }
            try {
                DataBaseCode.ofEnumByCode(objectCollectTasks[i].getDatabase_code());
            } catch (Exception e) {
                throw new BusinessException("第" + (i + 1) + "行表" + objectCollectTasks[i].getEn_name() + "采集编码不合法，" + e.getMessage());
            }
            try {
                CollectDataType.ofEnumByCode(objectCollectTasks[i].getCollect_data_type());
            } catch (Exception e) {
                throw new BusinessException("第" + (i + 1) + "行表" + objectCollectTasks[i].getEn_name() + "数据类型不合法，" + e.getMessage());
            }
            if (Dbo.queryNumber("select count(1) from " + ObjectCollectStruct.TableName + " where ocs_id=?", objectCollectTasks[i].getOcs_id()).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
                throw new BusinessException("第" + (i + 1) + "行表" + objectCollectTasks[i].getEn_name() + "采集列结构信息不存在");
            }
            if (Dbo.queryNumber("select count(1) from " + ObjectHandleType.TableName + " where ocs_id=?", objectCollectTasks[i].getOcs_id()).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
                throw new BusinessException("第" + (i + 1) + "行表" + objectCollectTasks[i].getEn_name() + "操作码表信息不存在");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    public void rewriteDataDictionary(long odc_id) {
        CommonUtils.isObjectCollectExist(odc_id);
        List<Object> dictionaryList = new ArrayList<>();
        List<Object> isDictionaryList = Dbo.queryOneColumnList("select is_dictionary from " + ObjectCollect.TableName + " where odc_id=?", odc_id);
        if (!isDictionaryList.isEmpty()) {
            if (IsFlag.Shi == IsFlag.ofEnumByCode(isDictionaryList.get(0).toString())) {
                log.info("已经存在数据字典，不需要重写数据字典");
            } else {
                List<ObjectCollectTask> objCollectTaskList = Dbo.queryList(ObjectCollectTask.class, "select * from " + ObjectCollectTask.TableName + " where odc_id=?", odc_id);
                for (ObjectCollectTask objectCollectTask : objCollectTaskList) {
                    Map<String, Object> tableMap = new HashMap<>();
                    tableMap.put("table_name", objectCollectTask.getEn_name());
                    tableMap.put("table_ch_name", objectCollectTask.getZh_name());
                    tableMap.put("updatetype", objectCollectTask.getUpdatetype());
                    List<ObjectCollectStruct> objCollStructList = getObjectCollectStructById(objectCollectTask.getOcs_id());
                    List<Map<String, Object>> columnList = new ArrayList<>();
                    for (ObjectCollectStruct object_collect_struct : objCollStructList) {
                        Map<String, Object> columnMap = new HashMap<>();
                        columnMap.put("column_name", object_collect_struct.getColumn_name());
                        columnMap.put("column_type", object_collect_struct.getColumn_type());
                        columnMap.put("columnposition", object_collect_struct.getColumnposition());
                        columnMap.put("is_operate", object_collect_struct.getIs_operate());
                        columnList.add(columnMap);
                    }
                    tableMap.put("columns", columnList);
                    List<ObjectHandleType> objHandleTypeList = Dbo.queryList(ObjectHandleType.class, "select * from " + ObjectHandleType.TableName + " where ocs_id=?", objectCollectTask.getOcs_id());
                    Map<String, Object> handleTypeMap = new HashMap<>();
                    for (ObjectHandleType object_handle_type : objHandleTypeList) {
                        String handle_type = object_handle_type.getHandle_type();
                        handleTypeMap.put(OperationType.ofValueByCode(handle_type), object_handle_type.getHandle_value());
                    }
                    tableMap.put("handle_type", handleTypeMap);
                    dictionaryList.add(tableMap);
                }
                ObjectCollect object_collect = Dbo.queryOneObject(ObjectCollect.class, "select agent_id,file_path from " + ObjectCollect.TableName + " where odc_id = ?", odc_id).orElseThrow(() -> new BusinessException("sql查询错误！"));
                try {
                    String url = AgentActionUtil.getUrl(object_collect.getAgent_id(), UserUtil.getUserId(), AgentActionUtil.WRITEDICTIONARY);
                    HttpClient.ResponseValue resVal = new HttpClient().addData("file_path", object_collect.getFile_path()).addData("dictionaryParam", PackUtil.packMsg(JsonUtil.toJson(dictionaryList))).post(url);
                    ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
                    if (!ar.isSuccess()) {
                        throw new BusinessException("半结构化采集重写数据字典连接agent服务失败" + ar.getMessage());
                    }
                } catch (Exception e) {
                    throw new BusinessException("与Agent端服务交互异常!!!" + e.getMessage());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objectCollectTasks", desc = "", range = "", isBean = true)
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "odc_id", desc = "", range = "")
    public void saveObjectCollectTask(long agent_id, long odc_id, ObjectCollectTask[] objectCollectTasks) {
        CommonUtils.isAgentExist(agent_id, UserUtil.getUserId());
        CommonUtils.isObjectCollectExist(odc_id);
        checkFieldsForSaveObjectCollectTask(objectCollectTasks);
        for (ObjectCollectTask object_collect_task : objectCollectTasks) {
            try {
                object_collect_task.update(Dbo.db());
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "en_name", desc = "", range = "")
    @Param(name = "ocs_id", desc = "", range = "")
    private void objectCollectTaskIsExist(String en_name, long ocs_id) {
        if (Dbo.queryNumber("SELECT count(1) FROM " + ObjectCollectTask.TableName + " WHERE en_name=? and ocs_id=?", en_name, ocs_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("对象采集对应信息表的英文名称重复");
        }
    }
}
