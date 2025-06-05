package hyren.serv6.agent.job.biz.core.objectstage.service;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.Node;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.ObjectCollectStruct;
import hyren.serv6.base.entity.ObjectHandleType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.constant.Constant;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.IOException;
import java.util.*;

public abstract class ObjectProcessAbstract implements ObjectProcessInterface {

    protected TableBean tableBean;

    protected ObjectTableBean objectTableBean;

    protected List<String> metaTypeList;

    protected List<String> metaColumnList;

    protected List<String> selectColumnList;

    protected Map<String, Boolean> isZipperKeyMap = new HashMap<>();

    private final List<ObjectCollectStruct> object_collect_structList;

    protected final Map<String, String> handleTypeMap;

    protected String operate_column;

    protected String etlDate;

    private final Set<Integer> columnLevel = new HashSet<>();

    protected ObjectProcessAbstract(TableBean tableBean, ObjectTableBean objectTableBean) {
        this.objectTableBean = objectTableBean;
        this.tableBean = tableBean;
        this.operate_column = tableBean.getOperate_column();
        this.etlDate = objectTableBean.getEtlDate();
        this.selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
        this.metaColumnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        this.metaTypeList = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        this.object_collect_structList = objectTableBean.getObject_collect_structList();
        this.handleTypeMap = getHandleTypeMap(objectTableBean.getObject_handle_typeList());
        List<String> primaryKeyList = StringUtil.split(tableBean.getPrimaryKeyInfo(), Constant.METAINFOSPLIT);
        for (int i = 0; i < primaryKeyList.size(); i++) {
            if (IsFlag.Shi.getCode().equals(primaryKeyList.get(i))) {
                this.isZipperKeyMap.put(selectColumnList.get(i), true);
            }
        }
        for (ObjectCollectStruct object_collect_struct : objectTableBean.getObject_collect_structList()) {
            columnLevel.add(object_collect_struct.getColumnposition().split(",").length);
        }
    }

    private Map<String, String> getHandleTypeMap(List<ObjectHandleType> object_handle_typeList) {
        Map<String, String> hMap = new HashMap<>();
        for (ObjectHandleType handle : object_handle_typeList) {
            hMap.put(handle.getHandle_value(), handle.getHandle_type());
        }
        return hMap;
    }

    protected List<Map<String, Object>> getListTiledAttributes(String lineValue, long num) {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            List<Node> nodeList = new ArrayList<>();
            getNodeList(lineValue, nodeList, null, 0);
            for (Node node : nodeList) {
                Map<String, Object> attributes = new HashMap<>();
                getTiledAttributes(node, attributes);
                list.add(attributes);
            }
        } catch (JsonProcessingException e) {
            throw new AppSystemException("半结构化对象采集第" + num + "行数据格式不正确,数据为:" + lineValue);
        } catch (IOException e) {
            throw new AppSystemException(e);
        }
        return list;
    }

    private void getTiledAttributes(Node node, Map<String, Object> attributes) {
        if (node.getParentNode() != null) {
            if (node.getAttributes() != null) {
                attributes.putAll(node.getAttributes());
            }
            getTiledAttributes(node.getParentNode(), attributes);
        } else {
            if (node.getAttributes() != null) {
                attributes.putAll(node.getAttributes());
            }
        }
    }

    private void getNodeList(String lineValue, List<Node> nodeList, Node parentNode, int index) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(lineValue);
        if (jsonNode.isObject()) {
            boolean flag = true;
            Node node = new Node();
            node.setParentNode(parentNode);
            Map<String, Object> jsonMap = JsonUtil.toObject(lineValue, new TypeReference<Map<String, Object>>() {
            });
            List<String> keyList = new ArrayList<>();
            for (String key : jsonMap.keySet()) {
                for (ObjectCollectStruct struct : object_collect_structList) {
                    String[] split = struct.getColumnposition().split(",");
                    if (split.length == index + 1 && key.equals(split[index])) {
                        if (node.getAttributes() != null) {
                            node.getAttributes().put(struct.getColumn_name(), getColumnValue(struct.getColumn_type(), jsonMap.get(key).toString()));
                        } else {
                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put(struct.getColumn_name(), getColumnValue(struct.getColumn_type(), jsonMap.get(key).toString()));
                            node.setAttributes(attributes);
                        }
                    } else if ((split.length == index + 2 && key.equals(split[index]) && !keyList.contains(key)) || ((!columnLevel.contains(index + 2)) && split.length > index + 1 && key.equals(split[index]) && !keyList.contains(key))) {
                        keyList.add(key);
                        flag = false;
                        getNodeList(JsonUtil.toJson(jsonMap.get(split[index])), nodeList, node, index + 1);
                    }
                }
            }
            if (flag) {
                nodeList.add(node);
            }
        } else {
            List<Map<String, Object>> jsonArray = JsonUtil.toObject(lineValue, new TypeReference<List<Map<String, Object>>>() {
            });
            for (Object object1 : jsonArray) {
                getNodeList(JsonUtil.toJson(object1), nodeList, parentNode, index);
            }
        }
    }

    private Object getColumnValue(String columnType, String columnValue) {
        Object str;
        columnType = columnType.toLowerCase();
        if (columnType.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = Boolean.parseBoolean(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.INT8.getMessage()) || columnType.contains(DataTypeConstant.BIGINT.getMessage()) || columnType.contains(DataTypeConstant.LONG.getMessage())) {
            str = Long.parseLong(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.INT.getMessage())) {
            str = Integer.parseInt(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.FLOAT.getMessage())) {
            str = Float.parseFloat(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.DOUBLE.getMessage()) || columnType.contains(DataTypeConstant.DECIMAL.getMessage()) || columnType.contains(DataTypeConstant.NUMERIC.getMessage())) {
            str = Double.parseDouble(columnValue.trim());
        } else {
            if (columnValue == null) {
                str = "";
            } else {
                str = columnValue.trim();
            }
        }
        return str;
    }
}
