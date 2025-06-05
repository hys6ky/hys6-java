package hyren.serv6.base.datatree.tree;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.BusinessException;
import java.util.*;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/2/20 0020 下午 09:49")
public class NodeDataConvertedTreeList {

    @Method(desc = "", logicStep = "")
    @Param(name = "nodeDataList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Node> dataConversionTreeInfo(List<Map<String, Object>> nodeDataList) {
        Map<String, Node> nodeMap = new HashMap<>();
        nodeDataList.forEach(dataRecord -> {
            Node node = new Node();
            node.setId(dataRecord.get("id").toString());
            node.setLabel(dataRecord.get("label").toString());
            node.setParent_id(dataRecord.get("parent_id").toString());
            if (null != dataRecord.get("description")) {
                node.setDescription(dataRecord.get("description").toString());
            }
            if (null != dataRecord.get("data_layer")) {
                node.setData_layer(dataRecord.get("data_layer").toString());
            }
            if (null != dataRecord.get("dsl_id")) {
                node.setDsl_id(dataRecord.get("dsl_id").toString());
            }
            if (null != dataRecord.get("dsl_store_type")) {
                node.setDsl_store_type(dataRecord.get("dsl_store_type").toString());
            }
            if (null != dataRecord.get("data_own_type")) {
                node.setData_own_type(dataRecord.get("data_own_type").toString());
            }
            if (null != dataRecord.get("data_source_id")) {
                node.setData_source_id(dataRecord.get("data_source_id").toString());
            }
            if (null != dataRecord.get("agent_id")) {
                node.setAgent_id(dataRecord.get("agent_id").toString());
            }
            if (null != dataRecord.get("classify_id")) {
                node.setClassify_id(dataRecord.get("classify_id").toString());
            }
            if (null != dataRecord.get("file_id")) {
                node.setFile_id(dataRecord.get("file_id").toString());
            }
            if (null != dataRecord.get("table_name")) {
                node.setTable_name(dataRecord.get("table_name").toString());
            }
            if (null != dataRecord.get("original_name")) {
                node.setOriginal_name(dataRecord.get("original_name").toString());
            }
            if (null != dataRecord.get("hyren_name")) {
                node.setHyren_name(dataRecord.get("hyren_name").toString());
            }
            if (null != dataRecord.get("tree_page_source")) {
                node.setTree_page_source(dataRecord.get("tree_page_source").toString());
            }
            nodeMap.put(node.getId(), node);
        });
        List<Map.Entry<String, Node>> list = new ArrayList<>(nodeMap.entrySet());
        list.sort(Comparator.comparing(o -> o.getValue().getId()));
        List<Node> treeList = new ArrayList<>();
        for (Map.Entry<String, Node> nodeEntry : list) {
            Node treeNodeData;
            Node node = nodeEntry.getValue();
            try {
                if (node.getParent_id() == null || "0".equals(node.getParent_id())) {
                    treeNodeData = node;
                    treeList.add(treeNodeData);
                } else {
                    if (null != nodeMap.get(node.getParent_id())) {
                        nodeMap.get(node.getParent_id()).addChild(node);
                    }
                }
            } catch (RuntimeException e) {
                throw new BusinessException(String.format("当前节点信息 node: %s , 所属的父id parent_id: %s , 发生异常e: %s", JsonUtil.toJson(node), node.getParent_id(), e));
            }
        }
        for (Node node : treeList) {
            node.sortChildren();
        }
        return treeList;
    }
}
