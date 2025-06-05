package hyren.serv6.k.scrap.tdbresult.tree;

import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.BusinessException;
import java.util.*;

public class NodeDataConvertedTreeList {

    public static List<EcharsTreeNode> echarsTreeNodesConversionTreeInfo(List<Map<String, Object>> nodeList) {
        Map<String, EcharsTreeNode> nodeMap = new HashMap<>();
        nodeList.forEach(dataRecord -> {
            EcharsTreeNode echarsTreeNode = new EcharsTreeNode();
            echarsTreeNode.setId(dataRecord.get("id").toString());
            echarsTreeNode.setName(dataRecord.get("name").toString());
            echarsTreeNode.setValue(dataRecord.get("value").toString());
            echarsTreeNode.setParent_id(dataRecord.get("parent_id").toString());
            nodeMap.put(echarsTreeNode.getId(), echarsTreeNode);
        });
        List<Map.Entry<String, EcharsTreeNode>> list = new ArrayList<>(nodeMap.entrySet());
        list.sort(Comparator.comparing(o -> o.getValue().getId()));
        List<EcharsTreeNode> treeList = new ArrayList<>();
        for (Map.Entry<String, EcharsTreeNode> nodeEntry : list) {
            EcharsTreeNode treeEcharsTreeNodeData;
            EcharsTreeNode echarsTreeNode = nodeEntry.getValue();
            try {
                if (echarsTreeNode.getParent_id() == null || "0".equals(echarsTreeNode.getParent_id())) {
                    treeEcharsTreeNodeData = echarsTreeNode;
                    treeList.add(treeEcharsTreeNodeData);
                } else {
                    if (null != nodeMap.get(echarsTreeNode.getParent_id())) {
                        nodeMap.get(echarsTreeNode.getParent_id()).addChild(echarsTreeNode);
                    }
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw new BusinessException("当前节点信息 node: " + JsonUtil.toJson(echarsTreeNode) + " 所属的父id parent_id: " + echarsTreeNode.getParent_id());
            }
        }
        return treeList;
    }
}
