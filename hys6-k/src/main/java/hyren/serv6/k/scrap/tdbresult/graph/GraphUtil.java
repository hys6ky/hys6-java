package hyren.serv6.k.scrap.tdbresult.graph;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.k.scrap.tdb.bean.AdaptRelationBean;
import hyren.serv6.k.scrap.tdb.bean.NodeRelationBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphUtil {

    public static Map<String, Object> dataConvertedGraphData(Map<String, Integer> category_info_map, List<Map<String, Object>> node_info_map_s, List<Map<String, Object>> link_map_s) {
        Map<String, Object> graph_data_map = new HashMap<>();
        List<Category> categories = new ArrayList<>();
        category_info_map.forEach((k, v) -> {
            Category categoryNode = new Category();
            categoryNode.setName(k);
            categories.add(categoryNode);
        });
        graph_data_map.put("categories", categories);
        Map<Integer, Map<String, Integer>> area_map = initDisplayArea(category_info_map);
        List<GraphNode> graphNodes = new ArrayList<>();
        for (Map<String, Object> node_info_map : node_info_map_s) {
            GraphNode graphNode = new GraphNode();
            graphNode.setId(node_info_map.get("id").toString());
            graphNode.setName(node_info_map.get("name").toString());
            graphNode.setValue(node_info_map.get("value").toString());
            Object category = node_info_map.get("category");
            graphNode.setCategory((Integer) category);
            int symbolSizeFactor = 1;
            for (Map<String, Object> link_map : link_map_s) {
                if (node_info_map.get("id").toString().equalsIgnoreCase(link_map.get("source").toString())) {
                    symbolSizeFactor++;
                }
            }
            graphNode.setSymbolSize(10.00 + symbolSizeFactor);
            Map<String, Integer> xy_map = area_map.get(category);
            graphNode.setX(2 * (Math.floor(Math.random() * 10000 + xy_map.get("x"))));
            graphNode.setY(Math.random() * 10000 + xy_map.get("y"));
            graphNodes.add(graphNode);
        }
        graph_data_map.put("nodes", graphNodes);
        List<Link> links = new ArrayList<>();
        for (Map<String, Object> link_map : link_map_s) {
            Link link = new Link();
            link.setSource(link_map.get("source").toString());
            link.setTarget(link_map.get("target").toString());
            links.add(link);
        }
        graph_data_map.put("links", links);
        return graph_data_map;
    }

    public static Map<String, Integer> extractCategoryNode(List<Map<String, Object>> tableFkDatas) {
        Map<String, Integer> category_base_info = new HashMap<>();
        List<String> category_name_s = new ArrayList<>();
        tableFkDatas.forEach(tableFkData -> {
            String fk_table_code = tableFkData.get("fk_table_code").toString();
            if (!category_name_s.contains(fk_table_code)) {
                category_name_s.add(fk_table_code);
            }
            String table_code = tableFkData.get("table_code").toString();
            if (!category_name_s.contains(table_code)) {
                category_name_s.add(table_code);
            }
        });
        for (int i = 0; i < category_name_s.size(); i++) {
            category_base_info.put(category_name_s.get(i), i);
        }
        return category_base_info;
    }

    public static List<Map<String, Object>> extractRelationNode(List<Map<String, Object>> tableFkDatas, Map<String, Integer> category_info_map) {
        List<Map<String, Object>> node_info_map_s = new ArrayList<>();
        for (Map<String, Object> tableFkData : tableFkDatas) {
            Map<String, Object> map;
            String id = tableFkData.get("fk_table_code") + "_" + tableFkData.get("fk_col_code").toString();
            String name = tableFkData.get("fk_col_code").toString();
            String value = tableFkData.get("fk_table_code").toString();
            int category = category_info_map.get(tableFkData.get("fk_table_code").toString());
            map = new HashMap<>();
            map.put("id", id);
            map.put("name", name);
            map.put("value", value);
            map.put("category", category);
            node_info_map_s.add(map);
            id = tableFkData.get("table_code") + "_" + tableFkData.get("col_code").toString();
            name = tableFkData.get("col_code").toString();
            value = tableFkData.get("table_code").toString();
            category = category_info_map.get(tableFkData.get("table_code").toString());
            map = new HashMap<>();
            map.put("id", id);
            map.put("name", name);
            map.put("value", value);
            map.put("category", category);
            node_info_map_s.add(map);
        }
        return node_info_map_s.stream().distinct().collect(Collectors.toList());
    }

    public static List<Map<String, Object>> extractLink(List<Map<String, Object>> tableFkDatas) {
        List<Map<String, Object>> link_map_s = new ArrayList<>();
        for (Map<String, Object> tableFkData : tableFkDatas) {
            Map<String, Object> map = new HashMap<>();
            String source = tableFkData.get("fk_table_code") + "_" + tableFkData.get("fk_col_code");
            String target = tableFkData.get("table_code") + "_" + tableFkData.get("col_code");
            map.put("source", source);
            map.put("target", target);
            link_map_s.add(map);
        }
        return link_map_s;
    }

    public static Map<Integer, Map<String, Integer>> initDisplayArea(Map<String, Integer> category_info_map) {
        int area_size = 10000;
        int area_measure = 0;
        for (int i = 1; i <= category_info_map.size(); i++) {
            if (Math.pow(i, 2) <= category_info_map.size()) {
                area_measure = i + 1;
            }
        }
        List<Map<String, Integer>> area_map_list = new ArrayList<>();
        for (int i = 1; i <= area_measure; i++) {
            for (int j = 1; j <= area_measure; j++) {
                Map<String, Integer> map = new HashMap<>();
                map.put("x", i * area_size);
                map.put("y", j * area_size);
                area_map_list.add(map);
            }
        }
        Map<Integer, Map<String, Integer>> area_map = new HashMap<>();
        category_info_map.forEach((k, v) -> area_map.put(v, area_map_list.get(v)));
        return area_map;
    }

    public static Map<String, Object> lpaOrLouvainConversion(List<NodeRelationBean> nodeRelationBeans, List<Map<String, Object>> dataMapList, String type) {
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> links = new ArrayList<>();
        List<Object> names = dataMapList.stream().map(data -> data.get("name")).distinct().collect(Collectors.toList());
        extractNodesAndRelationships(nodes, links, nodeRelationBeans);
        nodes = nodes.stream().filter(node -> names.contains(node.get("name"))).collect(Collectors.toList());
        links = links.stream().filter(link -> names.contains(link.get("source")) && names.contains(link.get("target"))).collect(Collectors.toList());
        List<Object> nodeNames = nodes.stream().map(node -> node.get("name")).collect(Collectors.toList());
        List<Object> categoryList;
        if (IsFlag.Shi == IsFlag.ofEnumByCode(type)) {
            categoryList = dataMapList.stream().filter(data -> nodeNames.contains(data.get("name"))).map(o -> o.get("label")).distinct().collect(Collectors.toList());
        } else {
            categoryList = dataMapList.stream().filter(data -> nodeNames.contains(data.get("name"))).map(o -> o.get("community")).distinct().collect(Collectors.toList());
        }
        Map<String, Integer> categoryMap = new HashMap<>();
        for (int i = 0; i < categoryList.size(); i++) {
            categoryMap.put(categoryList.get(i).toString(), i);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(type)) {
            for (Map<String, Object> node : nodes) {
                for (Map<String, Object> map : dataMapList) {
                    if (node.get("name").equals(map.get("name"))) {
                        node.put("category", categoryMap.get(map.get("label").toString()));
                    }
                }
            }
        } else {
            for (Map<String, Object> node : nodes) {
                for (Map<String, Object> map : dataMapList) {
                    if (node.get("name").equals(map.get("name"))) {
                        node.put("category", categoryMap.get(map.get("community").toString()));
                    }
                }
            }
        }
        Map<Integer, Map<String, Integer>> displayAreaMap = GraphUtil.initDisplayArea(categoryMap);
        for (Map<String, Object> node : nodes) {
            Map<String, Integer> xyMap = displayAreaMap.get(Integer.parseInt(node.get("category").toString()));
            node.put("x", (xyMap.get("x") + Math.random() * 10000) * 2);
            node.put("y", xyMap.get("y") + Math.random() * 10000);
        }
        Map<String, Object> dataMap = new HashMap<>();
        List<Object> categories = nodes.stream().map(data -> data.get("category")).distinct().collect(Collectors.toList());
        dataMap.put("categories", categories.stream().sorted().collect(Collectors.toList()));
        dataMap.put("nodes", nodes);
        dataMap.put("links", links);
        return dataMap;
    }

    public static Map<String, Object> longestAndShortestDataConversion(List<AdaptRelationBean> adaptRelationBeans, String columnNodeName1, String columnNodeName2) {
        List<Map<String, Object>> nodes = new ArrayList<>();
        Map<String, Object> dataMap = getEchartsData(adaptRelationBeans, nodes);
        for (Map<String, Object> node : nodes) {
            if (node.get("name").equals(columnNodeName1)) {
                node.put("category", 0);
            } else if (node.get("name").equals(columnNodeName2)) {
                node.put("category", 1);
            } else {
                node.put("category", 2);
            }
        }
        return dataMap;
    }

    public static Map<String, Object> convertToEchartsTree(List<NodeRelationBean> nodeRelationBeans) {
        Map<String, Object> treeMap = new HashMap<>();
        if (null != nodeRelationBeans && nodeRelationBeans.size() != 0) {
            Map<Long, Map<String, Object>> leftNode = nodeRelationBeans.get(0).getLeftNode();
            for (Map.Entry<Long, Map<String, Object>> entry : leftNode.entrySet()) {
                treeMap.put("name", entry.getValue().get("name"));
            }
            List<Map<String, Object>> childrenList = new ArrayList<>();
            for (NodeRelationBean nodeRelationBean : nodeRelationBeans) {
                Map<String, Object> childNodeMap = new HashMap<>();
                Map<Long, Map<String, Object>> rightNode = nodeRelationBean.getRightNode();
                for (Map.Entry<Long, Map<String, Object>> entry : rightNode.entrySet()) {
                    List<Object> valueList = new ArrayList<>();
                    for (Map.Entry<String, Object> objectEntry : entry.getValue().entrySet()) {
                        valueList.add(objectEntry.getKey() + ":" + objectEntry.getValue());
                    }
                    childNodeMap.put("name", entry.getValue().get("name"));
                    childNodeMap.put("value", valueList);
                    valueList.add(nodeRelationBean.getRelationType());
                }
                childrenList.add(childNodeMap);
            }
            treeMap.put("children", childrenList);
        }
        return treeMap;
    }

    public static Map<String, Object> convertToTriangle(List<AdaptRelationBean> adaptRelationBeans) {
        List<Map<String, Object>> nodes = new ArrayList<>();
        return getEchartsData(adaptRelationBeans, nodes);
    }

    public static Map<String, Object> convertToGraphData(List<NodeRelationBean> nodeRelationBeans) {
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> links = new ArrayList<>();
        extractNodesAndRelationships(nodes, links, nodeRelationBeans);
        Map<String, Object> dataMap = new HashMap<>();
        for (Map<String, Object> node : nodes) {
            node.put("x", Math.random() * 25000);
            node.put("y", Math.random() * 10000);
        }
        for (Map<String, Object> node : nodes) {
            double symbolSize = 5;
            for (Map<String, Object> link_map : links) {
                if (node.get("id").toString().equalsIgnoreCase(link_map.get("source").toString())) {
                    symbolSize++;
                }
            }
            node.put("symbolSize", symbolSize);
        }
        dataMap.put("nodes", nodes);
        dataMap.put("links", links);
        return dataMap;
    }

    private static Map<String, Object> getEchartsData(List<AdaptRelationBean> adaptRelationBeans, List<Map<String, Object>> nodes) {
        List<Map<String, Object>> links = new ArrayList<>();
        for (AdaptRelationBean adaptRelationBean : adaptRelationBeans) {
            Map<Long, Map<String, Object>> nodeCollection = adaptRelationBean.getNodeCollection();
            for (Map.Entry<Long, Map<String, Object>> entry : nodeCollection.entrySet()) {
                setNode(nodes, null, entry);
            }
            Map<Long, Map<String, Object>> relationCollection = adaptRelationBean.getRelationCollection();
            for (Map.Entry<Long, Map<String, Object>> entry : relationCollection.entrySet()) {
                Map<String, Object> linkMap = new HashMap<>();
                long source = Long.parseLong(entry.getValue().get("source").toString());
                long target = Long.parseLong(entry.getValue().get("target").toString());
                linkMap.put("source", adaptRelationBean.getNodeCollection().get(source).get("name").toString());
                linkMap.put("target", adaptRelationBean.getNodeCollection().get(target).get("name").toString());
                linkMap.put("type", entry.getValue().get("type"));
                links.add(linkMap);
            }
        }
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("nodes", nodes);
        dataMap.put("links", links.stream().distinct().collect(Collectors.toList()));
        return dataMap;
    }

    private static void extractNodesAndRelationships(List<Map<String, Object>> nodes, List<Map<String, Object>> links, List<NodeRelationBean> nodeRelationBeans) {
        for (NodeRelationBean nodeRelationBean : nodeRelationBeans) {
            Map<Long, Map<String, Object>> leftNode = nodeRelationBean.getLeftNode();
            Map<Long, Map<String, Object>> rightNode = nodeRelationBean.getRightNode();
            String source = null;
            for (Map.Entry<Long, Map<String, Object>> entry : leftNode.entrySet()) {
                setNode(nodes, nodeRelationBean, entry);
                source = entry.getValue().get("name").toString();
            }
            String target = null;
            for (Map.Entry<Long, Map<String, Object>> entry : rightNode.entrySet()) {
                setNode(nodes, nodeRelationBean, entry);
                target = entry.getValue().get("name").toString();
            }
            if (StringUtil.isNotBlank(source) && StringUtil.isNotBlank(target)) {
                Map<String, Object> linkMap = new HashMap<>();
                linkMap.put("source", source);
                linkMap.put("target", target);
                linkMap.put("relationType", nodeRelationBean.getRelationType());
                links.add(linkMap);
            }
        }
    }

    private static void setNode(List<Map<String, Object>> nodes, NodeRelationBean nodeRelationBean, Map.Entry<Long, Map<String, Object>> entry) {
        Map<String, Object> nodeMap = new HashMap<>();
        nodeMap.put("id", entry.getValue().get("name"));
        nodeMap.put("name", entry.getValue().get("name"));
        List<String> valueList = new ArrayList<>();
        valueList.add("id:" + entry.getKey());
        for (Map.Entry<String, Object> objectEntry : entry.getValue().entrySet()) {
            valueList.add(objectEntry.getKey() + ":" + objectEntry.getValue());
        }
        if (nodeRelationBean != null) {
            valueList.add("relationId:" + nodeRelationBean.getRelationId());
        }
        nodeMap.put("value", valueList);
        if (nodes.isEmpty()) {
            nodes.add(nodeMap);
        } else {
            List<Object> nameList = nodes.stream().map(node -> node.get("name")).collect(Collectors.toList());
            if (!nameList.contains(entry.getValue().get("name"))) {
                nodes.add(nodeMap);
            }
        }
    }
}
