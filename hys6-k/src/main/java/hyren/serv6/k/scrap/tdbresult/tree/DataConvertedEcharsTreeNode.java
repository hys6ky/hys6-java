package hyren.serv6.k.scrap.tdbresult.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataConvertedEcharsTreeNode {

    public static Map<String, Object> conversionRootNode(String root_name) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", root_name);
        map.put("name", root_name);
        map.put("value", root_name);
        map.put("parent_id", "0");
        return map;
    }

    public static List<Map<String, Object>> conversionJointPKInfos(List<Map<String, Object>> tableJoinPkData, String table_code) {
        List<Map<String, Object>> jointPKInfos = new ArrayList<>();
        for (int i = 0; i < tableJoinPkData.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            String join_pk_group_name = "联合主键" + (i + 1);
            String join_pk_id = table_code + "_" + join_pk_group_name;
            map.put("id", join_pk_id);
            map.put("name", join_pk_group_name);
            map.put("value", join_pk_group_name);
            map.put("parent_id", table_code);
            jointPKInfos.add(map);
            String[] field_s = tableJoinPkData.get(i).get("join_pk_col_code").toString().split(",");
            for (String field : field_s) {
                Map<String, Object> sub_map = new HashMap<>();
                sub_map.put("id", join_pk_id + "_" + field);
                sub_map.put("name", field);
                sub_map.put("value", field);
                sub_map.put("parent_id", join_pk_id);
                jointPKInfos.add(sub_map);
            }
        }
        return jointPKInfos;
    }

    public static List<Map<String, Object>> conversionTableFuncInfos(List<Map<String, Object>> tableFuncDepDatas, String table_code) {
        List<Map<String, Object>> jointPKInfos = new ArrayList<>();
        tableFuncDepDatas.forEach(tableFuncDepData -> {
            String left_field_name = tableFuncDepData.get("left_columns").toString();
            String left_field_id = table_code + "_" + tableFuncDepData.get("left_columns").toString();
            Map<String, Object> map = new HashMap<>();
            map.put("id", left_field_id);
            map.put("name", left_field_name);
            map.put("value", left_field_name);
            map.put("parent_id", table_code);
            jointPKInfos.add(map);
            String[] field_s = tableFuncDepData.get("right_columns").toString().split(",");
            for (String field : field_s) {
                Map<String, Object> sub_map = new HashMap<>();
                sub_map.put("id", table_code + "_" + left_field_name + "_" + field);
                sub_map.put("name", field);
                sub_map.put("value", field);
                sub_map.put("parent_id", left_field_id);
                jointPKInfos.add(sub_map);
            }
        });
        return jointPKInfos;
    }
}
