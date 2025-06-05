package hyren.serv6.k.dm.metadatamanage.drbtree.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.DataSourceType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/30 0030 上午 10:04")
public class DRBDataConvertedNodeData {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionStorageLayerTableInfos(List<Map<String, Object>> tableInfos, DataSourceType dataSourceType) {
        List<Map<String, Object>> dataNodes = new ArrayList<>();
        tableInfos.forEach(table_info -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", table_info.get("failure_table_id"));
            map.put("label", table_info.get("table_en_name"));
            map.put("parent_id", dataSourceType.getCode() + "_" + table_info.get("dsl_id"));
            map.put("description", "" + "存储层名称：" + table_info.get("dsl_name") + "\n" + "登记表名称：" + table_info.get("table_en_name") + "\n" + "表中文名称：" + table_info.get("table_cn_name") + "\n" + "原始表名称：" + table_info.get("table_en_name"));
            map.put("data_layer", dataSourceType.getCode());
            map.put("dsl_id", table_info.get("dsl_id"));
            map.put("dsl_store_type", table_info.get("store_type"));
            map.put("table_name", table_info.get("table_en_name"));
            map.put("original_name", table_info.get("table_cn_name"));
            map.put("hyren_name", table_info.get("table_en_name"));
            map.put("data_own_type", table_info.get("store_type"));
            map.put("file_id", table_info.get("failure_table_id"));
            dataNodes.add(map);
        });
        return dataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionKFKStorageLayerTableInfos(List<Map<String, Object>> tableInfos, DataSourceType dataSourceType) {
        List<Map<String, Object>> dataNodes = new ArrayList<>();
        tableInfos.forEach(table_info -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", table_info.get("failure_table_id"));
            map.put("label", table_info.get("table_en_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + table_info.get("ssj_job_id") + "_" + table_info.get("dsl_id"));
            map.put("description", "" + "存储层名称：" + table_info.get("dsl_name") + "\n" + "登记表名称：" + table_info.get("table_en_name") + "\n" + "表中文名称：" + table_info.get("table_cn_name") + "\n" + "原始表名称：" + table_info.get("table_en_name"));
            map.put("data_layer", dataSourceType.getCode());
            map.put("dsl_id", table_info.get("dsl_id"));
            map.put("dsl_store_type", table_info.get("store_type"));
            map.put("table_name", table_info.get("table_en_name"));
            map.put("original_name", table_info.get("table_cn_name"));
            map.put("hyren_name", table_info.get("table_en_name"));
            map.put("data_own_type", table_info.get("store_type"));
            map.put("file_id", table_info.get("failure_table_id") + "," + table_info.get("tab_id") + "," + table_info.get("dsl_id"));
            dataNodes.add(map);
        });
        return dataNodes;
    }
}
