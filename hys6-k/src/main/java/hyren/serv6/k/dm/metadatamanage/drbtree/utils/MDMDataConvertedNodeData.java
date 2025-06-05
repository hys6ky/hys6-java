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

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/4/1 0001 下午 02:43")
public class MDMDataConvertedNodeData {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionDCLStorageLayerTableInfos(List<Map<String, Object>> tableInfos) {
        List<Map<String, Object>> storageLayerTableNodes = new ArrayList<>();
        tableInfos.forEach(tableInfo -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.DCL.getCode() + "_" + tableInfo.get("dsl_id") + "_" + tableInfo.get("file_id"));
            map.put("label", tableInfo.get("hyren_name"));
            map.put("parent_id", DataSourceType.DCL.getCode() + "_" + tableInfo.get("dsl_id"));
            map.put("description", "" + "存储层名称：" + tableInfo.get("dsl_name") + "\n" + "登记表名称：" + tableInfo.get("hyren_name") + "\n" + "表中文名称：" + tableInfo.get("original_name") + "\n" + "原始表名称：" + tableInfo.get("table_name"));
            map.put("data_layer", DataSourceType.DCL.getCode());
            map.put("dsl_id", tableInfo.get("dsl_id"));
            map.put("dsl_store_type", tableInfo.get("store_type"));
            map.put("table_name", tableInfo.get("table_name"));
            map.put("original_name", tableInfo.get("original_name"));
            map.put("hyren_name", tableInfo.get("hyren_name"));
            map.put("data_own_type", tableInfo.get("store_type"));
            map.put("file_id", tableInfo.get("file_id"));
            storageLayerTableNodes.add(map);
        });
        return storageLayerTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionDMLStorageLayerTableInfos(List<Map<String, Object>> tableInfos) {
        List<Map<String, Object>> storageLayerTableNodes = new ArrayList<>();
        tableInfos.forEach(tableInfo -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.DML.getCode() + "_" + tableInfo.get("dsl_id") + "_" + tableInfo.get("datatable_id"));
            map.put("label", tableInfo.get("datatable_en_name"));
            map.put("parent_id", DataSourceType.DML.getCode() + "_" + tableInfo.get("dsl_id"));
            map.put("description", "" + "存储层名称：" + tableInfo.get("dsl_name") + "\n" + "登记表名称：" + tableInfo.get("datatable_en_name") + "\n" + "表中文名称：" + tableInfo.get("datatable_cn_name") + "\n" + "原始表名称：" + tableInfo.get("datatable_en_name"));
            map.put("data_layer", DataSourceType.DML.getCode());
            map.put("dsl_id", tableInfo.get("dsl_id"));
            map.put("dsl_store_type", tableInfo.get("store_type"));
            map.put("table_name", tableInfo.get("datatable_en_name"));
            map.put("original_name", tableInfo.get("datatable_cn_name"));
            map.put("hyren_name", tableInfo.get("datatable_en_name"));
            map.put("data_own_type", tableInfo.get("store_type"));
            map.put("file_id", tableInfo.get("datatable_id"));
            storageLayerTableNodes.add(map);
        });
        return storageLayerTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionUDLStorageLayerTableInfos(List<Map<String, Object>> tableInfos) {
        List<Map<String, Object>> storageLayerTableNodes = new ArrayList<>();
        tableInfos.forEach(tableInfo -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.UDL.getCode() + "_" + tableInfo.get("dsl_id") + "_" + tableInfo.get("table_id"));
            map.put("label", tableInfo.get("table_name"));
            map.put("parent_id", DataSourceType.UDL.getCode() + "_" + tableInfo.get("dsl_id"));
            map.put("description", "" + "存储层名称：" + tableInfo.get("dsl_name") + "\n" + "登记表名称：" + tableInfo.get("table_name") + "\n" + "表中文名称：" + tableInfo.get("ch_name") + "\n" + "原始表名称：" + tableInfo.get("table_name"));
            map.put("data_layer", DataSourceType.UDL.getCode());
            map.put("dsl_id", tableInfo.get("dsl_id"));
            map.put("dsl_store_type", tableInfo.get("store_type"));
            map.put("table_name", tableInfo.get("table_name"));
            map.put("original_name", tableInfo.get("ch_name"));
            map.put("hyren_name", tableInfo.get("table_name"));
            map.put("data_own_type", tableInfo.get("store_type"));
            map.put("file_id", tableInfo.get("table_id"));
            storageLayerTableNodes.add(map);
        });
        return storageLayerTableNodes;
    }
}
