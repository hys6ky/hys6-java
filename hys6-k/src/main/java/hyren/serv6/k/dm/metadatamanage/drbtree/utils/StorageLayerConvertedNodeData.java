package hyren.serv6.k.dm.metadatamanage.drbtree.utils;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.entity.DataStoreLayer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageLayerConvertedNodeData {

    @Method(desc = "", logicStep = "")
    @Param(name = "dataStorageLayers", desc = "", range = "")
    @Param(name = "dataSourceType", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionStorageLayers(List<DataStoreLayer> dataStorageLayers, DataSourceType dataSourceType) {
        List<Map<String, Object>> dataStorageLayerNodes = new ArrayList<>();
        dataStorageLayers.forEach(data_store_layer -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dataSourceType.getCode() + "_" + data_store_layer.getDsl_id());
            map.put("label", data_store_layer.getDsl_name());
            map.put("dsl_id", data_store_layer.getDsl_id());
            map.put("dsl_store_type", data_store_layer.getStore_type());
            map.put("parent_id", dataSourceType.getCode());
            map.put("description", data_store_layer.getDsl_remark());
            map.put("data_layer", dataSourceType.getCode());
            dataStorageLayerNodes.add(map);
        });
        return dataStorageLayerNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionKFKStorageLayers(List<Map<String, Object>> dbLayerList) {
        List<Map<String, Object>> dataStorageLayerNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : dbLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("ssj_job_id") + "_" + dbLayer.get("dsl_id"));
            map.put("label", dbLayer.get("dsl_name"));
            map.put("dsl_id", dbLayer.get("dsl_id"));
            map.put("data_layer", DataSourceType.KFK.getCode());
            map.put("table_name", dbLayer.get("ssd_table_name"));
            map.put("hyren_name", dbLayer.get("ssd_table_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("ssj_job_id"));
            dataStorageLayerNodes.add(map);
        }
        return dataStorageLayerNodes;
    }
}
