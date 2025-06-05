package hyren.serv6.k.dm.metadatamanage.drbtree.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.background.utils.TreeNodeDataQuery;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.user.User;
import hyren.serv6.k.dm.metadatamanage.query.MDMDataQuery;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/4/1 0001 下午 02:49")
public class MDMTreeNodeDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getISLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getDCLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        List<DataStoreLayer> dataStorageLayers = MDMDataQuery.getDCLExistTableDataStorageLayers();
        if (!dataStorageLayers.isEmpty()) {
            dataList.addAll(StorageLayerConvertedNodeData.conversionStorageLayers(dataStorageLayers, DataSourceType.DCL));
            dataStorageLayers.forEach(data_store_layer -> {
                List<Map<String, Object>> dclStorageLayerTableInfos = MDMDataQuery.getDCLStorageLayerTableInfos(data_store_layer);
                if (!dclStorageLayerTableInfos.isEmpty()) {
                    dataList.addAll(MDMDataConvertedNodeData.conversionDCLStorageLayerTableInfos(dclStorageLayerTableInfos));
                }
            });
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getDPLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getSFLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getAMLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getDQCDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        TreeNodeDataQuery.getDQCDataList(user, dataList, treeConf);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getUDLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        List<DataStoreLayer> data_store_layers = MDMDataQuery.getUDLDataStorageLayers();
        if (!data_store_layers.isEmpty()) {
            dataList.addAll(StorageLayerConvertedNodeData.conversionStorageLayers(data_store_layers, DataSourceType.UDL));
            data_store_layers.forEach(data_store_layer -> {
                List<Map<String, Object>> udlStorageLayerTableInfos = MDMDataQuery.getUDLStorageLayerTableInfos(data_store_layer);
                if (!udlStorageLayerTableInfos.isEmpty()) {
                    dataList.addAll(MDMDataConvertedNodeData.conversionUDLStorageLayerTableInfos(udlStorageLayerTableInfos));
                }
            });
        }
    }
}
