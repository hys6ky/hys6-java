package hyren.serv6.k.dm.metadatamanage.drbtree.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.background.query.KFKDataQuery;
import hyren.serv6.base.datatree.background.utils.DataConvertedNodeData;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.SdmConsumeConf;
import hyren.serv6.base.entity.SdmSpJobinfo;
import hyren.serv6.base.user.User;
import hyren.serv6.k.dm.metadatamanage.query.DRBDataQuery;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/30 0030 上午 10:00")
public class DRBTreeNodeDataQuery {

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
        List<DataStoreLayer> dataStorageLayers = DRBDataQuery.getExistTableDataStorageLayers(DataSourceType.DCL);
        dataList.addAll(StorageLayerConvertedNodeData.conversionStorageLayers(dataStorageLayers, DataSourceType.DCL));
        List<Map<String, Object>> tableInfos = DRBDataQuery.getStorageLayerTableInfos(DataSourceType.DCL);
        if (!tableInfos.isEmpty()) {
            dataList.addAll(DRBDataConvertedNodeData.conversionStorageLayerTableInfos(tableInfos, DataSourceType.DCL));
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
    public static void getDMLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        List<DataStoreLayer> dataStorageLayers = DRBDataQuery.getExistTableDataStorageLayers(DataSourceType.DML);
        dataList.addAll(StorageLayerConvertedNodeData.conversionStorageLayers(dataStorageLayers, DataSourceType.DML));
        List<Map<String, Object>> tableInfos = DRBDataQuery.getStorageLayerTableInfos(DataSourceType.DML);
        if (!tableInfos.isEmpty()) {
            dataList.addAll(DRBDataConvertedNodeData.conversionStorageLayerTableInfos(tableInfos, DataSourceType.DML));
        }
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
        List<DataStoreLayer> dataStorageLayers = DRBDataQuery.getExistTableDataStorageLayers(DataSourceType.DQC);
        dataList.addAll(StorageLayerConvertedNodeData.conversionStorageLayers(dataStorageLayers, DataSourceType.DQC));
        List<Map<String, Object>> tableInfos = DRBDataQuery.getStorageLayerTableInfos(DataSourceType.DQC);
        if (!tableInfos.isEmpty()) {
            dataList.addAll(DRBDataConvertedNodeData.conversionStorageLayerTableInfos(tableInfos, DataSourceType.DQC));
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getUDLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        List<DataStoreLayer> dataStorageLayers = DRBDataQuery.getExistTableDataStorageLayers(DataSourceType.UDL);
        dataList.addAll(StorageLayerConvertedNodeData.conversionStorageLayers(dataStorageLayers, DataSourceType.UDL));
        List<Map<String, Object>> tableInfos = DRBDataQuery.getStorageLayerTableInfos(DataSourceType.UDL);
        if (!tableInfos.isEmpty()) {
            dataList.addAll(DRBDataConvertedNodeData.conversionStorageLayerTableInfos(tableInfos, DataSourceType.UDL));
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getKFKDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        List<SdmConsumeConf> consumeInfoList = KFKDataQuery.getConsumeManageNameList(user);
        List<SdmSpJobinfo> consumeReceiveList = KFKDataQuery.getConsumeReceNameList(user);
        if (!consumeInfoList.isEmpty() && !consumeReceiveList.isEmpty()) {
            dataList.addAll(DataConvertedNodeData.conversionDataInfo());
            dataList.addAll(hyren.serv6.base.datatree.background.utils.DataConvertedNodeData.conversionKFKDataInfos(consumeInfoList));
            dataList.addAll(DataConvertedNodeData.conversionAnaKFKDataInfos(consumeReceiveList));
            List<Map<String, Object>> dbLayerList = KFKDataQuery.getKFKInvalidAnalyseLayerList();
            if (!dbLayerList.isEmpty()) {
                dataList.addAll(StorageLayerConvertedNodeData.conversionKFKStorageLayers(dbLayerList));
                List<Map<String, Object>> tableInfos = DRBDataQuery.getStorageKFKLayerTableInfos(DataSourceType.KFK);
                if (!tableInfos.isEmpty()) {
                    dataList.addAll(DRBDataConvertedNodeData.conversionKFKStorageLayerTableInfos(tableInfos, DataSourceType.KFK));
                }
            }
        }
    }
}
