package hyren.serv6.base.datatree.background.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.background.query.*;
import hyren.serv6.base.datatree.util.TreeConstant;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.SdmConsumeConf;
import hyren.serv6.base.entity.SdmSpJobinfo;
import hyren.serv6.base.user.User;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/13 0013 上午 09:44")
public class TreeNodeDataQuery {

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
        dataList.addAll(DCLDataQuery.getDCLDataInfos(treeConf));
        List<Map<String, Object>> dclDataSourceInfos = DCLDataQuery.getDCLBatchDataInfos(user);
        dataList.addAll(DataConvertedNodeData.conversionDCLBatchDataInfos(dclDataSourceInfos));
        dclDataSourceInfos.forEach(dclBatchDataInfo -> {
            String source_id = dclBatchDataInfo.get("source_id").toString();
            List<Map<String, Object>> dclBatchClassifyInfos = DCLDataQuery.getDCLBatchClassifyInfos(source_id, treeConf.getShowFileCollection(), user);
            if (!dclBatchClassifyInfos.isEmpty()) {
                dclBatchClassifyInfos.forEach(dclBatchClassifyInfo -> {
                    String classify_id = dclBatchClassifyInfo.get("classify_id").toString();
                    List<Map<String, Object>> dclBatchTableInfos = DCLDataQuery.getDCLBatchTableInfos(classify_id, user);
                    if (!dclBatchTableInfos.isEmpty()) {
                        dataList.add(DataConvertedNodeData.conversionDCLBatchClassifyInfos(dclBatchClassifyInfo));
                        dataList.addAll(DataConvertedNodeData.conversionDCLBatchTableInfos(dclBatchTableInfos));
                    }
                });
            }
            List<Map<String, Object>> dclBatchObjectCollectInfos = DCLDataQuery.getDCLBatchObjectCollectInfos(source_id, user);
            if (!dclBatchObjectCollectInfos.isEmpty()) {
                dclBatchObjectCollectInfos.forEach(dclBatchObjectCollectInfo -> {
                    String odc_id = dclBatchObjectCollectInfo.get("classify_id").toString();
                    List<Map<String, Object>> dclBatchTableInfos = DCLDataQuery.getDCLBatchObjectCollectTableInfos(odc_id, user);
                    if (!dclBatchTableInfos.isEmpty()) {
                        dataList.add(DataConvertedNodeData.conversionDCLBatchClassifyInfos(dclBatchObjectCollectInfo));
                        dataList.addAll(DataConvertedNodeData.conversionDCLBatchTableInfos(dclBatchTableInfos));
                    }
                });
            }
        });
        if (treeConf.getShowDCLRealtime()) {
            dataList.addAll(DataConvertedNodeData.conversionDCLRealtimeDataInfos(dclDataSourceInfos));
            dclDataSourceInfos.forEach(dclBatchDataInfo -> {
                String source_id = dclBatchDataInfo.get("source_id").toString();
                List<Map<String, Object>> dclRealtimeClassifyInfos = DCLDataQuery.getDCLRealtimeClassifyInfos(source_id, user);
                if (!dclRealtimeClassifyInfos.isEmpty()) {
                    dclRealtimeClassifyInfos.forEach(dclRealtimeClassifyInfo -> {
                        String classify_id = dclRealtimeClassifyInfo.get("classify_id").toString();
                        List<Map<String, Object>> dclRealtimeTableInfos = DCLDataQuery.getDCLRealtimeTableInfos(classify_id, user);
                        List<Map<String, Object>> dclRealtimeTopicInfos = DCLDataQuery.getDCLRealtimeTopicInfos(classify_id, user);
                        if (!dclRealtimeTableInfos.isEmpty() || !dclRealtimeTopicInfos.isEmpty()) {
                            dataList.add(DataConvertedNodeData.conversionDCLRealtimeClassifyInfos(dclRealtimeClassifyInfo));
                            if (!dclRealtimeTableInfos.isEmpty()) {
                                dataList.add(getDCLRealtimeClassifySub(TreeConstant.DCL_REALTIME_TABLE, classify_id));
                                dataList.addAll(DataConvertedNodeData.conversionDCLRealtimeTableInfos(dclRealtimeTableInfos));
                            }
                            if (!dclRealtimeTopicInfos.isEmpty()) {
                                dataList.add(getDCLRealtimeClassifySub(TreeConstant.DCL_REALTIME_TOPIC, classify_id));
                            }
                            dataList.addAll(DataConvertedNodeData.conversionDCLRealtimeTopicInfos(dclRealtimeTopicInfos));
                        }
                    });
                }
            });
        }
    }

    private static Map<String, Object> getDCLRealtimeClassifySub(String treeConstant, String classify_id) {
        boolean isTableFlag = treeConstant.equalsIgnoreCase(TreeConstant.DCL_REALTIME_TABLE);
        Map<String, Object> map = new HashMap<>();
        map.put("id", treeConstant + "_" + classify_id);
        map.put("label", isTableFlag ? "Table" : "Topic");
        map.put("parent_id", TreeConstant.DCL_REALTIME + "_" + classify_id);
        map.put("description", isTableFlag ? "实时数据表" : "Topic");
        map.put("data_layer", DataSourceType.DCL.getCode());
        return map;
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
        List<Map<String, Object>> dmlDataInfos = DMLDataQuery.getDMLDataInfos(user);
        dataList.addAll(DataConvertedNodeData.conversionDMLDataInfos(dmlDataInfos));
        if (!dmlDataInfos.isEmpty()) {
            for (Map<String, Object> dmlDataInfo : dmlDataInfos) {
                long data_mart_id = (long) dmlDataInfo.get("data_mart_id");
                List<Map<String, Object>> dmlCategoryInfos = DMLDataQuery.getDMLCategoryInfos(data_mart_id);
                if (!dmlCategoryInfos.isEmpty()) {
                    for (Map<String, Object> dmlCategoryInfo : dmlCategoryInfos) {
                        dataList.add(DataConvertedNodeData.conversionDMLCategoryInfos(dmlCategoryInfo));
                        long category_id = (long) dmlCategoryInfo.get("category_id");
                        List<Map<String, Object>> dmlTableInfos = DMLDataQuery.getDMLTableInfos(category_id);
                        if (!dmlTableInfos.isEmpty()) {
                            dataList.addAll(DataConvertedNodeData.conversionDMLTableInfos(dmlTableInfos));
                        }
                    }
                }
            }
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
        List<DataStoreLayer> data_store_layer_s = DQCDataQuery.getDQCDataInfos();
        dataList.addAll(DataConvertedNodeData.conversionDQCDataInfos(data_store_layer_s));
        if (!data_store_layer_s.isEmpty()) {
            data_store_layer_s.forEach(data_store_layer -> {
                long dsl_id = data_store_layer.getDsl_id();
                List<Map<String, Object>> dqcTableInfos = DQCDataQuery.getDQCTableInfos(dsl_id);
                if (!dqcTableInfos.isEmpty()) {
                    dataList.addAll(DataConvertedNodeData.conversionDQCTableInfos(dqcTableInfos));
                }
            });
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "dataList", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    public static void getUDLDataList(User user, List<Map<String, Object>> dataList, TreeConf treeConf) {
        List<DataStoreLayer> data_store_layers = UDLDataQuery.getUDLExistTableDataStorageLayers();
        if (!data_store_layers.isEmpty()) {
            dataList.addAll(DataConvertedNodeData.conversionUDLDataInfos(data_store_layers));
            data_store_layers.forEach(data_store_layer -> {
                List<Map<String, Object>> udlStorageLayerTableInfos = UDLDataQuery.getUDLStorageLayerTableInfos(data_store_layer);
                if (!udlStorageLayerTableInfos.isEmpty()) {
                    dataList.addAll(DataConvertedNodeData.conversionUDLTableInfos(udlStorageLayerTableInfos));
                }
            });
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
            dataList.addAll(DataConvertedNodeData.conversionKFKDataInfos(consumeInfoList));
            dataList.addAll(DataConvertedNodeData.conversionAnaKFKDataInfos(consumeReceiveList));
            List<Map<String, Object>> dbLayerList = KFKDataQuery.getDBLayerList();
            if (!dbLayerList.isEmpty()) {
                dataList.addAll(DataConvertedNodeData.conversionConsumeData(dbLayerList));
                dataList.addAll(DataConvertedNodeData.conversionConsumeTableData(dbLayerList));
            }
            List<Map<String, Object>> hbLayerList = KFKDataQuery.getHBLayerList();
            if (!hbLayerList.isEmpty()) {
                dataList.addAll(DataConvertedNodeData.conversionHBConsumeData(hbLayerList));
                dataList.addAll(DataConvertedNodeData.conversionHBTableConsumeData(hbLayerList));
            }
            List<Map<String, Object>> analyseLayerList = KFKDataQuery.getAnalyseLayerList();
            if (!analyseLayerList.isEmpty()) {
                dataList.addAll(DataConvertedNodeData.conversionAnalyseConsumeData(analyseLayerList));
                dataList.addAll(DataConvertedNodeData.conversionAnalyseTableConsumeData(analyseLayerList));
            }
        }
    }
}
