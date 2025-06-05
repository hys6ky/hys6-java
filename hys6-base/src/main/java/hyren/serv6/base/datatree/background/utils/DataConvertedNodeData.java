package hyren.serv6.base.datatree.background.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.util.TreeConstant;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.SdmConsumeConf;
import hyren.serv6.base.entity.SdmSpJobinfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/12 0012 下午 05:57")
public class DataConvertedNodeData {

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchDataInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionDCLBatchDataInfos(List<Map<String, Object>> dclBatchDataInfos) {
        List<Map<String, Object>> dclBatchDataNodes = new ArrayList<>();
        dclBatchDataInfos.forEach(o -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", TreeConstant.DCL_BATCH + "_" + o.get("source_id"));
            map.put("label", o.get("datasource_name"));
            map.put("parent_id", TreeConstant.DCL_BATCH);
            map.put("description", "数据源名称: " + o.get("datasource_name"));
            map.put("data_layer", DataSourceType.DCL.getCode());
            map.put("data_own_type", TreeConstant.DCL_BATCH);
            map.put("data_source_id", o.get("source_id"));
            dclBatchDataNodes.add(map);
        });
        return dclBatchDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchClassifyInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> conversionDCLBatchClassifyInfos(Map<String, Object> dclBatchClassifyInfo) {
        Map<String, Object> dclBatchClassifyNode = new HashMap<>();
        dclBatchClassifyNode.put("id", TreeConstant.DCL_BATCH + "_" + dclBatchClassifyInfo.get("classify_id"));
        dclBatchClassifyNode.put("label", dclBatchClassifyInfo.get("classify_name") + "【" + dclBatchClassifyInfo.get("classify_num").toString() + "】");
        dclBatchClassifyNode.put("parent_id", TreeConstant.DCL_BATCH + "_" + dclBatchClassifyInfo.get("source_id"));
        dclBatchClassifyNode.put("description", "分类名称: " + dclBatchClassifyInfo.get("classify_name") + "\n" + "分类描述: " + (dclBatchClassifyInfo.get("remark")));
        dclBatchClassifyNode.put("data_layer", DataSourceType.DCL.getCode());
        dclBatchClassifyNode.put("data_own_type", TreeConstant.DCL_BATCH);
        dclBatchClassifyNode.put("data_source_id", dclBatchClassifyInfo.get("source_id"));
        dclBatchClassifyNode.put("classify_id", dclBatchClassifyInfo.get("classify_id"));
        return dclBatchClassifyNode;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchTableRs", desc = "", range = "")
    public static List<Map<String, Object>> conversionDCLBatchTableInfos(List<Map<String, Object>> dclTableInfos) {
        List<Map<String, Object>> dclBatchTableNodes = new ArrayList<>();
        dclTableInfos.forEach(o -> {
            Map<String, Object> map = new HashMap<>();
            String table_name = o.get("table_name").toString();
            String classify_name = o.get("classify_name").toString();
            String task_name = o.get("task_name").toString();
            String original_name = o.get("original_name").toString();
            String hyren_name = o.get("hyren_name").toString();
            String source_id = o.get("source_id").toString();
            String classify_id = o.get("classify_id").toString();
            String file_id = o.get("file_id").toString();
            map.put("id", file_id);
            map.put("label", hyren_name);
            map.put("parent_id", TreeConstant.DCL_BATCH + "_" + o.get("classify_id"));
            map.put("description", "任务名称: " + task_name + "\n" + "分类名称: " + classify_name + "\n" + "原文件名: " + original_name + "\n" + "原始表名: " + table_name + "\n" + "系统表名: " + hyren_name);
            map.put("data_layer", DataSourceType.DCL.getCode());
            map.put("data_own_type", TreeConstant.DCL_BATCH);
            map.put("data_source_id", source_id);
            map.put("classify_id", classify_id);
            map.put("file_id", file_id);
            map.put("table_name", table_name);
            map.put("hyren_name", hyren_name);
            map.put("original_name", original_name);
            dclBatchTableNodes.add(map);
        });
        return dclBatchTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dmlDataInfos", desc = "", range = "")
    public static List<Map<String, Object>> conversionDMLDataInfos(List<Map<String, Object>> dmlDataInfos) {
        List<Map<String, Object>> dmlDataNodes = new ArrayList<>();
        for (Map<String, Object> dmlDataInfo : dmlDataInfos) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dmlDataInfo.get("data_mart_id"));
            map.put("label", dmlDataInfo.get("mart_name"));
            map.put("parent_id", DataSourceType.DML.getCode());
            map.put("data_layer", DataSourceType.DML.getCode());
            map.put("description", "加工编号: " + dmlDataInfo.get("mart_number") + "\n" + "加工名称: " + dmlDataInfo.get("mart_name") + "\n" + "加工描述: " + dmlDataInfo.get("mart_desc"));
            dmlDataNodes.add(map);
        }
        return dmlDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dmlDataInfos", desc = "", range = "")
    public static Map<String, Object> conversionDMLCategoryInfos(Map<String, Object> dmlCategoryInfo) {
        Map<String, Object> dmlDataNode = new HashMap<>();
        dmlDataNode.put("id", dmlCategoryInfo.get("category_id"));
        dmlDataNode.put("label", dmlCategoryInfo.get("category_name"));
        dmlDataNode.put("parent_id", dmlCategoryInfo.get("parent_category_id"));
        dmlDataNode.put("classify_id", dmlCategoryInfo.get("category_id"));
        dmlDataNode.put("data_layer", DataSourceType.DML.getCode());
        dmlDataNode.put("description", "分类编号: " + dmlCategoryInfo.get("category_id") + "\n" + "分类名称: " + dmlCategoryInfo.get("category_name") + "\n" + "分类描述: " + dmlCategoryInfo.get("category_desc"));
        return dmlDataNode;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dmlTableInfos", desc = "", range = "")
    public static List<Map<String, Object>> conversionDMLTableInfos(List<Map<String, Object>> dmlTableInfos) {
        List<Map<String, Object>> dmlTableNodes = new ArrayList<>();
        for (Map<String, Object> dmlTableInfo : dmlTableInfos) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dmlTableInfo.get("module_table_id"));
            map.put("label", dmlTableInfo.get("module_table_en_name"));
            map.put("parent_id", dmlTableInfo.get("category_id"));
            map.put("classify_id", dmlTableInfo.get("category_id"));
            map.put("file_id", dmlTableInfo.get("module_table_id"));
            map.put("table_name", dmlTableInfo.get("module_table_en_name"));
            map.put("hyren_name", dmlTableInfo.get("module_table_en_name"));
            map.put("original_name", dmlTableInfo.get("module_table_cn_name"));
            map.put("data_layer", DataSourceType.DML.getCode());
            map.put("description", "表英文名: " + dmlTableInfo.get("module_table_en_name") + "\n" + "表中文名: " + dmlTableInfo.get("module_table_cn_name") + "\n" + "表描述: " + dmlTableInfo.get("module_table_desc"));
            dmlTableNodes.add(map);
        }
        return dmlTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_store_layer_s", desc = "", range = "")
    public static List<Map<String, Object>> conversionDQCDataInfos(List<DataStoreLayer> data_store_layer_s) {
        List<Map<String, Object>> dqcDataNodes = new ArrayList<>();
        for (DataStoreLayer data_store_layer : data_store_layer_s) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.DQC.getCode() + "_" + data_store_layer.getDsl_id());
            map.put("label", data_store_layer.getDsl_name());
            map.put("parent_id", DataSourceType.DQC.getCode());
            map.put("data_layer", DataSourceType.DQC.getCode());
            map.put("dsl_id", data_store_layer.getDsl_id());
            map.put("description", "存储层编号: " + data_store_layer.getDsl_id() + "\n" + "存储层名称: " + data_store_layer.getDsl_name() + "\n" + "存储层描述: " + data_store_layer.getDsl_remark());
            dqcDataNodes.add(map);
        }
        return dqcDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchTableRs", desc = "", range = "")
    public static List<Map<String, Object>> conversionDQCTableInfos(List<Map<String, Object>> dqcTableInfos) {
        List<Map<String, Object>> dqcTableNodes = new ArrayList<>();
        dqcTableInfos.forEach(table_info -> {
            Map<String, Object> map = new HashMap<>();
            String dsl_id = table_info.get("dsl_id").toString();
            String file_id = table_info.get("record_id").toString();
            String table_name = table_info.get("table_name").toString();
            map.put("id", DataSourceType.DQC.getCode() + "_" + dsl_id + "_" + file_id);
            map.put("label", table_name);
            map.put("parent_id", DataSourceType.DQC.getCode() + "_" + dsl_id);
            map.put("description", "存储层名称: " + table_info.get("dsl_name") + "\n" + "登记表名称: " + table_name + "\n" + "表中文名称: " + table_name + "\n" + "原始表名称: " + table_name);
            map.put("data_layer", DataSourceType.DQC.getCode());
            map.put("dsl_id", dsl_id);
            map.put("file_id", file_id);
            map.put("table_name", table_name);
            map.put("original_name", table_name);
            map.put("hyren_name", table_name);
            dqcTableNodes.add(map);
        });
        return dqcTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_store_layer_s", desc = "", range = "")
    public static List<Map<String, Object>> conversionUDLDataInfos(List<DataStoreLayer> data_store_layer_s) {
        List<Map<String, Object>> udlDataNodes = new ArrayList<>();
        for (DataStoreLayer data_store_layer : data_store_layer_s) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.UDL.getCode() + "_" + data_store_layer.getDsl_id());
            map.put("label", data_store_layer.getDsl_name());
            map.put("parent_id", DataSourceType.UDL.getCode());
            map.put("data_layer", DataSourceType.UDL.getCode());
            map.put("dsl_id", data_store_layer.getDsl_id());
            map.put("description", "存储层编号: " + data_store_layer.getDsl_id() + "\n" + "存储层名称: " + data_store_layer.getDsl_name() + "\n" + "存储层描述: " + data_store_layer.getDsl_remark());
            udlDataNodes.add(map);
        }
        return udlDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchTableRs", desc = "", range = "")
    public static List<Map<String, Object>> conversionUDLTableInfos(List<Map<String, Object>> dqcTableInfos) {
        List<Map<String, Object>> udlTableNodes = new ArrayList<>();
        dqcTableInfos.forEach(table_info -> {
            Map<String, Object> map = new HashMap<>();
            String dsl_id = table_info.get("dsl_id").toString();
            String file_id = table_info.get("table_id").toString();
            String table_name = table_info.get("table_name").toString();
            String table_ch_name = table_info.get("ch_name").toString();
            map.put("id", DataSourceType.UDL.getCode() + "_" + dsl_id + "_" + file_id);
            map.put("label", table_name);
            map.put("parent_id", DataSourceType.UDL.getCode() + "_" + dsl_id);
            map.put("description", "存储层名称: " + table_info.get("dsl_name") + "\n" + "登记表名称: " + table_name + "\n" + "表中文名称: " + table_ch_name + "\n" + "原始表名称: " + table_name);
            map.put("data_layer", DataSourceType.UDL.getCode());
            map.put("dsl_id", dsl_id);
            map.put("file_id", file_id);
            map.put("table_name", table_name);
            map.put("original_name", table_name);
            map.put("hyren_name", table_name);
            udlTableNodes.add(map);
        });
        return udlTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionDataInfo() {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        Map<String, Object> consumeMap = new HashMap<>();
        consumeMap.put("id", DataSourceType.KFK.getCode() + "_consumer");
        consumeMap.put("label", "流数据分发");
        consumeMap.put("parent_id", DataSourceType.KFK.getCode());
        kfkDataNodes.add(consumeMap);
        Map<String, Object> receiveMap = new HashMap<>();
        receiveMap.put("id", DataSourceType.KFK.getCode() + "_job");
        receiveMap.put("label", "流数据分析");
        receiveMap.put("parent_id", DataSourceType.KFK.getCode());
        kfkDataNodes.add(receiveMap);
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "consumeReceiveList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionAnaKFKDataInfos(List<SdmSpJobinfo> consumeReceiveList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (SdmSpJobinfo spInfo : consumeReceiveList) {
            Map<String, Object> receiveMap = new HashMap<>();
            receiveMap.put("id", DataSourceType.KFK.getCode() + "_" + spInfo.getSsj_job_id());
            receiveMap.put("label", spInfo.getSsj_job_name());
            receiveMap.put("parent_id", DataSourceType.KFK.getCode() + "_job");
            receiveMap.put("ssj_job_id", spInfo.getSsj_job_id());
            receiveMap.put("description", "作业名称: " + spInfo.getSsj_job_name());
            kfkDataNodes.add(receiveMap);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "consumeInfoList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionKFKDataInfos(List<SdmConsumeConf> consumeInfoList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (SdmConsumeConf consumeInfo : consumeInfoList) {
            Map<String, Object> receiveMap = new HashMap<>();
            receiveMap.put("id", DataSourceType.KFK.getCode() + "_" + consumeInfo.getSdm_consum_id());
            receiveMap.put("label", consumeInfo.getSdm_cons_name());
            receiveMap.put("parent_id", DataSourceType.KFK.getCode() + "_consumer");
            receiveMap.put("sdm_consum_id", consumeInfo.getSdm_consum_id());
            receiveMap.put("description", "消费端配置名称: " + consumeInfo.getSdm_cons_name());
            kfkDataNodes.add(receiveMap);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionConsumeData(List<Map<String, Object>> dbLayerList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : dbLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id") + "_" + dbLayer.get("dsl_id"));
            map.put("label", dbLayer.get("dsl_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id"));
            map.put("description", "存储层名称: " + dbLayer.get("dsl_name"));
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionConsumeTableData(List<Map<String, Object>> dbLayerList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : dbLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id") + "_" + dbLayer.get("dsl_id") + "_" + dbLayer.get("sdm_con_db_id"));
            map.put("label", dbLayer.get("sdm_tb_name_en"));
            map.put("file_id", dbLayer.get("tab_id") + "," + dbLayer.get("dsl_id"));
            map.put("dsl_id", dbLayer.get("dsl_id"));
            map.put("data_layer", DataSourceType.KFK.getCode());
            map.put("table_name", dbLayer.get("sdm_tb_name_en"));
            map.put("hyren_name", dbLayer.get("sdm_tb_name_en"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id") + "_" + dbLayer.get("dsl_id"));
            map.put("description", "表英文名称: " + dbLayer.get("sdm_tb_name_en"));
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "hbLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionHBConsumeData(List<Map<String, Object>> hbLayerList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : hbLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id") + "_" + dbLayer.get("dsl_id"));
            map.put("label", dbLayer.get("dsl_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id"));
            map.put("description", "存储层名称: " + dbLayer.get("dsl_name"));
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "hbLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionHBTableConsumeData(List<Map<String, Object>> hbLayerList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : hbLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id") + "_" + dbLayer.get("dsl_id") + "_" + dbLayer.get("hbase_id"));
            map.put("label", dbLayer.get("hbase_name"));
            map.put("file_id", dbLayer.get("tab_id") + "," + dbLayer.get("dsl_id"));
            map.put("dsl_id", dbLayer.get("dsl_id"));
            map.put("table_name", dbLayer.get("hbase_name"));
            map.put("data_layer", DataSourceType.KFK.getCode());
            map.put("hyren_name", dbLayer.get("ssd_table_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("sdm_consum_id") + "_" + dbLayer.get("dsl_id"));
            map.put("description", "表英文名称: " + dbLayer.get("hbase_name"));
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "analyseLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionAnalyseConsumeData(List<Map<String, Object>> analyseLayerList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : analyseLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("ssj_job_id") + "_" + dbLayer.get("dsl_id"));
            map.put("label", dbLayer.get("dsl_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("ssj_job_id"));
            map.put("description", "存储层名称: " + dbLayer.get("dsl_name"));
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "analyseLayerList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionAnalyseTableConsumeData(List<Map<String, Object>> analyseLayerList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (Map<String, Object> dbLayer : analyseLayerList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("ssj_job_id") + "_" + dbLayer.get("dsl_id") + "_" + dbLayer.get("sdm_info_id"));
            map.put("label", dbLayer.get("ssd_table_name"));
            map.put("file_id", dbLayer.get("tab_id") + "," + dbLayer.get("dsl_id"));
            map.put("dsl_id", dbLayer.get("dsl_id"));
            map.put("table_name", dbLayer.get("ssd_table_name"));
            map.put("data_layer", DataSourceType.KFK.getCode());
            map.put("hyren_name", dbLayer.get("ssd_table_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + dbLayer.get("ssj_job_id") + "_" + dbLayer.get("dsl_id"));
            map.put("description", "表英文名称: " + dbLayer.get("ssd_table_name"));
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchDataInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionDCLRealtimeDataInfos(List<Map<String, Object>> dclBatchDataInfos) {
        List<Map<String, Object>> dclBatchDataNodes = new ArrayList<>();
        dclBatchDataInfos.forEach(o -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", TreeConstant.DCL_REALTIME + "_" + o.get("source_id"));
            map.put("label", o.get("datasource_name"));
            map.put("parent_id", TreeConstant.DCL_REALTIME);
            map.put("description", "数据源名称: " + o.get("datasource_name"));
            map.put("data_layer", DataSourceType.DCL.getCode());
            map.put("data_own_type", TreeConstant.DCL_REALTIME);
            map.put("data_source_id", o.get("source_id"));
            dclBatchDataNodes.add(map);
        });
        return dclBatchDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchClassifyInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> conversionDCLRealtimeClassifyInfos(Map<String, Object> dclRealtimeClassifyInfo) {
        Object classifyId = dclRealtimeClassifyInfo.get("classify_id");
        Map<String, Object> dclBatchClassifyNode = new HashMap<>();
        dclBatchClassifyNode.put("id", TreeConstant.DCL_REALTIME + "_" + classifyId);
        dclBatchClassifyNode.put("label", dclRealtimeClassifyInfo.get("classify_name") + "【" + dclRealtimeClassifyInfo.get("classify_num").toString() + "】");
        dclBatchClassifyNode.put("parent_id", TreeConstant.DCL_REALTIME + "_" + dclRealtimeClassifyInfo.get("source_id"));
        dclBatchClassifyNode.put("description", "分类名称: " + dclRealtimeClassifyInfo.get("classify_name") + "\n" + "分类描述: " + (dclRealtimeClassifyInfo.get("remark")));
        dclBatchClassifyNode.put("data_layer", DataSourceType.DCL.getCode());
        dclBatchClassifyNode.put("data_own_type", TreeConstant.DCL_REALTIME);
        dclBatchClassifyNode.put("data_source_id", dclRealtimeClassifyInfo.get("source_id"));
        dclBatchClassifyNode.put("classify_id", classifyId);
        return dclBatchClassifyNode;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dclBatchTableRs", desc = "", range = "")
    public static List<Map<String, Object>> conversionDCLRealtimeTableInfos(List<Map<String, Object>> dclTableInfos) {
        List<Map<String, Object>> dclBatchTableNodes = new ArrayList<>();
        dclTableInfos.forEach(o -> {
            Map<String, Object> map = new HashMap<>();
            String table_name = o.get("table_name").toString();
            String classify_name = o.get("classify_name").toString();
            String task_name = o.get("task_name").toString();
            String original_name = o.get("original_name").toString();
            String hyren_name = o.get("hyren_name").toString();
            String source_id = o.get("source_id").toString();
            String classify_id = o.get("classify_id").toString();
            String file_id = o.get("file_id").toString();
            map.put("id", file_id);
            map.put("label", hyren_name);
            map.put("parent_id", TreeConstant.DCL_REALTIME_TABLE + "_" + o.get("classify_id"));
            map.put("description", "任务名称: " + task_name + "\n" + "分类名称: " + classify_name + "\n" + "原文件名: " + original_name + "\n" + "原始表名: " + table_name + "\n" + "系统表名: " + hyren_name);
            map.put("data_layer", DataSourceType.DCL.getCode());
            map.put("data_own_type", TreeConstant.DCL_REALTIME_TABLE);
            map.put("data_source_id", source_id);
            map.put("classify_id", classify_id);
            map.put("file_id", file_id);
            map.put("table_name", table_name);
            map.put("hyren_name", hyren_name);
            map.put("original_name", original_name);
            dclBatchTableNodes.add(map);
        });
        return dclBatchTableNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "realtimeTopicInfos", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> conversionDCLRealtimeTopicInfos(List<Map<String, Object>> realtimeTopicInfos) {
        List<Map<String, Object>> cdcTopicNodes = new ArrayList<>();
        realtimeTopicInfos.forEach(o -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", o.get("topic_id"));
            Object top_name = o.get("sdm_top_name");
            map.put("label", top_name);
            map.put("parent_id", TreeConstant.DCL_REALTIME_TOPIC + "_" + o.get("classify_id"));
            Object top_cn_name = o.get("sdm_top_cn_name");
            Object sdm_zk_host = o.get("sdm_zk_host");
            Object sdm_bstp_serv = o.get("sdm_bstp_serv");
            Object sdm_partition = o.get("sdm_partition");
            Object sdm_replication = o.get("sdm_replication");
            String table_name = o.get("table_name").toString();
            String hyren_name = o.get("hyren_name").toString();
            map.put("data_layer", DataSourceType.DCL.getCode());
            map.put("data_own_type", TreeConstant.DCL_REALTIME_TOPIC);
            map.put("description", "Topic英文名: " + top_name + "\n" + "Topic中文名: " + top_cn_name + "\n" + "Topic分区数: " + sdm_partition + "\n" + "Topic分片数: " + sdm_replication + "\n" + "zk_host地址: " + sdm_zk_host + "\n" + "BootstrapServer地址: " + sdm_bstp_serv + "\n" + "原始表名: " + table_name + "\n" + "入库表名: " + hyren_name);
            cdcTopicNodes.add(map);
        });
        return cdcTopicNodes;
    }
}
