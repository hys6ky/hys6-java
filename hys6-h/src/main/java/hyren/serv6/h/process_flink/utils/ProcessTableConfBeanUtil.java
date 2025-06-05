package hyren.serv6.h.process_flink.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.codes.ProcessType;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAdded;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DcolRelationStore;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.entity.DmJobTableInfo;
import hyren.serv6.base.entity.DmModuleTable;
import hyren.serv6.base.entity.DmModuleTableFieldInfo;
import hyren.serv6.base.entity.DtabRelationStore;
import hyren.serv6.base.entity.SdmTopicInfo;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.SqlParamReplace;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.process_flink.bean.ProcessJobTableConfBean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessTableConfBeanUtil {

    public static ProcessJobTableConfBean getProcessTableConfBean(String moduleTableId, String jobTableId, String jobNameParam, String sqlParams) throws Exception {
        Validator.notBlank(moduleTableId, String.format("作业配置,模型表id不能为空: %s", moduleTableId));
        Validator.notBlank(moduleTableId, String.format("作业配置,加工表id不能为空: %s", jobTableId));
        Validator.notBlank(jobNameParam, String.format("作业配置,作业名不能为空: %s", jobNameParam));
        ProcessJobTableConfBean processJobTableConfBean = new ProcessJobTableConfBean(moduleTableId, jobTableId, jobNameParam, sqlParams);
        initBeans(processJobTableConfBean);
        return processJobTableConfBean;
    }

    public static void initBeans(ProcessJobTableConfBean processJobTableConfBean) throws Exception {
        Long moduleTableId = Long.parseLong(processJobTableConfBean.getModuleTableId());
        Long jobTableId = Long.parseLong(processJobTableConfBean.getJobTableId());
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DmModuleTable dmModuleTable = SqlOperator.queryOneObject(db, DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id = ?", moduleTableId).orElseThrow(() -> new Exception(String.format("无法查询到该模型表实体信息,ID为: %s", moduleTableId)));
            processJobTableConfBean.setDmModuleTable(dmModuleTable);
            DmJobTableInfo dmJobTableInfo = SqlOperator.queryOneObject(db, DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where module_table_id = ? and jobtab_id = ?", moduleTableId, jobTableId).orElse(null);
            Validator.notNull(dmJobTableInfo, String.format("作业表配置信息不存在,ID为: %s", jobTableId));
            processJobTableConfBean.setDmJobTableInfo(dmJobTableInfo);
            String tarTableName = dmJobTableInfo.getJobtab_en_name();
            processJobTableConfBean.setTarTableName(tarTableName);
            List<DmJobTableFieldInfo> dmJobTableFieldInfos = SqlOperator.queryList(db, DmJobTableFieldInfo.class, "select * from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ? order by jobtab_field_seq", jobTableId);
            Validator.notEmpty(dmJobTableFieldInfos, "根据表id: [ " + jobTableId + " ] 未找到对应的字段信息! 运行过和未运行都未找到数据!");
            handleFields(dmJobTableFieldInfos, processJobTableConfBean);
            processJobTableConfBean.setDmJobTableFieldInfos(dmJobTableFieldInfos);
            String beforeReplaceSql = replaceView(SqlParamReplace.replaceSqlParam(dmJobTableInfo.getJobtab_execute_sql(), processJobTableConfBean.getSqlParams()));
            processJobTableConfBean.setBeforeReplaceSql(beforeReplaceSql);
            processJobTableConfBean.setCompleteSql(ProcessingData.getdStoreReg(beforeReplaceSql, db));
            DtabRelationStore dtabRelationStore = SqlOperator.queryOneObject(db, DtabRelationStore.class, "select * from " + DtabRelationStore.TableName + " where tab_id = ?", moduleTableId).orElse(null);
            Validator.notNull(dtabRelationStore, String.format("无法查询到该模型表对应存储关系表信息,加工ID为:%s", moduleTableId));
            processJobTableConfBean.setDtabRelationStore(dtabRelationStore);
            DataStoreLayer dataStoreLayer = SqlOperator.queryOneObject(db, DataStoreLayer.class, "select * from data_store_layer where dsl_id = ?", dtabRelationStore.getDsl_id()).orElse(null);
            Validator.notNull(dataStoreLayer, String.format("无法查询到该模型表对应存储层信息,存储层ID为:%s", dtabRelationStore.getDsl_id()));
            processJobTableConfBean.setDataStoreLayer(dataStoreLayer);
            List<DataStoreLayerAttr> dataStoreLayerAttrs = SqlOperator.queryList(db, DataStoreLayerAttr.class, "select * from data_store_layer_attr where dsl_id = ?", dtabRelationStore.getDsl_id());
            Validator.notEmpty(dataStoreLayerAttrs, String.format("无法查询到该模型表对应存储层配置属性信息,存储层ID为: %s", dtabRelationStore.getDsl_id()));
            processJobTableConfBean.setDataStoreLayerAttrs(dataStoreLayerAttrs);
            Map<String, List<String>> fieldAdditionalInfoMap = new HashMap<>();
            Result result = SqlOperator.queryResult(db, "select mtfi.field_en_name,dsla.dsla_storelayer" + " from " + DmModuleTableFieldInfo.TableName + " mtfi" + " join " + DcolRelationStore.TableName + " dcs" + " on mtfi.module_field_id = dcs.col_id" + " join " + DataStoreLayerAdded.TableName + " dsla" + " on dcs.dslad_id = dsla.dslad_id" + " where mtfi.module_table_id = ?", moduleTableId);
            for (int i = 0; i < result.getRowCount(); i++) {
                String dsla_storelayer = result.getString(i, "dsla_storelayer");
                List<String> list = fieldAdditionalInfoMap.get(dsla_storelayer);
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(result.getString(i, "field_en_name"));
                fieldAdditionalInfoMap.put(dsla_storelayer, list);
            }
            processJobTableConfBean.setFieldAdditionalInfoMap(fieldAdditionalInfoMap);
            List<String> primaryColumnList = new ArrayList<String>();
            fieldAdditionalInfoMap.forEach((k, v) -> {
                if (StoreLayerAdded.ZhuJian == StoreLayerAdded.ofEnumByCode(k)) {
                    primaryColumnList.addAll(v);
                }
            });
            processJobTableConfBean.setPrimaryKeyInfos(primaryColumnList);
            log.info("模型表: [ {} ], 字段附加信息: [ {} ]", tarTableName, JsonUtil.toJson(fieldAdditionalInfoMap));
            Map<String, List<LayerBean>> layerBeansByTableMap = new HashMap<>();
            List<String> tableNameList = DruidParseQuerySql.parseSqlTableToList(processJobTableConfBean.getBeforeReplaceSql());
            Map<String, SdmTopicInfo> kafkaBeansByTableMap = new HashMap<String, SdmTopicInfo>();
            for (String tableName : tableNameList) {
                try {
                    List<LayerBean> layerBeans = ProcessingData.getLayerByTable(tableName, db);
                    layerBeansByTableMap.put(tableName, layerBeans);
                } catch (Exception e) {
                    SdmTopicInfo topic = SqlOperator.queryOneObject(db, SdmTopicInfo.class, "select * from " + SdmTopicInfo.TableName + " where sdm_top_name = ? ", tableName).orElse(null);
                    Validator.notNull(topic, "未找到存储层与主题：" + tableName);
                    kafkaBeansByTableMap.put(tableName, topic);
                }
            }
            processJobTableConfBean.setLayerBeansByTableMap(layerBeansByTableMap);
            processJobTableConfBean.setTopicBeansByTableMap(kafkaBeansByTableMap);
            Map<String, List<TableColumn>> topicColumnMap = new HashMap<String, List<TableColumn>>();
            for (String topic : kafkaBeansByTableMap.keySet()) {
                List<TableColumn> tableColumns = SqlOperator.queryList(db, TableColumn.class, "select tc.* from table_column tc left join table_storage_info tsi on tc.table_id = tsi.table_id where tsi.hyren_name = ?", topic);
                Validator.notEmpty(tableColumns, "未找到" + topic + "数据类型");
                topicColumnMap.put(topic, tableColumns);
            }
            processJobTableConfBean.setTopicColumnMap(topicColumnMap);
        }
    }

    private static String replaceView(String perhapsWithViewSql) {
        return new DruidParseQuerySql().GetNewSql(perhapsWithViewSql);
    }

    private static void handleFields(List<DmJobTableFieldInfo> dmJobTableFieldInfos, ProcessJobTableConfBean processJobTableConfBean) throws Exception {
        DmJobTableFieldInfo etl_job_nm_jtfi = new DmJobTableFieldInfo();
        etl_job_nm_jtfi.setJobtab_field_en_name(Constant._HYREN_JOB_NAME);
        etl_job_nm_jtfi.setJobtab_field_type(Constant._VARCAHR_300);
        dmJobTableFieldInfos.add(etl_job_nm_jtfi);
        if (StorageType.ZhuiJia.equals(StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type()))) {
            DmJobTableFieldInfo data_insr_dt_tfi = new DmJobTableFieldInfo();
            data_insr_dt_tfi.setJobtab_field_en_name(Constant._HYREN_S_DATE);
            data_insr_dt_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(data_insr_dt_tfi);
        }
        dmJobTableFieldInfos.forEach(dmJobTableFieldInfo -> dmJobTableFieldInfo.setJobtab_field_en_name(dmJobTableFieldInfo.getJobtab_field_en_name().toLowerCase()));
    }

    public static final String PROCESS_SERIALIZATION_FILE_DIR = System.getProperty("user.dir") + File.separator + "sparkAppConfDir" + File.separator;

    static {
        try {
            FileUtil.forceMkdir(new File(PROCESS_SERIALIZATION_FILE_DIR));
        } catch (IOException e) {
            throw new RuntimeException(String.format("创建用来保存加工任务的作业序列化配置文件目录失败! path: %s , 异常: %s", PROCESS_SERIALIZATION_FILE_DIR, e));
        }
    }

    public static void generateJobSerializeFile(ProcessJobTableConfBean processJobTableConfBean) throws IOException {
        String serializeFileName = processJobTableConfBean.getModuleTableId() + "_" + processJobTableConfBean.getJobTableId();
        File serializeFile = FileUtil.getFile(PROCESS_SERIALIZATION_FILE_DIR + serializeFileName);
        if (serializeFile.exists()) {
            FileUtil.forceDelete(serializeFile);
        }
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(serializeFile.toPath()))) {
            out.writeObject(processJobTableConfBean);
            log.info(String.format("Successfully serialized %s object into %s !", ProcessJobTableConfBean.class.getSimpleName(), serializeFile.getAbsolutePath()));
        } catch (IOException e) {
            throw new IOException(String.format("Serializing %s object into %s failed! ", ProcessJobTableConfBean.class.getSimpleName(), serializeFile.getAbsolutePath()), e);
        }
    }

    public static ProcessJobTableConfBean parsingJobSerializeFile(String serializeFileName) throws Exception {
        Object o;
        FileInputStream fileInputStream;
        try {
            File ddId_doiId_md5_file = new File(PROCESS_SERIALIZATION_FILE_DIR + serializeFileName);
            if (ddId_doiId_md5_file.exists()) {
                fileInputStream = new FileInputStream(ddId_doiId_md5_file);
            } else {
                log.warn("No configuration files found in the configuration directory" + " [ " + PROCESS_SERIALIZATION_FILE_DIR + " ]," + "attempt to find in the program running directory! file: " + serializeFileName);
                fileInputStream = new FileInputStream(serializeFileName);
            }
        } catch (FileNotFoundException e) {
            throw new Exception("No configuration files were found in both the configuration directory" + " and the current program running directory ! ");
        }
        try (ObjectInputStream in = new ObjectInputStream(fileInputStream)) {
            o = in.readObject();
            log.info(String.format("Successfully deserialized %s object to %s", serializeFileName, ProcessJobTableConfBean.class.getSimpleName()));
        } catch (Exception e) {
            throw new Exception(String.format("Deserialization of %s object to %s failed! ", ProcessJobTableConfBean.class.getName(), e));
        }
        if (o instanceof ProcessJobTableConfBean) {
            return (ProcessJobTableConfBean) o;
        } else {
            throw new Exception("File: " + serializeFileName + " Non ProcessConfBean object serialization file!");
        }
    }
}
