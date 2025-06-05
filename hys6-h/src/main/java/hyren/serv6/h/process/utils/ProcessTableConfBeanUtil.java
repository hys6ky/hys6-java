package hyren.serv6.h.process.utils;

import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.codes.ProcessType;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.SqlParamReplace;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import lombok.extern.slf4j.Slf4j;
import java.io.*;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.*;

@Slf4j
public class ProcessTableConfBeanUtil {

    public static ProcessJobTableConfBean getProcessTableConfBean(String moduleTableId, String jobTableId, String etldate, String jobNameParam, String sqlParams) throws Exception {
        Validator.notBlank(moduleTableId, String.format("作业配置,模型表id不能为空: %s", moduleTableId));
        Validator.notBlank(moduleTableId, String.format("作业配置,加工表id不能为空: %s", jobTableId));
        if (!DateUtil.validDateStr(etldate)) {
            throw new IllegalArgumentException(String.format("跑批日期不合法: %s", etldate));
        }
        Validator.notBlank(jobNameParam, String.format("作业配置,作业名不能为空: %s", jobNameParam));
        ProcessJobTableConfBean processJobTableConfBean = new ProcessJobTableConfBean(moduleTableId, jobTableId, etldate, jobNameParam, sqlParams);
        initBeans(processJobTableConfBean);
        checkReRun(processJobTableConfBean, etldate);
        return processJobTableConfBean;
    }

    public static void initBeans(ProcessJobTableConfBean processJobTableConfBean) throws Exception {
        Long moduleTableId = Long.parseLong(processJobTableConfBean.getModuleTableId());
        Long jobTableId = Long.parseLong(processJobTableConfBean.getJobTableId());
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DmModuleTable dmModuleTable = SqlOperator.queryOneObject(db, DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id = ?", moduleTableId).orElseThrow(() -> new Exception(String.format("无法查询到该模型表实体信息,ID为: %s", moduleTableId)));
            processJobTableConfBean.setDmModuleTable(dmModuleTable);
            StorageType storageType = StorageType.ofEnumByCode(dmModuleTable.getStorage_type());
            if (storageType == StorageType.QuanLiang || storageType == StorageType.LiShiLaLian || storageType == StorageType.ZengLiang) {
                processJobTableConfBean.setIsZipperFlag(IsFlag.Shi);
            } else if (storageType == StorageType.ZhuiJia || storageType == StorageType.TiHuan || storageType == StorageType.UpSet) {
                processJobTableConfBean.setIsZipperFlag(IsFlag.Fou);
            } else {
                throw new Exception("设置 IsZipperFlag 未知的进数方式! StorageType: " + storageType.getValue());
            }
            DmJobTableInfo dmJobTableInfo = SqlOperator.queryOneObject(db, DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where module_table_id = ? and jobtab_id = ?", moduleTableId, jobTableId).orElseThrow(() -> new BusinessException(String.format("作业表配置信息不存在,ID为: %s", jobTableId)));
            processJobTableConfBean.setDmJobTableInfo(dmJobTableInfo);
            processJobTableConfBean.setIsTempFlag(IsFlag.ofEnumByCode(dmJobTableInfo.getJobtab_is_temp()));
            String tarTableName = dmJobTableInfo.getJobtab_en_name();
            processJobTableConfBean.setTarTableName(tarTableName);
            List<DmJobTableFieldInfo> dmJobTableFieldInfos = SqlOperator.queryList(db, DmJobTableFieldInfo.class, "select * from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ? order by jobtab_field_seq", jobTableId);
            if (dmJobTableFieldInfos.isEmpty()) {
                throw new Exception("根据表id: [ " + jobTableId + " ] 未找到对应的字段信息! 运行过和未运行都未找到数据!");
            }
            handleFields(dmJobTableFieldInfos, processJobTableConfBean);
            processJobTableConfBean.setDmJobTableFieldInfos(dmJobTableFieldInfos);
            for (DmJobTableFieldInfo field_info : dmJobTableFieldInfos) {
                if (field_info.getJobtab_field_process() != null) {
                    ProcessType pt = ProcessType.ofEnumByCode(field_info.getJobtab_field_process());
                    if (pt == ProcessType.FenZhuYingShe) {
                        processJobTableConfBean.setGroup(true);
                        break;
                    }
                }
            }
            String beforeReplaceSql = replaceView(SqlParamReplace.replaceSqlParam(dmJobTableInfo.getJobtab_execute_sql(), processJobTableConfBean.getSqlParams()));
            processJobTableConfBean.setBeforeReplaceSql(beforeReplaceSql);
            processJobTableConfBean.setCompleteSql(ProcessingData.getdStoreReg(beforeReplaceSql, db));
            DtabRelationStore dtabRelationStore = SqlOperator.queryOneObject(db, DtabRelationStore.class, "select * from " + DtabRelationStore.TableName + " where tab_id = ?", moduleTableId).orElseThrow(() -> new Exception(String.format("无法查询到该模型表对应存储关系表信息,加工ID为:%s", moduleTableId)));
            processJobTableConfBean.setDtabRelationStore(dtabRelationStore);
            Long dslId = dtabRelationStore.getDsl_id();
            DataStoreLayer dataStoreLayer = SqlOperator.queryOneObject(db, DataStoreLayer.class, "select * from data_store_layer where dsl_id = ?", dslId).orElseThrow(() -> new Exception(String.format("无法查询到该模型表对应存储层信息,存储层ID为:%s", dslId)));
            processJobTableConfBean.setDataStoreLayer(dataStoreLayer);
            List<DataStoreLayerAttr> dataStoreLayerAttrs = SqlOperator.queryList(db, DataStoreLayerAttr.class, "select * from data_store_layer_attr where dsl_id = ?", dslId);
            Validator.notEmpty(dataStoreLayerAttrs, String.format("无法查询到该模型表对应存储层配置属性信息,存储层ID为: %s", dslId));
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
            log.info("模型表: [ {} ], 字段附加信息: [ {} ]", tarTableName, JsonUtil.toJson(fieldAdditionalInfoMap));
            Map<String, List<LayerBean>> layerBeansByTableMap = new HashMap<>();
            List<String> tableNameList = DruidParseQuerySql.parseSqlTableToList(processJobTableConfBean.getBeforeReplaceSql());
            for (String tableName : tableNameList) {
                List<LayerBean> layerBeans = ProcessingData.getLayerByTable(tableName, db);
                layerBeansByTableMap.put(tableName, layerBeans);
            }
            processJobTableConfBean.setLayerBeansByTableMap(layerBeansByTableMap);
        }
    }

    public static void checkReRun(ProcessJobTableConfBean processJobTableConfBean, String etlDate) {
        if (processJobTableConfBean.getIsTempFlag() == IsFlag.Shi) {
            processJobTableConfBean.setReRun(Boolean.FALSE);
        } else {
            JobExecuteState executeState = JobExecuteState.ofEnumByCode(processJobTableConfBean.getDtabRelationStore().getIs_successful());
            if (executeState == JobExecuteState.WanCheng) {
                LocalDate etlLocalDate = DateUtil.parseStr2DateWith8Char(etlDate);
                String curDate = processJobTableConfBean.getDmModuleTable().getEtl_date();
                LocalDate curLocalDate = DateUtil.parseStr2DateWith8Char(curDate);
                if (etlLocalDate.isBefore(curLocalDate)) {
                    throw new AppSystemException(String.format("重跑日期不能小于当前调度日期! etlDate: [ %s ] currentDate: [ %s ]", etlDate, curDate));
                }
                if (etlDate.equalsIgnoreCase(curDate)) {
                    processJobTableConfBean.setReRun(Boolean.TRUE);
                } else {
                    processJobTableConfBean.setReRun(Boolean.FALSE);
                }
            } else {
                processJobTableConfBean.setReRun(Boolean.TRUE);
            }
        }
    }

    private static String replaceView(String perhapsWithViewSql) {
        return new DruidParseQuerySql().GetNewSql(perhapsWithViewSql);
    }

    private static void handleFields(List<DmJobTableFieldInfo> dmJobTableFieldInfos, ProcessJobTableConfBean processJobTableConfBean) throws Exception {
        if (dmJobTableFieldInfos.isEmpty()) {
            throw new Exception("从数据库中获取的字段数量为0");
        }
        boolean flag = true;
        DmJobTableFieldInfo mappingFieldInfo = null;
        List<Integer> indexList = new ArrayList<>();
        for (int i = 0; i < dmJobTableFieldInfos.size(); i++) {
            DmJobTableFieldInfo field_info = dmJobTableFieldInfos.get(i);
            ProcessType processType = ProcessType.ofEnumByCode(field_info.getJobtab_field_process());
            if (field_info.getJobtab_field_process() != null && processType == ProcessType.FenZhuYingShe) {
                if (flag) {
                    mappingFieldInfo = new DmJobTableFieldInfo();
                    List<String> split = StringUtil.split(field_info.getJobtab_group_mapping(), "=");
                    mappingFieldInfo.setJobtab_field_en_name(split.get(0));
                    mappingFieldInfo.setJobtab_field_cn_name(split.get(0));
                    mappingFieldInfo.setJobtab_field_process(ProcessType.YingShe.getCode());
                    mappingFieldInfo.setJobtab_field_type(Constant._VARCAHR_300);
                    mappingFieldInfo.setJobtab_field_length(" ");
                    mappingFieldInfo.setJobtab_process_mapping(split.get(1));
                    flag = false;
                } else {
                    indexList.add(i);
                }
            }
        }
        Collections.reverse(indexList);
        for (int i : indexList) {
            dmJobTableFieldInfos.remove(i);
        }
        if (mappingFieldInfo != null) {
            dmJobTableFieldInfos.add(mappingFieldInfo);
        }
        DmJobTableFieldInfo etl_job_nm_jtfi = new DmJobTableFieldInfo();
        etl_job_nm_jtfi.setJobtab_field_en_name(Constant._HYREN_JOB_NAME);
        etl_job_nm_jtfi.setJobtab_field_type(Constant._VARCAHR_300);
        dmJobTableFieldInfos.add(etl_job_nm_jtfi);
        if (IsFlag.Shi == processJobTableConfBean.getIsZipperFlag()) {
            DmJobTableFieldInfo hyren_s_date_tfi = new DmJobTableFieldInfo();
            hyren_s_date_tfi.setJobtab_field_en_name(Constant._HYREN_S_DATE);
            hyren_s_date_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(hyren_s_date_tfi);
            DmJobTableFieldInfo hyren_e_date_tfi = new DmJobTableFieldInfo();
            hyren_e_date_tfi.setJobtab_field_en_name(Constant._HYREN_E_DATE);
            hyren_e_date_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(hyren_e_date_tfi);
            DmJobTableFieldInfo hyren_md5_val_tfi = new DmJobTableFieldInfo();
            hyren_md5_val_tfi.setJobtab_field_en_name(Constant._HYREN_MD5_VAL);
            hyren_md5_val_tfi.setJobtab_field_type(Constant._VARCAHR_32);
            dmJobTableFieldInfos.add(hyren_md5_val_tfi);
        } else {
            StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
            DmJobTableFieldInfo data_insr_dt_tfi = new DmJobTableFieldInfo();
            data_insr_dt_tfi.setJobtab_field_en_name(Constant._HYREN_S_DATE);
            data_insr_dt_tfi.setJobtab_field_type(Constant._VARCAHR_8);
            dmJobTableFieldInfos.add(data_insr_dt_tfi);
            if (storageType == StorageType.UpSet) {
                DmJobTableFieldInfo hyren_md5_val_tfi = new DmJobTableFieldInfo();
                hyren_md5_val_tfi.setJobtab_field_en_name(Constant._HYREN_MD5_VAL);
                hyren_md5_val_tfi.setJobtab_field_type(Constant._VARCAHR_32);
                dmJobTableFieldInfos.add(hyren_md5_val_tfi);
            }
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
