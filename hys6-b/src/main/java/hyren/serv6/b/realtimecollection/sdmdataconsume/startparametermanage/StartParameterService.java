package hyren.serv6.b.realtimecollection.sdmdataconsume.startparametermanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.realtimecollection.util.KafkaBeanUtil;
import hyren.serv6.b.realtimecollection.util.StreamingProRunner;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@DocClass(desc = "", author = "yec", createdate = "2021-05-07")
@Service
@Slf4j
public class StartParameterService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_sp_param", desc = "", range = "", isBean = true)
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "is_add", desc = "", range = "")
    public void saveStartParameters(SdmSpParam[] sdm_sp_param, long ssj_job_id, String is_add) {
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Dbo.execute("DELETE FROM sdm_sp_param WHERE ssj_job_id = ?", ssj_job_id);
        }
        for (SdmSpParam spParam : sdm_sp_param) {
            spParam.setSsp_param_id(PrimayKeyGener.getNextId());
            spParam.setSsj_job_id(ssj_job_id);
            if (IsFlag.Shi == IsFlag.ofEnumByCode(spParam.getIs_customize())) {
                spParam.setIs_customize(IsFlag.Shi.getCode());
            } else {
                spParam.setIs_customize(IsFlag.Fou.getCode());
            }
            spParam.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    public Map<String, Object> getJobParamsList(long ssj_job_id) {
        Map<String, Object> map = new HashMap<>();
        Result customizeResult = Dbo.queryResult("SELECT * FROM sdm_sp_param WHERE ssj_job_id = ? AND is_customize = ? ", ssj_job_id, IsFlag.Shi.getCode());
        Result typicalResult = Dbo.queryResult("SELECT * FROM sdm_sp_param WHERE ssj_job_id = ? AND is_customize = ? ", ssj_job_id, IsFlag.Fou.getCode());
        Map<String, Object> typicalMsg = new HashMap<>();
        List<Map<String, Object>> customizeMsg = new ArrayList<>();
        if (!customizeResult.isEmpty()) {
            for (int i = 0; i < customizeResult.getRowCount(); i++) {
                Map<String, Object> jsonMsg = new HashMap<>();
                jsonMsg.put("customize_param_key", customizeResult.getString(i, "ssp_param_key"));
                jsonMsg.put("customize_param_value", customizeResult.getString(i, "ssp_param_value"));
                customizeMsg.add(jsonMsg);
            }
        }
        for (int i = 0; i < typicalResult.getRowCount(); i++) {
            typicalMsg.put(typicalResult.getString(i, "ssp_param_key"), typicalResult.getString(i, "ssp_param_value"));
        }
        Result result = Dbo.queryResult("SELECT ssj_job_name FROM sdm_sp_jobinfo WHERE ssj_job_id = ? ", ssj_job_id);
        String ssj_job_name = result.getString(0, "ssj_job_name");
        map.put("typical", typicalMsg);
        map.put("customize", customizeMsg);
        map.put("ssj_job_name", ssj_job_name);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    public void startTask(long ssj_job_id) {
        List<Map<String, Object>> array = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> paramMap = new HashMap<>();
        String path = System.getProperty("user.dir");
        Map<String, Object> jobMap = Dbo.queryOneObject("SELECT t1.ssj_job_name,t1.ssj_job_desc,t1.ssj_strategy," + "t1.ssj_job_id,t2.ssp_param_value as ENGINE from " + SdmSpJobinfo.TableName + " t1 LEFT JOIN " + SdmSpParam.TableName + " t2 ON t1.ssj_job_id = t2.ssj_job_id " + "where t2.ssp_param_key = 'streaming_platform' AND t1.ssj_job_id = ? ", ssj_job_id);
        String engine = jobMap.get("engine").toString();
        map.put("desc", jobMap.get("ssj_job_desc"));
        map.put("strategy", jobMap.get("ssj_strategy"));
        map.put("algorithm", new ArrayList<>());
        map.put("ref", new ArrayList<>());
        map.put("configParams", new HashMap<>());
        addInputConfig(ssj_job_id, engine, array);
        addAnalysisConfig(ssj_job_id, engine, array);
        addOutputConfig(ssj_job_id, engine, array);
        map.put("compositor", array);
        paramMap.put(jobMap.get("ssj_job_name").toString(), map);
        String filename = ssj_job_id + ".json";
        KafkaBeanUtil.writeJsonFile(path, JsonUtil.toJson(paramMap), filename);
        startup(ssj_job_id, path, filename);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "engine", desc = "", range = "")
    @Param(name = "array", desc = "", range = "", nullable = true)
    public void addInputConfig(long ssj_job_id, String engine, List<Map<String, Object>> array) {
        Map<String, Object> inputMap = new HashMap<>();
        List<Map<String, Object>> arrayInfo = new ArrayList<>();
        if ("spark".equals(engine)) {
            inputMap.put("name", "batch.sources");
        } else if ("ss".equals(engine)) {
            inputMap.put("name", "ss.sources");
        } else {
            inputMap.put("name", "stream.sources");
        }
        List<Map<String, Object>> fileList = Dbo.queryList("select * from " + SdmJobInput.TableName + " t1 right join " + SdmSpTextfile.TableName + " t2 on t1.sdm_info_id = t2.sdm_info_id where t1.ssj_job_id = ?", ssj_job_id);
        if (!fileList.isEmpty()) {
            for (Map<String, Object> fileMap : fileList) {
                Map<String, Object> map = new HashMap<>();
                if (SdmSpFileType.CSV == SdmSpFileType.ofEnumByCode(fileMap.get("sst_file_type").toString())) {
                    map.put("format", "csv");
                    map.put("header", IsFlag.Shi.getValue().equals(fileMap.get("sst_is_header")) ? "true" : "false");
                } else if (SdmSpFileType.PARQUENT == SdmSpFileType.ofEnumByCode(fileMap.get("sst_file_type").toString())) {
                    map.put("format", "parquet");
                    if (!StringUtil.isEmpty(fileMap.get("sst_schema").toString())) {
                        map.put("schema", String.valueOf(fileMap.get("sst_schema")));
                    }
                } else {
                    map.put("format", "json");
                }
                map.put("path", fileMap.get("sst_file_path").toString());
                map.put("outputTable", fileMap.get("input_table_name").toString());
                arrayInfo.add(map);
            }
        }
        List<Map<String, Object>> baseList = Dbo.queryList("select * from " + SdmJobInput.TableName + " t1 right join " + SdmInputDatabase.TableName + " t2 on t1.sdm_info_id = t2.sdm_info_id where t1.ssj_job_id = ?", ssj_job_id);
        if (!baseList.isEmpty()) {
            for (Map<String, Object> baseMap : baseList) {
                HashMap<String, Object> map = new HashMap<>();
                map.put("url", baseMap.get("ssd_jdbc_url").toString());
                map.put("dbtable", baseMap.get("ssd_table_name").toString());
                map.put("driver", baseMap.get("ssd_database_drive").toString());
                map.put("format", "jdbc");
                map.put("outputTable", baseMap.get("input_table_name").toString());
                map.put("user", baseMap.get("ssd_user_name").toString());
                map.put("password", baseMap.get("ssd_user_password").toString());
                map.put("path", "-");
                arrayInfo.add(map);
            }
        }
        List<Map<String, Object>> streamList = Dbo.queryList("select * from " + SdmJobInput.TableName + " t1 right join " + SdmSpStream.TableName + " t2 on t1.sdm_info_id = t2.sdm_info_id where t1.ssj_job_id = ?", ssj_job_id);
        if (!streamList.isEmpty()) {
            for (Map<String, Object> streamMap : streamList) {
                HashMap<String, Object> map = new HashMap<>();
                if (SdmSpStreamVer.KAFKA.getCode().equals(streamMap.get("sss_kafka_version"))) {
                    if ("spark".equals(engine)) {
                        map.put("subscribe", streamMap.get("sss_topic_name").toString());
                        map.put("kafka.bootstrap.servers", streamMap.get("sss_bootstrap_server").toString());
                    } else if ("ss".equals(engine)) {
                        map.put("subscribe", streamMap.get("sss_topic_name").toString());
                        map.put("kafka.bootstrap.servers", streamMap.get("sss_bootstrap_server").toString());
                    } else {
                        map.put("topics", streamMap.get("sss_topic_name").toString());
                        map.put("bootstrap.servers", streamMap.get("sss_bootstrap_server").toString());
                    }
                    map.put("format", SdmSpStreamVer.KAFKA.getValue());
                } else if (SdmSpStreamVer.KAFKA8.getCode().equals(streamMap.get("sss_kafka_version"))) {
                    map.put("topics", streamMap.get("sss_topic_name").toString());
                    map.put("format", SdmSpStreamVer.KAFKA8.getValue());
                } else {
                    map.put("topics", streamMap.get("sss_topic_name").toString());
                    map.put("format", SdmSpStreamVer.KAFKA9.getValue());
                }
                map.put("outputTable", streamMap.get("input_table_name").toString());
                map.put("startingOffsets", streamMap.get("sss_consumer_offset").toString());
                map.put("path", "-");
                arrayInfo.add(map);
            }
        }
        inputMap.put("params", arrayInfo);
        array.add(inputMap);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "engine", desc = "", range = "")
    @Param(name = "array", desc = "", range = "", nullable = true)
    public void addAnalysisConfig(long ssj_job_id, String engine, List<Map<String, Object>> array) {
        Map<String, Object> arrayMap = new HashMap<>();
        List<Map<String, Object>> arrayInfo = new ArrayList<>();
        List<SdmSpAnalysis> analyList = Dbo.queryList(SdmSpAnalysis.class, "select * from " + SdmSpAnalysis.TableName + " where ssj_job_id = ?  order by analysis_number", ssj_job_id);
        if (!analyList.isEmpty()) {
            for (SdmSpAnalysis analyInfo : analyList) {
                if ("spark".equals(engine)) {
                    arrayMap.put("name", "batch.sql");
                } else if ("ss".equals(engine)) {
                    arrayMap.put("name", "ss.sql");
                } else {
                    arrayMap.put("name", "stream.sql");
                }
                HashMap<String, Object> map = new HashMap<>();
                map.put("sql", StringUtil.unicode2String(analyInfo.getAnalysis_sql()));
                map.put("outputTableName", analyInfo.getAnalysis_table_name());
                arrayInfo.add(map);
                arrayMap.put("params", arrayInfo);
                array.add(arrayMap);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "path", desc = "", range = "", nullable = true)
    @Param(name = "filename", desc = "", range = "")
    public void startup(long ssj_job_id, String path, String filename) {
        List<SdmSpParam> paramList = Dbo.queryList(SdmSpParam.class, "select * from " + SdmSpParam.TableName + " where ssj_job_id = ?", ssj_job_id);
        StreamingProRunner runner = new StreamingProRunner(path, paramList, filename, ssj_job_id);
        runner.runJob();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "engine", desc = "", range = "")
    @Param(name = "array", desc = "", range = "", nullable = true)
    public void addOutputConfig(long ssj_job_id, String engine, List<Map<String, Object>> array) {
        Map<String, Object> outMap = new HashMap<>();
        List<Map<String, Object>> arrayInfo = new ArrayList<>();
        String tableName = "";
        String format = "";
        if ("spark".equals(engine)) {
            outMap.put("name", "batch.outputs");
        } else if ("ss".equals(engine)) {
            outMap.put("name", "ss.outputs");
        } else {
            outMap.put("name", "stream.outputs");
        }
        Result resultList = Dbo.queryResult("select * from " + SdmSpParam.TableName + " where ssj_job_id = ?", ssj_job_id);
        Result jobResult = Dbo.queryResult("select * from " + SdmSpJobinfo.TableName + " where ssj_job_id = ?", ssj_job_id);
        List<Map<String, Object>> fileList = Dbo.queryList("select * from " + SdmSpOutput.TableName + " t1 right join " + SdmSpTextfile.TableName + " t2 on t1.sdm_info_id = t2.sdm_info_id where t1.ssj_job_id = ?", ssj_job_id);
        if (!fileList.isEmpty()) {
            for (Map<String, Object> fileMap : fileList) {
                HashMap<String, Object> map = new HashMap<>();
                if (SdmSpFileType.CSV.getCode().equals(fileMap.get("sst_file_type"))) {
                    map.put("format", "csv");
                    map.put("header", IsFlag.Shi.getValue().equals(fileMap.get("sst_is_header")) ? "true" : "false");
                } else if (SdmSpFileType.PARQUENT.getCode().equals(fileMap.get("sst_file_type"))) {
                    map.put("format", "parquet");
                    if (!StringUtil.isEmpty(fileMap.get("sst_schema").toString())) {
                        map.put("schema", String.valueOf(fileMap.get("sst_schema")));
                    }
                } else {
                    map.put("format", "json");
                }
                tableName = fileMap.get("output_table_name").toString();
                format = map.get("format").toString();
                for (int i = 0; i < resultList.getRowCount(); i++) {
                    if ("streaming_checkpoint".equals(resultList.getString(i, "ssp_param_key"))) {
                        if (StringUtil.isNotEmpty(resultList.getString(i, "ssp_param_value"))) {
                            map.put("checkpointLocation", "/hrds/" + resultList.getString(i, "ssp_param_value"));
                        } else {
                            map.put("checkpointLocation", "/tmp/" + tableName + "/" + format);
                        }
                    }
                }
                map.put("path", "/hrds" + fileMap.get("sst_file_path").toString() + "/" + jobResult.getString(0, "ssj_job_name") + "/" + map.get("format") + "/");
                map.put("inputTableName", fileMap.get("output_table_name").toString());
                map.put("mode", SdmSpOutputMode.ofValueByCode(fileMap.get("output_mode").toString()));
                Optional<StreamproSetting> streamproInfo = Dbo.queryOneObject(StreamproSetting.class, "select * from " + StreamproSetting.TableName + " where sdm_info_id = ?", fileMap.get("sdm_info_id"));
                if (streamproInfo.isPresent()) {
                    StreamproSetting settingInfo = streamproInfo.get();
                    map.put("httpUrl", settingInfo.getRs_url());
                    map.put("format", "http");
                    Map rs_para = JsonUtil.toObject(JsonUtil.toJson(settingInfo.getRs_para()), new TypeReference<Map>() {
                    });
                    Set<String> set = rs_para.keySet();
                    List<String> params = new ArrayList<>(set);
                    map.put("requestParams", String.join(",", params));
                    map.put("requestMode", "line");
                    if (settingInfo.getRs_processing().equals(SdmSpRsType.KafKa.getCode())) {
                        HashMap<String, String> streamMap = new HashMap<>();
                        SdmRestStream strInfo = Dbo.queryOneObject(SdmRestStream.class, "select * from " + SdmRestStream.TableName + " where rs_id = ?", settingInfo.getRs_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                        streamMap.put("mode", SdmSpOutputMode.ofValueByCode(fileMap.get("output_mode").toString()));
                        streamMap.put("format", SdmSpRsType.KafKa.getValue());
                        streamMap.put("kafka.bootstrap.servers", strInfo.getSss_bootstrap_server());
                        streamMap.put("topic", strInfo.getSss_topic_name());
                        map.put("resultProcessConf", streamMap);
                    }
                }
                arrayInfo.add(map);
            }
        }
        List<Map<String, Object>> data_baseList = Dbo.queryList("SELECT t1.ssj_job_id, t1.sdm_info_id, t1.output_type, t1.output_mode," + "t1.output_table_name,t2.ssd_table_name, t4.storage_property_key, t4.storage_property_val, t4.dsl_id " + "FROM sdm_sp_output t1 LEFT JOIN sdm_sp_database t2 " + "ON t1.sdm_info_id = t2.sdm_info_id " + "LEFT JOIN dtab_relation_store t3 ON t2.dsl_id = t3.dsl_id " + "LEFT JOIN data_store_layer_attr t4 ON t3.dsl_id = t4.dsl_id " + "WHERE t1.ssj_job_id = ? group by t1.sdm_info_id, t1.output_type, t1.output_mode," + "t1.output_table_name, t1.ssj_job_id, t2.ssd_table_name, t4.storage_property_key, t4.storage_property_val, t4.dsl_id", ssj_job_id);
        Result result = Dbo.queryResult("select store_type from " + DataStoreLayer.TableName + " where dsl_id = ?", data_baseList.get(0).get("dsl_id"));
        if (null != data_baseList.get(0).get("output_type") && SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(data_baseList.get(0).get("output_type").toString())) {
            HashMap<String, Object> maps = new HashMap<>();
            tableName = data_baseList.get(0).get("output_table_name").toString();
            maps.put("sdm_info_id", data_baseList.get(0).get("sdm_info_id").toString());
            maps.put("dbtable", data_baseList.get(0).get("ssd_table_name").toString());
            maps.put("inputTableName", data_baseList.get(0).get("output_table_name").toString());
            maps.put("format", "store_layer");
            maps.put("store_type", result.getString(0, "store_type"));
            maps.put("mode", SdmSpOutputMode.ofValueByCode(data_baseList.get(0).get("output_mode").toString()));
            maps.put("path", "-");
            for (int i = 0; i < resultList.getRowCount(); i++) {
                if ("streaming_checkpoint".equals(resultList.getString(i, "ssp_param_key"))) {
                    if (StringUtil.isNotEmpty(resultList.getString(i, "ssp_param_value"))) {
                        maps.put("checkpointLocation", resultList.getString(i, "ssp_param_value"));
                    } else {
                        maps.put("checkpointLocation", "/tmp/" + tableName + "/");
                    }
                }
            }
            if (!data_baseList.isEmpty()) {
                for (Map<String, Object> dataMap : data_baseList) {
                    maps.put(dataMap.get("storage_property_key").toString(), dataMap.get("storage_property_val").toString());
                }
                arrayInfo.add(maps);
            }
        }
        List<Map<String, Object>> streamList = Dbo.queryList("select * from " + SdmSpOutput.TableName + " t1 right join " + SdmSpStream.TableName + " t2 on t1.sdm_info_id = t2.sdm_info_id where t1.ssj_job_id = ?", ssj_job_id);
        if (!streamList.isEmpty()) {
            for (Map<String, Object> streamMap : streamList) {
                HashMap<String, Object> map = new HashMap<>();
                if (SdmSpStreamVer.KAFKA.getCode().equals(streamMap.get("sss_kafka_version"))) {
                    map.put("topic", streamMap.get("sss_topic_name").toString());
                    map.put("format", SdmSpStreamVer.KAFKA.getValue());
                } else if (SdmSpStreamVer.KAFKA8.getCode().equals(streamMap.get("sss_kafka_version"))) {
                    map.put("topic", streamMap.get("sss_topic_name").toString());
                    map.put("format", SdmSpStreamVer.KAFKA8.getValue());
                } else {
                    map.put("topic", streamMap.get("sss_topic_name").toString());
                    map.put("format", SdmSpStreamVer.KAFKA9.getValue());
                }
                tableName = streamMap.get("output_table_name").toString();
                format = map.get("format").toString();
                for (int i = 0; i < resultList.getRowCount(); i++) {
                    if ("streaming_checkpoint".equals(resultList.getString(i, "ssp_param_key"))) {
                        if (StringUtil.isNotEmpty(resultList.getString(i, "ssp_param_value"))) {
                            map.put("checkpointLocation", resultList.getString(i, "ssp_param_value"));
                        } else {
                            map.put("checkpointLocation", "/tmp/" + tableName + "/" + format);
                        }
                    }
                }
                map.put("mode", SdmSpOutputMode.ofValueByCode(streamMap.get("output_mode").toString()));
                map.put("inputTableName", streamMap.get("output_table_name").toString());
                map.put("kafka.bootstrap.servers", streamMap.get("sss_bootstrap_server").toString());
                map.put("path", "-");
                arrayInfo.add(map);
            }
        }
        List<SdmSpOutput> outInfoList = Dbo.queryList(SdmSpOutput.class, "select * from " + SdmSpOutput.TableName + " where output_type = ? AND ssj_job_id = ?", SdmSpInputOutputType.REST.getCode(), ssj_job_id);
        if (!outInfoList.isEmpty()) {
            for (SdmSpOutput outInfo : outInfoList) {
                Map<String, Object> map = new HashMap<>();
                map.put("inputTableName", outInfo.getOutput_table_name());
                map.put("mode", SdmSpOutputMode.ofValueByCode(outInfo.getOutput_mode()));
                map.put("path", "-");
                StreamproSetting setInfo = Dbo.queryOneObject(StreamproSetting.class, "select * from " + StreamproSetting.TableName + " where sdm_info_id = ?", outInfo.getSdm_info_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                Map<String, Object> kafkaMap = new HashMap<>();
                if (null != setInfo) {
                    map.put("httpUrl", setInfo.getRs_url());
                    map.put("format", "http");
                    Map rs_para = JsonUtil.toObject(JsonUtil.toJson(setInfo.getRs_para()), new TypeReference<Map>() {
                    });
                    map.put("requestParams", rs_para);
                    map.put("requestMode", "line");
                    kafkaMap.put("format", "normal");
                }
                if (null != setInfo) {
                    if (setInfo.getRs_processing().equals(SdmSpRsType.KafKa.getCode())) {
                        SdmRestStream streamInfo = Dbo.queryOneObject(SdmRestStream.class, "select * from " + SdmRestStream.TableName + " where rs_id = ?", setInfo.getRs_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                        kafkaMap.put("mode", SdmSpOutputMode.ofValueByCode(outInfo.getOutput_mode()));
                        kafkaMap.put("format", SdmSpRsType.KafKa.getValue());
                        kafkaMap.put("kafka.bootstrap.servers", streamInfo.getSss_bootstrap_server());
                        kafkaMap.put("topic", streamInfo.getSss_topic_name());
                        for (int i = 0; i < resultList.getRowCount(); i++) {
                            if ("streaming_checkpoint".equals(resultList.getString(i, "ssp_param_key"))) {
                                if (StringUtil.isNotEmpty(resultList.getString(i, "ssp_param_value"))) {
                                    kafkaMap.put("checkpointLocation", "/hrds/" + resultList.getString(i, "ssp_param_value"));
                                } else {
                                    kafkaMap.put("checkpointLocation", "/tmp/" + tableName + "/" + format);
                                }
                            }
                        }
                    }
                }
                map.put("resultProcessConf", kafkaMap);
                arrayInfo.add(map);
            }
        }
        outMap.put("params", arrayInfo);
        array.add(outMap);
    }
}
