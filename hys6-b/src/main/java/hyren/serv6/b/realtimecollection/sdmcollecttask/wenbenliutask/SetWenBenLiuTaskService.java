package hyren.serv6.b.realtimecollection.sdmcollecttask.wenbenliutask;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.realtimecollection.util.KafkaBeanUtil;
import hyren.serv6.b.realtimecollection.util.SdmAgentActionUtil;
import hyren.serv6.b.realtimecollection.realtimeCollectManagement.topic.SdmTopicService;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.stream.KafkaConstant;
import hyren.serv6.commons.utils.stream.KafkaMonitorManager;
import hyren.serv6.commons.utils.stream.TopicOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.File;
import java.util.*;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2020-04-15")
@Service
@Slf4j
public class SetWenBenLiuTaskService {

    private static final String UPLOADED = "uploaded";

    @Autowired
    SdmTopicService sdmTopicService;

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmReceiveConf", desc = "", range = "")
    @Param(name = "sdmRecParam", desc = "", range = "")
    @Param(name = "sdmMessInfo", desc = "", range = "")
    public void saveSdmReceiveConfManage(String sdmReceiveConf, String sdmRecParam, String sdmMessInfo) {
        SdmReceiveConf receiveConf = JsonUtil.toObject(sdmReceiveConf, new TypeReference<SdmReceiveConf>() {
        });
        List<SdmRecParam> sdm_rec_params = JsonUtil.toObject(sdmRecParam, new TypeReference<List<SdmRecParam>>() {
        });
        List<SdmMessInfo> sdm_mess_infos = JsonUtil.toObject(sdmMessInfo, new TypeReference<List<SdmMessInfo>>() {
        });
        log.info("sdm_mess_infos: {}", JsonUtil.toJson(sdm_mess_infos));
        checkSdmMessInfos(sdm_rec_params, "SAVE");
        if (Dbo.queryNumber("SELECT count(1) FROM " + SdmReceiveConf.TableName + " WHERE sdm_receive_name = ?", receiveConf.getSdm_receive_name()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该任务名称已存在!");
        }
        long sdm_receive_id = PrimayKeyGener.getNextId();
        receiveConf.setSdm_receive_id(sdm_receive_id);
        if (null != receiveConf.getSdm_dat_delimiter()) {
            receiveConf.setSdm_dat_delimiter(StringUtil.string2Unicode(receiveConf.getSdm_dat_delimiter()));
        }
        receiveConf.setCreate_date(DateUtil.getSysDate());
        receiveConf.setCreate_time(DateUtil.getSysTime());
        receiveConf.add(Dbo.db());
        for (SdmRecParam sdm_rec_param : sdm_rec_params) {
            if ("topic".equals(sdm_rec_param.getSdm_param_key())) {
                if (topicIsValid(sdm_rec_param.getSdm_param_value())) {
                    throw new BusinessException("该主题名称无效!");
                }
            }
            sdm_rec_param.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param.add(Dbo.db());
        }
        int number = 1;
        for (SdmMessInfo sdm_mess_info : sdm_mess_infos) {
            sdm_mess_info.setMess_info_id(PrimayKeyGener.getNextId());
            sdm_mess_info.setSdm_receive_id(sdm_receive_id);
            sdm_mess_info.setNum(number + "");
            number++;
            sdm_mess_info.add(Dbo.db());
        }
        saveMessInfo(number, receiveConf, sdm_receive_id);
    }

    private void checkSdmMessInfos(List<SdmRecParam> sdmRecParam, String flag) {
        String topic = StringUtils.EMPTY;
        for (SdmRecParam recParam : sdmRecParam) {
            if (recParam.getSdm_param_key().equals("topic"))
                topic = recParam.getSdm_param_value();
        }
        List<Map<String, Object>> list = Dbo.queryList("select DISTINCT sdm_receive_id from " + SdmRecParam.TableName + " where sdm_param_value = ?", topic);
        if (flag.equals("SAVE") && list.size() != 0) {
            throw new BusinessException("当前主题已存在任务，请新建主题后创建此任务！");
        }
        if (flag.equals("UPDATE") && list.size() != 1) {
            throw new BusinessException("当前主题已存在任务，请新建主题后创建此任务！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmReceiveConf", desc = "", range = "")
    @Param(name = "sdmRecParam", desc = "", range = "")
    @Param(name = "sdmMessInfo", desc = "", range = "")
    public void updateSdmReceiveConfManage(String sdmReceiveConf, String sdmRecParam, String sdmMessInfo) {
        SdmReceiveConf receiveConf = JsonUtil.toObject(sdmReceiveConf, new TypeReference<SdmReceiveConf>() {
        });
        List<SdmRecParam> sdm_rec_params = JsonUtil.toObject(sdmRecParam, new TypeReference<List<SdmRecParam>>() {
        });
        List<SdmMessInfo> sdm_mess_infos = JsonUtil.toObject(sdmMessInfo, new TypeReference<List<SdmMessInfo>>() {
        });
        checkSdmMessInfos(sdm_rec_params, "UPDATE");
        if (StringUtil.isNotEmpty(receiveConf.getIs_data_partition())) {
            receiveConf.setSdm_dat_delimiter(StringUtil.string2Unicode(receiveConf.getSdm_dat_delimiter()));
        }
        int ret = receiveConf.update(Dbo.db());
        if (ret != 1) {
            throw new BusinessException("更新流数据管理接收端信息失败!");
        }
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.execute(db, "delete from " + SdmRecParam.TableName + " where sdm_receive_id = ?", receiveConf.getSdm_receive_id());
            SqlOperator.execute(db, "delete from " + SdmMessInfo.TableName + " where sdm_receive_id = ?", receiveConf.getSdm_receive_id());
            db.commit();
        }
        for (SdmRecParam sdm_rec_param : sdm_rec_params) {
            if ("topic".equals(sdm_rec_param.getSdm_param_key())) {
                if (topicIsValid(sdm_rec_param.getSdm_param_value())) {
                    throw new BusinessException("该主题名称无效!");
                }
            }
            sdm_rec_param.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param.setSdm_receive_id(receiveConf.getSdm_receive_id());
            sdm_rec_param.add(Dbo.db());
        }
        int number = 1;
        for (SdmMessInfo sdm_mess_info : sdm_mess_infos) {
            sdm_mess_info.setMess_info_id(PrimayKeyGener.getNextId());
            sdm_mess_info.setSdm_receive_id(receiveConf.getSdm_receive_id());
            sdm_mess_info.setNum(number + "");
            number++;
            sdm_mess_info.add(Dbo.db());
        }
        saveMessInfo(number, receiveConf, receiveConf.getSdm_receive_id());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean topicIsValid(String topic_name) {
        Map<String, Object> map = Dbo.queryOneObject("SELECT * FROM sdm_topic_info WHERE sdm_top_name = ? ", topic_name);
        return map.isEmpty();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Param(name = "path", desc = "", range = "", valueIfNull = "")
    @Param(name = "isFile", desc = "", range = "", valueIfNull = "true")
    @Return(desc = "", range = "")
    public List<Object> selectFile(long sdm_agent_id, String path, String isFile) {
        String url = SdmAgentActionUtil.getUrl(sdm_agent_id, getUserId(), SdmAgentActionUtil.KAFKAFILECATALOGUE);
        Map<String, Object> params = new HashMap<>();
        params.put("pathVal", path);
        params.put("isFile", isFile);
        HttpClient.ResponseValue resVal = new HttpClient().addData("sendMsg", JsonUtil.toJson(params)).post(url);
        ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
        if (!ar.isSuccess()) {
            throw new BusinessException("连接失败");
        }
        return JsonUtil.toObject(JsonUtil.toJson(ar.getData()), new TypeReference<List<Object>>() {
        });
    }

    @Autowired
    KafkaMonitorManager manager;

    @Method(desc = "", logicStep = "")
    @Param(name = "file_path", desc = "", range = "", nullable = true)
    @Param(name = "file_exte_date", desc = "", range = "", nullable = true)
    @Param(name = "is_parse", desc = "", range = "")
    @Param(name = "data_separator", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    public void saveWenBenLiuTask(String file_path, String file_exte_date, String is_parse, String data_separator, long sdm_agent_id) {
        Map<String, Object> map = Dbo.queryOneObject("SELECT s.sdm_source_number FROM sdm_data_source s LEFT JOIN sdm_agent_info a " + "ON s.sdm_source_id = a.sdm_source_id WHERE a.sdm_agent_id = ?", sdm_agent_id);
        String url = SdmAgentActionUtil.getUrl(sdm_agent_id, getUserId(), SdmAgentActionUtil.KAFKADATADICTIONARY);
        HttpClient.ResponseValue resVal = new HttpClient().addData("file_path", file_path).post(url);
        ActionResult ret = JsonUtil.toObjectSafety(resVal.getBodyString(), ActionResult.class).orElseThrow(() -> new BusinessException("获取路径失败!"));
        List<String> split = StringUtil.split(file_exte_date, ".");
        String brokerServer = manager.parseBrokerServer();
        int brokerSize = StringUtil.split(brokerServer, ",").size();
        List<String> split1 = StringUtil.split(file_path, "/");
        List<String> split2 = StringUtil.split(file_path, "\\");
        String sdm_source_number = map.get("sdm_source_number").toString();
        List<Map<String, Object>> mapListJson = JsonUtil.toObject(JsonUtil.toJson(ret.getData()), new TypeReference<List<Map<String, Object>>>() {
        });
        for (Map<String, Object> mapArray : mapListJson) {
            String ori_table_name = (String) mapArray.get("table_name");
            SdmReceiveConf sdm_receive_conf = new SdmReceiveConf();
            String table_name = sdm_source_number + "_" + ori_table_name;
            String table_cn_name = (String) mapArray.get("table_cn_name");
            if (Dbo.queryNumber("SELECT count(1) FROM sdm_receive_conf WHERE sdm_receive_name = ?", table_name).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
                throw new BusinessException("topic名称" + table_name + "在现有系统中已存在");
            } else {
                sdm_receive_conf.setSdm_receive_name(table_name);
                autoCreateTopic(brokerSize, table_name);
            }
            String kafka_file_path;
            if (split1.size() > split2.size()) {
                kafka_file_path = file_path + "/" + split.get(0) + "/" + ori_table_name + "_" + file_exte_date;
            } else {
                kafka_file_path = file_path + File.separator + split.get(0) + File.separator + ori_table_name + "_" + file_exte_date;
            }
            long sdm_receive_id = PrimayKeyGener.getNextId();
            sdm_receive_conf.setSdm_rec_des(table_cn_name);
            sdm_receive_conf.setRa_file_path(kafka_file_path);
            sdm_receive_conf.setCreate_date(DateUtil.getSysDate());
            sdm_receive_conf.setCreate_time(DateUtil.getSysTime());
            sdm_receive_conf.setSdm_partition(SdmPatitionWay.SuiJiFenBu.getCode());
            sdm_receive_conf.setCode(DataBaseCode.UTF_8.getCode());
            sdm_receive_conf.setFile_initposition("0");
            sdm_receive_conf.setFile_readtype("0");
            sdm_receive_conf.setMonitor_type(IsFlag.Fou.getCode());
            sdm_receive_conf.setRead_mode(ObjectCollectType.HangCaiJi.getCode());
            sdm_receive_conf.setRead_type(IsFlag.Fou.getCode());
            sdm_receive_conf.setRun_way(ExecuteWay.MingLingChuFa.getCode());
            sdm_receive_conf.setCus_des_type(SdmCustomBusCla.NONE.getCode());
            sdm_receive_conf.setFault_alarm_mode("none");
            sdm_receive_conf.setIs_data_partition(is_parse);
            if (IsFlag.Shi == IsFlag.ofEnumByCode(is_parse) && null != data_separator) {
                sdm_receive_conf.setSdm_dat_delimiter(StringUtil.string2Unicode(data_separator));
            }
            sdm_receive_conf.setSdm_agent_id(sdm_agent_id);
            sdm_receive_conf.setSdm_receive_id(sdm_receive_id);
            sdm_receive_conf.add(Dbo.db());
            if (IsFlag.Shi == IsFlag.ofEnumByCode(is_parse)) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> columns = (List<Map<String, Object>>) mapArray.get("columns");
                for (Map<String, Object> column : columns) {
                    SdmMessInfo sdm_mess_info = new SdmMessInfo();
                    sdm_mess_info.setMess_info_id(PrimayKeyGener.getNextId());
                    sdm_mess_info.setNum(Integer.parseInt(column.get("column_id").toString()) + 1 + "");
                    sdm_mess_info.setSdm_is_send(IsFlag.Shi.getCode());
                    sdm_mess_info.setSdm_receive_id(sdm_receive_id);
                    sdm_mess_info.setSdm_var_name_en((String) column.get("column_name"));
                    sdm_mess_info.setSdm_var_name_cn((String) column.get("column_cn_name"));
                    sdm_mess_info.setSdm_var_type(columnTypeChange((String) column.get("column_type")));
                    sdm_mess_info.add(Dbo.db());
                }
            }
            List<SdmRecParam> paramList = new ArrayList<>();
            SdmRecParam sdm_rec_param = new SdmRecParam();
            sdm_rec_param.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param.setSdm_param_key(KafkaConstant.BOOTSTRAP_SERVERS);
            sdm_rec_param.setSdm_param_value(brokerServer);
            paramList.add(sdm_rec_param);
            SdmRecParam sdm_rec_param1 = new SdmRecParam();
            sdm_rec_param1.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param1.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param1.setSdm_param_key(KafkaConstant.ACKS);
            sdm_rec_param1.setSdm_param_value("1");
            paramList.add(sdm_rec_param1);
            SdmRecParam sdm_rec_param2 = new SdmRecParam();
            sdm_rec_param2.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param2.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param2.setSdm_param_key(KafkaConstant.RETRIES);
            sdm_rec_param2.setSdm_param_value("0");
            paramList.add(sdm_rec_param2);
            SdmRecParam sdm_rec_param3 = new SdmRecParam();
            sdm_rec_param3.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param3.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param3.setSdm_param_key(KafkaConstant.MAX_REQUEST_SIZE);
            sdm_rec_param3.setSdm_param_value("1048576");
            paramList.add(sdm_rec_param3);
            SdmRecParam sdm_rec_param4 = new SdmRecParam();
            sdm_rec_param4.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param4.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param4.setSdm_param_key(KafkaConstant.BATCH_SIZE);
            sdm_rec_param4.setSdm_param_value("16384");
            paramList.add(sdm_rec_param4);
            SdmRecParam sdm_rec_param5 = new SdmRecParam();
            sdm_rec_param5.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param5.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param5.setSdm_param_key(KafkaConstant.LINGER_MS);
            sdm_rec_param5.setSdm_param_value("1");
            paramList.add(sdm_rec_param5);
            SdmRecParam sdm_rec_param6 = new SdmRecParam();
            sdm_rec_param6.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param6.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param6.setSdm_param_key(KafkaConstant.BUFFER_MEMORY);
            sdm_rec_param6.setSdm_param_value("33554432");
            paramList.add(sdm_rec_param6);
            SdmRecParam sdm_rec_param7 = new SdmRecParam();
            sdm_rec_param7.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param7.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param7.setSdm_param_key(KafkaConstant.KEY_SERIALIZER);
            sdm_rec_param7.setSdm_param_value("String");
            paramList.add(sdm_rec_param7);
            SdmRecParam sdm_rec_param8 = new SdmRecParam();
            sdm_rec_param8.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param8.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param8.setSdm_param_key(KafkaConstant.VALUE_SERIALIZER);
            sdm_rec_param8.setSdm_param_value("String");
            paramList.add(sdm_rec_param8);
            SdmRecParam sdm_rec_param9 = new SdmRecParam();
            sdm_rec_param9.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param9.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param9.setSdm_param_key(KafkaConstant.COMPRESSION_TYPE);
            sdm_rec_param9.setSdm_param_value("none");
            paramList.add(sdm_rec_param9);
            SdmRecParam sdm_rec_param10 = new SdmRecParam();
            sdm_rec_param10.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param10.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param10.setSdm_param_key(KafkaConstant.MESSAGESIZE);
            sdm_rec_param10.setSdm_param_value("1048576");
            paramList.add(sdm_rec_param10);
            SdmRecParam sdm_rec_param11 = new SdmRecParam();
            sdm_rec_param11.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param11.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param11.setSdm_param_key(KafkaConstant.INTERCEPTOR_CLASSER);
            sdm_rec_param11.setSdm_param_value("");
            paramList.add(sdm_rec_param11);
            SdmRecParam sdm_rec_param12 = new SdmRecParam();
            sdm_rec_param12.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param12.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param12.setSdm_param_key(KafkaConstant.SYNC);
            sdm_rec_param12.setSdm_param_value("1");
            paramList.add(sdm_rec_param12);
            SdmRecParam sdm_rec_param13 = new SdmRecParam();
            sdm_rec_param13.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param13.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param13.setSdm_param_key(KafkaConstant.TOPIC);
            sdm_rec_param13.setSdm_param_value(table_name);
            paramList.add(sdm_rec_param13);
            for (SdmRecParam sdm_recInfo : paramList) {
                sdm_recInfo.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    public void downLoadFileTemplate() {
        String fileName = "dd_data.json";
        String rootPath = System.getProperty("user.dir") + File.separator + UPLOADED;
        KafkaBeanUtil.downloadFile(rootPath, fileName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "column_type", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    private String columnTypeChange(String column_type) {
        if (column_type.toLowerCase().contains("integer") || column_type.toLowerCase().contains("bigint") || column_type.toLowerCase().contains("smallint")) {
            return SdmVariableType.ZhengShu.getCode();
        } else if (column_type.toLowerCase().contains("numeric") || column_type.toLowerCase().contains("double") || column_type.toLowerCase().contains("decimal") || column_type.toLowerCase().contains("float") || column_type.toLowerCase().contains("decfloat")) {
            return SdmVariableType.FuDianShu.getCode();
        } else if (column_type.toLowerCase().contains("longvarchar") || column_type.toLowerCase().contains("clob") || column_type.toLowerCase().contains("blob")) {
            return SdmVariableType.ZiJieShuZu.getCode();
        } else {
            return SdmVariableType.ZiFuChuan.getCode();
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "number", desc = "", range = "")
    @Param(name = "sdmReceiveConf", desc = "", range = "", isBean = true)
    @Param(name = "sdm_receive_id", desc = "", range = "")
    public void saveMessInfo(int number, SdmReceiveConf sdmReceiveConf, long sdm_receive_id) {
        List<SdmMessInfo> messList = new ArrayList<>();
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sdmReceiveConf.getIs_file_attr_ip())) {
            SdmMessInfo messInfo = new SdmMessInfo();
            messInfo.setMess_info_id(PrimayKeyGener.getNextId());
            messInfo.setSdm_receive_id(sdm_receive_id);
            messInfo.setNum(number + "");
            messInfo.setSdm_var_name_en(KafkaConstant.FILE_ATTR_IP);
            messInfo.setSdm_var_name_cn(KafkaConstant.FILE_ATTR_IP_CN);
            messInfo.setSdm_var_type(SdmVariableType.ZiFuChuan.getCode());
            number++;
            messList.add(messInfo);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sdmReceiveConf.getIs_file_name())) {
            SdmMessInfo messInfo = new SdmMessInfo();
            messInfo.setMess_info_id(PrimayKeyGener.getNextId());
            messInfo.setSdm_receive_id(sdm_receive_id);
            messInfo.setNum(number + "");
            messInfo.setSdm_var_name_en(KafkaConstant.FILE_NAME);
            messInfo.setSdm_var_name_cn(KafkaConstant.FILE_NAME_CN);
            messInfo.setSdm_var_type(SdmVariableType.ZiFuChuan.getCode());
            number++;
            messList.add(messInfo);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sdmReceiveConf.getIs_file_size())) {
            SdmMessInfo messInfo = new SdmMessInfo();
            messInfo.setMess_info_id(PrimayKeyGener.getNextId());
            messInfo.setSdm_receive_id(sdm_receive_id);
            messInfo.setNum(number + "");
            messInfo.setSdm_var_name_en(KafkaConstant.FILE_SIZE);
            messInfo.setSdm_var_name_cn(KafkaConstant.FILE_SIZE_CN);
            messInfo.setSdm_var_type(SdmVariableType.ZiFuChuan.getCode());
            number++;
            messList.add(messInfo);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sdmReceiveConf.getIs_file_time())) {
            final SdmMessInfo messInfo = new SdmMessInfo();
            messInfo.setMess_info_id(PrimayKeyGener.getNextId());
            messInfo.setSdm_receive_id(sdm_receive_id);
            messInfo.setNum(number + "");
            messInfo.setSdm_var_name_en(KafkaConstant.FILE_TIME);
            messInfo.setSdm_var_name_cn(KafkaConstant.FILE_TIME_CN);
            messInfo.setSdm_var_type(SdmVariableType.ZiFuChuan.getCode());
            number++;
            messList.add(messInfo);
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(sdmReceiveConf.getIs_full_path())) {
            final SdmMessInfo messInfo = new SdmMessInfo();
            messInfo.setMess_info_id(PrimayKeyGener.getNextId());
            messInfo.setSdm_receive_id(sdm_receive_id);
            messInfo.setNum(number + "");
            messInfo.setSdm_var_name_en(KafkaConstant.FULL_PATH);
            messInfo.setSdm_var_name_cn(KafkaConstant.FULL_PATH_CN);
            messInfo.setSdm_var_type(SdmVariableType.ZiFuChuan.getCode());
            messList.add(messInfo);
        }
        for (SdmMessInfo mess_info : messList) {
            mess_info.setMess_info_id(PrimayKeyGener.getNextId());
            mess_info.setSdm_receive_id(sdm_receive_id);
            mess_info.setSdm_is_send(IsFlag.Shi.getCode());
            mess_info.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "brokerSize", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    public void autoCreateTopic(int brokerSize, String table_name) {
        SdmTopicInfo sdm_topic_info = new SdmTopicInfo();
        sdm_topic_info.setSdm_zk_host(PropertyParaValue.getString("kafka_zk_address", "hyshf@beyondsoft.com"));
        sdm_topic_info.setSdm_top_name(table_name);
        sdm_topic_info.setSdm_top_cn_name(table_name);
        sdm_topic_info.setSdm_partition("1");
        if (brokerSize <= 3) {
            sdm_topic_info.setSdm_replication((long) brokerSize);
        } else {
            sdm_topic_info.setSdm_replication("3");
        }
        sdm_topic_info.setUser_id("1001");
        sdmTopicService.saveSdmTopicInfo(sdm_topic_info);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_source_id", desc = "", range = "")
    @Param(name = "sdm_agent_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<AgentInfo> selectTaskConfiguration(long sdm_source_id, String sdm_agent_type) {
        return Dbo.queryList(AgentInfo.class, "select * from " + AgentInfo.TableName + " where source_id = ? and agent_type = ?", sdm_source_id, sdm_agent_type);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> selectTaskManage(long sdm_agent_id) {
        return Dbo.queryList("select sdm_agent_id as agent_id,* from " + SdmReceiveConf.TableName + " where sdm_agent_id = ? ", sdm_agent_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> selectWenBenTask(long sdm_agent_id, long sdm_receive_id) {
        Map<String, Object> map = new HashMap<>();
        AgentInfo agent_info = Dbo.queryOneObject(AgentInfo.class, "SELECT agent_id,agent_name,agent_type,agent_ip,agent_port,agent_status from " + AgentInfo.TableName + " WHERE agent_id = ? ", sdm_agent_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        map.put("sdmAgentInfo", agent_info);
        List<SdmTopicInfo> topicInfoList = Dbo.queryList(SdmTopicInfo.class, "select topic_id,sdm_top_value,sdm_top_name from " + SdmTopicInfo.TableName + " where user_id = ?", getUserId());
        map.put("topic_list", topicInfoList);
        SdmReceiveConf sdmReceiveInfo = Dbo.queryOneObject(SdmReceiveConf.class, "SELECT * from " + SdmReceiveConf.TableName + " where sdm_receive_id = ?", sdm_receive_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        if (StringUtil.isNotEmpty(sdmReceiveInfo.getSdm_dat_delimiter())) {
            sdmReceiveInfo.setSdm_dat_delimiter(StringUtil.unicode2String(sdmReceiveInfo.getSdm_dat_delimiter()));
        }
        map.put("sdmReceiveInfo", sdmReceiveInfo);
        List<SdmRecParam> paramInfoList = Dbo.queryList(SdmRecParam.class, "select * from " + SdmRecParam.TableName + " where sdm_receive_id = ?", sdm_receive_id);
        map.put("sdmParam_list", paramInfoList);
        List<SdmMessInfo> messInfoList = Dbo.queryList(SdmMessInfo.class, "select * from " + SdmMessInfo.TableName + " where sdm_receive_id = ? order by mess_info_id", sdm_receive_id);
        map.put("sdmMess_list", messInfoList);
        String brokerServer = manager.parseBrokerServer();
        map.put("brokerServer", brokerServer);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    public void deleteWenBenTask(long sdm_receive_id) {
        Map<String, Object> sdmRecParam = Dbo.queryOneObject("SELECT * FROM " + SdmRecParam.TableName + " WHERE sdm_param_key = 'topic' AND sdm_receive_id = ?", sdm_receive_id);
        String topic_name = sdmRecParam.get("sdm_param_value").toString();
        Optional<SdmTopicInfo> sdmTopicInfo = Dbo.queryOneObject(SdmTopicInfo.class, "SELECT * FROM " + SdmTopicInfo.TableName + " WHERE sdm_top_name = ?", topic_name);
        if (sdmTopicInfo.isPresent()) {
            TopicOperator to = new TopicOperator(sdmTopicInfo.get().getSdm_top_name(), sdmTopicInfo.get().getSdm_zk_host());
            to.deleteTopic();
        }
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.execute(db, "delete from " + SdmTopicInfo.TableName + " where sdm_top_name = ?", topic_name);
            SqlOperator.execute(db, "delete from " + SdmUserPermission.TableName + " where sdm_receive_id = ?", sdm_receive_id);
            SqlOperator.execute(db, "delete from " + SdmReceiveConf.TableName + " where sdm_receive_id = ?", sdm_receive_id);
            SqlOperator.execute(db, "delete from " + SdmMessInfo.TableName + " where sdm_receive_id = ?", sdm_receive_id);
            SqlOperator.execute(db, "delete from " + SdmRecParam.TableName + " where sdm_receive_id = ?", sdm_receive_id);
            db.commit();
        }
    }

    private Map<String, Object> getSendData(long sdm_receive_id, SdmReceiveConf sdm_receive_conf, AgentInfo agentInfo) {
        Map<String, Object> sendJo = new HashMap<>();
        Map<String, Object> recConfJo = new HashMap<>();
        Map<String, Object> kfkParJo = new HashMap<>();
        Map<String, Object> bus_class = new HashMap<>();
        Map<String, Object> msgJo = new HashMap<>();
        List<SdmMessInfo> messInfoList = Dbo.queryList(SdmMessInfo.class, "select * from " + SdmMessInfo.TableName + " where sdm_receive_id = ? order by mess_info_id", sdm_receive_id);
        sendJo.put("messInfoList", messInfoList);
        List<SdmRecParam> paramInfoList = Dbo.queryList(SdmRecParam.class, "select * from " + SdmRecParam.TableName + " where sdm_receive_id = ?", sdm_receive_id);
        msgJo.put("type", sdm_receive_conf.getMsgtype());
        msgJo.put("header", sdm_receive_conf.getMsgheader());
        sendJo.put("msgtype", msgJo);
        recConfJo.put("sdm_receive_id", sdm_receive_conf.getSdm_receive_id());
        recConfJo.put("sdm_receive_name", sdm_receive_conf.getSdm_receive_name());
        recConfJo.put("sdm_rec_des", sdm_receive_conf.getSdm_rec_des());
        recConfJo.put("sdm_server_ip", agentInfo.getAgent_ip());
        recConfJo.put("file_handle", sdm_receive_conf.getFile_handle());
        recConfJo.put("code", sdm_receive_conf.getCode());
        recConfJo.put("file_initposition", sdm_receive_conf.getFile_initposition());
        recConfJo.put("file_read_num", sdm_receive_conf.getFile_read_num());
        recConfJo.put("ra_file_path", sdm_receive_conf.getRa_file_path());
        recConfJo.put("monitor_type", sdm_receive_conf.getMonitor_type());
        recConfJo.put("thread_num", sdm_receive_conf.getThread_num());
        recConfJo.put("read_mode", sdm_receive_conf.getRead_mode());
        recConfJo.put("is_data_partition", sdm_receive_conf.getIs_data_partition());
        if (StringUtil.isNotEmpty(sdm_receive_conf.getSdm_dat_delimiter())) {
            recConfJo.put("sdm_dat_delimiter", StringUtil.unicode2String(sdm_receive_conf.getSdm_dat_delimiter()));
        }
        recConfJo.put("file_match_rule", sdm_receive_conf.getFile_match_rule());
        recConfJo.put("read_type", sdm_receive_conf.getRead_type());
        recConfJo.put("is_obj", sdm_receive_conf.getIs_obj());
        recConfJo.put("file_readtype", sdm_receive_conf.getFile_readtype());
        recConfJo.put("sdm_email", sdm_receive_conf.getSdm_email());
        recConfJo.put("run_way", sdm_receive_conf.getRun_way());
        if (sdm_receive_conf.getCheck_cycle() != null && !StringUtil.isEmpty(sdm_receive_conf.getCheck_cycle().toString())) {
            int ii = sdm_receive_conf.getCheck_cycle() * 1000 * 60;
            recConfJo.put("check_cycle", ii + "");
        } else {
            recConfJo.put("check_cycle", "");
        }
        if (!StringUtil.isEmpty(sdm_receive_conf.getSdm_rec_port())) {
            recConfJo.put("sdm_rec_port", sdm_receive_conf.getSdm_rec_port());
        } else {
            recConfJo.put("sdm_rec_port", agentInfo.getAgent_port());
        }
        kfkParJo.put("sdm_partition", sdm_receive_conf.getSdm_partition());
        if (SdmPatitionWay.SuiJiFenBu != SdmPatitionWay.ofEnumByCode(sdm_receive_conf.getSdm_partition())) {
            kfkParJo.put("sdm_partition_name", sdm_receive_conf.getSdm_partition_name());
        } else {
            kfkParJo.put("sdm_partition_name", "");
        }
        for (SdmRecParam rsParams : paramInfoList) {
            if (KafkaConstant.TOPIC.equals(rsParams.getSdm_param_key())) {
                if (Dbo.queryNumber("select count(1) from " + SdmTopicInfo.TableName + " where sdm_top_name = ?", rsParams.getSdm_param_value()).orElseThrow(() -> new BusinessException("sql查询错误！")) < 1) {
                    throw new BusinessException("topic值异常!");
                }
                kfkParJo.put(KafkaConstant.TOPIC, rsParams.getSdm_param_value());
            } else if (KafkaConstant.BOOTSTRAP_SERVERS.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.BOOTSTRAP_SERVERS, rsParams.getSdm_param_value());
            } else if (KafkaConstant.ACKS.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.ACKS, rsParams.getSdm_param_value());
            } else if (KafkaConstant.RETRIES.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.RETRIES, rsParams.getSdm_param_value());
            } else if (KafkaConstant.MAX_REQUEST_SIZE.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.MAX_REQUEST_SIZE, rsParams.getSdm_param_value());
            } else if (KafkaConstant.BATCH_SIZE.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.BATCH_SIZE, rsParams.getSdm_param_value());
            } else if (KafkaConstant.LINGER_MS.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.LINGER_MS, rsParams.getSdm_param_value());
            } else if (KafkaConstant.BUFFER_MEMORY.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.BUFFER_MEMORY, rsParams.getSdm_param_value());
            } else if (KafkaConstant.KEY_SERIALIZER.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.KEY_SERIALIZER, rsParams.getSdm_param_value());
            } else if (KafkaConstant.VALUE_SERIALIZER.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.VALUE_SERIALIZER, rsParams.getSdm_param_value());
            } else if (KafkaConstant.COMPRESSION_TYPE.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.COMPRESSION_TYPE, rsParams.getSdm_param_value());
            } else if (KafkaConstant.INTERCEPTOR_CLASSER.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.INTERCEPTOR_CLASSER, rsParams.getSdm_param_value());
            } else if (KafkaConstant.SYNC.equals(rsParams.getSdm_param_key())) {
                kfkParJo.put(KafkaConstant.SYNC, rsParams.getSdm_param_value());
            } else if (KafkaConstant.MESSAGESIZE.equals(rsParams.getSdm_param_key())) {
                recConfJo.put(KafkaConstant.MESSAGESIZE, rsParams.getSdm_param_value());
            }
        }
        bus_class.put("sdm_bus_pro_cla", sdm_receive_conf.getSdm_bus_pro_cla());
        if (SdmCustomBusCla.NONE == SdmCustomBusCla.ofEnumByCode(sdm_receive_conf.getCus_des_type())) {
            bus_class.put("cus_des_type", "0");
        } else if (SdmCustomBusCla.Java == SdmCustomBusCla.ofEnumByCode(sdm_receive_conf.getCus_des_type())) {
            bus_class.put("cus_des_type", "1");
        } else {
            bus_class.put("cus_des_type", "2");
        }
        sendJo.put("sdm_receive_conf", recConfJo);
        sendJo.put("business_class", bus_class);
        sendJo.put("kafka_params", kfkParJo);
        return sendJo;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    public void sendWenBenTask(long sdm_receive_id) {
        SdmReceiveConf sdm_receive_conf = Dbo.queryOneObject(SdmReceiveConf.class, "SELECT * from " + SdmReceiveConf.TableName + " where sdm_receive_id = ? ", sdm_receive_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        AgentInfo agentInfo = Dbo.queryOneObject(AgentInfo.class, "SELECT * from " + AgentInfo.TableName + " where agent_id = ? ", sdm_receive_conf.getSdm_agent_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        Map<String, Object> sendJo = getSendData(sdm_receive_id, sdm_receive_conf, agentInfo);
        if (AgentType.XiaoXiLiu == AgentType.ofEnumByCode(agentInfo.getAgent_type())) {
            String url = SdmAgentActionUtil.getUrl(agentInfo.getAgent_id(), agentInfo.getUser_id(), SdmAgentActionUtil.KAFKARESTSTARTINFO);
            HttpClient.ResponseValue resVal = new HttpClient().addData("sendMsg", JsonUtil.toJson(sendJo)).post(url);
            ActionResult actionResult = ActionResult.toActionResult(resVal.getBodyString());
            if (!actionResult.isSuccess()) {
                throw new BusinessException("发送任务失败,请检查数据流端口号是否被占用!");
            }
        } else {
            String url = SdmAgentActionUtil.getUrl(agentInfo.getAgent_id(), agentInfo.getUser_id(), SdmAgentActionUtil.KAFKAWENBENSTARTINFO);
            HttpClient.ResponseValue resVal = new HttpClient().addData("sendMsg", JsonUtil.toJson(sendJo)).post(url);
            ActionResult actionResult = ActionResult.toActionResult(resVal.getBodyString());
            if (!actionResult.isSuccess()) {
                throw new BusinessException("发送任务失败!");
            }
        }
    }

    public String getSendDataByJobId(long sdm_receive_id) {
        SdmReceiveConf sdm_receive_conf = Dbo.queryOneObject(SdmReceiveConf.class, "SELECT * from " + SdmReceiveConf.TableName + " where sdm_receive_id = ? ", sdm_receive_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        AgentInfo agentInfo = Dbo.queryOneObject(AgentInfo.class, "SELECT * from " + AgentInfo.TableName + " where agent_id = ? ", sdm_receive_conf.getSdm_agent_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        Map<String, Object> sendJo = getSendData(sdm_receive_id, sdm_receive_conf, agentInfo);
        return JsonUtil.toJson(sendJo);
    }
}
