package hyren.serv6.b.realtimecollection.sdmdatadistribute.consumeroperation;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.meta.MetaOperator;
import fd.ng.db.meta.TableMeta;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.realtimecollection.bean.SdmDBAdditionBean;
import hyren.serv6.b.realtimecollection.util.KafkaBeanUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.CreateDataTable;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.compress.ZipUtils;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageLayerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2021-06-08")
@Slf4j
@Service
public class SdmConsumerOperationService {

    private static final String STARTTIME = "startTime";

    private static final String ENDTIME = "endTime";

    private static final String STREAMZIP = "streamDistribute.zip";

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getConsumerMsgList() {
        Result result = Dbo.queryResult("SELECT sdm_consum_id,sdm_cons_name,user_id," + "create_date,create_time,con_with_par from " + SdmConsumeConf.TableName + " where user_id = ?", getUserId());
        for (int i = 0; i < result.getRowCount(); i++) {
            List<String> describeList = new ArrayList<>();
            List<Map<String, Object>> describeMsgList = Dbo.queryList("select sdm_conf_describe from " + SdmConsumeDes.TableName + " where sdm_consum_id = ?", Long.parseLong(result.getString(i, "sdm_consum_id")));
            describeMsgList.forEach(describe -> describeList.add(describe.get("sdm_conf_describe").toString()));
            result.setValue(i, "sdm_conf_describe", JsonUtil.toJson(describeList));
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_conf", desc = "", range = "", isBean = true)
    @Param(name = "sdm_cons_para", desc = "", range = "", isBean = true)
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "sdm_coner_file", desc = "", range = "", isBean = true)
    @Param(name = "is_add", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> saveConsumerMsg(SdmConsumeConf sdm_consume_conf, SdmConsPara[] sdm_cons_para, SdmConsumeDes sdm_consume_des, SdmConerFile sdm_coner_file, String is_add) {
        Map<String, Object> map = new HashMap<>();
        long sdm_consum_id = PrimayKeyGener.getNextId();
        long sdm_des_id = PrimayKeyGener.getNextId();
        if (StringUtil.isNotBlank(sdm_consume_conf.getDeadline())) {
            sdm_consume_conf.setDeadline(getDateFormat(sdm_consume_conf.getDeadline()));
        }
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Optional<SdmConsumeConf> consumeInfo = Dbo.queryOneObject(SdmConsumeConf.class, "select * from " + SdmConsumeConf.TableName + " where sdm_cons_name = ? and sdm_consum_id != ?", sdm_consume_conf.getSdm_cons_name(), sdm_consume_conf.getSdm_consum_id());
            if (consumeInfo.isPresent()) {
                throw new BusinessException("该消费任务名称已存在!");
            }
            sdm_consume_conf.update(Dbo.db());
            sdm_consume_des.setSdm_des_id(sdm_consume_des.getSdm_des_id());
            sdm_consume_des.update(Dbo.db());
            Dbo.execute("delete from " + SdmConsPara.TableName + " where sdm_consum_id = ?", sdm_consume_conf.getSdm_consum_id());
            saveSdmParamsMsg(sdm_cons_para, sdm_consume_conf.getSdm_consum_id());
            if (SdmConsumeDestination.ErJinZhiWenJian == SdmConsumeDestination.ofEnumByCode(sdm_consume_des.getSdm_conf_describe())) {
                Dbo.execute("delete from " + SdmConerFile.TableName + " where sdm_des_id = ?", sdm_consume_des.getSdm_des_id());
            }
        } else if (IsFlag.Shi == IsFlag.ofEnumByCode(is_add)) {
            Optional<SdmConsumeConf> consumeInfo = Dbo.queryOneObject(SdmConsumeConf.class, "select * from " + SdmConsumeConf.TableName + " where sdm_cons_name = ? ", sdm_consume_conf.getSdm_cons_name());
            if (null == sdm_consume_conf.getSdm_consum_id()) {
                if (consumeInfo.isPresent()) {
                    throw new BusinessException("该消费任务名称已存在!");
                }
                sdm_consume_conf.setUser_id(getUserId());
                sdm_consume_conf.setCreate_date(DateUtil.getSysDate());
                sdm_consume_conf.setCreate_time(DateUtil.getSysTime());
                sdm_consume_conf.setSdm_consum_id(sdm_consum_id);
                sdm_consume_conf.setDeadline(sdm_consume_conf.getDeadline());
                sdm_consume_conf.add(Dbo.db());
                saveSdmParamsMsg(sdm_cons_para, sdm_consume_conf.getSdm_consum_id());
            }
            sdm_consume_des.setSdm_consum_id(sdm_consume_conf.getSdm_consum_id());
            sdm_consume_des.setSdm_des_id(sdm_des_id);
            if (StringUtil.isEmpty(sdm_consume_des.getCus_des_type())) {
                sdm_consume_des.setCus_des_type("0");
            }
            sdm_consume_des.add(Dbo.db());
        }
        if (null != sdm_coner_file && SdmConsumeDestination.ErJinZhiWenJian == SdmConsumeDestination.ofEnumByCode(sdm_consume_des.getSdm_conf_describe())) {
            sdm_coner_file.setFile_id(PrimayKeyGener.getNextId());
            sdm_coner_file.setSdm_des_id(sdm_consume_des.getSdm_des_id());
            sdm_coner_file.setUser_id(getUserId());
            sdm_coner_file.add(Dbo.db());
        }
        map.put("sdm_consume_id", sdm_consume_des.getSdm_consum_id());
        map.put("sdm_des_id", sdm_consume_des.getSdm_des_id());
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_cons_para", desc = "", range = "", isBean = true)
    @Param(name = "sdm_consum_id", desc = "", range = "")
    public void saveSdmParamsMsg(SdmConsPara[] sdm_cons_para, long sdm_consum_id) {
        for (SdmConsPara cons_para : sdm_cons_para) {
            if ("topic".equals(cons_para.getSdm_conf_para_na())) {
                if (!topicIsValid(cons_para.getSdm_cons_para_val())) {
                    throw new BusinessException("该主题名称无效!");
                }
            }
            cons_para.setSdm_conf_para_id(PrimayKeyGener.getNextId());
            cons_para.setSdm_consum_id(sdm_consum_id);
            cons_para.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> showConsumeHistory() {
        return Dbo.queryList("SELECT para.sdm_cons_para_val AS groupid, cons.sdm_cons_para_val AS topic FROM " + SdmConsPara.TableName + " para LEFT JOIN " + SdmConsPara.TableName + " cons ON para.sdm_consum_id = cons.sdm_consum_id " + " WHERE para.sdm_conf_para_na = 'groupid' AND cons.sdm_conf_para_na = 'topic'");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> getProducerNameMsg(String topic_name) {
        List<String> producerName = new ArrayList<>();
        Result result = Dbo.queryResult("SELECT srp.*,src.sdm_receive_name FROM sdm_rec_param srp " + " JOIN sdm_receive_conf src ON srp.sdm_receive_id = src.sdm_receive_id " + " WHERE srp.sdm_param_key = 'topic' AND srp.sdm_param_value = ?", topic_name);
        for (int i = 0; i < result.getRowCount(); i++) {
            producerName.add(result.getString(i, "sdm_receive_name"));
        }
        return producerName;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getProduceParamsMsg(long sdm_receive_id) {
        return Dbo.queryResult("select sdm_var_name_en,sdm_var_name_cn,sdm_describe,sdm_var_type from " + SdmMessInfo.TableName + " where sdm_receive_id = ?", sdm_receive_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_conf", desc = "", range = "", isBean = true)
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    public void saveConsumerMsgToPartition(SdmConsumeConf sdm_consume_conf, SdmConsumeDes[] sdm_consume_des) {
        Result result = Dbo.queryResult("SELECT count(1) FROM " + SdmConsumeConf.TableName + " where sdm_cons_name = ? and sdm_consum_id != ?", sdm_consume_conf.getSdm_cons_name(), sdm_consume_conf.getSdm_consum_id());
        if (Integer.parseInt(result.getString(0, "count")) > 0) {
            throw new BusinessException("消费端名称重复!");
        }
        if (sdm_consume_conf.update(Dbo.db()) != 1) {
            throw new BusinessException("更新消费端基本配置信息失败");
        }
        for (SdmConsumeDes consume_des : sdm_consume_des) {
            if (consume_des.update(Dbo.db()) != 1) {
                throw new BusinessException("更新消费端基本配置信息失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    public void deletePatitionMsg(long sdm_consum_id, long sdm_des_id) {
        Result result = Dbo.queryResult("select sdm_des_id,sdm_conf_describe from " + SdmConsumeDes.TableName + " where sdm_des_id = ?", sdm_des_id);
        deleteNextPageMsg(sdm_consum_id, sdm_des_id, result.getString(0, "sdm_conf_describe"));
        Dbo.execute("delete from " + SdmConsumeDes.TableName + " where sdm_des_id = ?", sdm_des_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    @Param(name = "sdm_conf_describe", desc = "", range = "")
    public void deleteNextPageMsg(long sdm_consum_id, long sdm_des_id, String sdm_conf_describe) {
        if (SdmConsumeDestination.ShuJuKu == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Result dbResult = Dbo.queryResult("select sdm_con_db_id,sdm_des_id,tab_id from " + SdmConToDb.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!dbResult.isEmpty()) {
                Result colResult = Dbo.queryResult("select sdm_col_id,consumer_id,dslad_id,col_id from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(dbResult.getString(0, "sdm_con_db_id")));
                Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", Long.parseLong(dbResult.getString(0, "tab_id")));
                for (int i = 0; i < colResult.getRowCount(); i++) {
                    Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ?", Long.parseLong(colResult.getString(i, "col_id")));
                }
                Dbo.execute("delete from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(dbResult.getString(0, "sdm_con_db_id")));
                Dbo.execute("delete from " + SdmConToDb.TableName + " where sdm_consum_id = ?", sdm_consum_id);
            }
        } else if (SdmConsumeDestination.Hbase == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Result hbResult = Dbo.queryResult("select hbase_id,sdm_des_id,tab_id from " + SdmConHbase.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!hbResult.isEmpty()) {
                Result colResult = Dbo.queryResult("select sdm_col_id,consumer_id,dslad_id,col_id from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(hbResult.getString(0, "hbase_id")));
                Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", Long.parseLong(hbResult.getString(0, "tab_id")));
                for (int i = 0; i < colResult.getRowCount(); i++) {
                    Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ?", Long.parseLong(colResult.getString(i, "col_id")));
                }
                Dbo.execute("delete from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(hbResult.getString(0, "hbase_id")));
                Dbo.execute("delete from " + SdmConHbase.TableName + " where sdm_des_id = ?", sdm_des_id);
            }
        } else if (SdmConsumeDestination.RestFuWu == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Result restResult = Dbo.queryResult("select rest_id from " + SdmConRest.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!restResult.isEmpty()) {
                Dbo.execute("delete from " + SdmConRest.TableName + " where sdm_des_id = ?", sdm_des_id);
                Dbo.execute("delete from " + SdmConExtCol.TableName + " where consumer_id = ?", Long.parseLong(restResult.getString(0, "rest_id")));
            }
        } else if (SdmConsumeDestination.LiuWenJian == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Result wenJianResult = Dbo.queryResult("select file_id from " + SdmConFile.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!wenJianResult.isEmpty()) {
                Dbo.execute("delete from " + SdmConFile.TableName + " where sdm_des_id = ?", sdm_des_id);
                Dbo.execute("delete from " + SdmConExtCol.TableName + " where consumer_id = ?", Long.parseLong(wenJianResult.getString(0, "file_id")));
            }
        } else if (SdmConsumeDestination.ErJinZhiWenJian == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Dbo.execute("delete from " + SdmConerFile.TableName + " where sdm_des_id = ?", sdm_des_id);
        } else if (SdmConsumeDestination.Kafka == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Result kafkaResult = Dbo.queryResult("select kafka_id from " + SdmConKafka.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!kafkaResult.isEmpty()) {
                Dbo.execute("delete from " + SdmConKafka.TableName + " where sdm_des_id = ?", sdm_des_id);
                Dbo.execute("delete from " + SdmConExtCol.TableName + " where consumer_id = ?", Long.parseLong(kafkaResult.getString(0, "kafka_id")));
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    public void deleteConsumerMsg(long sdm_consum_id) {
        Result result = Dbo.queryResult("select sdm_des_id,sdm_conf_describe from " + SdmConsumeDes.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        Dbo.execute("delete from " + SdmConsumeConf.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        Dbo.execute("delete from " + SdmConsPara.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        Dbo.execute("delete from " + SdmConsumeDes.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        for (int j = 0; j < result.getRowCount(); j++) {
            long sdm_des_id = Long.parseLong(result.getString(j, "sdm_des_id"));
            String sdm_conf_describe = result.getString(j, "sdm_conf_describe");
            deleteNextPageMsg(sdm_consum_id, sdm_des_id, sdm_conf_describe);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    public Map<String, Object> getConsumeMsg(long sdm_consum_id) {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> confMsg = Dbo.queryOneObject("select * from " + SdmConsumeConf.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        List<Map<String, Object>> paraMsg = Dbo.queryList("select sdm_conf_para_na,sdm_cons_para_val from " + SdmConsPara.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        List<Map<String, Object>> consumeDesMsg = Dbo.queryList("select * from " + SdmConsumeDes.TableName + " where sdm_consum_id = ?", sdm_consum_id);
        map.put("confMsg", confMsg);
        map.put("paraMsg", paraMsg);
        map.put("consumeDesMsg", consumeDesMsg);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    public Map<String, Object> getManageMsg(long sdm_consum_id, long sdm_des_id) {
        List<String> msgList = new ArrayList<>();
        Map<String, Object> map = Dbo.queryOneObject("select * from " + SdmConsumeDes.TableName + " where sdm_des_id = ?", sdm_des_id);
        String sdm_conf_describe = map.get("sdm_conf_describe").toString();
        if (SdmConsumeDestination.ShuJuKu == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Map<String, Object> result = Dbo.queryOneObject("select * from " + SdmConToDb.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!result.isEmpty()) {
                getDataManageParams(Long.parseLong(result.get("sdm_con_db_id").toString()), msgList, map);
                map.put("dataMsg", result);
            }
        } else if (SdmConsumeDestination.Hbase == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Map<String, Object> result = Dbo.queryOneObject("select * from " + SdmConHbase.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!result.isEmpty()) {
                getDataManageParams(Long.parseLong(result.get("hbase_id").toString()), msgList, map);
                map.put("hbMsg", result);
            }
        } else if (SdmConsumeDestination.RestFuWu == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Map<String, Object> result = Dbo.queryOneObject("select * from " + SdmConRest.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!result.isEmpty()) {
                List<Map<String, Object>> restExtMsg = Dbo.queryList("select * from " + SdmConExtCol.TableName + " where consumer_id = ?", result.get("rest_id"));
                if (!restExtMsg.isEmpty()) {
                    map.put("restExtMsg", restExtMsg);
                }
                map.put("restMsg", result);
            }
        } else if (SdmConsumeDestination.LiuWenJian == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Map<String, Object> result = Dbo.queryOneObject("select * from " + SdmConFile.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!result.isEmpty()) {
                List<Map<String, Object>> fileExtMsg = Dbo.queryList("select * from " + SdmConExtCol.TableName + " where consumer_id = ?", result.get("file_id"));
                if (!fileExtMsg.isEmpty()) {
                    map.put("fileExtMsg", fileExtMsg);
                }
                map.put("fileMsg", result);
            }
        } else if (SdmConsumeDestination.ErJinZhiWenJian == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Map<String, Object> result = Dbo.queryOneObject("select * from " + SdmConerFile.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!result.isEmpty()) {
                map.put("binFileMsg", result);
            }
        } else if (SdmConsumeDestination.Kafka == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
            Map<String, Object> result = Dbo.queryOneObject("select * from " + SdmConKafka.TableName + " where sdm_des_id = ?", sdm_des_id);
            if (!result.isEmpty()) {
                List<Map<String, Object>> kafkaExtMsg = Dbo.queryList("select * from " + SdmConExtCol.TableName + " where consumer_id = ?", result.get("kafka_id"));
                if (!kafkaExtMsg.isEmpty()) {
                    map.put("kafkaExtMsg", kafkaExtMsg);
                }
                map.put("kafkaMsg", result);
            }
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "id", desc = "", range = "")
    @Param(name = "msgList", desc = "", range = "")
    @Param(name = "map", desc = "", range = "")
    public void getDataManageParams(long id, List<String> msgList, Map<String, Object> map) {
        List<Map<String, Object>> configMsg = Dbo.queryList("SELECT t1.sdm_col_name_en,t1.num,sdm_col_name_cn," + "t1.is_empty,t1.is_custom,t1.sdm_var_type,t1.remark,t2.dsla_storelayer FROM " + SdmConDbCol.TableName + " t1 JOIN " + DataStoreLayerAdded.TableName + " t2 ON t1.dslad_id = t2.dslad_id WHERE t1.consumer_id = ?", id);
        List<Map<String, Object>> noAddMsg = Dbo.queryList("SELECT sdm_col_name_en,num,sdm_col_name_cn," + "is_empty,is_custom,sdm_var_type,remark FROM " + SdmConDbCol.TableName + " where dslad_id = ? and consumer_id = ? ", Long.parseLong(IsFlag.Fou.getCode()), id);
        noAddMsg.forEach(db -> db.put("dsla_storelayer", ""));
        Map<String, List<Map<String, Object>>> mapMsg = configMsg.stream().collect(Collectors.groupingBy(dbcof -> dbcof.get("sdm_col_name_en").toString()));
        Set<Map<String, Object>> mapList = new HashSet<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : mapMsg.entrySet()) {
            List<Map<String, Object>> list = entry.getValue();
            List<String> dsla_storelayer = list.stream().map(value -> value.get("dsla_storelayer").toString()).collect(Collectors.toList());
            for (Map<String, Object> objectMap : entry.getValue()) {
                objectMap.remove("dsla_storelayer");
                objectMap.put("dsla_storelayer", dsla_storelayer);
                mapList.add(objectMap);
            }
        }
        msgList.add(JsonUtil.toJson(mapList));
        msgList.add(JsonUtil.toJson(noAddMsg));
        map.put("paramsList", msgList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "sdm_con_to_db", desc = "", range = "", isBean = true)
    @Param(name = "dqTableColumnBeans", desc = "", range = "", isBean = true)
    @Param(name = "is_add", desc = "", range = "")
    public void saveDataBaseMsg(long sdm_consum_id, long sdm_des_id, long dsl_id, long sdm_receive_id, SdmConToDb sdm_con_to_db, SdmDBAdditionBean[] dqTableColumnBeans, String is_add) {
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Result dbResult = Dbo.queryResult("select sdm_con_db_id,sdm_des_id,tab_id from " + SdmConToDb.TableName + " where sdm_consum_id = ?", sdm_consum_id);
            Result colResult = Dbo.queryResult("select sdm_col_id,consumer_id,dslad_id,col_id from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(dbResult.getString(0, "sdm_con_db_id")));
            Dbo.execute("delete from " + SdmConToDb.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", Long.parseLong(dbResult.getString(0, "tab_id")));
            Dbo.execute("delete from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(dbResult.getString(0, "sdm_con_db_id")));
            Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ?", Long.parseLong(colResult.getString(0, "col_id")));
        }
        long table_id = PrimayKeyGener.getNextId();
        long sdm_con_db_id = PrimayKeyGener.getNextId();
        sdm_con_to_db.setSdm_con_db_id(sdm_con_db_id);
        sdm_con_to_db.setSdm_des_id(sdm_des_id);
        sdm_con_to_db.setSdm_consum_id(sdm_consum_id);
        sdm_con_to_db.setDsl_id(dsl_id);
        sdm_con_to_db.setUser_id(getUserId());
        sdm_con_to_db.setTab_id(table_id);
        sdm_con_to_db.add(Dbo.db());
        DtabRelationStore relationTable = new DtabRelationStore();
        relationTable.setTab_id(table_id);
        relationTable.setDsl_id(dsl_id);
        relationTable.setIs_successful(JobExecuteState.WanCheng.getCode());
        relationTable.setData_source(StoreLayerDataSource.SD.getCode());
        relationTable.add(Dbo.db());
        saveSdmDataMsg(dqTableColumnBeans, sdm_con_db_id, sdm_receive_id, dsl_id, sdm_con_to_db.getSdm_tb_name_en(), table_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "sdm_con_hbase", desc = "", range = "", isBean = true)
    @Param(name = "dqTableColumnBeans", desc = "", range = "", isBean = true)
    @Param(name = "is_add", desc = "", range = "")
    public void saveHbaseMsg(long sdm_des_id, long dsl_id, long sdm_receive_id, SdmConHbase sdm_con_hbase, SdmDBAdditionBean[] dqTableColumnBeans, String is_add) {
        LayerBean layerBean = SqlOperator.queryOneObject(Dbo.db(), LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
        Store_type store_type = Store_type.ofEnumByCode(layerBean.getStore_type());
        if (store_type == Store_type.HBASE) {
            throw new BusinessException("创建 HBase 类型存储层数表，暂未实现!");
        }
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Result hbResult = Dbo.queryResult("select hbase_id,tab_id from " + SdmConHbase.TableName + " where sdm_des_id = ?", sdm_des_id);
            Result colResult = Dbo.queryResult("select sdm_col_id,consumer_id,dslad_id,col_id from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(hbResult.getString(0, "hbase_id")));
            Dbo.execute("delete from " + SdmConHbase.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", Long.parseLong(hbResult.getString(0, "tab_id")));
            Dbo.execute("delete from " + SdmConDbCol.TableName + " where consumer_id = ?", Long.parseLong(hbResult.getString(0, "hbase_id")));
            Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ?", Long.parseLong(colResult.getString(0, "col_id")));
        }
        long hbaseId = PrimayKeyGener.getNextId();
        long table_id = PrimayKeyGener.getNextId();
        sdm_con_hbase.setHbase_id(hbaseId);
        sdm_con_hbase.setSdm_des_id(sdm_des_id);
        sdm_con_hbase.setDsl_id(dsl_id);
        sdm_con_hbase.setUser_id(getUserId());
        sdm_con_hbase.setTab_id(table_id);
        sdm_con_hbase.add(Dbo.db());
        DtabRelationStore relationTable = new DtabRelationStore();
        relationTable.setTab_id(table_id);
        relationTable.setDsl_id(dsl_id);
        relationTable.setIs_successful(JobExecuteState.WanCheng.getCode());
        relationTable.setData_source(StoreLayerDataSource.SD.getCode());
        relationTable.add(Dbo.db());
        saveSdmDataMsg(dqTableColumnBeans, hbaseId, sdm_receive_id, dsl_id, sdm_con_hbase.getHbase_name(), table_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dqTableColumnBeans", desc = "", range = "", isBean = true)
    @Param(name = "id", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "table_id", desc = "", range = "")
    private void saveSdmDataMsg(SdmDBAdditionBean[] dqTableColumnBeans, long id, long sdm_receive_id, long dsl_id, String table_name, long table_id) {
        LayerBean layerBean = SqlOperator.queryOneObject(Dbo.db(), LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
        DqTableInfo dqTableInfo = new DqTableInfo();
        dqTableInfo.setTable_id(table_id);
        dqTableInfo.setTable_name(table_name);
        dqTableInfo.setCreate_date(DateUtil.getSysDate());
        dqTableInfo.setEnd_date(Constant._MAX_DATE_8);
        dqTableInfo.setCreate_id(getUserId());
        dqTableInfo.setIs_trace(IsFlag.Fou.getCode());
        dqTableInfo.setTable_space("");
        dqTableInfo.add(Dbo.db());
        List<DqTableColumn> dqTableColumns = new ArrayList<>();
        for (int i = 0; i < dqTableColumnBeans.length; i++) {
            SdmConDbCol sdm_con_db_col = new SdmConDbCol();
            SdmDBAdditionBean dqTableColumnBean = dqTableColumnBeans[i];
            DqTableColumn dqTableColumn = new DqTableColumn();
            if (StringUtil.isNotBlank(dqTableColumnBean.getColumn_name()) && StringUtil.isNotBlank(dqTableColumnBean.getColumn_type())) {
                BeanUtil.copyProperties(dqTableColumnBean, dqTableColumn);
                dqTableColumn.setField_id(PrimayKeyGener.getNextId());
                dqTableColumn.setTable_id(dqTableInfo.getTable_id());
                dqTableColumns.add(dqTableColumn);
                DcolRelationStore dcol_relation_store;
                List<Long> dslad_id_list = new ArrayList<>();
                if (null != dqTableColumnBean.getDslad_id_s() && dqTableColumnBean.getDslad_id_s().length > 0) {
                    for (long dslad_id : dqTableColumnBean.getDslad_id_s()) {
                        DataStoreLayerAdded dsla = Dbo.queryOneObject(DataStoreLayerAdded.class, "SELECT * FROM " + DataStoreLayerAdded.TableName + " WHERE dslad_id=?", dslad_id).orElseThrow(() -> new BusinessException("获取数据存储附加信息失败!"));
                        StoreLayerAdded storeLayerAdded = StoreLayerAdded.ofEnumByCode(dsla.getDsla_storelayer());
                        long csi_number = i;
                        if (storeLayerAdded == StoreLayerAdded.RowKey) {
                            csi_number = Long.parseLong(dqTableColumnBean.getCsi_number());
                        }
                        dcol_relation_store = new DcolRelationStore();
                        dcol_relation_store.setCol_id(dqTableColumn.getField_id());
                        dcol_relation_store.setDslad_id(dslad_id);
                        dcol_relation_store.setData_source(StoreLayerDataSource.UD.getCode());
                        dcol_relation_store.setCsi_number(csi_number);
                        dcol_relation_store.add(Dbo.db());
                        dslad_id_list.add(dslad_id);
                        sdm_con_db_col.setDslad_id(dslad_id);
                        sdm_con_db_col.setCol_id(dqTableColumn.getField_id());
                        sdm_con_db_col.setSdm_col_id(PrimayKeyGener.getNextId());
                        sdm_con_db_col.setSdm_col_name_en(dqTableColumnBean.getColumn_name());
                        sdm_con_db_col.setSdm_col_name_cn(dqTableColumnBean.getField_ch_name());
                        sdm_con_db_col.setSdm_var_type(dqTableColumnBean.getColumn_type());
                        sdm_con_db_col.setIs_empty(dqTableColumnBean.getIs_null());
                        sdm_con_db_col.setConsumer_id(id);
                        sdm_con_db_col.setRemark(dqTableColumnBean.getDq_remark());
                        sdm_con_db_col.setSdm_receive_id(sdm_receive_id);
                        sdm_con_db_col.setIs_send(IsFlag.Shi.getCode());
                        sdm_con_db_col.setNum(Long.parseLong(String.valueOf(i)));
                        sdm_con_db_col.setIs_custom(dqTableColumnBean.getIs_custom());
                        sdm_con_db_col.add(Dbo.db());
                    }
                } else {
                    sdm_con_db_col.setDslad_id(IsFlag.Fou.getCode());
                    sdm_con_db_col.setCol_id(dqTableColumn.getField_id());
                    sdm_con_db_col.setSdm_col_id(PrimayKeyGener.getNextId());
                    sdm_con_db_col.setSdm_col_name_en(dqTableColumnBean.getColumn_name());
                    sdm_con_db_col.setSdm_col_name_cn(dqTableColumnBean.getField_ch_name());
                    sdm_con_db_col.setSdm_var_type(dqTableColumnBean.getColumn_type());
                    sdm_con_db_col.setIs_empty(dqTableColumnBean.getIs_null());
                    sdm_con_db_col.setConsumer_id(id);
                    sdm_con_db_col.setRemark(dqTableColumnBean.getDq_remark());
                    sdm_con_db_col.setSdm_receive_id(sdm_receive_id);
                    sdm_con_db_col.setIs_send(IsFlag.Shi.getCode());
                    sdm_con_db_col.setNum(Long.parseLong(String.valueOf(i)));
                    sdm_con_db_col.setIs_custom(dqTableColumnBean.getIs_custom());
                    sdm_con_db_col.add(Dbo.db());
                }
            }
        }
        dqTableColumns.forEach(dq_table_column -> dq_table_column.add(Dbo.db()));
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), dsl_id)) {
            if (db.isExistTable(table_name)) {
                List<String> columnEnNameList = new ArrayList<>();
                DatabaseMetaData data = db.getConnection().getMetaData();
                if (db.getDbtype() == Dbtype.ORACLE) {
                    table_name = table_name.toUpperCase();
                }
                ResultSet columnsList = data.getColumns(null, "%", table_name, "%");
                while (columnsList.next()) {
                    String colName = columnsList.getString("COLUMN_NAME");
                    columnEnNameList.add(colName);
                }
                for (SdmDBAdditionBean dqTableColumnBean : dqTableColumnBeans) {
                    String columnName = dqTableColumnBean.getColumn_name();
                    if (db.getDbtype() == Dbtype.ORACLE) {
                        columnName = columnName.toUpperCase();
                    }
                    if (!columnEnNameList.contains(columnName) && IsFlag.Fou == IsFlag.ofEnumByCode(dqTableColumnBean.getIs_null())) {
                        throw new BusinessException("页面字段不能被追加入表中");
                    } else if (!columnEnNameList.contains(columnName) && IsFlag.Shi == IsFlag.ofEnumByCode(dqTableColumnBean.getIs_null())) {
                        if (Store_type.HIVE == Store_type.ofEnumByCode(layerBean.getStore_type())) {
                            Dbo.execute(db, "ALTER TABLE " + table_name + " ADD " + "columns(" + columnName + " " + dqTableColumnBean.getColumn_type() + ")");
                        } else if ((Store_type.DATABASE == Store_type.ofEnumByCode(layerBean.getStore_type()))) {
                            Dbo.execute(db, "ALTER TABLE " + table_name + " ADD " + columnName + " " + dqTableColumnBean.getColumn_type());
                            db.commit();
                        }
                    }
                }
            } else {
                CreateDataTable.createDataTableByStorageLayer(Dbo.db(), layerBean, dqTableInfo, dqTableColumns);
            }
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getStorageLayerConfInfo(long dsl_id) {
        Validator.notEmpty(String.valueOf(dsl_id), "存储层配置id不能为空! dsl_id=" + dsl_id);
        return StorageLayerUtil.getStorageLayerConfInfo(Dbo.db(), dsl_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Object> getFiledType(long dsl_id) {
        Validator.notEmpty(String.valueOf(dsl_id), "存储层配置id不能为空! dsl_id=" + dsl_id);
        Result result = Dbo.queryResult("select storage_property_val from " + DataStoreLayerAttr.TableName + " where dsl_id = ? and storage_property_key = 'database_type'", dsl_id);
        return Dbo.queryOneColumnList("select database_type1 from " + DatabaseTypeMapping.TableName + " where lower( database_name1 ) = lower( ? ) group by database_type1 ", result.getString(0, "storage_property_val"));
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<DataStoreLayer> searchDataStore() {
        return Dbo.queryList(DataStoreLayer.class, "SELECT * from " + DataStoreLayer.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    public void downLoadFile(long sdm_consum_id) {
        String rootPath = System.getProperty("user.dir");
        generateConfigMsg(sdm_consum_id);
        String fileName = sdm_consum_id + ".json";
        ZipUtils.compress(rootPath + File.separator + STREAMZIP, rootPath + File.separator + "stream-consumer-command.sh", rootPath + File.separator + fileName);
        KafkaBeanUtil.downloadFile(rootPath, STREAMZIP);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> selectTopicName(String topic_name) {
        if (StringUtils.isBlank(topic_name)) {
            throw new BusinessException("请输入主题名称...");
        }
        return Dbo.queryList("SELECT * FROM " + SdmTopicInfo.TableName + " t1 LEFT JOIN sdm_user_permission t2 ON " + " t1.topic_id = t2.topic_id WHERE (t1.sdm_top_name LIKE '%" + topic_name + "%'" + " OR t1.sdm_top_cn_name LIKE '%" + topic_name + "%' ) AND t2.application_status = ?", FlowApplyStatus.ShenQingTongGuo.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "sdm_con_rest", desc = "", range = "", isBean = true)
    @Param(name = "sdm_con_ext_col", desc = "", range = "", isBean = true)
    @Param(name = "is_add", desc = "", range = "")
    public void saveRestMsg(long sdm_des_id, long sdm_receive_id, SdmConRest sdm_con_rest, SdmConExtCol[] sdm_con_ext_col, String is_add) {
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Result result = Dbo.queryResult("select rest_id from " + SdmConRest.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + SdmConRest.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + SdmConExtCol.TableName + " where consumer_id = ?", Long.parseLong(result.getString(0, "rest_id")));
        }
        long rest_id = PrimayKeyGener.getNextId();
        sdm_con_rest.setRest_id(rest_id);
        sdm_con_rest.setUser_id(getUserId());
        sdm_con_rest.setSdm_des_id(sdm_des_id);
        sdm_con_rest.add(Dbo.db());
        int num = 1;
        for (SdmConExtCol ext_col : sdm_con_ext_col) {
            ext_col.setSdm_col_id(PrimayKeyGener.getNextId());
            ext_col.setIs_send(IsFlag.Shi.getCode());
            ext_col.setConsumer_id(rest_id);
            ext_col.setNum(Long.parseLong(String.valueOf(num)));
            ext_col.setSdm_receive_id(sdm_receive_id);
            ext_col.add(Dbo.db());
            num++;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "sdm_con_file", desc = "", range = "", isBean = true)
    @Param(name = "sdm_con_ext_col", desc = "", range = "", isBean = true)
    @Param(name = "is_add", desc = "", range = "")
    public void saveFileMsg(long sdm_des_id, long sdm_receive_id, SdmConFile sdm_con_file, SdmConExtCol[] sdm_con_ext_col, String is_add) {
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Result result = Dbo.queryResult("select file_id from " + SdmConFile.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + SdmConFile.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + SdmConExtCol.TableName + " where consumer_id = ?", Long.parseLong(result.getString(0, "file_id")));
        }
        long file_id = PrimayKeyGener.getNextId();
        sdm_con_file.setFile_id(file_id);
        sdm_con_file.setUser_id(getUserId());
        sdm_con_file.setSdm_des_id(sdm_des_id);
        sdm_con_file.add(Dbo.db());
        int num = 1;
        for (SdmConExtCol ext_col : sdm_con_ext_col) {
            ext_col.setSdm_col_id(PrimayKeyGener.getNextId());
            ext_col.setIs_send(IsFlag.Shi.getCode());
            ext_col.setConsumer_id(file_id);
            ext_col.setNum(Long.parseLong(String.valueOf(num)));
            ext_col.setSdm_receive_id(sdm_receive_id);
            ext_col.add(Dbo.db());
            num++;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "sdm_con_kafka", desc = "", range = "", isBean = true)
    @Param(name = "sdm_con_ext_col", desc = "", range = "", isBean = true)
    @Param(name = "is_add", desc = "", range = "")
    public void saveKafkaMsg(long sdm_des_id, long sdm_receive_id, SdmConKafka sdm_con_kafka, SdmConExtCol[] sdm_con_ext_col, String is_add) {
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_add)) {
            Result result = Dbo.queryResult("select kafka_id from " + SdmConKafka.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + SdmConKafka.TableName + " where sdm_des_id = ?", sdm_des_id);
            Dbo.execute("delete from " + SdmConExtCol.TableName + " where consumer_id = ?", Long.parseLong(result.getString(0, "kafka_id")));
        }
        long kafka_id = PrimayKeyGener.getNextId();
        sdm_con_kafka.setKafka_id(kafka_id);
        sdm_con_kafka.setSdm_des_id(sdm_des_id);
        sdm_con_kafka.setUser_id(getUserId());
        sdm_con_kafka.add(Dbo.db());
        int num = 1;
        for (SdmConExtCol ext_col : sdm_con_ext_col) {
            ext_col.setSdm_col_id(PrimayKeyGener.getNextId());
            ext_col.setIs_send(IsFlag.Shi.getCode());
            ext_col.setConsumer_id(kafka_id);
            ext_col.setNum(Long.parseLong(String.valueOf(num)));
            ext_col.setSdm_receive_id(sdm_receive_id);
            ext_col.add(Dbo.db());
            num++;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<String> getConnectTable(long dsl_id) {
        DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), dsl_id);
        List<TableMeta> tables = null;
        List<String> tableList = new ArrayList<>();
        try {
            tables = MetaOperator.getTables(db);
            for (TableMeta table : tables) {
                String tableName = table.getTableName();
                tableList.add(tableName);
            }
        } catch (Exception e) {
            log.error("获取表名失败!");
        }
        return tableList;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result viewConsumerList() {
        Result rsConsume = Dbo.queryResult("SELECT sdm_consum_id,sdm_cons_name,user_id,create_date,create_time,con_with_par FROM " + SdmConsumeConf.TableName + " WHERE user_id = ? ORDER BY create_date desc,create_time desc", getUserId());
        for (int i = 0; i < rsConsume.getRowCount(); i++) {
            rsConsume.setValue(i, "con_with_par", IsFlag.ofValueByCode(rsConsume.getString(i, "con_with_par")));
            Result result = Dbo.queryResult("SELECT sdm_des_id, sdm_conf_describe, sdm_cons_des FROM " + SdmConsumeDes.TableName + " WHERE sdm_consum_id = ?", Long.parseLong(rsConsume.getString(i, "sdm_consum_id")));
            String sdm_conf_describe = result.getString(0, "sdm_conf_describe");
            rsConsume.setValue(i, "sdm_conf_describe", SdmConsumeDestination.ofValueByCode(sdm_conf_describe));
        }
        return rsConsume;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public void generateConfigMsg(long sdm_consum_id) {
        String rootPath = System.getProperty("user.dir");
        Map<String, Object> jsonMsg = new HashMap<>();
        Result result = Dbo.queryResult("SELECT partition,sdm_cons_des,sdm_thr_partition,sdm_conf_describe," + " descustom_buscla,cus_des_type,thread_num,sdm_bus_pro_cla,sdm_des_id,des_class," + " hdfs_file_type,external_file_type FROM " + SdmConsumeDes.TableName + " WHERE sdm_consum_id = ?", sdm_consum_id);
        SdmConsumeConf sdm_consume_conf = new SdmConsumeConf();
        sdm_consume_conf.setSdm_consum_id(sdm_consum_id);
        Map<String, Object> paramConf = getParam_conf_jo(sdm_consum_id);
        Map<String, Object> consumeConfMsg = getSdmConsumeConfMsg(sdm_consume_conf);
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> objMsg = new HashMap<>();
        Map<String, Object> descMsg = new HashMap<>();
        String sdm_conf_describe = result.getString(0, "sdm_conf_describe");
        if (IsFlag.Fou == IsFlag.ofEnumByCode(consumeConfMsg.get("con_with_par").toString())) {
            long sdm_des_id = Long.parseLong(result.getString(0, "sdm_des_id"));
            SdmConsumeDes sdm_consume_des = new SdmConsumeDes();
            sdm_consume_des.setSdm_des_id(sdm_des_id);
            if (SdmConsumeDestination.ShuJuKu == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                putShuJuKuJson(sdm_consume_des, params);
            } else if (SdmConsumeDestination.Hbase == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                putHbaseJson(sdm_consume_des, params);
            } else if (SdmConsumeDestination.ErJinZhiWenJian == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                putErFileJson(sdm_consume_des, params, paramConf);
            } else if (SdmConsumeDestination.Kafka == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                putKafkaJson(sdm_consume_des, params);
            } else if (SdmConsumeDestination.LiuWenJian == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
                String hdfs_file_type = result.getString(0, "external_file_type");
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                consumeConfMsg.put("hdfs_file_type", hdfs_file_type);
                putFileJson(sdm_consume_des, params);
            } else if (SdmConsumeDestination.RestFuWu == SdmConsumeDestination.ofEnumByCode(sdm_conf_describe)) {
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                putRestJson(sdm_consume_des, params);
            } else {
                consumeConfMsg.put("sdm_conf_describe", sdm_conf_describe);
                params.put("des_class", result.getString(0, "des_class"));
                params.put("descustom_buscla", result.getString(0, "descustom_buscla"));
            }
            consumeConfMsg.put("thread_num", result.getString(0, "thread_num"));
            consumeConfMsg.put("partition", result.getString(0, "partition"));
            consumeConfMsg.put("sdm_thr_partition", result.getString(0, "sdm_thr_partition"));
            params.put("sdm_bus_pro_cla", result.getString(0, "sdm_bus_pro_cla"));
            params.put("cus_des_type", result.getString(0, "cus_des_type"));
            paramConf.remove("messageSize");
            jsonMsg.put("param_conf", paramConf);
            consumeConfMsg.put("params", params);
            jsonMsg.put("consume_conf", consumeConfMsg);
        } else {
            for (int i = 0; i < result.getRowCount(); i++) {
                long sdm_des_id = Long.parseLong(result.getString(i, "sdm_des_id"));
                SdmConsumeDes sdm_consume_des = new SdmConsumeDes();
                sdm_consume_des.setSdm_des_id(sdm_des_id);
                params.put("sdm_bus_pro_cla", result.getString(i, "sdm_bus_pro_cla"));
                params.put("cus_des_type", result.getString(i, "cus_des_type"));
                params.put("consume_type", result.getString(i, "cus_des_type"));
                if (SdmConsumeDestination.ShuJuKu == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                    putShuJuKuJson(sdm_consume_des, params);
                } else if (SdmConsumeDestination.Hbase == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    putHbaseJson(sdm_consume_des, params);
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                } else if (SdmConsumeDestination.ErJinZhiWenJian == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    putErFileJson(sdm_consume_des, params, paramConf);
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                } else if (SdmConsumeDestination.Kafka == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    putKafkaJson(sdm_consume_des, params);
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                } else if (SdmConsumeDestination.LiuWenJian == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    putFileJson(sdm_consume_des, params);
                    String hdfs_file_type = result.getString(i, "external_file_type");
                    consumeConfMsg.put("hdfs_file_type", hdfs_file_type);
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                } else if (SdmConsumeDestination.RestFuWu == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    putRestJson(sdm_consume_des, params);
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                } else if (SdmConsumeDestination.ZiDingYeWuLei == SdmConsumeDestination.ofEnumByCode(result.getString(i, "sdm_conf_describe"))) {
                    params.put("des_class", result.getString(i, "des_class"));
                    params.put("descustom_buscla", result.getString(i, "descustom_buscla"));
                    int desc = Integer.parseInt(result.getString(i, "sdm_conf_describe"));
                    descMsg.put(result.getString(i, "partition").trim(), desc);
                }
                objMsg.put(result.getString(i, "partition").trim(), JsonUtil.toJson(params));
            }
            consumeConfMsg.put("sdm_conf_describe", descMsg);
            paramConf.remove("messageSize");
            jsonMsg.put("param_conf", paramConf);
            consumeConfMsg.put("params", objMsg);
            jsonMsg.put("consume_conf", consumeConfMsg);
        }
        String filename = sdm_consum_id + ".json";
        KafkaBeanUtil.writeJsonFile(rootPath, JsonUtil.toJson(jsonMsg), filename);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "params", desc = "", range = "")
    @Param(name = "paramConf", desc = "", range = "")
    private void putErFileJson(SdmConsumeDes sdm_consume_des, Map<String, Object> params, Map<String, Object> paramConf) {
        Result result = Dbo.queryResult("SELECT file_name,file_path,time_interval FROM " + SdmConerFile.TableName + " WHERE sdm_des_id = ?", sdm_consume_des.getSdm_des_id());
        params.put("file_name", result.getString(0, "file_name"));
        params.put("file_path", result.getString(0, "file_path"));
        if (!StringUtil.isEmpty(result.getString(0, "time_interval"))) {
            int num = Integer.parseInt(result.getString(0, "time_interval")) * 1000;
            params.put("time_interval", num);
        } else {
            params.put("time_interval", "10000");
        }
        if (!Objects.isNull(paramConf.get("messageSize"))) {
            params.put("messageSize", paramConf.get("messageSize"));
        }
        params.put("descustom_buscla", SdmCustomBusCla.NONE.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "params", desc = "", range = "")
    private void putFileJson(SdmConsumeDes sdm_consume_des, Map<String, Object> params) {
        Result fileResult = Dbo.queryResult("SELECT file_bus_type,file_bus_class,file_id,file_name,file_path," + "spilt_flag,file_limit FROM " + SdmConFile.TableName + " WHERE sdm_des_id = ? ", sdm_consume_des.getSdm_des_id());
        params.put("fileName", fileResult.getString(0, "file_name"));
        params.put("filePath", fileResult.getString(0, "file_path"));
        if (IsFlag.Shi == IsFlag.ofEnumByCode(fileResult.getString(0, "spilt_flag"))) {
            params.put("spilt_flag", true);
            params.put("file_limit", fileResult.getString(0, "file_limit"));
        } else {
            params.put("spilt_flag", false);
            params.put("file_limit", "");
        }
        Result fileInfoMsg = Dbo.queryResult("SELECT sdm_col_name_en,sdm_col_name_cn,sdm_describe," + "sdm_var_type,is_send,num FROM " + SdmConExtCol.TableName + " WHERE consumer_id = ? AND is_send = ?", Long.parseLong(fileResult.getString(0, "file_id")), IsFlag.Shi.getCode());
        Map<String, Object> jsonFileMsg = new HashMap<>();
        if (fileInfoMsg.isEmpty()) {
            Map<String, Object> fileMsg = new HashMap<>();
            fileMsg.put("type", "String");
            fileMsg.put("number", 0);
            fileMsg.put("is_send", "0");
            jsonFileMsg.put("line", fileMsg);
        } else {
            for (int i = 0; i < fileInfoMsg.getRowCount(); i++) {
                Map<String, Object> objMsg = new HashMap<>();
                fileInfoMsg.setValue(i, "fileInfoMsg", fileInfoMsg.getString(i, "sdm_var_type"));
                objMsg.put("type", fileInfoMsg.getString(i, "sdm_var_type"));
                objMsg.put("number", Integer.valueOf(fileInfoMsg.getString(i, "num")));
                objMsg.put("is_send", fileInfoMsg.getString(i, "is_send"));
                jsonFileMsg.put(fileInfoMsg.getString(i, "sdm_col_name_en"), objMsg);
            }
        }
        params.put("columns", jsonFileMsg);
        params.put("descustom_buscla", fileResult.getString(0, "file_bus_type"));
        params.put("des_class", fileResult.getString(0, "file_bus_class"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "params", desc = "", range = "")
    public void putShuJuKuJson(SdmConsumeDes sdm_consume_des, Map<String, Object> params) {
        Result result = Dbo.queryResult("SELECT * from " + SdmConToDb.TableName + " WHERE sdm_des_id = ?", sdm_consume_des.getSdm_des_id());
        params.put("dataBaseSet", result);
        Long dsl_id = Long.parseLong(result.getString(0, "dsl_id"));
        List<DataStoreLayerAttr> data_store_layer_attrs = Dbo.queryList(DataStoreLayerAttr.class, "select * from " + DataStoreLayerAttr.TableName + " where dsl_id = ?", dsl_id);
        for (DataStoreLayerAttr attr : data_store_layer_attrs) {
            result.setValue(0, attr.getStorage_property_key(), attr.getStorage_property_val());
        }
        result.setValue(0, "dsl_id", String.valueOf(dsl_id));
        Object dataBaseSet = params.get("dataBaseSet");
        Map map = JsonUtil.toObject(JsonUtil.toJson(dataBaseSet), new TypeReference<Map>() {
        });
        map.putAll(getDbConf(sdm_consume_des.getSdm_des_id()));
        params.put("dataBaseSet", map);
        Result colMsg = Dbo.queryResult("SELECT sdm_col_name_en,sdm_col_name_cn,sdm_var_type,num,is_send FROM " + SdmConDbCol.TableName + " WHERE consumer_id = ? AND is_send = ?", Long.parseLong(result.getString(0, "sdm_con_db_id")), IsFlag.Shi.getCode());
        Map<String, Object> dataJsonMsg = new HashMap<>();
        if (colMsg.isEmpty()) {
            Map<String, Object> dataMsg = new HashMap<>();
            dataMsg.put("type", "String");
            dataMsg.put("number", 0);
            dataMsg.put("is_send", IsFlag.Shi.getCode());
            dataJsonMsg.put("line", dataMsg);
        } else {
            for (int i = 0; i < colMsg.getRowCount(); i++) {
                Map<String, Object> objMsg = new HashMap<>();
                colMsg.setValue(i, "sdm_var_type", colMsg.getString(i, "sdm_var_type"));
                objMsg.put("type", colMsg.getString(i, "sdm_var_type"));
                objMsg.put("number", Integer.valueOf(colMsg.getString(i, "num")));
                objMsg.put("is_send", colMsg.getString(i, "is_send"));
                dataJsonMsg.put(colMsg.getString(i, "sdm_col_name_en"), objMsg);
            }
        }
        Map<String, Object> objMsg = new HashMap<>();
        objMsg.put(result.getString(0, "sdm_tb_name_en"), dataJsonMsg);
        params.put("hbInfo", objMsg);
        params.put("descustom_buscla", result.getString(0, "db_bus_type"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "params", desc = "", range = "")
    private void putHbaseJson(SdmConsumeDes sdm_consume_des, Map<String, Object> params) {
        Result result = Dbo.queryResult("SELECT dsl_id,hbase_bus_type,hbase_bus_class,hbase_id,hbase_name," + " hbase_family,pre_partition,rowkey_separator FROM " + SdmConHbase.TableName + " WHERE sdm_des_id = ?", sdm_consume_des.getSdm_des_id());
        Result dbResult = Dbo.queryResult("SELECT sdm_col_name_en,sdm_var_type,num,is_send FROM " + SdmConDbCol.TableName + " WHERE consumer_id = ? AND is_send = ?", Long.parseLong(result.getString(0, "hbase_id")), IsFlag.Shi.getCode());
        Map<String, Object> jsonMsg = new HashMap<>();
        if (dbResult.isEmpty()) {
            Map<String, Object> dbConMsg = new HashMap<>();
            dbConMsg.put("type", "String");
            dbConMsg.put("number", 0);
            dbConMsg.put("is_send", "0");
            jsonMsg.put("line", dbConMsg);
        } else {
            for (int i = 0; i < dbResult.getRowCount(); i++) {
                Map<String, Object> colConfMsg = new HashMap<>();
                dbResult.setValue(i, "sdm_var_type", dbResult.getString(i, "sdm_var_type"));
                colConfMsg.put("type", dbResult.getString(i, "sdm_var_type"));
                colConfMsg.put("number", Integer.valueOf(dbResult.getString(i, "num")));
                colConfMsg.put("is_send", dbResult.getString(i, "is_send"));
                colConfMsg.put("rowkey_column", dbResult.getString(i, "sdm_col_name_en"));
                jsonMsg.put(dbResult.getString(i, "sdm_col_name_en"), colConfMsg);
            }
        }
        Map<String, Object> famConfMsg = new HashMap<>();
        Map<String, Object> tableConfMsg = new HashMap<>();
        famConfMsg.put(result.getString(0, "hbase_family"), jsonMsg);
        tableConfMsg.put(result.getString(0, "hbase_name"), famConfMsg);
        params.put("hbaseNameMsg", tableConfMsg);
        params.put("partitionRule", result.getString(0, "pre_partition"));
        params.put("rowkey_separator", result.getString(0, "rowkey_separator"));
        params.put("descustom_buscla", result.getString(0, "hbase_bus_type"));
        params.put("des_class", result.getString(0, "hbase_bus_class"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "params", desc = "", range = "")
    private void putKafkaJson(SdmConsumeDes sdm_consume_des, Map<String, Object> params) {
        Result result = Dbo.queryResult("SELECT sdm_partition,sdm_partition_name,topic,bootstrap_servers," + "acks,retries,max_request_size,batch_size,linger_ms,buffer_memory," + "kafka_bus_type,kafka_bus_class,kafka_id," + "compression_type,sync,interceptor_classes FROM " + SdmConKafka.TableName + " WHERE sdm_des_id = ?", sdm_consume_des.getSdm_des_id());
        params.put("sdm_partition", result.getString(0, "sdm_partition"));
        params.put("sdm_partition_name", result.getString(0, "sdm_partition_name"));
        params.put("topic", result.getString(0, "topic"));
        params.put("bootstrap.servers", result.getString(0, "bootstrap_servers"));
        params.put("acks", result.getString(0, "acks"));
        params.put("retries", result.getString(0, "retries"));
        params.put("max.request.size", result.getString(0, "max_request_size"));
        params.put("batch.size", result.getString(0, "batch_size"));
        params.put("linger.ms", result.getString(0, "linger_ms"));
        params.put("buffer.memory", result.getString(0, "buffer_memory"));
        params.put("compression.type", result.getString(0, "compression_type"));
        params.put("sync", result.getString(0, "sync"));
        params.put("interceptor.classes", result.getString(0, "interceptor_classes"));
        SdmConKafka sdm_con_kafka = new SdmConKafka();
        sdm_con_kafka.setKafka_id(result.getString(0, "kafka_id"));
        Result colInfo = Dbo.queryResult("SELECT sdm_col_name_en,sdm_col_name_cn,sdm_describe," + "sdm_var_type,is_send,num FROM " + SdmConExtCol.TableName + " WHERE consumer_id = ? AND is_send = ?", sdm_con_kafka.getKafka_id(), IsFlag.Shi.getCode());
        Map<String, Object> kafkaJsonMsg = new HashMap<>();
        if (colInfo.isEmpty()) {
            Map<String, Object> kafkaMsg = new HashMap<>();
            kafkaMsg.put("type", "String");
            kafkaMsg.put("number", 0);
            kafkaMsg.put("is_send", "0");
            kafkaJsonMsg.put("line", kafkaMsg);
        } else {
            for (int i = 0; i < colInfo.getRowCount(); i++) {
                Map<String, Object> objMsg = new HashMap<>();
                colInfo.setValue(i, "sdm_var_type", SdmVariableType.ofValueByCode(colInfo.getString(i, "sdm_var_type")));
                objMsg.put("type", colInfo.getString(i, "sdm_var_type"));
                objMsg.put("number", Integer.valueOf(colInfo.getString(i, "num")));
                objMsg.put("is_send", colInfo.getString(i, "is_send"));
                kafkaJsonMsg.put(colInfo.getString(i, "sdm_col_name_en"), objMsg);
            }
        }
        params.put("columns", kafkaJsonMsg);
        params.put("descustom_buscla", SdmCustomBusCla.ofValueByCode(result.getString(0, "kafka_bus_type")));
        params.put("des_class", result.getString(0, "kafka_bus_class"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_des", desc = "", range = "", isBean = true)
    @Param(name = "params", desc = "", range = "")
    private void putRestJson(SdmConsumeDes sdm_consume_des, Map<String, Object> params) {
        Result result = Dbo.queryResult("SELECT rest_bus_type,rest_bus_class,rest_id,rest_port," + " rest_ip,rest_parameter FROM " + SdmConRest.TableName + " WHERE sdm_des_id = ? ", sdm_consume_des.getSdm_des_id());
        params.put("rest_port", result.getString(0, "rest_port"));
        params.put("rest_ip", result.getString(0, "rest_ip"));
        Result restResult = Dbo.queryResult("SELECT sdm_col_name_en,sdm_col_name_cn,sdm_describe," + "sdm_var_type,is_send,num FROM " + SdmConExtCol.TableName + " WHERE consumer_id = ? AND is_send = ?", Long.parseLong(result.getString(0, "rest_id")), IsFlag.Shi.getCode());
        Map<String, Object> restJsonMsg = new HashMap<>();
        if (restResult.isEmpty()) {
            Map<String, Object> restMsg = new HashMap<>();
            restMsg.put("type", "String");
            restMsg.put("number", 0);
            restMsg.put("is_send", "0");
            restJsonMsg.put("line", restMsg);
        } else {
            for (int i = 0; i < restResult.getRowCount(); i++) {
                Map<String, Object> objMsg = new HashMap<>();
                restResult.setValue(i, "sdm_var_type", restResult.getString(i, "sdm_var_type"));
                objMsg.put("type", restResult.getString(i, "sdm_var_type"));
                objMsg.put("number", Integer.valueOf(restResult.getString(i, "num")));
                objMsg.put("is_send", restResult.getString(i, "is_send"));
                restJsonMsg.put(restResult.getString(i, "sdm_col_name_en"), objMsg);
            }
        }
        params.put("columns", restJsonMsg);
        params.put("descustom_buscla", result.getString(0, "rest_bus_type"));
        params.put("des_class", result.getString(0, "rest_bus_class"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consume_conf", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    private Map<String, Object> getSdmConsumeConfMsg(SdmConsumeConf sdm_consume_conf) {
        Map<String, Object> result = Dbo.queryOneObject("SELECT consumer_type,con_with_par,consum_thread_cycle,deadline,run_time_long," + "end_type,data_volume,time_type FROM " + SdmConsumeConf.TableName + " WHERE sdm_consum_id = ? ", sdm_consume_conf.getSdm_consum_id());
        Long run_time_millisecond = null;
        String deadline_str = result.get("deadline").toString();
        Long run_time_str = Long.parseLong(result.get("run_time_long").toString());
        String time_type = result.get("time_type").toString();
        String consum_thread_cycle = result.get("consum_thread_cycle").toString();
        if (ConsumerCyc.AnShiJianJieShu == ConsumerCyc.ofEnumByCode(consum_thread_cycle)) {
            Long run_time_long = null;
            if (0L != run_time_str) {
                run_time_long = run_time_str;
            }
            if (StringUtil.isNotBlank(time_type)) {
                if (TimeType.Second == TimeType.ofEnumByCode(time_type)) {
                    run_time_millisecond = run_time_long * 1000;
                } else if (TimeType.Minute == TimeType.ofEnumByCode(time_type)) {
                    run_time_millisecond = run_time_long * 1000 * 60;
                } else if (TimeType.Hour == TimeType.ofEnumByCode(time_type)) {
                    run_time_millisecond = run_time_long * 1000 * 60 * 60;
                } else if (TimeType.Day == TimeType.ofEnumByCode(time_type)) {
                    run_time_millisecond = run_time_long * 1000 * 60 * 60 * 24;
                }
            }
            if (!StringUtil.isEmpty(deadline_str)) {
                try {
                    SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss");
                    Date date = format.parse(deadline_str);
                    result.put("deadline", String.valueOf(date.getTime()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (null != run_time_millisecond) {
            result.put("run_time_long", run_time_millisecond.toString());
        }
        return JsonUtil.toObject(JsonUtil.toJson(result), new TypeReference<Map<String, Object>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean topicIsValid(String topic_name) {
        List<Map<String, Object>> topicList = Dbo.queryList("SELECT * FROM sdm_topic_info t1 JOIN sdm_user_permission t2 " + " ON t1.topic_id = t2.topic_id WHERE t1.sdm_top_name = ? AND t2.application_status = ? ", topic_name, FlowApplyStatus.ShenQingTongGuo.getCode());
        if (topicList.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_consum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getParam_conf_jo(long sdm_consum_id) {
        Validator.notNull(sdm_consum_id, "消费端配置ID不能为空");
        List<SdmConsPara> consParaList = Dbo.queryList(SdmConsPara.class, "SELECT sdm_conf_para_na,sdm_cons_para_val FROM " + SdmConsPara.TableName + " WHERE sdm_consum_id = ?", sdm_consum_id);
        Map<String, Object> obj = new HashMap<>();
        consParaList.forEach(sdm_cons_para -> {
            if (STARTTIME.equals(sdm_cons_para.getSdm_conf_para_na())) {
                if (StringUtil.isBlank(sdm_cons_para.getSdm_cons_para_val())) {
                    sdm_cons_para.setSdm_cons_para_val("-1");
                } else {
                    sdm_cons_para.setSdm_cons_para_val(getDateFormat(sdm_cons_para.getSdm_cons_para_val()));
                }
            } else if (ENDTIME.equals(sdm_cons_para.getSdm_conf_para_na())) {
                String sdm_cons_para_val = sdm_cons_para.getSdm_cons_para_val();
                if (StringUtil.isBlank(sdm_cons_para_val)) {
                    sdm_cons_para.setSdm_cons_para_val("9999999999999");
                } else {
                    sdm_cons_para.setSdm_cons_para_val(getDateFormat(sdm_cons_para.getSdm_cons_para_val()));
                }
            }
            obj.put(sdm_cons_para.getSdm_conf_para_na(), sdm_cons_para.getSdm_cons_para_val());
        });
        return obj;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dateTime", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getDateFormat(String dateTime) {
        return dateTime.replaceAll("-", "").replaceAll(":", "").replaceAll(" ", "");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_des_id", desc = "", range = "")
    public Map<String, Object> getDbConf(long sdm_des_id) {
        List<DataStoreLayerAttr> data_store_layer_attrs = Dbo.queryList(DataStoreLayerAttr.class, "SELECT  storage_property_key,storage_property_val  FROM " + DataStoreLayerAttr.TableName + " WHERE dsl_id = (SELECT t1.dsl_id FROM " + DtabRelationStore.TableName + " t1 JOIN " + SdmConToDb.TableName + " t2 ON t1.tab_id = t2.tab_id WHERE t2.sdm_des_id = ?)", sdm_des_id);
        Map<String, String> collect = data_store_layer_attrs.stream().collect(Collectors.toMap(DataStoreLayerAttr::getStorage_property_key, DataStoreLayerAttr::getStorage_property_val));
        return JsonUtil.toObject(JsonUtil.toJson(collect), new TypeReference<Map<String, Object>>() {
        });
    }
}
