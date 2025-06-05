package hyren.serv6.b.realtimecollection.sdmdatadistribute.consumeroperation;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.b.realtimecollection.bean.SdmDBAdditionBean;
import hyren.serv6.base.entity.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataCollectionO/sdmdatadistribute/consumeroperation")
@Validated
@Api("/流数据分发,消费信息管理及配置")
@Slf4j
public class SdmConsumerOperationController {

    @Autowired
    SdmConsumerOperationService sdmConsumerOperationService;

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getConsumerMsgList")
    public Result getConsumerMsgList() {
        return sdmConsumerOperationService.getConsumerMsgList();
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/saveConsumerMsg")
    public Map<String, Object> saveConsumerMsg(@RequestBody Map<String, Object> req) {
        SdmConsumeConf sdm_consume_conf = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConsumeConf>() {
        });
        SdmConsPara[] sdm_cons_para = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdm_cons_para"), new TypeReference<SdmConsPara[]>() {
        });
        SdmConsumeDes sdm_consume_des = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConsumeDes>() {
        });
        SdmConerFile sdm_coner_file = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConerFile>() {
        });
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        return sdmConsumerOperationService.saveConsumerMsg(sdm_consume_conf, sdm_cons_para, sdm_consume_des, sdm_coner_file, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveSdmParamsMsg")
    public void saveSdmParamsMsg(@RequestBody Map<String, Object> req) {
        SdmConsPara[] sdm_cons_para = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdm_cons_para"), new TypeReference<SdmConsPara[]>() {
        });
        long sdm_consum_id = ReqDataUtils.getLongData(req, "sdm_consum_id");
        sdmConsumerOperationService.saveSdmParamsMsg(sdm_cons_para, sdm_consum_id);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/showConsumeHistory")
    public List<Map<String, Object>> showConsumeHistory() {
        return sdmConsumerOperationService.showConsumeHistory();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_name", value = "", dataTypeClass = String.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getProducerNameMsg")
    public List<String> getProducerNameMsg(String topic_name) {
        return sdmConsumerOperationService.getProducerNameMsg(topic_name);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_receive_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getProduceParamsMsg")
    public Result getProduceParamsMsg(long sdm_receive_id) {
        return sdmConsumerOperationService.getProduceParamsMsg(sdm_receive_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveConsumerMsgToPartition")
    public void saveConsumerMsgToPartition(@RequestBody Map<String, Object> req) {
        SdmConsumeConf sdm_consume_conf = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdm_consume_conf"), new TypeReference<SdmConsumeConf>() {
        });
        SdmConsumeDes[] sdm_consume_des = JsonUtil.toObject(ReqDataUtils.getStringData(req, "sdm_consume_des"), new TypeReference<SdmConsumeDes[]>() {
        });
        sdmConsumerOperationService.saveConsumerMsgToPartition(sdm_consume_conf, sdm_consume_des);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_des_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/deletePatitionMsg")
    public void deletePatitionMsg(long sdm_consum_id, long sdm_des_id) {
        sdmConsumerOperationService.deletePatitionMsg(sdm_consum_id, sdm_des_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_des_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_conf_describe", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/deleteNextPageMsg")
    public void deleteNextPageMsg(long sdm_consum_id, long sdm_des_id, String sdm_conf_describe) {
        sdmConsumerOperationService.deleteNextPageMsg(sdm_consum_id, sdm_des_id, sdm_conf_describe);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteConsumerMsg")
    public void deleteConsumerMsg(long sdm_consum_id) {
        sdmConsumerOperationService.deleteConsumerMsg(sdm_consum_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getConsumeMsg")
    public Map<String, Object> getConsumeMsg(long sdm_consum_id) {
        return sdmConsumerOperationService.getConsumeMsg(sdm_consum_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_des_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getManageMsg")
    public Map<String, Object> getManageMsg(long sdm_consum_id, long sdm_des_id) {
        return sdmConsumerOperationService.getManageMsg(sdm_consum_id, sdm_des_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/getDataManageParams")
    public void getDataManageParams(@RequestBody Map<String, Object> req) {
        long id = ReqDataUtils.getLongData(req, "id");
        String msgList = ReqDataUtils.getStringData(req, "msgList");
        String map = ReqDataUtils.getStringData(req, "map");
        List<String> stringList = JsonUtil.toObject(msgList, new TypeReference<List<String>>() {
        });
        Map<String, Object> map1 = JsonUtil.toObject(map, new TypeReference<Map<String, Object>>() {
        });
        sdmConsumerOperationService.getDataManageParams(id, stringList, map1);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveDataBaseMsg")
    public void saveDataBaseMsg(@RequestBody Map<String, Object> req) {
        long sdm_consum_id = ReqDataUtils.getLongData(req, "sdm_consum_id");
        long sdm_des_id = ReqDataUtils.getLongData(req, "sdm_des_id");
        long dsl_id = ReqDataUtils.getLongData(req, "dsl_id");
        long sdm_receive_id = ReqDataUtils.getLongData(req, "sdm_receive_id");
        String reqdqTableColumnBeans = ReqDataUtils.getStringData(req, "dqTableColumnBeans");
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        SdmConToDb sdmConToDb = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConToDb>() {
        });
        SdmDBAdditionBean[] dqTableColumnBeans = JsonUtil.toObject(reqdqTableColumnBeans, new TypeReference<SdmDBAdditionBean[]>() {
        });
        sdmConsumerOperationService.saveDataBaseMsg(sdm_consum_id, sdm_des_id, dsl_id, sdm_receive_id, sdmConToDb, dqTableColumnBeans, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveHbaseMsg")
    public void saveHbaseMsg(@RequestBody Map<String, Object> req) {
        long sdm_des_id = ReqDataUtils.getLongData(req, "sdm_des_id");
        long dsl_id = ReqDataUtils.getLongData(req, "dsl_id");
        long sdm_receive_id = ReqDataUtils.getLongData(req, "sdm_receive_id");
        String reqdqTableColumnBeans = ReqDataUtils.getStringData(req, "dqTableColumnBeans");
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        SdmConHbase sdm_con_hbase = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConHbase>() {
        });
        SdmDBAdditionBean[] dqTableColumnBeans = JsonUtil.toObject(reqdqTableColumnBeans, new TypeReference<SdmDBAdditionBean[]>() {
        });
        sdmConsumerOperationService.saveHbaseMsg(sdm_des_id, dsl_id, sdm_receive_id, sdm_con_hbase, dqTableColumnBeans, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageLayerConfInfo")
    public Map<String, Object> getStorageLayerConfInfo(long dsl_id) {
        return sdmConsumerOperationService.getStorageLayerConfInfo(dsl_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getFiledType")
    public List<Object> getFiledType(long dsl_id) {
        return sdmConsumerOperationService.getFiledType(dsl_id);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/searchDataStore")
    public List<DataStoreLayer> searchDataStore() {
        return sdmConsumerOperationService.searchDataStore();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/downLoadFile")
    public void downLoadFile(long sdm_consum_id) {
        sdmConsumerOperationService.downLoadFile(sdm_consum_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_name", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/selectTopicName")
    public List<Map<String, Object>> selectTopicName(String topic_name) {
        return sdmConsumerOperationService.selectTopicName(topic_name);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveRestMsg")
    public void saveRestMsg(@RequestBody Map<String, Object> req) {
        long sdm_des_id = ReqDataUtils.getLongData(req, "sdm_des_id");
        long sdm_receive_id = ReqDataUtils.getLongData(req, "sdm_receive_id");
        String sdm_con_ext_col = ReqDataUtils.getStringData(req, "sdm_con_ext_col");
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        SdmConRest sdmConRest = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConRest>() {
        });
        SdmConExtCol[] sdmConExtCols = JsonUtil.toObject(sdm_con_ext_col, new TypeReference<SdmConExtCol[]>() {
        });
        sdmConsumerOperationService.saveRestMsg(sdm_des_id, sdm_receive_id, sdmConRest, sdmConExtCols, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveFileMsg")
    public void saveFileMsg(@RequestBody Map<String, Object> req) {
        long sdm_des_id = ReqDataUtils.getLongData(req, "sdm_des_id");
        long sdm_receive_id = ReqDataUtils.getLongData(req, "sdm_receive_id");
        String sdm_con_ext_col = ReqDataUtils.getStringData(req, "sdm_con_ext_col");
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        SdmConFile sdmConFile = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConFile>() {
        });
        SdmConExtCol[] sdmConExtCols = JsonUtil.toObject(sdm_con_ext_col, new TypeReference<SdmConExtCol[]>() {
        });
        sdmConsumerOperationService.saveFileMsg(sdm_des_id, sdm_receive_id, sdmConFile, sdmConExtCols, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveKafkaMsg")
    public void saveKafkaMsg(@RequestBody Map<String, Object> req) {
        long sdm_des_id = ReqDataUtils.getLongData(req, "sdm_des_id");
        long sdm_receive_id = ReqDataUtils.getLongData(req, "sdm_receive_id");
        String sdm_con_ext_col = ReqDataUtils.getStringData(req, "sdm_con_ext_col");
        String is_add = ReqDataUtils.getStringData(req, "is_add");
        SdmConKafka sdmConKafka = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConKafka>() {
        });
        SdmConExtCol[] sdmConExtCols = JsonUtil.toObject(sdm_con_ext_col, new TypeReference<SdmConExtCol[]>() {
        });
        sdmConsumerOperationService.saveKafkaMsg(sdm_des_id, sdm_receive_id, sdmConKafka, sdmConExtCols, is_add);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getConnectTable")
    public List<String> getConnectTable(long dsl_id) {
        return sdmConsumerOperationService.getConnectTable(dsl_id);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/viewConsumerList")
    public Result viewConsumerList() {
        return sdmConsumerOperationService.viewConsumerList();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_consum_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/generateConfigMsg")
    public void generateConfigMsg(long sdm_consum_id) {
        sdmConsumerOperationService.generateConfigMsg(sdm_consum_id);
    }

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/putShuJuKuJson")
    public void putShuJuKuJson(@RequestBody Map<String, Object> req) {
        String params = ReqDataUtils.getStringData(req, "params");
        SdmConsumeDes sdmConsumeDes = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<SdmConsumeDes>() {
        });
        Map<String, Object> map = JsonUtil.toObject(params, new TypeReference<Map<String, Object>>() {
        });
        sdmConsumerOperationService.putShuJuKuJson(sdmConsumeDes, map);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "topic_name", value = "", dataTypeClass = String.class)
    @Return(desc = "", range = "")
    @RequestMapping("/topicIsValid")
    public boolean topicIsValid(String topic_name) {
        return sdmConsumerOperationService.topicIsValid(topic_name);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "dateTime", value = "", dataTypeClass = String.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getDateFormat")
    public String getDateFormat(String dateTime) {
        return sdmConsumerOperationService.getDateFormat(dateTime);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_des_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getDbConf")
    public Map<String, Object> getDbConf(long sdm_des_id) {
        return sdmConsumerOperationService.getDbConf(sdm_des_id);
    }
}
