package hyren.serv6.b.realtimecollection.sdmcollecttask.restjieshoutask;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.stream.KafkaMonitorManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2020-04-20")
@Slf4j
@Service
public class SetRestJieShouTaskService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmReceiveConf", desc = "", range = "", isBean = true)
    @Param(name = "sdmRecParam", desc = "", range = "", isBean = true)
    @Param(name = "sdmMessInfo", desc = "", range = "", isBean = true)
    public void saveSdmRestManage(SdmReceiveConf sdmReceiveConf, SdmRecParam[] sdmRecParam, SdmMessInfo[] sdmMessInfo) {
        checkSdmMessInfos(sdmRecParam, sdmMessInfo, "SAVE");
        if (Dbo.queryNumber("SELECT count(1) FROM " + SdmReceiveConf.TableName + " WHERE sdm_receive_name = ?", sdmReceiveConf.getSdm_receive_name()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该任务名称已存在!");
        }
        long sdm_receive_id = PrimayKeyGener.getNextId();
        sdmReceiveConf.setSdm_receive_id(sdm_receive_id);
        if (StringUtil.isNotEmpty(sdmReceiveConf.getIs_data_partition())) {
            sdmReceiveConf.setSdm_dat_delimiter(StringUtil.string2Unicode(sdmReceiveConf.getSdm_dat_delimiter()));
        }
        sdmReceiveConf.setCreate_date(DateUtil.getSysDate());
        sdmReceiveConf.setCreate_time(DateUtil.getSysTime());
        sdmReceiveConf.add(Dbo.db());
        for (SdmRecParam sdm_rec_param : sdmRecParam) {
            if ("topic".equals(sdm_rec_param.getSdm_param_key())) {
                if (topicIsValid(sdm_rec_param.getSdm_param_value())) {
                    throw new BusinessException("该主题名称无效!");
                }
            }
            sdm_rec_param.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param.setSdm_receive_id(sdm_receive_id);
            sdm_rec_param.add(Dbo.db());
        }
        for (SdmMessInfo sdm_mess_info : sdmMessInfo) {
            int number = 1;
            sdm_mess_info.setMess_info_id(PrimayKeyGener.getNextId());
            sdm_mess_info.setNum(number + "");
            sdm_mess_info.setSdm_receive_id(sdm_receive_id);
            number++;
            sdm_mess_info.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean topicIsValid(String topic_name) {
        Map<String, Object> map = Dbo.queryOneObject("SELECT * FROM sdm_topic_info WHERE sdm_top_name = ? ", topic_name);
        return map.isEmpty();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmReceiveConf", desc = "", range = "", isBean = true)
    @Param(name = "sdmRecParam", desc = "", range = "", isBean = true)
    @Param(name = "sdmMessInfo", desc = "", range = "", isBean = true)
    public void updateSdmRestManage(SdmReceiveConf sdmReceiveConf, SdmRecParam[] sdmRecParam, SdmMessInfo[] sdmMessInfo) {
        checkSdmMessInfos(sdmRecParam, sdmMessInfo, "UPDATE");
        int ret = sdmReceiveConf.update(Dbo.db());
        if (ret != 1) {
            throw new BusinessException("更新流数据管理接收端信息失败!");
        }
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.execute(db, "delete from " + SdmRecParam.TableName + " where sdm_receive_id = ?", sdmReceiveConf.getSdm_receive_id());
            SqlOperator.execute(db, "delete from " + SdmMessInfo.TableName + " where sdm_receive_id = ?", sdmReceiveConf.getSdm_receive_id());
            db.commit();
        }
        for (SdmRecParam sdm_rec_param : sdmRecParam) {
            if ("topic".equals(sdm_rec_param.getSdm_param_key())) {
                if (topicIsValid(sdm_rec_param.getSdm_param_value())) {
                    throw new BusinessException("该主题名称无效!");
                }
            }
            sdm_rec_param.setRec_param_id(PrimayKeyGener.getNextId());
            sdm_rec_param.setSdm_receive_id(sdmReceiveConf.getSdm_receive_id());
            sdm_rec_param.add(Dbo.db());
        }
        for (SdmMessInfo sdm_mess_info : sdmMessInfo) {
            int number = 1;
            sdm_mess_info.setMess_info_id(PrimayKeyGener.getNextId());
            sdm_mess_info.setNum(number + "");
            sdm_mess_info.setSdm_receive_id(sdmReceiveConf.getSdm_receive_id());
            number++;
            sdm_mess_info.add(Dbo.db());
        }
    }

    private void checkSdmMessInfos(SdmRecParam[] sdmRecParam, SdmMessInfo[] sdmMessInfo, String flag) {
        try {
            String topic = StringUtils.EMPTY;
            for (SdmRecParam recParam : sdmRecParam) {
                if (recParam.getSdm_param_key().equals("topic"))
                    topic = recParam.getSdm_param_value();
            }
            List<Map<String, Object>> list = Dbo.queryList("select DISTINCT sdm_receive_id from " + SdmRecParam.TableName + " where sdm_param_value = ?", topic);
            if (list.size() == 0) {
                return;
            }
            if (flag.equals("UPDATE") && list.size() == 1) {
                return;
            }
            Long sdm_receive_id = Long.valueOf(list.get(0).get("sdm_receive_id").toString());
            List<SdmMessInfo> messInfoList = Dbo.queryList(SdmMessInfo.class, "select * from " + SdmMessInfo.TableName + " where sdm_receive_id = ? order by mess_info_id", sdm_receive_id);
            List<SdmMessInfo> sdmMessInfos = Arrays.asList(sdmMessInfo);
            List<String> dbList = messInfoList.stream().map(SdmMessInfo::getSdm_var_name_en).collect(Collectors.toList());
            if (dbList.size() == 0) {
                return;
            }
            List<String> webList = sdmMessInfos.stream().map(SdmMessInfo::getSdm_var_name_en).collect(Collectors.toList());
            Collections.sort(dbList);
            Collections.sort(webList);
            if (dbList.size() != webList.size()) {
                throw new BusinessException("同一主题下只能生产同一结构数据,当前主题已新建结构为：" + JsonUtil.toJson(dbList));
            }
            for (int i = 0; i < dbList.size(); i++) {
                if (!dbList.get(i).equals(webList.get(i))) {
                    throw new BusinessException("同一主题下只能生产同一结构数据,当前主题已新建结构为：" + JsonUtil.toJson(dbList));
                }
            }
        } catch (NumberFormatException | BusinessException e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    @Autowired
    KafkaMonitorManager manager;

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_agent_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    public Map<String, Object> selectRestJieShouTask(long sdm_agent_id, long sdm_receive_id) {
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
}
