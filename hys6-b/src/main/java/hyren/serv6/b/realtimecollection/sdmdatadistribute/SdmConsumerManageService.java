package hyren.serv6.b.realtimecollection.sdmdatadistribute;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.FlowApplyStatus;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmVariableType;
import hyren.serv6.base.codes.TopicSource;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.stream.KafkaMonitorManager;
import hyren.serv6.commons.utils.stream.SqlSearchFromKafka;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;
import static hyren.serv6.base.user.UserUtil.getUserId;
import static hyren.serv6.base.user.UserUtil.getUser;

@DocClass(desc = "", author = "yec", createdate = "2021-05-31")
@Service
@Slf4j
public class SdmConsumerManageService {

    @Autowired
    KafkaMonitorManager manager;

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<String> getConsumerTopicList() {
        List<Map<String, Object>> mapTaskList = Dbo.queryList("SELECT t1.sdm_top_name, t1.topic_id, t2.application_status," + "t2.app_id, t4.sdm_receive_id, t4.sdm_receive_name, t5.agent_name," + "t5.user_id, t6.datasource_name FROM " + SdmTopicInfo.TableName + " t1 LEFT JOIN " + SdmUserPermission.TableName + " t2 ON t1.topic_id = t2.topic_id" + " AND t2.consume_user = ? LEFT JOIN " + SdmRecParam.TableName + " t3 ON t1.sdm_top_name = t3.sdm_param_value AND t3.sdm_param_key = 'topic'" + " LEFT JOIN " + SdmReceiveConf.TableName + " t4 ON t4.sdm_receive_id = t3.sdm_receive_id " + " LEFT JOIN " + AgentInfo.TableName + " t5 ON t4.sdm_agent_id = t5.agent_id " + " LEFT JOIN " + DataSource.TableName + " t6 ON t5.source_id = t6.source_id" + " WHERE t1.topic_source != ?", getUserId(), TopicSource.CDC.getCode());
        List<Map<String, Object>> newArray = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        List<Map<String, Object>> array = JsonUtil.toObject(JsonUtil.toJson(mapTaskList), new TypeReference<List<Map<String, Object>>>() {
        });
        for (Object mapTask : array) {
            Map<String, Object> objJson = JsonUtil.toObject(JsonUtil.toJson(mapTask), new TypeReference<Map<String, Object>>() {
            });
            Object topic_id = map.get(objJson.get("topic_id").toString());
            Map<String, Object> newObj = JsonUtil.toObject(JsonUtil.toJson(topic_id), new TypeReference<Map<String, Object>>() {
            });
            if (newObj != null) {
                String recNameOfOld = (String) newObj.get("sdm_receive_name");
                String recNameOfNew = (String) objJson.get("sdm_receive_name");
                String sourceNameOfOld = (String) newObj.get("sdm_source_name");
                String sourceNameOfNew = (String) objJson.get("sdm_source_name");
                String agentNameOfOld = (String) newObj.get("sdm_agent_name");
                String agentNameOfNew = (String) objJson.get("sdm_agent_name");
                if (recNameOfOld != recNameOfNew) {
                    if (null == recNameOfOld) {
                        newObj.put("sdm_receive_name", recNameOfNew);
                    } else if (null == recNameOfNew) {
                        newObj.put("sdm_receive_name", recNameOfOld);
                    } else {
                        newObj.put("sdm_receive_name", recNameOfOld + "、" + recNameOfNew);
                    }
                }
                if (sourceNameOfOld != sourceNameOfNew) {
                    if (null == sourceNameOfOld) {
                        newObj.put("sdm_source_name", sourceNameOfNew);
                    } else if (null == sourceNameOfNew) {
                        newObj.put("sdm_source_name", sourceNameOfOld);
                    } else {
                        newObj.put("sdm_source_name", sourceNameOfOld + "、" + sourceNameOfNew);
                    }
                }
                if (agentNameOfOld != agentNameOfNew) {
                    if (null == agentNameOfOld) {
                        newObj.put("sdm_agent_name", agentNameOfNew);
                    } else if (null == agentNameOfNew) {
                        newObj.put("sdm_agent_name", agentNameOfOld);
                    } else {
                        newObj.put("sdm_agent_name", agentNameOfOld + "、" + agentNameOfNew);
                    }
                }
                newObj.put("user_id", objJson.get("user_id"));
            } else {
                map.put(objJson.get("topic_id").toString(), objJson);
                newArray.add(objJson);
            }
        }
        List<String> listTop = new ArrayList<>();
        Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            listTop.add(JsonUtil.toJson(next.getValue()));
        }
        List<String> finList = new ArrayList<>();
        for (String statusMap : listTop) {
            Map mapSta = JsonUtil.toObject(statusMap, new TypeReference<Map>() {
            });
            Set set = new HashSet();
            if (Objects.isNull(mapSta.get("application_status"))) {
                mapSta.put("application_status", FlowApplyStatus.WeiShenQing.getValue());
            } else {
                mapSta.put("application_status", FlowApplyStatus.ofValueByCode(mapSta.get("application_status").toString()));
            }
            finList.add(JsonUtil.toJson(mapSta));
            set.add(finList);
        }
        return finList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "app_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public int cancelApplicantion(long app_id) {
        return Dbo.execute("delete from " + SdmUserPermission.TableName + " where app_id = ?", app_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "app_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public int againApplicantion(long app_id) {
        return Dbo.execute("update " + SdmUserPermission.TableName + " set application_status = " + FlowApplyStatus.ShenQingZhong.getCode() + " where app_id = ?", app_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_id", desc = "", range = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    public void applyConsume(long topic_id, long sdm_receive_id, long user_id) {
        Optional<SdmUserPermission> permissionInfo = Dbo.queryOneObject(SdmUserPermission.class, "select * from " + SdmUserPermission.TableName + " where topic_id = ? and consume_user = ?", topic_id, getUserId());
        if (permissionInfo.isPresent()) {
            throw new BusinessException("已申请消费，请勿重复申请!");
        }
        SdmUserPermission user_permission = new SdmUserPermission();
        user_permission.setApp_id(PrimayKeyGener.getNextId());
        user_permission.setTopic_id(topic_id);
        user_permission.setApplication_status(FlowApplyStatus.ShenQingZhong.getCode());
        user_permission.setConsume_user(getUserId());
        user_permission.setProduce_user(user_id);
        user_permission.setSdm_receive_id(sdm_receive_id);
        user_permission.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getTopicMessInfo(long sdm_receive_id, int currPage, int pageSize) {
        Map<String, Object> messInfoMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + SdmMessInfo.TableName + " where sdm_receive_id = ?");
        asmSql.addParam(sdm_receive_id);
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<SdmMessInfo> messInfoList = Dbo.queryPagedList(SdmMessInfo.class, page, asmSql.sql(), asmSql.params());
        for (SdmMessInfo messInfo : messInfoList) {
            messInfo.setSdm_var_type(SdmVariableType.ofValueByCode(messInfo.getSdm_var_type()));
            messInfo.setSdm_is_send(IsFlag.ofValueByCode(messInfo.getSdm_is_send()));
        }
        messInfoMap.put("messInfoList", messInfoList);
        messInfoMap.put("totalSize", page.getTotalSize());
        return messInfoMap;
    }

    @Autowired
    SqlSearchFromKafka sqlSearchFromKafka;

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_receive_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDataPreview(long sdm_receive_id) {
        try {
            List<Map<String, Object>> msgArray = new ArrayList<>();
            Map<String, Object> resJo = new HashMap<>();
            String msg = "";
            String log = "";
            Map<String, Object> mapMess = Dbo.queryOneObject("select sdm_param_value as topic_name from " + SdmRecParam.TableName + " where sdm_receive_id = ? AND sdm_param_key = 'topic'", sdm_receive_id);
            String sql = "select * from \"" + mapMess.get("topic_name") + "\" ";
            String execute = sqlSearchFromKafka.execute(sql);
            Map<String, Object> msgObject = JsonUtil.toObject(execute, new TypeReference<Map<String, Object>>() {
            });
            boolean flag = Boolean.parseBoolean(msgObject.get("error").toString());
            if (!flag) {
                log = "0";
                msg = msgObject.get("msg").toString();
                msgArray = JsonUtil.toObject(msg, new TypeReference<List<Map<String, Object>>>() {
                });
            } else {
                log = "1";
            }
            StringBuilder result = new StringBuilder();
            if (msgArray.size() > 0) {
                result.append("<table id=\"\" class=\"table table-bordered table-condensed table-hover\"><thead><tr>");
                Map<String, Object> jsonObject2 = msgArray.get(0);
                Set<String> keySet = jsonObject2.keySet();
                for (String columnName : keySet) {
                    result.append("<th>").append(columnName).append("</th>");
                }
                result.append("</tr></thead><tbody id=\"testTbody\">");
                for (int i = 0; i < msgArray.size(); i++) {
                    Map<String, Object> jsonObject3 = msgArray.get(i);
                    result.append("<tr class=\"text-center\">");
                    for (String columnName : keySet) {
                        result.append("<td>");
                        result.append(jsonObject3.get(columnName).toString());
                        result.append("</td>");
                    }
                    result.append("</tr>");
                }
                result.append("</tbody>");
                result.append("</table>");
            }
            resJo.put("log", log);
            resJo.put("msgArray", result.toString());
            resJo.put("data", msgArray);
            return resJo;
        } catch (BusinessException e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public HashMap<String, Object> consumerTopic(long topic_id) {
        Optional<SdmTopicInfo> sdm_topic_info = Dbo.queryOneObject(SdmTopicInfo.class, "select sdm_partition,sdm_top_name from sdm_topic_info where topic_id = ?", topic_id);
        HashMap<String, Object> map = new HashMap<>();
        if (sdm_topic_info.isPresent()) {
            long sdm_partition = sdm_topic_info.get().getSdm_partition();
            String sdm_top_name = sdm_topic_info.get().getSdm_top_name();
            map.put("sdm_partition", sdm_partition);
            map.put("topic_name", sdm_top_name);
        }
        String brokerServer = manager.parseBrokerServer();
        map.put("brokerServer", brokerServer);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> getKAFKATreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.TRUE);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(TreePageSource.STREAM_MANAGE, getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }
}
