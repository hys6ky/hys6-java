package hyren.serv6.b.realtimecollection.realtimeCollectManagement.topic;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.SdmTopicInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.commons.utils.stream.TopicOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ExecutionException;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2021-03-29")
@Service
@Slf4j
public class SdmTopicService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmTopicInfo", desc = "", range = "", isBean = true)
    public void saveExistSdmTopicInfo(SdmTopicInfo sdmTopicInfo) {
        List<String> topicList = getTopicList(sdmTopicInfo.getSdm_zk_host());
        if (topicList.contains(sdmTopicInfo.getSdm_top_name())) {
            isExistTopicName(sdmTopicInfo.getSdm_top_name());
            sdmTopicInfo.setTopic_id(PrimayKeyGener.getNextId());
            sdmTopicInfo.setUser_id(getUserId());
            sdmTopicInfo.setCreate_date(DateUtil.getSysDate());
            sdmTopicInfo.setCreate_time(DateUtil.getSysTime());
            sdmTopicInfo.add(Dbo.db());
        } else {
            throw new BusinessException("该主题:" + sdmTopicInfo.getSdm_top_name() + "在kafka中不存在,登记失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmTopicInfo", desc = "", range = "", isBean = true)
    public void saveSdmTopicInfo(SdmTopicInfo sdmTopicInfo) {
        fieldLegalityValidation(sdmTopicInfo.getSdm_top_name(), sdmTopicInfo.getSdm_top_cn_name());
        isExistTopicName(sdmTopicInfo.getSdm_top_name());
        sdmTopicInfo.setTopic_id(PrimayKeyGener.getNextId());
        if (null == sdmTopicInfo.getUser_id()) {
            sdmTopicInfo.setUser_id(getUserId());
        }
        sdmTopicInfo.setCreate_date(DateUtil.getSysDate());
        sdmTopicInfo.setCreate_time(DateUtil.getSysTime());
        sdmTopicInfo.add(Dbo.db());
        List<String> topicList = getTopicList(sdmTopicInfo.getSdm_zk_host());
        if (!topicList.contains(sdmTopicInfo.getSdm_top_name())) {
            TopicOperator to = new TopicOperator(sdmTopicInfo.getSdm_top_name(), sdmTopicInfo.getSdm_zk_host());
            int sdm_partition = sdmTopicInfo.getSdm_partition().intValue();
            int sdm_replication = sdmTopicInfo.getSdm_replication().intValue();
            to.createTopic(sdm_partition, sdm_replication);
        } else {
            throw new BusinessException("该主题:" + sdmTopicInfo.getSdm_top_name() + "在kafka队列中已存在,请勿重复添加!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "topic_id", desc = "", range = "")
    public void deleteSdmTopicInfo(long topic_id) {
        SdmTopicInfo sdmTopicInfo = Dbo.queryOneObject(SdmTopicInfo.class, "select * from sdm_topic_info where topic_id=?", topic_id).orElseThrow(() -> new BusinessException("sql查询错误！"));
        DboExecute.deletesOrThrow("根据topic_id删除sdm_topic_info信息表数据失败，topic_id=" + topic_id, "delete from " + SdmTopicInfo.TableName + " where topic_id = ? and user_id = ?", topic_id, getUserId());
        TopicOperator to = new TopicOperator(sdmTopicInfo.getSdm_top_name(), sdmTopicInfo.getSdm_zk_host());
        to.deleteTopic();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchSdmTopicInfo(int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + SdmTopicInfo.TableName + " where user_id = ?");
        asmSql.addParam(getUserId());
        Map<String, Object> sdmTopicInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<SdmTopicInfo> sdmTopic = Dbo.queryPagedList(SdmTopicInfo.class, page, asmSql.sql(), asmSql.params());
        sdmTopicInfoMap.put("sdmTopic", sdmTopic);
        sdmTopicInfoMap.put("totalSize", page.getTotalSize());
        sdmTopicInfoMap.put("zk_host", PropertyParaValue.getString("kafka_zk_address", "hyshf@beyondsoft.com"));
        return sdmTopicInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_top_name", desc = "", range = "")
    @Param(name = "sdm_zk_host", desc = "", range = "")
    @Param(name = "sdm_top_cn_name", desc = "", range = "")
    @Param(name = "sdm_partition", desc = "", range = "")
    @Param(name = "sdm_replication", desc = "", range = "")
    @Param(name = "sdm_top_value", desc = "", range = "", nullable = true)
    @Param(name = "topic_id", desc = "", range = "")
    public void updateSdmTopicInfo(String sdm_top_name, String sdm_zk_host, String sdm_top_cn_name, long sdm_partition, long sdm_replication, String sdm_top_value, long topic_id) {
        Dbo.execute("update " + SdmTopicInfo.TableName + " set sdm_top_name=?,sdm_zk_host=?,sdm_top_cn_name=?,sdm_partition=?,sdm_replication=?,sdm_top_value=? where topic_id=?", sdm_top_name, sdm_zk_host, sdm_top_cn_name, sdm_partition, sdm_replication, sdm_top_value, topic_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_top_name", desc = "", range = "")
    @Param(name = "sdm_top_cn_name", desc = "", range = "")
    private void fieldLegalityValidation(String sdm_top_name, String sdm_top_cn_name) {
        if (StringUtil.isBlank(sdm_top_name)) {
            throw new BusinessException("sdm_top_name不为空且不为空格，sdm_top_name=" + sdm_top_name);
        }
        if (StringUtil.isBlank(sdm_top_cn_name)) {
            throw new BusinessException("sdm_top_cn_name不为空且不为空格，sdm_top_cn_name=" + sdm_top_cn_name);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_top_name", desc = "", range = "")
    private void isExistTopicName(String sdm_top_name) {
        if (Dbo.queryNumber("select count(1) from " + SdmTopicInfo.TableName + " where sdm_top_name=? and user_id=?", sdm_top_name, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该主题名称已存在:" + sdm_top_name + ",请勿重复添加!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "zookeeperHost", desc = "", range = "")
    public List<String> getTopicList(String zookeeperHost) {
        TopicOperator topicOperator = new TopicOperator(zookeeperHost);
        Properties adminClientProperties = topicOperator.getAdminClientProperties();
        Set<String> topics = null;
        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            ListTopicsResult topicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> topicsFuture = topicsResult.names();
            topics = topicsFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        List<String> allTopicList = new ArrayList<>(topics);
        return new ArrayList<>(allTopicList);
    }
}
