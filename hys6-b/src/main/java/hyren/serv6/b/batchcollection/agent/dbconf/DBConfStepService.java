package hyren.serv6.b.batchcollection.agent.dbconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.SqlOperator.Assembler;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.bean.StoreConnectionBean;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.codes.DataExtractType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.database.DatabaseConnUtil;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import hyren.serv6.commons.utils.fileutil.read.ReadLog;
import io.swagger.annotations.Api;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Api("配置源DB属性")
@Service
@DocClass(desc = "", author = "WangZhengcheng")
public class DBConfStepService {

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getDBConfInfo(long databaseId) {
        return Dbo.queryResult("select t1.dsl_id, t1.database_id, t1.agent_id, t1.database_number, t1.task_name,t1.collect_type, " + " t1.db_agent, t1.is_sendok, t1.fetch_size, t2.classify_id, t2.classify_num, " + " t2.classify_name, t2.remark " + " from " + DatabaseSet.TableName + " t1 " + " left join " + CollectJobClassify.TableName + " t2 on t1.classify_id = t2.classify_id" + " where database_id = ? AND t1.collect_type = ? ", databaseId, CollectType.ShuJuKuChouShu.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Result addDBConfInfo(long databaseId, long agent_id) {
        return Dbo.queryResult("select t1.dsl_id, t1.database_id, t1.agent_id, t1.database_number, t1.task_name,t1.collect_type, " + " t1.db_agent, t1.is_sendok, t2.classify_id, t2.classify_num, " + " t2.classify_name, t2.remark, t1.fetch_size" + " from " + DatabaseSet.TableName + " t1 " + " left join " + CollectJobClassify.TableName + " t2 on " + " t1.classify_id = t2.classify_id  join " + AgentInfo.TableName + " ai on t1.agent_id = ai.agent_id " + "where  t1.is_sendok = ? AND ai.agent_type = ? AND ai.user_id = ? " + "AND ai.source_id = ? AND ai.agent_id = ? AND t1.collect_type = ?", IsFlag.Fou.getCode(), AgentType.ShuJuKu.getCode(), getUserId(), databaseId, agent_id, CollectType.ShuJuKuChouShu.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db_name", desc = "", range = "")
    @Param(name = "port", desc = "", range = "", nullable = true, valueIfNull = "")
    @Return(desc = "", range = "", isBean = true)
    public DBConnectionProp getDBConnectionProp(String db_name, String port) {
        return DatabaseConnUtil.getConnParamInfo(db_name, port);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getHisConnection(long agentId) {
        return Dbo.queryResult("select ds.dsl_id from " + DatabaseSet.TableName + " ds" + " where ds.database_id in" + " (select max(database_id) as id from database_set group by database_port, database_ip)" + " and ds.agent_id = ? ", agentId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classifyId", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean checkClassifyId(long classifyId) {
        long count = Dbo.queryNumber(" SELECT count(1) FROM " + CollectJobClassify.TableName + " WHERE classify_id = ? AND user_id = ? ", classifyId, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("采集作业分类信息不存在");
        }
        long val = Dbo.queryNumber("SELECT count(1) FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id " + " JOIN " + DatabaseSet.TableName + " das ON ai.agent_id = das.agent_id " + " WHERE das.classify_id = ? AND ai.user_id = ? ", classifyId, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        return val == 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<CollectJobClassify> getClassifyInfo(long sourceId) {
        return Dbo.queryList(CollectJobClassify.class, "SELECT cjc.* FROM " + DataSource.TableName + " ds " + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + CollectJobClassify.TableName + " cjc ON ai.agent_id = cjc.agent_id" + " WHERE ds.source_id = ? AND cjc.user_id = ? order by cjc.classify_num ", sourceId, getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify", desc = "", range = "", isBean = true)
    @Param(name = "sourceId", desc = "", range = "")
    public void saveClassifyInfo(CollectJobClassify classify, long sourceId) {
        verifyClassifyEntity(classify, true);
        long val = Dbo.queryNumber("SELECT count(1) FROM " + CollectJobClassify.TableName + " cjc " + " LEFT JOIN " + AgentInfo.TableName + " ai ON cjc.agent_id=ai.agent_id" + " LEFT JOIN " + DataSource.TableName + " ds ON ds.source_id=ai.source_id" + " WHERE cjc.classify_num=? AND ds.source_id=?", classify.getClassify_num(), sourceId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 0) {
            throw new BusinessException("分类编号重复，请重新输入");
        }
        classify.setClassify_id(PrimayKeyGener.getNextId());
        classify.setUser_id(getUserId());
        classify.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify", desc = "", range = "", isBean = true)
    @Param(name = "sourceId", desc = "", range = "")
    public void updateClassifyInfo(CollectJobClassify classify, long sourceId) {
        verifyClassifyEntity(classify, false);
        long val = Dbo.queryNumber("SELECT count(1) FROM " + CollectJobClassify.TableName + " cjc " + " LEFT JOIN " + AgentInfo.TableName + " ai ON cjc.agent_id=ai.agent_id" + " LEFT JOIN " + DataSource.TableName + " ds ON ds.source_id=ai.source_id" + " WHERE cjc.classify_id=? AND ds.source_id=? AND ai.user_id = ? ", classify.getClassify_id(), sourceId, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("待更新的数据不存在");
        }
        long count = Dbo.queryNumber("SELECT count(1) FROM " + CollectJobClassify.TableName + " cjc" + " LEFT JOIN " + AgentInfo.TableName + " ai ON cjc.agent_id=ai.agent_id" + " LEFT JOIN " + DataSource.TableName + " ds ON ds.source_id=ai.source_id" + " WHERE cjc.classify_num = ? AND ds.source_id = ? AND ds.create_user_id = ? and cjc.classify_id != ?", classify.getClassify_num(), sourceId, getUserId(), classify.getClassify_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 0) {
            throw new BusinessException("分类编号重复，请重新输入");
        }
        if (classify.update(Dbo.db()) != 1) {
            throw new BusinessException("保存分类信息失败！data=" + classify);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classifyId", desc = "", range = "")
    public void deleteClassifyInfo(long classifyId) {
        boolean flag = checkClassifyId(classifyId);
        if (!flag) {
            throw new BusinessException("待删除的采集任务分类已被使用，不能删除");
        }
        DboExecute.deletesOrThrowNoMsg("delete from " + CollectJobClassify.TableName + " where classify_id = ?", classifyId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public long saveDbConf(DatabaseSet databaseSet) {
        verifyDatabaseSetEntity(databaseSet);
        if (StringUtil.isNotBlank(databaseSet.getDatabase_number())) {
            String sql = "select count(1) from " + DatabaseSet.TableName + " where database_number = ?";
            Assembler assembler = Assembler.newInstance().addSql(sql).addParam(databaseSet.getDatabase_number());
            if (databaseSet.getDatabase_id() != null) {
                assembler.addSql(" AND database_id != ?").addParam(databaseSet.getDatabase_id());
            }
            long val = Dbo.queryNumber(assembler.sql(), assembler.params()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (val != 0) {
                throw new BusinessException("作业编号重复，请重新定义作业编号");
            }
        }
        if (databaseSet.getDatabase_id() != null) {
            long val = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", databaseSet.getDatabase_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (val != 1) {
                throw new BusinessException("待更新的数据不存在");
            }
            databaseSet.setDb_agent(IsFlag.Fou.getCode());
            databaseSet.setCp_or(JsonUtil.toJson(Constant.DATABASE_CLEAN));
            databaseSet.update(Dbo.db());
            Dbo.execute(" UPDATE " + DataExtractionDef.TableName + " SET data_extract_type = ? WHERE table_id " + "in (SELECT table_id FROM " + TableInfo.TableName + " WHERE database_id = ?)", DataExtractType.ShuJuKuChouQuLuoDi.getCode(), databaseSet.getDatabase_id());
        } else {
            long id = PrimayKeyGener.getNextId();
            databaseSet.setDatabase_id(id);
            databaseSet.setDb_agent(IsFlag.Fou.getCode());
            databaseSet.setIs_sendok(IsFlag.Fou.getCode());
            databaseSet.setCollect_type(CollectType.ShuJuKuChouShu.getCode());
            databaseSet.setCp_or(JsonUtil.toJson(Constant.DATABASE_CLEAN));
            databaseSet.add(Dbo.db());
        }
        return databaseSet.getDatabase_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "", isBean = true)
    public void testConnection(DatabaseSet databaseSet) {
        String url = AgentActionUtil.getUrl(databaseSet.getAgent_id(), getUserId(), AgentActionUtil.TESTCONNECTION);
        StoreConnectionBean storeConnectionBean = this.setStoreConnectionBean(databaseSet.getDsl_id());
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("database_drive", storeConnectionBean.getDatabase_driver()).addData("jdbc_url", storeConnectionBean.getJdbc_url()).addData("user_name", storeConnectionBean.getUser_name()).addData("database_pad", storeConnectionBean.getDatabase_pwd()).addData("database_type", storeConnectionBean.getDatabase_type()).addData("database_name", storeConnectionBean.getDatabase_name()).addData("fetch_size", databaseSet.getFetch_size()).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                Map<String, String> exeMap = JsonUtil.toObject(JsonUtil.toJson(ar.getData()), new TypeReference<Map<String, String>>() {
                });
                throw new BusinessException("测试连接失败：" + exeMap.get("exMessage"));
            }
        } catch (Exception e) {
            throw new BusinessException("与Agent端交互异常!!!" + e.getMessage());
        }
    }

    public StoreConnectionBean setStoreConnectionBean(Long dslId) {
        Result connectionResult = getDBConnectionDerils(dslId);
        StoreConnectionBean storeConnectionBean = new StoreConnectionBean();
        if (!connectionResult.isEmpty()) {
            for (Map<String, Object> map : connectionResult.toList()) {
                if (map.get("storage_property_key").equals("database_type")) {
                    storeConnectionBean.setDatabase_type(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("database_driver")) {
                    storeConnectionBean.setDatabase_driver(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("user_name")) {
                    storeConnectionBean.setUser_name(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("database_pwd")) {
                    storeConnectionBean.setDatabase_pwd(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("database_name")) {
                    storeConnectionBean.setDatabase_name(map.get("storage_property_val").toString());
                }
                if (map.get("storage_property_key").equals("jdbc_url")) {
                    storeConnectionBean.setJdbc_url(map.get("storage_property_val").toString());
                }
            }
        }
        return storeConnectionBean;
    }

    private Result getDBConnectionDerils(Long dslId) {
        return Dbo.queryResult(" select t1.storage_property_key, t1.storage_property_val,t2.store_type " + " FROM " + DataStoreLayerAttr.TableName + " t1 JOIN " + DataStoreLayer.TableName + " t2 ON t1.dsl_id = t2.dsl_id  WHERE t1.dsl_id = ?", dslId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "", nullable = true, valueIfNull = "100")
    @Return(desc = "", range = "")
    public String viewLog(long agentId, int readNum) {
        AgentDownInfo agentDownInfo = Dbo.queryOneObject(AgentDownInfo.class, "select * from " + AgentDownInfo.TableName + " where agent_id = ? and user_id = ?", agentId, getUserId()).orElseThrow(() -> new BusinessException("根据AgentID和userID未能找到Agent下载信息"));
        String logDir = agentDownInfo.getLog_dir();
        SSHDetails sshDetails = new SSHDetails();
        sshDetails.setHost(agentDownInfo.getAgent_ip());
        sshDetails.setPort(CommonVariables.SFTP_PORT);
        sshDetails.setUser_name(agentDownInfo.getUser_name());
        sshDetails.setPwd(agentDownInfo.getPasswd());
        if (readNum > 1000) {
            readNum = 1000;
        }
        String taskLog = ReadLog.readAgentLog(logDir, sshDetails, readNum);
        if (StringUtil.isBlank(taskLog)) {
            return "未获取到日志";
        }
        return taskLog;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "entity", desc = "", range = "")
    @Param(name = "isAdd", desc = "", range = "")
    private void verifyClassifyEntity(CollectJobClassify entity, boolean isAdd) {
        if (!isAdd) {
            if (entity.getClassify_id() == null) {
                throw new BusinessException("分类id不能为空");
            }
        }
        if (StringUtil.isBlank(entity.getClassify_num())) {
            throw new BusinessException("分类编号不能为空");
        }
        if (StringUtil.isBlank(entity.getClassify_name())) {
            throw new BusinessException("分类名称不能为空");
        }
        if (entity.getAgent_id() == null) {
            throw new BusinessException("AgentID不能为空");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "")
    private void verifyDatabaseSetEntity(DatabaseSet databaseSet) {
        Validator.notNull(databaseSet.getDsl_id(), "保存数据库配置信息时dsl_id不能为空");
        if (databaseSet.getClassify_id() == null) {
            throw new BusinessException("保存数据库配置信息时分类信息不能为空");
        }
        if (StringUtil.isNotBlank(databaseSet.getDatabase_number()) && databaseSet.getDatabase_number().length() > 10) {
            throw new BusinessException("保存数据库配置信息时作业编号不为能空，并且长度不能超过10");
        }
        if (databaseSet.getAgent_id() == null) {
            throw new BusinessException("保存数据库配置信息时必须关联Agent信息不能为空");
        }
    }

    @Method(desc = "", logicStep = "")
    public List<Object> getDatabaseInfo() {
        return Dbo.queryOneColumnList("select database_name from " + DatabaseInfo.TableName);
    }
}
