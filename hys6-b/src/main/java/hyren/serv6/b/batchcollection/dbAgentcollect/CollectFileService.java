package hyren.serv6.b.batchcollection.dbAgentcollect;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.CollectJobClassify;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Api("数据文件采集配置管理")
@Service
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-04-10 16:08")
public class CollectFileService {

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getInitDataFileData(long colSetId, long agent_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM  " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("任务( %s )不存在!!!", colSetId);
        }
        return Dbo.queryOneObject("SELECT t1.*,t2.classify_num,t2.classify_name FROM " + DatabaseSet.TableName + " t1 JOIN " + CollectJobClassify.TableName + " t2 ON t1.classify_id = t2.classify_id WHERE t2.user_id = ? AND t1.is_sendok = ? AND t1.database_id = ? AND t1.agent_id = ?", getUserId(), IsFlag.Shi.getCode(), colSetId, agent_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> addDataFileData(long source_id, long agent_id) {
        return Dbo.queryOneObject("SELECT t1.*,t2.classify_num,t2.classify_name FROM " + DatabaseSet.TableName + " t1 JOIN " + CollectJobClassify.TableName + " t2 ON t1.classify_id = t2.classify_id join " + AgentInfo.TableName + " ai on t1.agent_id = ai.agent_id " + " where t1.is_sendok = ? AND ai.agent_type = ? AND ai.user_id = ? AND ai.source_id = ? AND t1.agent_id = ?", IsFlag.Fou.getCode(), AgentType.DBWenJian.getCode(), getUserId(), source_id, agent_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public String saveDataFile(DatabaseSet database_set) {
        CheckParam.checkData("采集任务名称不能为空", database_set.getTask_name());
        CheckParam.checkData("采集数据文件路径不能为空", database_set.getPlane_url());
        CheckParam.checkData("分类编号不能为空", String.valueOf(database_set.getClassify_id()));
        if (StringUtils.isNotBlank(database_set.getDatabase_number()) && database_set.getDatabase_number().length() > 10) {
            throw new BusinessException("保存数据库配置信息时作业编号不为能空，并且长度不能超过10");
        }
        checkDatabaseInfo(database_set);
        String database_id = String.valueOf(PrimayKeyGener.getNextId());
        database_set.setDb_agent(IsFlag.Shi.getCode());
        database_set.setDatabase_id(Long.valueOf(database_id));
        database_set.setIs_sendok(IsFlag.Fou.getCode());
        database_set.setCp_or(JsonUtil.toJson(Constant.DATABASE_CLEAN));
        database_set.setCollect_type(CollectType.ShuJuKuCaiJi.getCode());
        database_set.add(Dbo.db());
        return database_id;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public String updateDataFile(DatabaseSet database_set) {
        if (database_set.getDatabase_id() == null) {
            CheckParam.throwErrorMsg("任务ID不能为空");
        }
        CheckParam.checkData("采集数据文件路径不能为空", database_set.getPlane_url());
        if (database_set.getClassify_id() == null) {
            CheckParam.throwErrorMsg("分类编号不能为空");
        }
        checkDatabaseInfo(database_set);
        database_set.update(Dbo.db());
        return database_set.getDatabase_id().toString();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "path", desc = "", valueIfNull = "", range = "")
    @Return(desc = "", range = "")
    public List<Map> selectPath(long agent_id, String path) {
        String url = AgentActionUtil.getUrl(agent_id, getUserId(), AgentActionUtil.GETSYSTEMFILEINFO);
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("pathVal", path).addData("isFile", "true").post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("连接远程Agent获取文件夹失败");
            }
            List<Map> list = new ArrayList<>();
            list.addAll((Collection<? extends Map>) ar.getData());
            return list;
        } catch (Exception e) {
            throw new BusinessException("与Agent端交互异常!!!" + e.getMessage());
        }
    }

    void checkDatabaseInfo(DatabaseSet database_set) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE task_name = '").append(database_set.getTask_name()).append("'");
        if (database_set.getDatabase_id() != null) {
            sql.append(" AND database_id != ").append(database_set.getDatabase_id());
        }
        long countNum = Dbo.queryNumber(sql.toString()).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 1) {
            CheckParam.throwErrorMsg("采集任务名称(%s),已经存在", database_set.getTask_name());
        }
        sql.delete(0, sql.length());
        if (StringUtil.isNotBlank(database_set.getDatabase_number())) {
            sql.append("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_number = ").append("'" + database_set.getDatabase_number() + "'");
            if (database_set.getDatabase_id() != null) {
                sql.append(" AND database_id != ").append(database_set.getDatabase_id());
            }
            countNum = Dbo.queryNumber(sql.toString()).orElseThrow(() -> new BusinessException("SQL查询异常"));
            if (countNum == 1) {
                CheckParam.throwErrorMsg("采集作业编号(%s),已经存在", database_set.getDatabase_number());
            }
        }
    }
}
