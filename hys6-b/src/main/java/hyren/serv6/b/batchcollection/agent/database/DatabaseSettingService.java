package hyren.serv6.b.batchcollection.agent.database;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.codes.DataExtractType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Api("数据库采集管理类")
@Service
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-08-17 14:32")
public class DatabaseSettingService {

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", range = "", desc = "")
    @Param(name = "agent_id", range = "", desc = "")
    @Return(desc = "", range = "")
    public Result getInitDatabase(long source_id, long agent_id) {
        return Dbo.queryResult("SELECT t1.*, t2.classify_id, t2.classify_num, " + " t2.classify_name, t2.remark " + " FROM " + DatabaseSet.TableName + " t1 " + " JOIN " + CollectJobClassify.TableName + " t2 ON " + " t1.classify_id = t2.classify_id  JOIN " + AgentInfo.TableName + " ai ON t1.agent_id = ai.agent_id " + "WHERE  t1.is_sendok = ? AND ai.agent_type = ? AND ai.user_id = ? " + "AND ai.source_id = ? AND ai.agent_id = ? AND t1.collect_type = ?", IsFlag.Fou.getCode(), AgentType.ShuJuKu.getCode(), getUserId(), source_id, agent_id, CollectType.ShuJuKuCaiJi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Return(desc = "", range = "")
    public String editorDatabase(long databaseId) {
        long countNum = Dbo.queryNumber("SELECT count(1) " + " FROM " + DatabaseSet.TableName + " das " + " JOIN " + AgentInfo.TableName + " ai ON ai.agent_id = das.agent_id " + " WHERE das.database_id = ? AND ai.user_id = ? AND das.is_sendok = ?", databaseId, getUserId(), IsFlag.Shi.getCode()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            CheckParam.throwErrorMsg("根据用户ID(%s),未找到任务ID为(%s)的数据信息", getUserId(), databaseId);
        }
        return AesUtil.encrypt(Dbo.queryResult("SELECT t1.*, t2.classify_id, t2.classify_num,t2.classify_name, t2.remark " + " FROM " + DatabaseSet.TableName + " t1 " + " JOIN " + CollectJobClassify.TableName + " t2 ON " + " t1.classify_id = t2.classify_id  WHERE database_id = ? AND t1.is_sendok = ? AND t1.collect_type = ?", databaseId, IsFlag.Shi.getCode(), CollectType.ShuJuKuCaiJi.getCode()).toJSON());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Return(desc = "", range = "")
    public String editorDatabaseSSCJ(long databaseId) {
        long countNum = Dbo.queryNumber("SELECT count(1) " + " FROM " + DatabaseSet.TableName + " das " + " JOIN " + AgentInfo.TableName + " ai ON ai.agent_id = das.agent_id " + " WHERE das.database_id = ? AND ai.user_id = ? AND das.is_sendok = ?", databaseId, getUserId(), IsFlag.Shi.getCode()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            CheckParam.throwErrorMsg("根据用户ID(%s),未找到任务ID为(%s)的数据信息", getUserId(), databaseId);
        }
        return AesUtil.encrypt(Dbo.queryResult("SELECT t1.*, t2.classify_id, t2.classify_num,t2.classify_name, t2.remark " + " FROM " + DatabaseSet.TableName + " t1 " + " JOIN " + CollectJobClassify.TableName + " t2 ON " + " t1.classify_id = t2.classify_id  WHERE database_id = ? AND t1.is_sendok = ? AND t1.collect_type = ?", databaseId, IsFlag.Shi.getCode(), CollectType.ShiShiCaiJi.getCode()).toJSON());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Long saveDatabaseInfo(DatabaseSet databaseSet) {
        verifyDatabaseSetEntity(databaseSet);
        long val = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE task_name = ?", databaseSet.getTask_name()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 0) {
            CheckParam.throwErrorMsg("任务名称(%s)重复，请重新定义任务名称", databaseSet.getTask_name());
        }
        if (StringUtil.isNotBlank(databaseSet.getDatabase_number())) {
            val = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_number = ?", databaseSet.getDatabase_number()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (val != 0) {
                CheckParam.throwErrorMsg("作业编号(%s)重复，请重新定义作业编号", databaseSet.getDatabase_number());
            }
        }
        databaseSet.setDatabase_id(PrimayKeyGener.getNextId());
        if (databaseSet.getCollect_type().equals(CollectType.ShiShiCaiJi.getCode())) {
            databaseSet.setCollect_type(CollectType.ShiShiCaiJi.getCode());
        } else {
            databaseSet.setCollect_type(CollectType.ShuJuKuCaiJi.getCode());
        }
        databaseSet.setDb_agent(IsFlag.Fou.getCode());
        databaseSet.setIs_sendok(IsFlag.Fou.getCode());
        databaseSet.setCp_or(JsonUtil.toJson(Constant.DEFAULT_TABLE_CLEAN_ORDER));
        databaseSet.add(Dbo.db());
        return databaseSet.getDatabase_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Long updateDatabaseInfo(DatabaseSet databaseSet) {
        Validator.notNull(databaseSet.getDatabase_id(), "更新时未获取到主键ID信息");
        verifyDatabaseSetEntity(databaseSet);
        long val = Dbo.queryNumber("SELECT COUNT(1) from " + DatabaseSet.TableName + " WHERE task_name = ? AND database_id != ?", databaseSet.getTask_name(), databaseSet.getDatabase_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 0) {
            CheckParam.throwErrorMsg("任务名称(%s)重复，请重新定义任务名称", databaseSet.getTask_name());
        }
        if (StringUtil.isNotBlank(databaseSet.getDatabase_number())) {
            val = Dbo.queryNumber("SELECT COUNT(1) from " + DatabaseSet.TableName + " WHERE database_number = ? AND database_id != ?", databaseSet.getDatabase_number(), databaseSet.getDatabase_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (val != 0) {
                CheckParam.throwErrorMsg("作业编号(%s)重复，请重新定义作业编号", databaseSet.getDatabase_number());
            }
        }
        try {
            databaseSet.update(Dbo.db());
            Dbo.execute(" UPDATE " + DataExtractionDef.TableName + " SET data_extract_type = ? WHERE table_id " + "in (SELECT table_id FROM " + TableInfo.TableName + " WHERE database_id = ?)", DataExtractType.YuanShuJuGeShi.getCode(), databaseSet.getDatabase_id());
        } catch (Exception e) {
            if (!(e instanceof ProEntity.EntityDealZeroException)) {
                throw new BusinessException(e.getMessage());
            }
        }
        return databaseSet.getDatabase_id();
    }

    private void verifyDatabaseSetEntity(DatabaseSet databaseSet) {
        Validator.notNull(databaseSet.getClassify_id(), "分类信息不能为空");
        Validator.notNull(databaseSet.getAgent_id(), "必须关联Agent信息不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", range = "", desc = "")
    @Param(name = "agent_id", range = "", desc = "")
    @Return(desc = "", range = "")
    public Result getInitDatabaseSSCJ(long source_id, long agent_id) {
        return Dbo.queryResult("SELECT t1.*, t2.classify_id, t2.classify_num, " + " t2.classify_name, t2.remark " + " FROM " + DatabaseSet.TableName + " t1 " + " JOIN " + CollectJobClassify.TableName + " t2 ON " + " t1.classify_id = t2.classify_id  JOIN " + AgentInfo.TableName + " ai ON t1.agent_id = ai.agent_id " + "WHERE  t1.is_sendok = ? AND ai.agent_type = ? AND ai.user_id = ? " + "AND ai.source_id = ? AND ai.agent_id = ? AND t1.collect_type = ?", IsFlag.Fou.getCode(), AgentType.ShuJuKu.getCode(), getUserId(), source_id, agent_id, CollectType.ShiShiCaiJi.getCode());
    }
}
