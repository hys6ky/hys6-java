package hyren.serv6.f.dataRegister.source.register;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.CollectJobClassify;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-07-06 10:02")
public class ResourceRecodingService {

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", range = "", desc = "")
    @Return(desc = "", range = "")
    public Result getInitStorageData(long source_id) {
        return Dbo.queryResult("SELECT t1.*, t2.classify_id, t2.classify_num,t2.classify_name, t2.remark from " + DatabaseSet.TableName + " t1 join " + CollectJobClassify.TableName + " t2 on t1.source_id = t2.source_id join " + DataSource.TableName + " t3 on t1.source_id = t3.source_id where t1.is_sendok = ? and t1.collect_type = ? and t3.source_id = ? ", IsFlag.Fou.getCode(), CollectType.TieYuanDengJi.getCode(), source_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result editStorageData(long databaseId) {
        long countNum = Dbo.queryNumber("SELECT count(1) " + " FROM " + DatabaseSet.TableName + " das " + " WHERE das.database_id = ? AND das.is_sendok = ?", databaseId, IsFlag.Shi.getCode()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException(String.format("根据用户ID(%s),未找到任务ID为(%s)的数据信息", UserUtil.getUserId(), databaseId));
        }
        return Dbo.queryResult("SELECT t1.*, t2.classify_id, t2.classify_num,t2.classify_name, t2.remark " + " FROM " + DatabaseSet.TableName + " t1 " + " JOIN " + CollectJobClassify.TableName + " t2 ON " + " t1.classify_id = t2.classify_id  WHERE database_id = ? AND t1.is_sendok = ? AND t1.collect_type = ?", databaseId, IsFlag.Shi.getCode(), CollectType.TieYuanDengJi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Long saveRegisterData(DatabaseSet databaseSet) {
        verifyDatabaseSetEntity(databaseSet);
        long val = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE task_name = ?", databaseSet.getTask_name()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 0) {
            throw new BusinessException(String.format("任务名称(%s)重复，请重新定义任务名称", databaseSet.getTask_name()));
        }
        if (StringUtil.isNotBlank(databaseSet.getDatabase_number())) {
            val = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_number = ?", databaseSet.getDatabase_number()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (val != 0) {
                throw new BusinessException(String.format("作业编号(%s)重复，请重新定义作业编号", databaseSet.getDatabase_number()));
            }
        }
        databaseSet.setDatabase_id(PrimayKeyGener.getNextId());
        databaseSet.setCollect_type(CollectType.TieYuanDengJi.getCode());
        databaseSet.setDb_agent(IsFlag.Fou.getCode());
        databaseSet.setIs_sendok(IsFlag.Fou.getCode());
        databaseSet.setCp_or(Constant.DEFAULT_TABLE_CLEAN_ORDER.toString());
        databaseSet.add(Dbo.db());
        return databaseSet.getDatabase_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "databaseSet", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Long updateRegisterData(DatabaseSet databaseSet) {
        Validator.notNull(databaseSet.getDatabase_id(), "更新时未获取到主键ID信息");
        verifyDatabaseSetEntity(databaseSet);
        long val = Dbo.queryNumber("SELECT COUNT(1) from " + DatabaseSet.TableName + " WHERE task_name = ? AND database_id != ?", databaseSet.getTask_name(), databaseSet.getDatabase_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 0) {
            throw new BusinessException(String.format("任务名称(%s)重复，请重新定义任务名称", databaseSet.getTask_name()));
        }
        if (StringUtil.isNotBlank(databaseSet.getDatabase_number())) {
            val = Dbo.queryNumber("SELECT COUNT(1) from " + DatabaseSet.TableName + " WHERE database_number = ? AND database_id != ?", databaseSet.getDatabase_number(), databaseSet.getDatabase_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
            if (val != 0) {
                throw new BusinessException(String.format("作业编号(%s)重复，请重新定义作业编号", databaseSet.getDatabase_number()));
            }
        }
        try {
            databaseSet.update(Dbo.db());
        } catch (Exception e) {
            if (!(e instanceof ProEntity.EntityDealZeroException)) {
                throw new BusinessException(e.getMessage());
            }
        }
        return databaseSet.getDatabase_id();
    }

    private void verifyDatabaseSetEntity(DatabaseSet databaseSet) {
        Validator.notNull(databaseSet.getClassify_id(), "保存贴源登记信息时分类信息不能为空");
        if (databaseSet.getDsl_id() == 0) {
            throw new BusinessException("保存贴源登记信息时存储层信息不能为空");
        }
        Validator.notNull(databaseSet.getFetch_size(), "保存贴源登记信息时fetch_size不能为空");
    }
}
