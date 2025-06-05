package hyren.serv6.f.dataRegister.source.dbconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.CollectJobClassify;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.entity.DatabaseInfo;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.database.DatabaseConnUtil;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import io.swagger.annotations.Api;
import org.springframework.stereotype.Service;
import java.util.List;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Api("配置源DB属性")
@Service
@DocClass(desc = "", author = "WangZhengcheng")
public class DBConfStepService {

    @Method(desc = "", logicStep = "")
    public List<Object> getDatabaseInfo() {
        return Dbo.queryOneColumnList("select database_name from " + DatabaseInfo.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<CollectJobClassify> getClassifyInfo(long sourceId) {
        return Dbo.queryList(CollectJobClassify.class, "SELECT cjc.* FROM " + DataSource.TableName + " ds " + " JOIN " + CollectJobClassify.TableName + " cjc ON ds.source_id = cjc.source_id" + " WHERE ds.source_id = ? AND cjc.user_id = ? order by cjc.classify_num ", sourceId, getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify", desc = "", range = "", isBean = true)
    @Param(name = "sourceId", desc = "", range = "")
    public void saveClassifyInfo(CollectJobClassify classify, long sourceId) {
        verifyClassifyEntity(classify, true);
        long val = Dbo.queryNumber("SELECT count(1) FROM " + CollectJobClassify.TableName + " cjc " + " LEFT JOIN " + DataSource.TableName + " ds ON cjc.source_id=ds.source_id" + " WHERE cjc.classify_num=? AND ds.source_id=?", classify.getClassify_num(), sourceId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 0) {
            throw new BusinessException("分类编号重复，请重新输入");
        }
        classify.setClassify_id(PrimayKeyGener.getNextId());
        classify.setUser_id(getUserId());
        classify.setSource_id(sourceId);
        classify.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    public void testConnection(DatabaseSet databaseSet) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), databaseSet.getDsl_id())) {
            if (!db.isConnected()) {
                throw new BusinessException("测试连接失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db_name", desc = "", range = "")
    @Param(name = "port", desc = "", range = "", nullable = true, valueIfNull = "")
    @Return(desc = "", range = "", isBean = true)
    public DBConnectionProp getDBConnectionProp(String db_name, String port) {
        return DatabaseConnUtil.getConnParamInfo(db_name, port);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "classify", desc = "", range = "", isBean = true)
    @Param(name = "sourceId", desc = "", range = "")
    public void updateClassifyInfo(CollectJobClassify classify, long sourceId) {
        verifyClassifyEntity(classify, false);
        long val = Dbo.queryNumber("SELECT count(1) FROM " + CollectJobClassify.TableName + " cjc " + " LEFT JOIN " + DataSource.TableName + " ds ON ds.source_id=cjc.source_id" + " WHERE cjc.classify_id=? AND ds.source_id=? AND cjc.user_id = ? ", classify.getClassify_id(), sourceId, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("待更新的数据不存在");
        }
        long count = Dbo.queryNumber("SELECT count(1) FROM " + CollectJobClassify.TableName + " cjc" + " LEFT JOIN " + DataSource.TableName + " ds ON ds.source_id=cjc.source_id" + " WHERE cjc.classify_num = ? AND ds.source_id = ? AND ds.create_user_id = ? and cjc.classify_id != ?", classify.getClassify_num(), sourceId, getUserId(), classify.getClassify_id()).orElseThrow(() -> new BusinessException("SQL查询错误"));
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
    @Param(name = "classifyId", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean checkClassifyId(long classifyId) {
        long count = Dbo.queryNumber(" SELECT count(1) FROM " + CollectJobClassify.TableName + " WHERE classify_id = ? AND user_id = ? ", classifyId, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("采集作业分类信息不存在");
        }
        long val = Dbo.queryNumber("SELECT count(1) FROM " + DataSource.TableName + " ds" + " JOIN " + DatabaseSet.TableName + " das ON ds.source_id = das.source_id " + " WHERE das.classify_id = ?", classifyId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        return val == 0;
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
    }
}
