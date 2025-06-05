package hyren.serv6.m.task;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.entity.*;
import hyren.serv6.m.util.IdGenerator;
import hyren.serv6.m.vo.etl.MetaEtlBean;
import hyren.serv6.m.vo.query.MetaTaskQueryVo;
import hyren.serv6.m.vo.save.MetaTaskSaveVo;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.beans.BeanUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

@Component
@Service("metaTaskService")
public class MetaTaskService {

    public MetaTaskQueryVo queryById(Long taskId) {
        return queryById(taskId, Dbo.db());
    }

    public MetaTaskQueryVo queryById(Long taskId, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, MetaTaskQueryVo.class, "select * from " + MetaTask.TableName + " where task_id=?", taskId).orElseThrow(() -> new SystemBusinessException("数据不存在"));
    }

    public List<MetaTaskQueryVo> queryByPage(MetaTaskQueryVo metaTaskQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + MetaTask.TableName + " where 1=1");
        assembler.addLikeParam("task_name ", StringUtil.isBlank(metaTaskQueryVo.getTask_name()) ? null : "%" + metaTaskQueryVo.getTask_name() + "%");
        assembler.addSqlAndParam("task_type", StringUtil.isBlank(metaTaskQueryVo.getTask_type()) ? null : metaTaskQueryVo.getTask_type());
        assembler.addSqlAndParam("source_id", metaTaskQueryVo.getSource_id());
        return Dbo.queryPagedList(MetaTaskQueryVo.class, page, assembler);
    }

    public MetaTask insert(MetaTaskSaveVo metaTaskSaveVo) {
        if (getTaskNameNum(metaTaskSaveVo.getTask_name(), null)) {
            MetaTask metaTask = new MetaTask();
            BeanUtils.copyProperties(metaTaskSaveVo, metaTask);
            metaTask.setTask_id(IdGenerator.nextId());
            metaTask.setCreated_id(UserUtil.getUserId());
            metaTask.setCreated_by(UserUtil.getUser().getRoleName());
            metaTask.setCreated_date(DateUtil.getSysDate());
            metaTask.setCreated_time(DateUtil.getSysTime());
            metaTask.add(Dbo.db());
            return metaTask;
        } else {
            throw new BusinessException("任务名称不能重复!");
        }
    }

    public MetaTask update(MetaTaskSaveVo metaTaskSaveVo) {
        MetaTaskQueryVo queryVo = queryById(metaTaskSaveVo.getTask_id());
        MetaTask metaTask = new MetaTask();
        BeanUtils.copyProperties(queryVo, metaTask);
        metaTask.setTask_name(metaTaskSaveVo.getTask_name());
        metaTask.setUpdated_id(UserUtil.getUserId());
        metaTask.setUpdated_by(UserUtil.getUser().getRoleName());
        metaTask.setUpdated_date(DateUtil.getSysDate());
        metaTask.setUpdated_time(DateUtil.getSysTime());
        metaTask.update(Dbo.db());
        Dbo.commitTransaction();
        return metaTask;
    }

    public void updateTask(MetaTask metaTask) {
        if (getTaskNameNum(metaTask.getTask_name(), metaTask.getTask_id())) {
            Dbo.execute(" update " + MetaTask.TableName + " set task_name = ?" + " where task_id = ?", metaTask.getTask_name(), metaTask.getTask_id());
            Dbo.commitTransaction();
        } else {
            throw new BusinessException("任务名称不能重复");
        }
    }

    public Boolean getTaskNameNum(String task_name, Long task_id) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("select count(1) from " + MetaTask.TableName + " where task_name = ? ").addParam(task_name);
        if (task_id != null && task_id != 0) {
            sql.addSql("And task_id != ?").addParam(task_id);
        }
        long num = Dbo.queryNumber(sql.sql(), sql.params()).orElse(0);
        if (num != 0) {
            return false;
        } else {
            return true;
        }
    }

    public boolean deleteById(Long taskId) {
        OptionalLong metaTaskObjNum = Dbo.queryNumber("select COUNT(1) from " + MetaTaskObj.TableName + " where task_id = ?", taskId);
        if (metaTaskObjNum.isPresent() && metaTaskObjNum.getAsLong() == 0L) {
            MetaTask metaTask = new MetaTask();
            metaTask.setTask_id(taskId);
            metaTask.delete(Dbo.db());
            Dbo.commitTransaction();
            return true;
        } else {
            throw new BusinessException("任务中存在表信息，请先删除任务中的表信息");
        }
    }

    public List<MetaTaskQueryVo> queryBySourceIdAndTaskType(Long sourceId, MetaObjTypeEnum objTypeEnum) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + MetaTask.TableName + " where 1=1");
        assembler.addSqlAndParam("source_id", sourceId);
        assembler.addSqlAndParam("task_type", objTypeEnum.getCode());
        return Dbo.queryList(MetaTaskQueryVo.class, assembler);
    }

    public void genEtl(Long taskId) {
        MetaTaskQueryVo taskQueryVo = queryById(taskId);
        MetaEtlBean etlBean = new MetaEtlBean();
    }

    public Map<String, List<String>> getObjTypeMap(MetaTaskQueryVo taskQueryVo) {
        return getObjTypeMap(taskQueryVo, Dbo.db());
    }

    public Map<String, List<String>> getObjTypeMap(MetaTaskQueryVo taskQueryVo, DatabaseWrapper db) {
        List<String> tblNameList = SqlOperator.queryOneColumnList(db, "select en_name from " + MetaSourceObjCache.TableName + " moi " + " join " + MetaTaskObj.TableName + "  mto on mto.obj_id=moi.obj_id " + " where task_id=? ", taskQueryVo.getTask_id());
        Map<String, List<String>> map = new HashMap<>();
        map.put(taskQueryVo.getTask_type(), tblNameList);
        return map;
    }
}
