package hyren.serv6.t.taskDev;

import fd.ng.core.utils.BeanUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.user.User;
import hyren.serv6.t.contants.DateConstants;
import hyren.serv6.t.contants.TaskStatusEnum;
import hyren.serv6.t.contants.ReqStatusEnum;
import hyren.serv6.t.entity.TskBizReq;
import hyren.serv6.t.entity.TskDataReq;
import hyren.serv6.t.entity.TskTaskDev;
import hyren.serv6.t.util.IdGenerator;
import hyren.serv6.t.vo.query.TskTaskDevQueryVo;
import hyren.serv6.t.vo.save.TskTaskDevUpdateVo;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import java.util.*;

@Service("tskTaskDevService")
public class TskTaskDevService {

    public TskTaskDevQueryVo queryById(Long taskId) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select ttd.*,tdr.data_req_name,tbr.biz_name " + " from " + TskTaskDev.TableName + " ttd " + " join " + TskDataReq.TableName + " tdr on ttd.data_req_id=tdr.data_req_id" + " join " + TskBizReq.TableName + " tbr on ttd.biz_id=tbr.biz_id where task_id=?");
        assembler.addParam(taskId);
        return Dbo.queryOneObject(TskTaskDevQueryVo.class, assembler).orElseThrow(() -> new SystemBusinessException("开发任务不存在"));
    }

    public List<TskTaskDevQueryVo> queryByPage(TskTaskDevQueryVo taskDevQueryVo, Page page) {
        User user = ContextDataHolder.getUserInfo(User.class);
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select ttd.*,tdr.data_req_name,tbr.biz_name " + " from " + TskTaskDev.TableName + " ttd " + " join " + TskDataReq.TableName + " tdr on ttd.data_req_id=tdr.data_req_id" + " join " + TskBizReq.TableName + " tbr on ttd.biz_id=tbr.biz_id" + " where 1=1 ");
        if (checkOnlyUserDataPerm(user)) {
            assembler.addLikeParam("ttd.owner_id", "%" + user.getUserId() + "%");
        }
        if (!StringUtils.isEmpty(taskDevQueryVo.getOwner_id())) {
            String[] ownerIdArr = taskDevQueryVo.getOwner_id().split(",");
            assembler.addSql(" and ( ");
            for (int i = 0; i < ownerIdArr.length; i++) {
                String ownerId = ownerIdArr[i];
                if (i != ownerIdArr.length - 1) {
                    assembler.addSql(" ttd.owner_id like ? or ").addParam("%" + ownerId + "%");
                } else {
                    assembler.addSql(" ttd.owner_id like ?  ").addParam("%" + ownerId + "%");
                }
            }
            assembler.addSql(" ) ");
        }
        if (!StringUtils.isEmpty(taskDevQueryVo.getTask_name())) {
            assembler.addLikeParam("ttd.task_name", "%" + taskDevQueryVo.getTask_name() + "%");
        }
        if (Objects.nonNull(taskDevQueryVo.getBiz_id())) {
            assembler.addSql(" and ttd.biz_id=? ").addParam(taskDevQueryVo.getBiz_id());
        }
        if (Objects.nonNull(taskDevQueryVo.getData_req_id())) {
            assembler.addSql(" and ttd.data_req_id=? ").addParam(taskDevQueryVo.getData_req_id());
        }
        if (!StringUtils.isEmpty(taskDevQueryVo.getStart_date())) {
            assembler.addSql(" and ttd.start_date >=? ").addParam(taskDevQueryVo.getStart_date());
        }
        if (!StringUtils.isEmpty(taskDevQueryVo.getEnd_date())) {
            assembler.addSql(" and ttd.end_date<=? ").addParam(taskDevQueryVo.getEnd_date());
        }
        if (!StringUtils.isEmpty(taskDevQueryVo.getTask_status())) {
            String[] taskStatusArr = taskDevQueryVo.getTask_status().split(",");
            assembler.addSql(" and ttd.task_status in ( ");
            for (int i = 0; i < taskStatusArr.length; i++) {
                if (i != taskStatusArr.length - 1) {
                    assembler.addSql(" ?, ");
                } else {
                    assembler.addSql(" ?  ");
                }
            }
            assembler.addSql(" ) ");
            for (String taskStatus : taskStatusArr) {
                assembler.addParam(taskStatus);
            }
        }
        if (!StringUtils.isEmpty(taskDevQueryVo.getTask_category())) {
            String[] taskCategoryArr = taskDevQueryVo.getTask_category().split(",");
            assembler.addSql(" and ttd.task_category in ( ");
            for (int i = 0; i < taskCategoryArr.length; i++) {
                if (i != taskCategoryArr.length - 1) {
                    assembler.addSql(" ?, ");
                } else {
                    assembler.addSql(" ?  ");
                }
            }
            assembler.addSql(" ) ");
            for (String taskCategory : taskCategoryArr) {
                assembler.addParam(taskCategory);
            }
        }
        assembler.addSql(" order by ttd.created_time desc ");
        return Dbo.queryPagedList(TskTaskDevQueryVo.class, page, assembler);
    }

    private boolean checkOnlyUserDataPerm(User user) {
        List<String> isAdminList = Dbo.queryOneColumnList("select sr.is_admin " + " from sys_user su " + " join sys_role sr on su.role_id=sr.role_id " + " where user_id=?", user.getUserId());
        if (isAdminList.contains("02")) {
            return true;
        }
        return false;
    }

    public TskTaskDev insert(TskTaskDev tskTaskDev) {
        tskTaskDev.setTask_status(TaskStatusEnum.TO_BE_DEV.getCode());
        if (checkDateReqRepeat(tskTaskDev.getTask_name())) {
            throw new SystemBusinessException("任务名称重复");
        }
        tskTaskDev.setTask_id(IdGenerator.nextId());
        User user = ContextDataHolder.getUserInfo(User.class);
        tskTaskDev.setCreated_id(user.getUserId());
        tskTaskDev.setCreated_by(user.getUsername());
        tskTaskDev.setCreated_time(DateFormatUtils.format(new Date(), DateConstants.FORMAT_DATE_TIME));
        tskTaskDev.add(Dbo.db());
        return tskTaskDev;
    }

    private boolean checkDateReqRepeat(String task_name) {
        List<String> nameList = Dbo.queryOneColumnList("select task_name from " + TskTaskDev.TableName + " where task_name=?", task_name);
        if (nameList.size() > 0) {
            return true;
        }
        return false;
    }

    public TskTaskDev update(TskTaskDevUpdateVo taskDevUpdateVo) {
        TskTaskDevQueryVo taskDevQueryVo = queryById(taskDevUpdateVo.getTask_id());
        if (!taskDevUpdateVo.getTask_name().equals(taskDevQueryVo.getTask_name()) && checkDateReqRepeat(taskDevUpdateVo.getTask_name())) {
            throw new SystemBusinessException("任务名称重复");
        }
        User user = ContextDataHolder.getUserInfo(User.class);
        TskTaskDev dbEntity = new TskTaskDev();
        BeanUtil.copyProperties(taskDevQueryVo, dbEntity);
        BeanUtil.copyProperties(taskDevUpdateVo, dbEntity);
        dbEntity.setUpdated_id(user.getUserId());
        dbEntity.setUpdated_by(user.getUsername());
        dbEntity.setUpdated_time(DateFormatUtils.format(new Date(), DateConstants.FORMAT_DATE_TIME));
        dbEntity.update(Dbo.db());
        Dbo.commitTransaction();
        return dbEntity;
    }

    public boolean deleteById(Long taskId) {
        TskTaskDevQueryVo taskDevQueryVo = queryById(taskId);
        if (TaskStatusEnum.TO_BE_DEV != TaskStatusEnum.ofEnumByCode(taskDevQueryVo.getTask_status())) {
            throw new SystemBusinessException("非待开发的任务，无法删除！");
        }
        TskTaskDev tskTaskDev = new TskTaskDev();
        BeanUtil.copyProperties(taskDevQueryVo, tskTaskDev);
        tskTaskDev.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public void updateTaskStatus(Long taskId, String taskStatus) {
        TaskStatusEnum taskStatusEnum = TaskStatusEnum.ofEnumByCode(taskStatus);
        TskTaskDevQueryVo tskTaskDevQueryVo = queryById(taskId);
        Dbo.execute("update " + TskTaskDev.TableName + " set task_status=? where task_id=?", taskStatus, taskId);
        if (taskStatusEnum == TaskStatusEnum.DEV_CPLT) {
            changeDataReqStatusByTask(tskTaskDevQueryVo);
            changeBizReqStatusByTask(tskTaskDevQueryVo);
        }
        Dbo.commitTransaction();
    }

    private void changeDataReqStatusByTask(TskTaskDevQueryVo taskDevQueryVo) {
        Dbo.execute("update " + TskDataReq.TableName + " set req_status=? where data_req_id=?", ReqStatusEnum.DEV_ING.getCode(), taskDevQueryVo.getData_req_id());
    }

    private void changeBizReqStatusByTask(TskTaskDevQueryVo taskDevQueryVo) {
        Dbo.execute("update " + TskBizReq.TableName + " set biz_status=? where biz_id=?", ReqStatusEnum.DEV_ING.getCode(), taskDevQueryVo.getBiz_id());
    }

    public void batchDeleteByIds(String ids) {
        String[] idsStrArr = ids.split(",");
        Long[] idsLongArr = new Long[idsStrArr.length];
        for (String reqId : idsStrArr) {
            TskTaskDevQueryVo taskQueryVo = queryById(Long.valueOf(reqId));
            if (!TaskStatusEnum.TO_BE_DEV.getCode().equals(taskQueryVo.getTask_status())) {
                throw new SystemBusinessException("非待开发的任务，无法删除！");
            }
        }
        StringBuilder idParaNum = new StringBuilder();
        for (int i = 0; i < idsStrArr.length; i++) {
            idsLongArr[i] = Long.valueOf(idsStrArr[i]);
            if (i == idsStrArr.length - 1) {
                idParaNum.append("?");
            } else {
                idParaNum.append("?,");
            }
        }
        Dbo.execute("delete from " + TskTaskDev.TableName + " where task_id in ( " + idParaNum + " )", idsLongArr);
        Dbo.commitTransaction();
    }
}
