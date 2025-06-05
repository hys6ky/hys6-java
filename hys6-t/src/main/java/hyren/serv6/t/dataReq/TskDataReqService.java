package hyren.serv6.t.dataReq;

import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import hyren.serv6.t.bizReq.BizReqServeImpl;
import hyren.serv6.t.contants.DateConstants;
import hyren.serv6.t.contants.TaskStatusEnum;
import hyren.serv6.t.contants.ReqCategoryEnum;
import hyren.serv6.t.contants.ReqStatusEnum;
import hyren.serv6.t.entity.TskBizReq;
import hyren.serv6.t.entity.TskDataReq;
import hyren.serv6.t.entity.TskTaskDev;
import hyren.serv6.t.entity.TskTblAssign;
import hyren.serv6.t.tableAssign.TskTableAssignServe;
import hyren.serv6.t.taskDev.TskTaskDevService;
import hyren.serv6.t.util.IdGenerator;
import hyren.serv6.t.vo.query.TskDataReqQueryVo;
import hyren.serv6.t.vo.save.TskDataReqUpdateVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.util.StringUtils;
import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service("tskDataReqService")
public class TskDataReqService {

    @Resource
    private TskTableAssignServe tskTableAssignServe;

    @Resource
    private BizReqServeImpl bizReqServe;

    @Resource
    private TskTaskDevService taskDevService;

    public TskDataReqQueryVo queryById(Long dataReqId) {
        TskDataReqQueryVo tskDataReqQueryVo = Dbo.queryOneObject(TskDataReqQueryVo.class, "select tdr.*,tbr.biz_name " + " from " + TskDataReq.TableName + " tdr " + " join " + TskBizReq.TableName + " tbr on tdr.biz_id=tbr.biz_id " + " where data_req_id=?", dataReqId).orElseThrow(() -> new SystemBusinessException("数据不存在"));
        getTaskNum(tskDataReqQueryVo);
        return tskDataReqQueryVo;
    }

    public List<TskDataReqQueryVo> queryByPage(TskDataReqQueryVo tskDataReqQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select tdr.*,tbr.biz_name,tbr.data_type " + " from " + TskDataReq.TableName + " tdr " + " join " + TskBizReq.TableName + " tbr on tdr.biz_id=tbr.biz_id " + " where 1=1 ");
        if (Objects.nonNull(tskDataReqQueryVo.getData_req_id())) {
            assembler.addLikeParam("tdr.data_req_id", tskDataReqQueryVo.getData_req_id().toString());
        }
        if (!StringUtils.isEmpty(tskDataReqQueryVo.getData_req_name())) {
            assembler.addLikeParam("tdr.data_req_name", "%" + tskDataReqQueryVo.getData_req_name() + "%");
        }
        if (!StringUtils.isEmpty(tskDataReqQueryVo.getReq_status())) {
            String[] reqStatusArr = tskDataReqQueryVo.getReq_status().split(",");
            assembler.addSql(" and tdr.req_status in ( ");
            for (int i = 0; i < reqStatusArr.length; i++) {
                if (i != reqStatusArr.length - 1) {
                    assembler.addSql(" ?, ");
                } else {
                    assembler.addSql(" ?  ");
                }
            }
            assembler.addSql(" ) ");
            for (String reqStatus : reqStatusArr) {
                assembler.addParam(reqStatus);
            }
        }
        if (!StringUtils.isEmpty(tskDataReqQueryVo.getCreated_time())) {
            assembler.addLikeParam("tdr.created_time", tskDataReqQueryVo.getCreated_time() + "%");
        }
        if (!StringUtils.isEmpty(tskDataReqQueryVo.getBiz_id())) {
            assembler.addSql(" and tdr.biz_id=? ").addParam(tskDataReqQueryVo.getBiz_id());
        }
        if (!StringUtils.isEmpty(tskDataReqQueryVo.getOwner_id())) {
            String[] ownerIdArr = tskDataReqQueryVo.getOwner_id().split(",");
            assembler.addSql(" and ( ");
            for (int i = 0; i < ownerIdArr.length; i++) {
                String ownerId = ownerIdArr[i];
                if (i != ownerIdArr.length - 1) {
                    assembler.addSql(" tdr.owner_id like ? or ").addParam("%" + ownerId + "%");
                } else {
                    assembler.addSql(" tdr.owner_id like ?  ").addParam("%" + ownerId + "%");
                }
            }
            assembler.addSql(" ) ");
        }
        assembler.addSql(" order by tdr.created_time desc ");
        List<TskDataReqQueryVo> tskDataReqQueryVos = Dbo.queryPagedList(TskDataReqQueryVo.class, page, assembler);
        tskDataReqQueryVos.stream().forEach(dataReqQueryVo -> {
            getTaskNum(dataReqQueryVo);
        });
        return tskDataReqQueryVos;
    }

    private void getTaskNum(TskDataReqQueryVo dataReqQueryVo) {
        dataReqQueryVo.setTask_num(Dbo.queryNumber("select count(*) from " + TskTaskDev.TableName + " where data_req_id=? ", dataReqQueryVo.getData_req_id()).orElse(0));
    }

    public TskDataReq insert(TskDataReq tskDataReq) {
        if (checkDateReqRepeat(tskDataReq.getData_req_name(), tskDataReq.getBiz_id())) {
            throw new SystemBusinessException("需求名称重复");
        }
        tskDataReq.setReq_status(TaskStatusEnum.TO_BE_DEV.getCode());
        tskDataReq.setData_req_id(IdGenerator.nextId());
        User user = ContextDataHolder.getUserInfo(User.class);
        tskDataReq.setCreated_id(user.getUserId());
        tskDataReq.setCreated_by(user.getUsername());
        tskDataReq.setCreated_time(DateFormatUtils.format(new Date(), DateConstants.FORMAT_DATE_TIME));
        tskDataReq.add(Dbo.db());
        List<TskTblAssign> tskTblAssigns = Dbo.queryList(TskTblAssign.class, "select * from " + TskTblAssign.TableName + " where CATEGORY_ID = ? ", tskDataReq.getBiz_id());
        for (TskTblAssign tskTblAssign : tskTblAssigns) {
            tskTblAssign.setId(IdGenerator.nextId());
            tskTblAssign.setCategory_id(tskDataReq.getData_req_id());
            tskTblAssign.setCategory(ReqCategoryEnum.DATA.getCode());
        }
        tskTableAssignServe.deleteByCategoryId(tskDataReq.getData_req_id(), ReqCategoryEnum.DATA);
        tskTableAssignServe.batchSave(tskTblAssigns);
        return tskDataReq;
    }

    private boolean checkDateReqRepeat(String data_req_name, long biz_id) {
        List<String> reqNameList = Dbo.queryOneColumnList("select data_req_name from " + TskDataReq.TableName + " where data_req_name=? and biz_id=?", data_req_name, biz_id);
        if (reqNameList.size() > 0) {
            return true;
        }
        return false;
    }

    public TskDataReq update(TskDataReqUpdateVo dataReqUpdateVo) {
        TskDataReqQueryVo reqQueryVo = queryById(dataReqUpdateVo.getData_req_id());
        if (!reqQueryVo.getData_req_name().equals(dataReqUpdateVo.getData_req_name()) && checkDateReqRepeat(dataReqUpdateVo.getData_req_name(), reqQueryVo.getBiz_id())) {
            throw new SystemBusinessException("需求名称重复");
        }
        User user = ContextDataHolder.getUserInfo(User.class);
        TskDataReq dbEntity = new TskDataReq();
        BeanUtil.copyProperties(reqQueryVo, dbEntity);
        BeanUtil.copyProperties(dataReqUpdateVo, dbEntity);
        dbEntity.setUpdated_id(user.getUserId());
        dbEntity.setUpdated_by(user.getUsername());
        dbEntity.setUpdated_time(DateFormatUtils.format(new Date(), DateConstants.FORMAT_DATE_TIME));
        dbEntity.update(Dbo.db());
        Dbo.commitTransaction();
        return dbEntity;
    }

    public boolean deleteById(Long dataReqId) {
        TskDataReqQueryVo dataReqQueryVo = queryById(dataReqId);
        if (ReqStatusEnum.TO_BE_DEV != ReqStatusEnum.ofEnumByCode(dataReqQueryVo.getReq_status())) {
            throw new SystemBusinessException("非待开发的数据需求无法删除！");
        }
        if (dataReqQueryVo.getTask_num() == 0) {
            Dbo.execute("delete from " + TskDataReq.TableName + " where data_req_id=?", dataReqId);
            Dbo.commitTransaction();
        } else {
            throw new SystemBusinessException("此需求下含有开发任务,请删除任务后再进行该操作");
        }
        return true;
    }

    public Boolean batchDeleteByIds(String ids) {
        String[] idsStrArr = ids.split(",");
        Long[] idsLongArr = new Long[idsStrArr.length];
        for (String reqId : idsStrArr) {
            TskDataReqQueryVo tskDataReqQueryVo = queryById(Long.valueOf(reqId));
            if (tskDataReqQueryVo.getTask_num() > 0) {
                throw new SystemBusinessException("需求:{}下含有开发任务,请删除任务后再进行该操作", tskDataReqQueryVo.getData_req_name());
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
        Dbo.execute("delete from " + TskDataReq.TableName + " where data_req_id in (" + idParaNum + ")", idsLongArr);
        Dbo.commitTransaction();
        return true;
    }

    public void relationTable(List<TskTblAssign> assignList, String data_type) {
        if (assignList.size() == 0) {
            throw new BusinessException("请勾选表信息！");
        }
        checkTskBiz(assignList.get(0).getCategory_id(), data_type);
        tskTableAssignServe.deleteByCategoryId(assignList.get(0).getCategory_id(), ReqCategoryEnum.DATA);
        for (TskTblAssign tskTblAssign : assignList) {
            tskTblAssign.setId(IdGenerator.nextId());
            tskTblAssign.setCategory(ReqCategoryEnum.DATA.getCode());
        }
        tskTableAssignServe.batchSave(assignList);
    }

    public void checkTskBiz(Long category_id, String data_type) {
        TskBizReq tskBizReq = Dbo.queryOneObject(TskBizReq.class, " SELECT t2.* FROM tsk_data_req t1 JOIN tsk_biz_req t2 ON" + " t1.biz_id = t2.biz_id WHERE t1.DATA_REQ_ID = ? ", category_id).orElseThrow(() -> new BusinessException("获取业务需求信息出现问题！"));
        if ("".equals(tskBizReq.getData_type()) || null == tskBizReq.getData_type()) {
            Dbo.execute("UPDATE " + TskBizReq.TableName + " SET data_type = ? where  biz_id = ? ", data_type, tskBizReq.getBiz_id());
            Dbo.commitTransaction();
        }
    }

    public List<Node> getDateReqTreeTable(Long id, ReqCategoryEnum categoryEnum) {
        TskBizReq tskBizReq = Dbo.queryOneObject(TskBizReq.class, "SELECT t1.* FROM " + TskBizReq.TableName + " t1  LEFT JOIN " + " " + TskDataReq.TableName + "  t2 ON t1.BIZ_ID = t2.biz_id WHERE  t2.DATA_REQ_ID = ? ", id).orElseThrow(() -> new BusinessException("未找到业务资源信息"));
        if (StringUtil.isEmpty(tskBizReq.getData_type())) {
            throw new BusinessException("业务表信息资源来源为空！");
        }
        List<TskTblAssign> tskTblAssigns = tskTableAssignServe.queryList(id, categoryEnum);
        if (tskBizReq.getData_type().equals("0")) {
            List<Node> treeDataInfo = bizReqServe.getTreeDataInfo();
            setTree(treeDataInfo, tskTblAssigns);
            return treeDataInfo;
        } else {
            List<Node> metaTreeDataInfo = bizReqServe.getMetaTreeDataInfo("0");
            setTree(metaTreeDataInfo, tskTblAssigns);
            return metaTreeDataInfo;
        }
    }

    public List<Node> getTreeTable(Long id, ReqCategoryEnum categoryEnum) {
        TskBizReq tskBizReq = Dbo.queryOneObject(TskBizReq.class, "select * from " + TskBizReq.TableName + " where biz_id = ? ", id).orElseThrow(() -> new BusinessException("未找到业务资源信息"));
        if (StringUtil.isEmpty(tskBizReq.getData_type())) {
            throw new BusinessException("业务表信息资源来源为空！");
        }
        List<TskTblAssign> tskTblAssigns = tskTableAssignServe.queryList(id, categoryEnum);
        if (tskBizReq.getData_type().equals("0")) {
            List<Node> treeDataInfo = bizReqServe.getTreeDataInfo();
            setTree(treeDataInfo, tskTblAssigns);
            return treeDataInfo;
        } else {
            List<Node> metaTreeDataInfo = bizReqServe.getMetaTreeDataInfo("1");
            setTree(metaTreeDataInfo, tskTblAssigns);
            return metaTreeDataInfo;
        }
    }

    public void setTree(List<Node> treeDataInfo, List<TskTblAssign> tskTblAssigns) {
        for (int i = treeDataInfo.size() - 1; i >= 0; i--) {
            if (treeDataInfo.get(i).getChildren().size() == 0 && treeDataInfo.get(i).getFile_id() == null || "".equals(treeDataInfo.get(i).getFile_id())) {
                treeDataInfo.remove(i);
            } else {
                if (treeDataInfo.get(i).getChildren().size() != 0) {
                    setTree(treeDataInfo.get(i).getChildren(), tskTblAssigns);
                    if (treeDataInfo.get(i).getChildren().size() == 0 && treeDataInfo.get(i).getFile_id() == null || "".equals(treeDataInfo.get(i).getFile_id())) {
                        treeDataInfo.remove(i);
                    }
                } else {
                    String fileId = treeDataInfo.get(i).getFile_id();
                    if (fileId != null && !"".equals(fileId)) {
                        List<TskTblAssign> collect = tskTblAssigns.stream().filter(tskTblAssign -> tskTblAssign.getTbl_id().equals(fileId)).collect(Collectors.toList());
                        if (collect.size() == 0) {
                            treeDataInfo.remove(i);
                        }
                    }
                }
            }
        }
    }

    public void changeStatus(Long id, ReqStatusEnum status) {
        if (ReqStatusEnum.FINISH == status) {
            long taskNum = Dbo.queryNumber("select count(*) from " + TskTaskDev.TableName + " where data_req_id=? ", id).orElse(0);
            if (taskNum == 0) {
                throw new SystemBusinessException("尚未创建开发任务，无法关闭");
            }
            long uncompletedTaskNum = Dbo.queryNumber("select count(*) from " + TskTaskDev.TableName + " where data_req_id=? and task_status<> ? ", id, TaskStatusEnum.TEST_CPLT.getCode()).orElse(0);
            if (uncompletedTaskNum > 0) {
                throw new SystemBusinessException("此需求下含有未完成的开发任务,无法关闭");
            }
        }
        Dbo.execute("update " + TskDataReq.TableName + " set req_status=? where data_req_id=?", status.getCode(), id);
        Dbo.commitTransaction();
    }
}
