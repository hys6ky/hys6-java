package hyren.serv6.t.bizReq;

import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import hyren.serv6.t.contants.TaskStatusEnum;
import hyren.serv6.t.contants.ReqCategoryEnum;
import hyren.serv6.t.contants.ReqStatusEnum;
import hyren.serv6.t.entity.TskBizReq;
import hyren.serv6.t.entity.TskDataReq;
import hyren.serv6.t.entity.TskTblAssign;
import hyren.serv6.t.tableAssign.TskTableAssignServe;
import hyren.serv6.t.util.IdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Slf4j
@Service
public class BizReqServeImpl {

    @Value("${fileupload.Repository}")
    public String path;

    private TskTableAssignServe tskTableAssignServe;

    public BizReqServeImpl(TskTableAssignServe tskTableAssignServe) {
        this.tskTableAssignServe = tskTableAssignServe;
    }

    public List<Map<String, Object>> queryBizList(TskBizReq tskBizReq, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("SELECT biz.*,COALESCE(dat.data_num,0) AS data_num  FROM " + TskBizReq.TableName + " biz  " + " LEFT JOIN ( SELECT biz_id,COUNT(biz_id) AS data_num  from " + TskDataReq.TableName + " GROUP BY biz_id  ) dat  ON biz.biz_id = dat.biz_id where 1=1  ");
        if (tskBizReq != null) {
            if (tskBizReq.getBiz_name() != null && !"".equals(tskBizReq.getBiz_name())) {
                assembler.addLikeParam(" BIZ_NAME ", "%" + tskBizReq.getBiz_name() + "%");
            }
            if (tskBizReq.getCreated_time() != null && !"".equals(tskBizReq.getCreated_time())) {
                assembler.addLikeParam(" CREATED_TIME ", tskBizReq.getCreated_time() + "%");
            }
            if (tskBizReq.getOwner_name() != null && !"".equals(tskBizReq.getOwner_name())) {
                assembler.addLikeParam(" OWNER_NAME ", "%" + tskBizReq.getOwner_name() + "%");
            }
            if (tskBizReq.getDept() != null && !"".equals(tskBizReq.getDept())) {
                assembler.addLikeParam(" dept  ", "%" + tskBizReq.getDept() + "%");
            }
            if (tskBizReq.getBiz_status() != null && !"".equals(tskBizReq.getBiz_status())) {
                String[] status = tskBizReq.getBiz_status().split(",");
                StringBuilder sb = new StringBuilder();
                assembler.addSql(" AND BIZ_STATUS in ( ");
                for (String sta : status) {
                    sb.append("?,");
                    assembler.addParam(sta);
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                assembler.addSql(sb.toString());
            }
        }
        assembler.addSql(" order by biz.created_time desc");
        return Dbo.queryPagedList(page, assembler);
    }

    public void saveBizReq(TskBizReq tskBizReq) {
        tskBizReq.add(Dbo.db());
    }

    public void updateBizReq(TskBizReq tskBizReq) {
        try {
            Dbo.beginTransaction();
            tskBizReq.update(Dbo.db());
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            log.error("错误信息", e);
            throw new SystemBusinessException("修改信息报错");
        }
    }

    public String uploadFile(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new SystemBusinessException("上传文件不能为空");
        }
        String filename = file.getOriginalFilename();
        String substring = filename.substring(filename.lastIndexOf(".") + 1);
        String uuidFilename = UUID.randomUUID().toString() + "." + substring;
        File newFile = new File(path, uuidFilename);
        try {
            file.transferTo(newFile);
        } catch (IOException e) {
            log.error("错误信息", e);
            e.printStackTrace();
        }
        return newFile.getAbsolutePath();
    }

    public void deleteBizReq(Long biz_id) {
        checkDataReq(biz_id);
        if (!queryOneBizList(biz_id).getBiz_status().trim().equals(TaskStatusEnum.TO_BE_DEV.getCode().trim())) {
            throw new SystemBusinessException("非待开发的任务，无法删除！");
        }
        try {
            Dbo.beginTransaction();
            deleteTblAssign(biz_id);
            Dbo.execute("DELETE FROM   " + TskBizReq.TableName + "  WHERE biz_id = ?", biz_id);
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            log.error("错误信息", e);
            throw new SystemBusinessException("删除业务需求信息失败");
        }
    }

    public void deleteTblAssign(Long biz_id) {
        try {
            Dbo.beginTransaction();
            Dbo.execute(" DELETE FROM  " + TskTblAssign.TableName + " where CATEGORY_ID = ? AND CATEGORY = ?", biz_id, ReqCategoryEnum.BIZ.getCode());
            Dbo.commitTransaction();
        } catch (Exception e) {
            Dbo.rollbackTransaction();
            log.error("错误信息", e);
            throw new SystemBusinessException("删除业务需求中表关联信息失败");
        }
    }

    public TskBizReq queryOneBizList(Long biz_id) {
        return Dbo.queryOneObject(TskBizReq.class, "Select * from " + TskBizReq.TableName + "  WHERE biz_id = ?", biz_id).orElseThrow(() -> (new SystemBusinessException("查询不到业务表信息")));
    }

    public void bizNameCheck(String biz_name) {
        if (Dbo.queryNumber("SELECT COUNT(1) FROM " + TskBizReq.TableName + " WHERE biz_name = ?", biz_name).getAsLong() > 0) {
            throw new SystemBusinessException("需求名称已存在,请修改需求名称");
        }
    }

    public void bizNameCheck(String biz_name, Long biz_id) {
        if (Dbo.queryNumber("SELECT COUNT(1) FROM " + TskBizReq.TableName + " WHERE biz_name = ? AND biz_id != ?", biz_name, biz_id).getAsLong() > 0) {
            throw new SystemBusinessException("需求名称已存在,请修改需求名称");
        }
    }

    public void relationTable(List<TskTblAssign> assignList, String data_type) {
        Dbo.execute("UPDATE " + TskBizReq.TableName + " SET data_type = ? where  biz_id = ? ", data_type, assignList.get(0).getCategory_id());
        if (assignList.size() == 0) {
            throw new BusinessException("请勾选表信息");
        }
        tskTableAssignServe.deleteByCategoryId(assignList.get(0).getCategory_id(), ReqCategoryEnum.BIZ);
        for (TskTblAssign tskTblAssign : assignList) {
            tskTblAssign.setId(IdGenerator.nextId());
            tskTblAssign.setCategory(ReqCategoryEnum.BIZ.getCode());
        }
        tskTableAssignServe.batchSave(assignList);
        Dbo.commitTransaction();
    }

    public void checkDataReq(Long biz_id) {
        if (Dbo.queryNumber("SELECT * FROM " + TskDataReq.TableName + " WHERE biz_id = ?", biz_id).orElse(0) > 0) {
            throw new SystemBusinessException("此业务需求下含有数据需求,请删除数据需求后再进行该操作");
        }
    }

    public void tableBizReq(String ids) {
        for (String id : ids.split(",")) {
            deleteBizReq(Long.valueOf(id));
        }
    }

    public void stopState(Long biz_id) {
        checkDataState(biz_id);
        Dbo.execute("UPDATE " + TskBizReq.TableName + " SET biz_status= ? WHERE biz_id = ?", ReqStatusEnum.FINISH.getCode(), biz_id);
        Dbo.commitTransaction();
    }

    public void checkDataState(Long biz_id) {
        long data_over = Dbo.queryNumber("SELECT COUNT(1)  FROM " + TskDataReq.TableName + "  WHERE req_status = ?  AND biz_id = ? ", ReqStatusEnum.FINISH.getCode(), biz_id).orElse(0);
        long data_all = Dbo.queryNumber("SELECT COUNT(1)  FROM tsk_data_req  WHERE  biz_id = ? ", biz_id).orElse(0);
        if (data_all == 0) {
            throw new SystemBusinessException("尚未创建数据需求，无法关闭");
        }
        if (data_all == 0 || data_over == 0 || data_all != data_over) {
            throw new SystemBusinessException("此需求下含有未完成的数据需求,无法关闭");
        }
    }

    public List<Node> getTreeDataInfo() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        User userInfo = ContextDataHolder.getUserInfo(User.class);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(TreePageSource.MARKET, userInfo, treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }

    public List<Node> getMetaTreeDataInfo(String isProc) {
        List<Map<String, Object>> metaDataTree = getMetaDataTree(isProc);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(metaDataTree);
    }

    public List<Map<String, Object>> getMetaDataTree(String isProc) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        List<Map<String, Object>> metaList = Dbo.queryList("SELECT SOURCE_ID AS id , SOURCE_NAME AS  label ,'0' AS parent_id   FROM meta_data_source ");
        metaList.forEach(meta -> {
            List<Map<String, Object>> mapList = new ArrayList<>();
            Map<String, Object> tblMap = new HashMap<>();
            Map<String, Object> viewMap = new HashMap<>();
            Map<String, Object> meterViewMap = new HashMap<>();
            tblMap.put("id", IdGenerator.nextId());
            viewMap.put("id", IdGenerator.nextId());
            meterViewMap.put("id", IdGenerator.nextId());
            tblMap.put("parent_id", meta.get("id"));
            viewMap.put("parent_id", meta.get("id"));
            meterViewMap.put("parent_id", meta.get("id"));
            tblMap.put("type", "0");
            viewMap.put("type", "1");
            meterViewMap.put("type", "3");
            tblMap.put("label", "表信息");
            viewMap.put("label", "视图");
            meterViewMap.put("label", "物化视图");
            for (Map<String, Object> map : getMetaObjInfo((Long) tblMap.get("id"), (Long) meta.get("id"), (String) tblMap.get("type"))) {
                dataList.add(map);
            }
            for (Map<String, Object> map : getMetaObjInfo((Long) viewMap.get("id"), (Long) meta.get("id"), (String) viewMap.get("type"))) {
                dataList.add(map);
            }
            for (Map<String, Object> map : getMetaObjInfo((Long) meterViewMap.get("id"), (Long) meta.get("id"), (String) meterViewMap.get("type"))) {
                dataList.add(map);
            }
            dataList.add(meta);
            dataList.add(tblMap);
            dataList.add(viewMap);
            if (isProc.equals("1")) {
                Map<String, Object> procMap = new HashMap<>();
                procMap.put("label", "存储过程");
                procMap.put("type", "2");
                procMap.put("parent_id", meta.get("id"));
                procMap.put("id", IdGenerator.nextId());
                for (Map<String, Object> map : getMetaObjInfo((Long) procMap.get("id"), (Long) meta.get("id"), (String) procMap.get("type"))) {
                    dataList.add(map);
                }
                dataList.add(procMap);
            }
            dataList.add(meterViewMap);
        });
        return dataList;
    }

    public List<Map<String, Object>> getMetaObjInfo(Long parent_id, Long source_id, String type) {
        return Dbo.queryList("SELECT OBJ_ID AS id , EN_NAME AS label, " + parent_id + " AS parent_id ," + "  OBJ_ID AS data_source_id, 'META' as data_layer ,EN_NAME AS table_name ,CH_NAME  as original_name,EN_NAME AS hyren_name, OBJ_ID AS  file_id " + " ,* FROM META_OBJ_INFO  where source_id = ? AND type = ? ", source_id, type);
    }
}
