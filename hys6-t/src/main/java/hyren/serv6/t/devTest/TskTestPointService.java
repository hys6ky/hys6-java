package hyren.serv6.t.devTest;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.t.entity.TskTaskDev;
import hyren.serv6.t.entity.TskTaskPointRel;
import hyren.serv6.t.entity.TskTestPoint;
import hyren.serv6.t.util.IdGenerator;
import hyren.serv6.t.vo.query.TskTestPointQueryVo;
import hyren.serv6.t.vo.save.TskTestPointSaveVo;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.beans.BeanUtils;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

@Service("tskTestPointService")
public class TskTestPointService {

    public TskTestPointQueryVo queryById(Long pointId) {
        return Dbo.queryOneObject(TskTestPointQueryVo.class, "select * from " + TskTestPoint.TableName + " where point_id=?", pointId).orElse(null);
    }

    public List<TskTestPointQueryVo> queryByPage(TskTestPointQueryVo tskTestPointQueryVo, Page page) {
        String pointName = tskTestPointQueryVo.getPoint_name();
        String pointType = tskTestPointQueryVo.getPoint_type();
        String taskCategory = tskTestPointQueryVo.getTask_category();
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + TskTestPoint.TableName + " where 1=1");
        if (StringUtil.isNotBlank(pointName)) {
            assembler.addLikeParam(" and point_name ", "%" + pointName + "%");
        }
        if (StringUtil.isNotBlank(pointType)) {
            assembler.addSqlAndParam(" point_type ", pointType);
        }
        if (StringUtil.isNotBlank(taskCategory)) {
            assembler.addSqlAndParam(" task_category ", taskCategory);
        }
        assembler.addSql(" order by created_time desc");
        return Dbo.queryPagedList(TskTestPointQueryVo.class, page, assembler);
    }

    public TskTestPoint insert(TskTestPointSaveVo tskTestPointSaveVo) {
        TskTestPoint tskTestPoint = new TskTestPoint();
        BeanUtils.copyProperties(tskTestPointSaveVo, tskTestPoint);
        long count = Dbo.queryNumber("select count(*) from " + TskTestPoint.TableName + " where point_name = ?", tskTestPoint.getPoint_name()).orElse(0);
        if (count > 0) {
            throw new SystemBusinessException("要点名称重复！");
        }
        tskTestPoint.setPoint_id(IdGenerator.nextId());
        Long userId = UserUtil.getUserId();
        tskTestPoint.setCreated_id(userId);
        tskTestPoint.setUpdated_id(userId);
        String username = UserUtil.getUser().getUsername();
        tskTestPoint.setCreated_by(username);
        tskTestPoint.setUpdated_by(username);
        String now = DateUtil.getTimestamp(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        tskTestPoint.setCreated_time(now);
        tskTestPoint.setUpdated_time(now);
        tskTestPoint.add(Dbo.db());
        return tskTestPoint;
    }

    public TskTestPoint update(TskTestPointSaveVo tskTestPointSaveVo) {
        TskTestPoint tskTestPoint = new TskTestPoint();
        BeanUtils.copyProperties(tskTestPointSaveVo, tskTestPoint);
        List<TskTestPoint> tskTestPoints = Dbo.queryList(TskTestPoint.class, "select * from " + TskTestPoint.TableName + " where point_name = ?", tskTestPoint.getPoint_name());
        if (!tskTestPoints.isEmpty() && tskTestPoints.size() >= 2 || tskTestPoints.size() == 1 && !tskTestPoint.getPoint_id().equals(tskTestPoints.get(0).getPoint_id())) {
            throw new SystemBusinessException("要点名称重复！");
        }
        tskTestPoint.setUpdated_id(UserUtil.getUserId());
        tskTestPoint.setUpdated_by(UserUtil.getUser().getUsername());
        tskTestPoint.setUpdated_time(DateUtil.getTimestamp(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        tskTestPoint.update(Dbo.db());
        Dbo.commitTransaction();
        return tskTestPoint;
    }

    public boolean deleteById(Long pointId) {
        TskTestPoint tskTestPoint = new TskTestPoint();
        tskTestPoint.setPoint_id(pointId);
        List<TskTaskDev> tskTaskDevs = Dbo.queryList(TskTaskDev.class, "select ttd.* " + "from " + TskTaskDev.TableName + " ttd left join " + TskTaskPointRel.TableName + " ttpr on ttd.task_id=ttpr.task_id left join " + TskTestPoint.TableName + " ttp on ttpr.point_id=ttp.point_id where ttp.point_id = ?", pointId);
        List<String> tskTaskName = tskTaskDevs.stream().map(TskTaskDev::getTask_name).collect(Collectors.toList());
        if (!tskTaskDevs.isEmpty()) {
            throw new SystemBusinessException("不可删除！有以下任务使用到了此要点：" + tskTaskName);
        }
        tskTestPoint.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public void batchDelete(Long[] pointIds) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.addSql("select ttd.* " + "from " + TskTaskDev.TableName + " ttd left join " + TskTaskPointRel.TableName + " ttpr on ttd.task_id=ttpr.task_id left join " + TskTestPoint.TableName + " ttp on ttpr.point_id=ttp.point_id ");
        asmSql.addORParam("ttp.point_id", pointIds);
        List<TskTaskDev> tskTaskDevs = Dbo.queryList(TskTaskDev.class, asmSql.sql(), asmSql.params());
        List<String> tskTaskName = tskTaskDevs.stream().map(TskTaskDev::getTask_name).collect(Collectors.toList());
        if (!tskTaskDevs.isEmpty()) {
            throw new SystemBusinessException("不可删除！有以下任务使用到了此要点：" + tskTaskName);
        }
        SqlOperator.Assembler asmSql1 = SqlOperator.Assembler.newInstance();
        asmSql1.addSql("delete from " + TskTestPoint.TableName + "");
        asmSql1.addORParam("point_id", pointIds);
        Dbo.execute(asmSql1.sql(), asmSql1.params());
        Dbo.commitTransaction();
    }
}
