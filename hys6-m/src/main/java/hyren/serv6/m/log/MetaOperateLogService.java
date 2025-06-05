package hyren.serv6.m.log;

import fd.ng.core.utils.DateUtil;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.m.entity.MetaOperateLog;
import hyren.serv6.m.vo.query.MetaOperateLogQueryVo;
import hyren.serv6.m.vo.save.MetaOperateLogSaveVo;
import org.springframework.stereotype.Service;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import org.springframework.beans.BeanUtils;
import hyren.serv6.m.util.IdGenerator;
import java.util.List;

@Service("metaOperateLogService")
public class MetaOperateLogService {

    public MetaOperateLogQueryVo queryById(Long id) {
        return Dbo.queryOneObject(MetaOperateLogQueryVo.class, "select * from " + MetaOperateLog.TableName + " where id=?", id).orElse(null);
    }

    public List<MetaOperateLogQueryVo> queryByPage(MetaOperateLogQueryVo metaOperateLogQueryVo, Page page) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + MetaOperateLog.TableName);
        return Dbo.queryPagedList(MetaOperateLogQueryVo.class, page, assembler);
    }

    public MetaOperateLog insert(MetaOperateLogSaveVo metaOperateLogSaveVo) {
        MetaOperateLog metaOperateLog = new MetaOperateLog();
        BeanUtils.copyProperties(metaOperateLogSaveVo, metaOperateLog);
        metaOperateLog.setId(IdGenerator.nextId());
        metaOperateLog.add(Dbo.db());
        return metaOperateLog;
    }

    public MetaOperateLog update(MetaOperateLogSaveVo metaOperateLogSaveVo) {
        MetaOperateLog queryVo = queryById(metaOperateLogSaveVo.getId());
        MetaOperateLog metaOperateLog = new MetaOperateLog();
        BeanUtils.copyProperties(queryVo, metaOperateLog);
        BeanUtils.copyProperties(metaOperateLogSaveVo, metaOperateLog);
        metaOperateLog.update(Dbo.db());
        Dbo.commitTransaction();
        return metaOperateLog;
    }

    public boolean deleteById(Long id) {
        MetaOperateLog metaOperateLog = new MetaOperateLog();
        metaOperateLog.setId(id);
        metaOperateLog.delete(Dbo.db());
        Dbo.commitTransaction();
        return true;
    }

    public void save(String operateMsg) {
        MetaOperateLog metaOperateLog = new MetaOperateLog();
        metaOperateLog.setId(IdGenerator.nextId());
        metaOperateLog.setCreated_id(UserUtil.getUserId());
        metaOperateLog.setCreated_by(UserUtil.getUser().getRoleName());
        metaOperateLog.setCreated_date(DateUtil.getSysDate());
        metaOperateLog.setCreated_time(DateUtil.getSysTime());
        metaOperateLog.setOperate(operateMsg);
        metaOperateLog.add(Dbo.db());
        Dbo.commitTransaction();
    }
}
