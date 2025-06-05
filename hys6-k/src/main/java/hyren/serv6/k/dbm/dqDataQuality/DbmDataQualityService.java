package hyren.serv6.k.dbm.dqDataQuality;

import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.k.entity.DbmDataQuality;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class DbmDataQualityService {

    public void saveDataQuality(DbmDataQuality dbmDataQuality) {
        CheckDataQuality(dbmDataQuality);
        dbmDataQuality.setDq_id(PrimaryKeyUtils.nextId());
        dbmDataQuality.setCreated_id(UserUtil.getUserId());
        dbmDataQuality.setCreated_by(UserUtil.getUser().getUsername());
        dbmDataQuality.setCreated_date(DateUtil.getSysDate());
        dbmDataQuality.setCreated_time(DateUtil.getSysTime());
        dbmDataQuality.add(Dbo.db());
    }

    public List<DbmDataQuality> getDataQuality(Long basic_id, Page page) {
        return Dbo.queryPagedList(DbmDataQuality.class, page, "SELECT * FROM " + DbmDataQuality.TableName + " where basic_id = ?", basic_id);
    }

    public DbmDataQuality getOneDataQuality(Long dqId) {
        return Dbo.queryOneObject(DbmDataQuality.class, "select * from " + DbmDataQuality.TableName + " where dq_id = ? ", dqId).orElseThrow(() -> new BusinessException("为获取到数据质量信息"));
    }

    public void CheckDataQuality(DbmDataQuality dbmDataQuality) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("select count(1) from " + DbmDataQuality.TableName + " where ( dq_num = ? or dq_name = ? ) AND BASIC_ID = ? ").addParam(dbmDataQuality.getDq_num()).addParam(dbmDataQuality.getDq_name()).addParam(dbmDataQuality.getBasic_id());
        if (dbmDataQuality.getDq_id() != null && !dbmDataQuality.getDq_id().equals("")) {
            sql.addSql(" AND dq_id != ?").addParam(dbmDataQuality.getDq_id());
        }
        long l = Dbo.queryNumber(sql.sql(), sql.params()).orElse(0);
        if (l != 0) {
            throw new BusinessException("数据质量维度编号 或 数据质量维度名称 出现重复");
        }
    }

    public void upDataQuality(DbmDataQuality dbmDataQuality) {
        CheckDataQuality(dbmDataQuality);
        dbmDataQuality.setUpdated_id(UserUtil.getUserId());
        dbmDataQuality.setUpdated_by(UserUtil.getUser().getUsername());
        dbmDataQuality.setUpdated_date(DateUtil.getSysDate());
        dbmDataQuality.setUpdated_time(DateUtil.getSysTime());
        dbmDataQuality.update(Dbo.db());
    }

    public void delQuality(Long dqId) {
        Dbo.execute("delete from " + DbmDataQuality.TableName + " where dq_id = ?", dqId);
        Dbo.commitTransaction();
    }
}
