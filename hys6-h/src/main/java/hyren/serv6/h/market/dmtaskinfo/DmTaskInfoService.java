package hyren.serv6.h.market.dmtaskinfo;

import fd.ng.core.utils.DateUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DmModuleTable;
import hyren.serv6.base.entity.DmTaskInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class DmTaskInfoService {

    public boolean addDmTaskInfo(DmTaskInfo dmTaskInfo) {
        try {
            dmTaskInfo.setTask_id(PrimayKeyGener.getNextId());
            dmTaskInfo.setTask_create_date(DateUtil.getSysDate());
            int add = dmTaskInfo.add(Dbo.db());
            return add == 1;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public boolean delDmTaskInfo(Long dmTaskInfoId) {
        try {
            int execute = Dbo.execute("delete from " + DmTaskInfo.TableName + " where task_id = ?", dmTaskInfoId);
            return execute == 1;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public boolean updateDmTaskInfo(DmTaskInfo dmTaskInfo) {
        try {
            dmTaskInfo.setTask_create_date(DateUtil.getSysDate());
            int update = dmTaskInfo.update(Dbo.db());
            return update == 1;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public List<DmTaskInfo> findDmTaskInfos() {
        try {
            return Dbo.queryList(DmTaskInfo.class, "select * from " + DmTaskInfo.TableName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public Map<String, Object> findDmTaskInfoById(Long dmTaskInfoId) {
        try {
            return Dbo.queryOneObject(" SELECT  dti.*,  dd.module_table_en_name FROM  " + DmTaskInfo.TableName + " dti  " + " JOIN " + DmModuleTable.TableName + " dd ON dti.module_table_id = dd.module_table_id  " + " WHERE dti.task_id = ?", dmTaskInfoId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行错误");
        }
    }

    public boolean addDmTaskInfos(List<DmTaskInfo> dmTaskInfos) {
        if (dmTaskInfos.isEmpty())
            return false;
        for (DmTaskInfo dmTaskInfo : dmTaskInfos) {
            boolean b = addDmTaskInfo(dmTaskInfo);
            if (!b) {
                return false;
            }
        }
        return true;
    }

    public List<DmTaskInfo> findDmTaskInfosByTableId(Long dataTableId) {
        return Dbo.queryList(DmTaskInfo.class, "select * from " + DmTaskInfo.TableName + " where module_table_id = ?", dataTableId);
    }
}
