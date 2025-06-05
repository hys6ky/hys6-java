package hyren.serv6.h.market.dminfo;

import fd.ng.core.utils.DateUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.h.market.dmjobtable.DmJobTableInfoService;
import hyren.serv6.h.market.dmmoduletable.DmModuleTableService;
import hyren.serv6.h.market.dmtaskinfo.DmTaskInfoService;
import hyren.serv6.h.market.moduletablefield.DataTableFieldService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class DmInfoService {

    public static String defaultStoragePath = "/data";

    public boolean addDmInfo(DmInfo dmInfo) {
        try {
            if (dmInfo.getData_mart_id() == null) {
                dmInfo.setData_mart_id(PrimayKeyGener.getNextId());
                dmInfo.setCreate_date(DateUtil.getSysDate());
                dmInfo.setCreate_time(DateUtil.getSysTime());
                dmInfo.setCreate_id(UserUtil.getUserId());
                dmInfo.setMart_storage_path(defaultStoragePath);
                int add = dmInfo.add(Dbo.db());
                return add == 1;
            } else {
                return updateDmInfo(dmInfo);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Autowired
    DmModuleTableService dmModuleTableService;

    @Autowired
    DmTaskInfoService dmTaskInfoService;

    @Autowired
    DmJobTableInfoService dmJobTableInfoService;

    @Autowired
    DataTableFieldService dataTableFieldService;

    public boolean delDmInfo(Long dmInfoId) {
        try {
            Dbo.execute("delete from " + DmInfo.TableName + " where data_mart_id = ?", dmInfoId);
            Dbo.execute("delete from " + DmCategory.TableName + " where data_mart_id = ?", dmInfoId);
            List<DmModuleTable> dmModuleTableByDmInfoId = dmModuleTableService.findDmModuleTableByDmInfoId(dmInfoId);
            for (DmModuleTable dmModuleTable : dmModuleTableByDmInfoId) {
                List<DmTaskInfo> dmTaskInfos = dmTaskInfoService.findDmTaskInfosByTableId(dmModuleTable.getModule_table_id());
                for (DmTaskInfo dmTaskInfo : dmTaskInfos) {
                    List<DmJobTableInfo> jobs = dmJobTableInfoService.findJobs(dmTaskInfo.getTask_id().toString());
                    for (DmJobTableInfo job : jobs) {
                        Dbo.execute("delete from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ?", job.getJobtab_id());
                        Dbo.execute("delete from " + DmDatatableSource.TableName + " where jobtab_id = ?", job.getJobtab_id());
                        Dbo.execute(" delete from " + DmOwnSourceField.TableName + " where own_source_table_id in ( select own_source_table_id from " + DmMapInfo.TableName + " where jobtab_id = ?)", job.getJobtab_id());
                        Dbo.execute("delete from " + DmMapInfo.TableName + " where jobtab_id = ?", job.getJobtab_id());
                    }
                    Dbo.execute(" delete from " + DmJobTableInfo.TableName + " where task_id = ?", dmTaskInfo.getTask_id());
                }
                Dbo.execute("delete from " + DmTaskInfo.TableName + " where module_table_id = ?", dmModuleTable.getModule_table_id());
                List<DmModuleTableFieldInfo> fieldsByTableId = dataTableFieldService.findFieldsByTableId(dmModuleTable.getModule_table_id());
                for (DmModuleTableFieldInfo dmModuleTableFieldInfo : fieldsByTableId) {
                    Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ?", dmModuleTableFieldInfo.getModule_field_id());
                }
                Dbo.execute("delete from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", dmModuleTable.getModule_table_id());
                Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", dmModuleTable.getModule_table_id());
            }
            Dbo.execute("delete from " + DmModuleTable.TableName + " where data_mart_id = ?", dmInfoId);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean updateDmInfo(DmInfo dmInfo) {
        dmInfo.setCreate_date(DateUtil.getSysDate());
        dmInfo.setCreate_time(DateUtil.getSysTime());
        dmInfo.setCreate_id(UserUtil.getUserId());
        dmInfo.setMart_storage_path(defaultStoragePath);
        try {
            int update = dmInfo.update(Dbo.db());
            return update == 1;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<DmInfo> findDmInfos() {
        try {
            return Dbo.queryList(DmInfo.class, "select * from " + DmInfo.TableName);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public DmInfo findDmInfoById(Long dmInfoId) {
        try {
            Optional<DmInfo> dmInfo = Dbo.queryOneObject(DmInfo.class, "select * from " + DmInfo.TableName + " where data_mart_id = ?", dmInfoId);
            return dmInfo.orElse(null);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<DmInfo> findDmInfosByUserId() {
        return Dbo.queryList(DmInfo.class, "SELECT * FROM " + DmInfo.TableName + " where create_id = ? order by " + "mart_name asc", UserUtil.getUserId());
    }
}
