package hyren.serv6.h.market.dmmoduletable;

import fd.ng.core.utils.DateUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.market.ReqDataUtils;
import hyren.serv6.h.market.dmjobtable.DmJobTableInfoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
public class DmModuleTableService {

    public Long addDmModuleTable(long dslId, DmModuleTable dmDatatable) {
        try {
            Optional<DmModuleTable> dmModuleTable = checkTableName(dmDatatable.getModule_table_en_name());
            if (dmModuleTable.isPresent()) {
                throw new BusinessException(" table name repeat, please change...");
            }
            Long tableId = PrimayKeyGener.getNextId();
            dmDatatable.setModule_table_id(tableId);
            dmDatatable.setModule_table_c_date(DateUtil.getSysDate());
            dmDatatable.setModule_table_c_time(DateUtil.getSysTime());
            dmDatatable.setEtl_date(Constant._DEFAULT_DATE_8);
            dmDatatable.setData_u_date(Constant._MAX_DATE_8);
            dmDatatable.setDdl_u_date(Constant._MAX_DATE_8);
            dmDatatable.setModule_table_d_date(Constant._MAX_DATE_8);
            dmDatatable.setData_u_time(ReqDataUtils.DEFAULT_TIME);
            dmDatatable.setDdl_u_time(ReqDataUtils.DEFAULT_TIME);
            dmDatatable.add(Dbo.db());
            DtabRelationStore dtabRelationStore = new DtabRelationStore();
            dtabRelationStore.setDsl_id(dslId);
            dtabRelationStore.setTab_id(dmDatatable.getModule_table_id());
            dtabRelationStore.setData_source(StoreLayerDataSource.DM.getCode());
            dtabRelationStore.setIs_successful("100");
            dtabRelationStore.add(Dbo.db());
            return tableId;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean delDmModuleTable(Long dmDatatableId) {
        try {
            int execute = Dbo.execute("delete from " + DmModuleTable.TableName + " where module_table_id = ?", dmDatatableId);
            if (execute == 1) {
                Dbo.execute(" delete from " + DtabRelationStore.TableName + " where tab_id = ? ", dmDatatableId);
                List<DmModuleTableFieldInfo> dmModuleTableFieldInfos = Dbo.queryList(DmModuleTableFieldInfo.class, "select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", dmDatatableId);
                for (DmModuleTableFieldInfo dmModuleTableFieldInfo : dmModuleTableFieldInfos) {
                    Dbo.execute(" delete from " + DmModuleTableFieldInfo.TableName + " where module_field_id = ?", dmModuleTableFieldInfo.getModule_field_id());
                    Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ?", dmModuleTableFieldInfo.getModule_field_id());
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Autowired
    DmJobTableInfoService dmJobTableInfoService;

    private static final String TEMP = "_temp_";

    public boolean updateDmModuleTable(long dslId, DmModuleTable dmDatatable) {
        try {
            String moduleTableEnName = dmDatatable.getModule_table_en_name();
            Optional<DmModuleTable> dmModuleTable = checkTableName(moduleTableEnName);
            if (!dmModuleTable.isPresent()) {
                throw new BusinessException(" table data failed, please change...");
            }
            if (!dmModuleTable.get().getModule_table_id().equals(dmDatatable.getModule_table_id())) {
                throw new BusinessException(" table name repeat, please change...");
            }
            List<DmJobTableInfo> jobsByModuleTableId = dmJobTableInfoService.findJobsByModuleTableId(dmDatatable.getModule_table_id());
            for (DmJobTableInfo dmJobTableInfo : jobsByModuleTableId) {
                if (dmJobTableInfo.getJobtab_is_temp().equals(IsFlag.Shi.getCode())) {
                    String[] split = dmJobTableInfo.getJobtab_en_name().split(TEMP);
                    dmJobTableInfo.setJobtab_en_name(dmDatatable.getModule_table_en_name() + TEMP + split[1]);
                    dmJobTableInfo.update(Dbo.db());
                } else {
                    dmJobTableInfo.setJobtab_en_name(dmDatatable.getModule_table_en_name());
                    dmJobTableInfo.update(Dbo.db());
                }
            }
            Optional<DtabRelationStore> dtabRelationStore = Dbo.queryOneObject(DtabRelationStore.class, "select * from " + DtabRelationStore.TableName + " where tab_id = ?", dmDatatable.getModule_table_id());
            int result = 0;
            if (dtabRelationStore.isPresent()) {
                DtabRelationStore update = dtabRelationStore.get();
                Dbo.execute(" delete from " + DtabRelationStore.TableName + " where dsl_id = ? and tab_id = ?", update.getDsl_id(), update.getTab_id());
                update.setDsl_id(dslId);
                result = update.add(Dbo.db());
            } else {
                throw new BusinessException("this dataTable is not have a dbSource");
            }
            int update = dmDatatable.update(Dbo.db());
            return update == 1 && result == 1;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public List<DmModuleTable> findDmModuleTables() {
        try {
            return Dbo.queryList(DmModuleTable.class, "select * from " + DmModuleTable.TableName);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<DmModuleTable> findDmModuleTableByDmInfoId(Long dmInfoId) {
        try {
            return Dbo.queryList(DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where data_mart_id = ?", dmInfoId);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public DmModuleTable findDmModuleTableById(Long dmDatatableId) {
        try {
            Optional<DmModuleTable> dmModuleTable = Dbo.queryOneObject(DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id = ?", dmDatatableId);
            return dmModuleTable.orElse(null);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<DmModuleTableDto> findDmModuleTablesByDmInfoIdAndCateId(Long dmInfoId, Long cateId) {
        List<DmModuleTableDto> dmModuleTableDtos = new ArrayList<>();
        if (cateId == null) {
            dmModuleTableDtos = Dbo.queryList(DmModuleTableDto.class, "select * from " + DmModuleTable.TableName + " where data_mart_id = ? order by module_table_id desc", dmInfoId);
        } else {
            dmModuleTableDtos = Dbo.queryList(DmModuleTableDto.class, "select * from " + DmModuleTable.TableName + " where data_mart_id = ? and category_id = ? order by module_table_id desc", dmInfoId, cateId);
        }
        for (DmModuleTableDto dmModuleTableDto : dmModuleTableDtos) {
            long jobNum = Dbo.queryNumber(" select count(*) from " + DmTaskInfo.TableName + " dti " + " join " + DmJobTableInfo.TableName + " djti on dti.task_id = djti.task_id " + " where dti.module_table_id = ?", dmModuleTableDto.getModule_table_id()).orElseThrow(() -> new BusinessException(" find jobNum failed"));
            dmModuleTableDto.setJobNum(jobNum);
        }
        return dmModuleTableDtos;
    }

    public List<Map<String, Object>> findDataByDmTableName(String dmTableName) {
        try {
            return Dbo.queryList("select * from " + dmTableName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("sql 执行失败...");
        }
    }

    public List<Map<String, Object>> queryDmModuleTableByDataTableId(long datatable_id) {
        return Dbo.queryList("SELECT t1.*,t2.*,t3.category_name FROM " + DmModuleTable.TableName + " t1 left join " + DtabRelationStore.TableName + " t2 on t1.module_table_id = t2.tab_id and t2.data_source= ? left join " + DmCategory.TableName + " t3 on t3.category_id=t1.category_id" + " where t1.module_table_id= ? ", StoreLayerDataSource.DM.getCode(), datatable_id);
    }

    public Optional<DmModuleTable> checkTableName(String tableName) {
        return Dbo.queryOneObject(DmModuleTable.class, "select * from " + DmModuleTable.TableName + "  where module_table_en_name = ?", tableName);
    }

    public boolean delDmModuleTableByIds(List<String> ids) {
        if (ids.isEmpty()) {
            throw new BusinessException(" please check del table...");
        }
        try {
            for (String id : ids) {
                boolean b = delDmModuleTable(Long.parseLong(id));
                if (!b) {
                    return false;
                }
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean checkModuleTableFields(String moduleTableId) {
        long fieldNumber = Dbo.queryNumber("select count(*) from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", Long.parseLong(moduleTableId)).orElseThrow(() -> new BusinessException("sql failed"));
        return fieldNumber != 0;
    }

    public boolean checkModuleTableIsRun(String moduleTableId) {
        List<String> etlDate = Dbo.queryOneColumnList(" select etl_date from " + DmModuleTable.TableName + " where module_table_id = ?", Long.parseLong(moduleTableId));
        if (!etlDate.isEmpty()) {
            return !etlDate.get(0).equals(Constant._MAX_DATE_8);
        }
        return true;
    }
}
