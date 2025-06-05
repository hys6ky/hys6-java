package hyren.serv6.h.market.moduletablefield;

import fd.ng.core.utils.StringUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.entity.DataStoreLayerAdded;
import hyren.serv6.base.entity.DcolRelationStore;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.entity.DmModuleTableFieldInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.market.dcolRelationStore.DcolRelationStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
public class DataTableFieldService {

    @Autowired
    DcolRelationStoreService dcolRelationStoreService;

    public List<DmModuleTableFieldInfoDto> saveDatatableFieldInfos(String datatable_id, List<DmModuleTableFieldInfoDto> datatableFieldInfos) {
        List<DmModuleTableFieldInfoDto> result = new ArrayList<>();
        List<Long> dbFieldIds = Dbo.queryOneColumnList(" select module_field_id from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", Long.parseLong(datatable_id));
        List<Long> updateFieldIds = new ArrayList<>();
        try {
            Dbo.execute(" delete from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", Long.parseLong(datatable_id));
            dcolRelationStoreService.deleteDcolRelattontore(datatableFieldInfos);
            for (DmModuleTableFieldInfoDto datatableFieldInfo : datatableFieldInfos) {
                if (datatableFieldInfo.getModule_field_id() != null) {
                    updateFieldIds.add(datatableFieldInfo.getModule_field_id());
                } else {
                    datatableFieldInfo.setModule_field_id(PrimayKeyGener.getNextId());
                }
                datatableFieldInfo.setModule_table_id(datatable_id);
                if (datatableFieldInfo.getField_length().equals("0")) {
                    datatableFieldInfo.setField_length(null);
                }
                DmModuleTableFieldInfo fieldInfo = new DmModuleTableFieldInfo();
                BeanUtils.copyProperties(datatableFieldInfo, fieldInfo);
                result.add(datatableFieldInfo);
                fieldInfo.add(Dbo.db());
            }
            delJobFields(updateFieldIds, dbFieldIds);
            return result;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new BusinessException(" dataTable field add failed...");
        }
    }

    private static void delJobFields(List<Long> updateFieldIds, List<Long> dbFieldIds) {
        List<Long> delFields = new ArrayList<>();
        for (Long dbFieldId : dbFieldIds) {
            if (!updateFieldIds.contains(dbFieldId)) {
                delFields.add(dbFieldId);
            }
        }
        for (Long dbFieldId : delFields) {
            Dbo.execute(" delete from " + DmJobTableFieldInfo.TableName + " where module_field_id = ?", dbFieldId);
        }
    }

    public List<Map<String, Object>> findModuleTableFieldInfos(String dataTableId) {
        List<Map<String, Object>> mapcolumnList = null;
        try {
            mapcolumnList = Dbo.queryList("SELECT DISTINCT ds.*, dr.data_source  FROM " + DmModuleTableFieldInfo.TableName + " ds" + " LEFT JOIN " + DcolRelationStore.TableName + " dr ON ds.module_field_id = dr.col_id " + " LEFT JOIN " + DataStoreLayerAdded.TableName + " da ON dr.dslad_id = da.dslad_id " + " WHERE ds.module_table_id = ?", Long.parseLong(dataTableId));
            for (Map<String, Object> map : mapcolumnList) {
                List<String> storelayerList = Dbo.queryOneColumnList("SELECT da.dsla_storelayer  FROM " + DmModuleTableFieldInfo.TableName + " ds" + " LEFT JOIN " + DcolRelationStore.TableName + " dr ON ds.module_field_id = dr.col_id " + " LEFT JOIN " + DataStoreLayerAdded.TableName + " da ON dr.dslad_id = da.dslad_id " + " WHERE ds.module_field_id = ?", Long.parseLong(map.get("module_field_id").toString()));
                for (String s : storelayerList) {
                    if (!StringUtil.isBlank(s)) {
                        map.put(StoreLayerAdded.ofValueByCode(s), true);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mapcolumnList;
    }

    public List<DmModuleTableFieldInfo> findFieldsByTableId(Long moduleTableId) {
        return Dbo.queryList(DmModuleTableFieldInfo.class, "select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", moduleTableId);
    }
}
