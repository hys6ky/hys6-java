package hyren.serv6.h.market.dcolRelationStore;

import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.DcolRelationStore;
import hyren.serv6.base.entity.DmModuleTableFieldInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.h.market.moduletablefield.DmModuleTableFieldInfoDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DcolRelationStoreService {

    public void saveDcolRelationStore(List<DmModuleTableFieldInfoDto> datatableFieldInfos, List<DcolRelationStore> dcolRelationStores) {
        try {
            for (DcolRelationStore dcolRelationStore : dcolRelationStores) {
                List<DmModuleTableFieldInfo> collect = datatableFieldInfos.stream().filter(s -> s.getCsi_number().equals(dcolRelationStore.getCsi_number().toString())).collect(Collectors.toList());
                dcolRelationStore.setCol_id(collect.get(0).getModule_field_id());
                dcolRelationStore.setData_source(StoreLayerDataSource.DM.getCode());
                dcolRelationStore.add(Dbo.db());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(" add dcol failed...");
        }
    }

    public void deleteDcolRelattontore(List<DmModuleTableFieldInfoDto> datatableFieldInfos) {
        try {
            for (DmModuleTableFieldInfo datatableFieldInfo : datatableFieldInfos) {
                List<DcolRelationStore> dcolRelationStores = Dbo.queryList(DcolRelationStore.class, "select * from " + DcolRelationStore.TableName + " where col_id = ? ", datatableFieldInfo.getModule_field_id());
                if (null != dcolRelationStores && dcolRelationStores.size() != 0) {
                    Dbo.execute("DELETE FROM " + DcolRelationStore.TableName + " WHERE col_id = ? ", datatableFieldInfo.getModule_field_id());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(" del DcolRelationStore failed...");
        }
    }
}
