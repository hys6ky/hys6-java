package hyren.serv6.f.dataRegister.source.stodestconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DtabRelationStore;
import hyren.serv6.base.entity.TableStorageInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Api("定义存储目的地配置")
@Service
@DocClass(desc = "", author = "WangZhengcheng")
public class StoDestStepService {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getStoDestByTableId(long tableId) {
        Result storageData = getStorageData();
        Map<String, Object> resultMap = new HashMap<>();
        Result tbStoRela = Dbo.queryResult("SELECT t2.dsl_id,t1.hyren_name FROM " + TableStorageInfo.TableName + " t1" + " JOIN " + DtabRelationStore.TableName + " t2 ON  t1.storage_id = t2.tab_id" + " WHERE t1.table_id = ? AND t2.data_source = ?", tableId, StoreLayerDataSource.DB.getCode());
        if (tbStoRela.isEmpty()) {
            resultMap.put("hyren_name", "");
            resultMap.put("tableStorage", storageData.toList());
            return resultMap;
        }
        for (int i = 0; i < tbStoRela.getRowCount(); i++) {
            long dslId = tbStoRela.getLong(i, "dsl_id");
            for (int j = 0; j < storageData.getRowCount(); j++) {
                long dslIdFromResult = storageData.getLong(j, "dsl_id");
                if (dslId == dslIdFromResult) {
                    storageData.setObject(j, "usedflag", IsFlag.Shi.getCode());
                    resultMap.put("hyren_name", tbStoRela.getString(i, "hyren_name"));
                }
            }
        }
        resultMap.put("tableStorage", storageData.toList());
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dslId", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getStoDestDetail(long dslId) {
        return AesUtil.encrypt(Dbo.queryResult(" select t1.storage_property_key, t1.storage_property_val,t2.store_type " + " FROM " + DataStoreLayerAttr.TableName + " t1 JOIN " + DataStoreLayer.TableName + " t2 ON t1.dsl_id = t2.dsl_id  WHERE t1.dsl_id = ?", dslId).toJSON());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getStorageData() {
        Result result = Dbo.queryResult("SELECT dsl_id, dsl_name, store_type, '0' as usedflag FROM " + DataStoreLayer.TableName);
        if (result.isEmpty()) {
            throw new BusinessException("系统中未定义存储目的地信息，请联系管理员");
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            Store_type storeType = Store_type.ofEnumByCode(result.getString(i, "store_type"));
            if (storeType == Store_type.DATABASE) {
                Map<String, Object> map = Dbo.queryOneObject("SELECT storage_property_val FROM " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND dsl_id = ?", StorageTypeKey.database_type, result.getLong(i, "dsl_id"));
                if (Objects.isNull(map.get("storage_property_val"))) {
                    throw new BusinessException("storage_property_key and dsl_id find storage_property_val is null.");
                }
                result.setObject(i, "store_name", map.get("storage_property_val").toString());
            } else {
                result.setObject(i, "store_name", storeType.getValue());
            }
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getStorageDataBySource() {
        Result result = Dbo.queryResult("SELECT dsl_id, dsl_name, store_type, '0' as usedflag FROM " + DataStoreLayer.TableName + " where dsl_source =?", IsFlag.Shi.getCode());
        if (result.isEmpty()) {
            throw new BusinessException("系统中未定义存储目的地信息，请联系管理员");
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            Store_type storeType = Store_type.ofEnumByCode(result.getString(i, "store_type"));
            if (storeType == Store_type.DATABASE) {
                Map<String, Object> map = Dbo.queryOneObject("SELECT storage_property_val FROM " + DataStoreLayerAttr.TableName + " where storage_property_key = ? AND dsl_id = ?", StorageTypeKey.database_type, result.getLong(i, "dsl_id"));
                if (Objects.isNull(map.get("storage_property_val"))) {
                    throw new BusinessException("storage_property_key and dsl_id find storage_property_val is null.");
                }
                result.setObject(i, "store_name", map.get("storage_property_val").toString());
            } else {
                result.setObject(i, "store_name", storeType.getValue());
            }
        }
        return result;
    }
}
