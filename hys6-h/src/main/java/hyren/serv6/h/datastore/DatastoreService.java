package hyren.serv6.h.datastore;

import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAdded;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.utils.Aes.AesUtil;

@Service
public class DatastoreService {

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public String searchDataStoreById(long dsl_id) {
        Map<String, Object> storeLayer = Dbo.queryOneObject("select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id);
        List<Map<String, Object>> layerAndAdded = Dbo.queryList("select * from " + DataStoreLayerAdded.TableName + " where dsl_id=?", dsl_id);
        List<Map<String, Object>> layerAndAttr = Dbo.queryList("select * from " + DataStoreLayerAttr.TableName + " where dsl_id=? order by is_file", dsl_id);
        storeLayer.put("layerAndAdded", layerAndAdded);
        storeLayer.put("layerAndAttr", layerAndAttr);
        return AesUtil.encrypt(JsonUtil.toJson(storeLayer));
    }
}
