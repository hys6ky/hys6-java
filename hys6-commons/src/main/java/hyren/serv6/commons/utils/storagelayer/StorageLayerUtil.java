package hyren.serv6.commons.utils.storagelayer;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAdded;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DatabaseTypeMapping;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "BY-HLL", createdate = "2021/6/29 0029 下午 03:28")
public class StorageLayerUtil {

    public static Map<String, Object> getStorageLayerConfInfo(DatabaseWrapper db, long dsl_id) {
        Map<String, Object> slci = new HashMap<>();
        DataStoreLayer dsl = Dbo.queryOneObject(db, DataStoreLayer.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id).orElseThrow(() -> new BusinessException("获取存储层信息失败! dsl_id=" + dsl_id));
        slci.put("dsl", dsl);
        List<DataStoreLayerAttr> dsl_attr_s = Dbo.queryList(db, DataStoreLayerAttr.class, "select * from " + DataStoreLayerAttr.TableName + " where dsl_id=?", dsl_id);
        slci.put("dsl_attr_s", dsl_attr_s);
        List<DataStoreLayerAdded> dsl_added_s = Dbo.queryList(db, DataStoreLayerAdded.class, "select * from " + DataStoreLayerAdded.TableName + " where dsl_id=?", dsl_id);
        slci.put("dsl_added_s", dsl_added_s);
        String store_type = dsl.getStore_type();
        if (Store_type.HIVE == Store_type.ofEnumByCode(store_type) || Store_type.DATABASE == Store_type.ofEnumByCode(store_type)) {
            String database_name = dsl.getDatabase_name();
            if (Store_type.HIVE == Store_type.ofEnumByCode(store_type)) {
                database_name = Store_type.HIVE.name();
            }
            List<String> filedTypes = new ArrayList<>();
            List<String> databaseTypes = Dbo.queryOneColumnList(Dbo.db(), "select database_type1 from " + DatabaseTypeMapping.TableName + " where database_name1 = ?" + " union " + "select database_type2 from " + DatabaseTypeMapping.TableName + " where database_name2 = ?", database_name, database_name);
            if (databaseTypes.isEmpty()) {
                throw new BusinessException("该存储类型或者数据库没有配置类型列表数据，请联系管理员处理！！！" + database_name);
            }
            for (String databaseType : databaseTypes) {
                if (databaseType.contains(Constant.LXKH) && databaseType.contains(Constant.RXKH)) {
                    filedTypes.add(databaseType.substring(0, databaseType.indexOf(Constant.LXKH)));
                } else {
                    filedTypes.add(databaseType);
                }
            }
            filedTypes = filedTypes.stream().distinct().collect(Collectors.toList());
            slci.put("filedTypes", filedTypes);
        } else {
            throw new BusinessException("该存储类型没有配置类型列表数据，请联系管理员处理！！！" + Store_type.ofEnumByCode(store_type));
        }
        return slci;
    }
}
