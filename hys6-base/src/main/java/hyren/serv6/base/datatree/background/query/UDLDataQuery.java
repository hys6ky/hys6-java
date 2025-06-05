package hyren.serv6.base.datatree.background.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/7/7 0007 上午 11:14")
public class UDLDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqTableInfo getUDLTableInfo(String table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DqTableInfo dq_table_info = new DqTableInfo();
            dq_table_info.setTable_id(Long.parseLong(table_id));
            return SqlOperator.queryOneObject(db, DqTableInfo.class, "select * from " + DqTableInfo.TableName + " where table_id" + "=?", dq_table_info.getTable_id()).orElseThrow(() -> new BusinessException("获取UDL数据表信息的sql失败!"));
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getUDLTableColumns(String table_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DqTableInfo dq_table_info = new DqTableInfo();
            dq_table_info.setTable_id(Long.parseLong(table_id));
            List<Map<String, Object>> column_list = SqlOperator.queryList(db, "select field_id AS column_id,column_name as column_name, field_ch_name as column_ch_name," + " concat(column_type,'(',column_length,')') AS column_type,'0' AS is_primary_key" + " FROM " + DqTableColumn.TableName + " WHERE table_id=?", dq_table_info.getTable_id());
            for (Map<String, Object> column_map : column_list) {
                List<String> dsla_storelayers = SqlOperator.queryOneColumnList(db, "SELECT dsladd.dsla_storelayer FROM " + DqTableColumn.TableName + " dtc" + " JOIN " + DcolRelationStore.TableName + " dcrs ON dtc.field_id=dcrs.col_id" + " JOIN " + DataStoreLayerAdded.TableName + " dsladd ON dsladd.dslad_id=dcrs.dslad_id" + " WHERE dtc.field_id=?", column_map.get("column_id"));
                if (!dsla_storelayers.isEmpty()) {
                    dsla_storelayers.forEach(dsla_storelayer -> {
                        StoreLayerAdded storeLayerAdded = StoreLayerAdded.ofEnumByCode(dsla_storelayer);
                        if (storeLayerAdded == StoreLayerAdded.ZhuJian) {
                            column_map.put("is_primary_key", IsFlag.Shi.getCode());
                        }
                    });
                }
            }
            return column_list;
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<DataStoreLayer> getUDLExistTableDataStorageLayers() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, DataStoreLayer.class, "SELECT DISTINCT dsl.* from " + DataStoreLayer.TableName + " dsl JOIN " + DtabRelationStore.TableName + " dtrs ON dtrs.dsl_id = dsl.dsl_id" + " WHERE dtrs.data_source=?", StoreLayerDataSource.UD.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_store_layer", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getUDLStorageLayerTableInfos(DataStoreLayer data_store_layer) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT * FROM " + DqTableInfo.TableName + " dti" + " LEFT JOIN " + DtabRelationStore.TableName + " dtrs ON dtrs.tab_id=dti.table_id" + " LEFT JOIN " + DataStoreLayer.TableName + " dsl ON dsl.dsl_id=dtrs.dsl_id" + " WHERE dtrs.data_source=? AND dsl.dsl_id=?", StoreLayerDataSource.UD.getCode(), data_store_layer.getDsl_id());
        }
    }
}
