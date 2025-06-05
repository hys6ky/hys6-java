package hyren.serv6.k.dm.metadatamanage.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.DboExecute;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/4/1 0001 下午 02:11")
public class MDMDataQuery {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<DataStoreLayer> getDCLExistTableDataStorageLayers() {
        return Dbo.queryList(DataStoreLayer.class, "SELECT dsl.* FROM " + TableInfo.TableName + " ti" + " JOIN " + TableStorageInfo.TableName + " tsi ON tsi.table_id = ti.table_id" + " JOIN " + DtabRelationStore.TableName + " dtrs ON dtrs.tab_id = tsi.storage_id" + " JOIN " + DataStoreLayer.TableName + " dsl ON dsl.dsl_id = dtrs.dsl_id" + " WHERE dtrs.data_source in (?,?,?) AND is_successful=? GROUP BY dsl.dsl_id", StoreLayerDataSource.DB.getCode(), StoreLayerDataSource.DBA.getCode(), StoreLayerDataSource.OBJ.getCode(), JobExecuteState.WanCheng.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_store_layer", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDCLStorageLayerTableInfos(DataStoreLayer data_store_layer) {
        return Dbo.queryList("SELECT dsl.*,dsr.* FROM " + DataStoreLayer.TableName + " dsl" + " JOIN " + DtabRelationStore.TableName + " dtrs ON dsl.dsl_id = dtrs.dsl_id" + " JOIN " + TableStorageInfo.TableName + " tsi ON dtrs.tab_id = tsi.storage_id" + " JOIN " + TableInfo.TableName + " ti ON ti.table_id = tsi.table_id" + " JOIN " + DataStoreReg.TableName + " dsr ON dsr.table_id = ti.table_id" + " WHERE dsl.dsl_id = ? and dtrs.data_source in (?,?,?)", data_store_layer.getDsl_id(), StoreLayerDataSource.DB.getCode(), StoreLayerDataSource.DBA.getCode(), StoreLayerDataSource.OBJ.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DataStoreReg getDCLDataStoreRegInfo(String file_id) {
        DataStoreReg dsr = new DataStoreReg();
        dsr.setFile_id(file_id);
        return Dbo.queryOneObject(DataStoreReg.class, "SELECT * from " + DataStoreReg.TableName + " WHERE file_id =?", dsr.getFile_id()).orElseThrow(() -> (new BusinessException("获取数据登记信息的SQL失败!")));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dsr", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DtabRelationStore getDCLTableSpecifyStorageRelationship(long dsl_id, DataStoreReg dsr) {
        return Dbo.queryOneObject(DtabRelationStore.class, "SELECT dtrs.* from " + DataStoreLayer.TableName + " dsl left join " + DtabRelationStore.TableName + " dtrs" + " on dsl.dsl_id=dtrs.dsl_id left join " + TableStorageInfo.TableName + " tsi on tsi.storage_id=dtrs.tab_id" + " left join " + TableInfo.TableName + " ti on ti.table_id=tsi.table_id" + " where dsl.dsl_id=? and ti.table_id=? and dtrs.data_source in (?,?,?)", dsl_id, dsr.getTable_id(), StoreLayerDataSource.DB.getCode(), StoreLayerDataSource.DBA.getCode(), StoreLayerDataSource.OBJ.getCode()).orElseThrow(() -> (new BusinessException("根据存储层id,表id和 存储层关系-数据来源 获取数据表的存储关系信息的SQL出错!")));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsr", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<DtabRelationStore> getDCLTableStorageRelationships(DataStoreReg dsr) {
        return Dbo.queryList(DtabRelationStore.class, "SELECT dtrs.* from " + DataStoreLayer.TableName + " dsl left join " + DtabRelationStore.TableName + " dtrs" + " on dsl.dsl_id=dtrs.dsl_id left join " + TableStorageInfo.TableName + " tsi on tsi.storage_id=dtrs.tab_id" + " left join " + TableInfo.TableName + " ti on ti.table_id=tsi.table_id" + " where ti.table_id=? and dtrs.data_source in (?,?,?)", dsr.getTable_id(), StoreLayerDataSource.DB.getCode(), StoreLayerDataSource.DBA.getCode(), StoreLayerDataSource.OBJ.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqIndex3record getDQCDqIndex3record(String file_id) {
        DqIndex3record dq_index3record = new DqIndex3record();
        dq_index3record.setRecord_id(Long.parseLong(file_id));
        return Dbo.queryOneObject(DqIndex3record.class, "SELECT * from " + DqIndex3record.TableName + " WHERE " + "record_id=?", dq_index3record.getRecord_id()).orElseThrow(() -> (new BusinessException("获取DQC数据登记信息的SQL失败!")));
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<DataStoreLayer> getUDLDataStorageLayers() {
        return Dbo.queryList(DataStoreLayer.class, "SELECT * from " + DataStoreLayer.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_store_layer", desc = "", range = "")
    public static List<Map<String, Object>> getUDLStorageLayerTableInfos(DataStoreLayer data_store_layer) {
        return Dbo.queryList("select dsl.*,dqt.* from " + DqTableInfo.TableName + " dqt" + " JOIN " + DtabRelationStore.TableName + " dtrs ON dtrs.tab_id = dqt.table_id" + " JOIN " + DataStoreLayer.TableName + " dsl ON dsl.dsl_id = dtrs.dsl_id" + " WHERE dsl.dsl_id=? and is_successful=? and dtrs.data_source=?", data_store_layer.getDsl_id(), JobExecuteState.WanCheng.getCode(), StoreLayerDataSource.UD.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqTableInfo getUDLTableInfo(String table_id) {
        DqTableInfo dq_table_info = new DqTableInfo();
        dq_table_info.setTable_id(Long.parseLong(table_id));
        return Dbo.queryOneObject(DqTableInfo.class, "select * from " + DqTableInfo.TableName + " where table_id" + "=?", dq_table_info.getTable_id()).orElseThrow(() -> new BusinessException("获取UDL数据表信息的sql失败!"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dti", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DtabRelationStore getUDLTableSpecifyStorageRelationship(long dsl_id, DqTableInfo dti) {
        return Dbo.queryOneObject(DtabRelationStore.class, "SELECT dtrs.* from " + DataStoreLayer.TableName + " dsl" + " left join " + DtabRelationStore.TableName + " dtrs on dsl.dsl_id=dtrs.dsl_id" + " left join " + DqTableInfo.TableName + " dti on dti.table_id=dtrs.tab_id" + " where dsl.dsl_id=? and dti.table_id=? and dtrs.data_source in (?)", dsl_id, dti.getTable_id(), StoreLayerDataSource.UD.getCode()).orElseThrow(() -> (new BusinessException("根据存储层id,表id和 存储层关系-数据来源 获取数据表的存储关系信息的SQL出错!")));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsr", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<DtabRelationStore> getUDLTableStorageRelationships(DqTableInfo dti) {
        return Dbo.queryList(DtabRelationStore.class, "SELECT dtrs.* from " + DataStoreLayer.TableName + " dsl" + " left join " + DtabRelationStore.TableName + " dtrs on dsl.dsl_id=dtrs.dsl_id" + " left join " + DqTableInfo.TableName + " dti on dti.table_id=dtrs.tab_id" + " where dti.table_id=? and dtrs.data_source in (?)", dti.getTable_id(), StoreLayerDataSource.UD.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void deleteDCLDataStoreRegInfo(String file_id) {
        DataStoreReg dsr = new DataStoreReg();
        dsr.setFile_id(file_id);
        int execute = Dbo.execute("DELETE FROM " + DataStoreReg.TableName + " WHERE file_id =?", dsr.getFile_id());
        if (execute != 1) {
            throw new BusinessException("删除采集表登记信息失败! file_id=" + file_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "record_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void deleteDQCDqIndex3record(String record_id) {
        DqIndex3record dq_index3record = new DqIndex3record();
        dq_index3record.setRecord_id(Long.parseLong(record_id));
        DboExecute.deletesOrThrow("删除DQC登记信息失败!", "DELETE FROM " + DqIndex3record.TableName + " WHERE record_id=?", dq_index3record.getRecord_id());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void deleteUDLDqTableInfo(long table_id) {
        DboExecute.deletesOrThrow("删除UDL登记信息失败!", "DELETE FROM " + DqTableInfo.TableName + " WHERE table_id=?", table_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dti", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DtabRelationStore getKFKTableSpecifyStorageRelationship(long dsl_id, DqTableInfo dti) {
        return Dbo.queryOneObject(DtabRelationStore.class, "SELECT dtrs.* from " + DataStoreLayer.TableName + " dsl" + " left join " + DtabRelationStore.TableName + " dtrs on dsl.dsl_id=dtrs.dsl_id" + " left join " + DqTableInfo.TableName + " dti on dti.table_id=dtrs.tab_id" + " where dsl.dsl_id=? and dti.table_id=? and dtrs.data_source in (?)", dsl_id, dti.getTable_id(), StoreLayerDataSource.SD.getCode()).orElseThrow(() -> (new BusinessException("根据存储层id,表id和 存储层关系-数据来源 获取数据表的存储关系信息的SQL出错!")));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void deleteKFKTableSpecifyStorageRelationship(long table_id) {
        Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", table_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsr", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<DtabRelationStore> getKFKTableStorageRelationships(DqTableInfo dti) {
        return Dbo.queryList(DtabRelationStore.class, "SELECT dtrs.* from " + DataStoreLayer.TableName + " dsl" + " left join " + DtabRelationStore.TableName + " dtrs on dsl.dsl_id=dtrs.dsl_id" + " left join " + DqTableInfo.TableName + " dti on dti.table_id=dtrs.tab_id" + " where dti.table_id=? and dtrs.data_source in (?)", dti.getTable_id(), StoreLayerDataSource.SD.getCode());
    }
}
