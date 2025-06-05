package hyren.serv6.k.dm.metadatamanage.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.k.entity.DqFailureTable;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/7 0007 上午 11:10")
public class DRBDataQuery {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<DqFailureTable> getAllTableInfos() {
        return Dbo.queryList(DqFailureTable.class, "SELECT * FROM " + DqFailureTable.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<DataStoreLayer> getExistTableDataStorageLayers(DataSourceType dataSourceType) {
        return Dbo.queryList(DataStoreLayer.class, "SELECT dsl.* FROM " + DataStoreLayer.TableName + " dsl" + " JOIN " + DqFailureTable.TableName + " dft ON dsl.dsl_id=dft.dsl_id" + " WHERE dft.table_source=? GROUP BY dsl.dsl_id", dataSourceType.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getStorageLayerTableInfos(DataSourceType dataSourceType) {
        return Dbo.queryList("SELECT dsl.*,dft.* FROM " + DqFailureTable.TableName + " dft" + " JOIN " + DataStoreLayer.TableName + " dsl ON dsl.dsl_id = dft.dsl_id" + " WHERE dft.table_source=?", dataSourceType.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getStorageKFKLayerTableInfos(DataSourceType dataSourceType) {
        return Dbo.queryList("SELECT dsl.*, dft.*, da.tab_id,op.ssj_job_id, op.sdm_info_id FROM " + " dq_failure_table dft JOIN data_store_layer dsl ON dsl.dsl_id = dft.dsl_id " + " JOIN sdm_sp_database da ON da.tab_id = dft.file_id" + " JOIN sdm_sp_output op ON op.sdm_info_id = da.sdm_info_id WHERE " + " dft.data_source = ?", StoreLayerDataSource.SD.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "failure_table_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqFailureTable getDRBTableInfo(long failure_table_id) {
        DqFailureTable dft = new DqFailureTable();
        dft.setFailure_table_id(failure_table_id);
        return Dbo.queryOneObject(DqFailureTable.class, "SELECT * FROM " + DqFailureTable.TableName + " WHERE failure_table_id = ?", dft.getFailure_table_id()).orElseThrow(() -> (new BusinessException("根据id获取回收站表信息的SQL失败!")));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void deleteDqFailureTableInfo(long failure_table_id) {
        DqFailureTable dft = new DqFailureTable();
        dft.setFailure_table_id(failure_table_id);
        int execute = Dbo.execute("DELETE FROM " + DqFailureTable.TableName + " WHERE failure_table_id=?", dft.getFailure_table_id());
        if (execute != 1) {
            throw new BusinessException("删除回收站表登记信息失败! failure_table_id=" + failure_table_id);
        }
    }
}
