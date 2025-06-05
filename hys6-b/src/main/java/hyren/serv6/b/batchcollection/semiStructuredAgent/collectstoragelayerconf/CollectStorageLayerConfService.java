package hyren.serv6.b.batchcollection.semiStructuredAgent.collectstoragelayerconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.bean.ColStoParam;
import hyren.serv6.b.agent.bean.DataStoRelaParam;
import hyren.serv6.b.agent.tools.CommonUtils;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@DocClass(desc = "", author = "dhw", createdate = "2020/6/12 18:09")
public class CollectStorageLayerConfService {

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCollectStorageLayerInfo(long odc_id) {
        CommonUtils.isObjectCollectExist(odc_id);
        Result result = Dbo.queryResult("select * from " + ObjectCollectTask.TableName + " where odc_id=?", odc_id);
        Result collectStorageLayerInfo = Dbo.queryResult("select * from " + ObjectCollectTask.TableName + " oct left join " + DtabRelationStore.TableName + " drs on oct.ocs_id=drs.tab_id " + " where oct.odc_id=? and drs.data_source=?", odc_id, StoreLayerDataSource.OBJ.getCode());
        if (!collectStorageLayerInfo.isEmpty()) {
            for (int j = 0; j < result.getRowCount(); j++) {
                long struct_id = result.getLong(j, "ocs_id");
                List<String> dslIds = new ArrayList<>();
                for (int i = 0; i < collectStorageLayerInfo.getRowCount(); i++) {
                    long col_id = collectStorageLayerInfo.getLong(i, "tab_id");
                    if (col_id == struct_id) {
                        String dslId = collectStorageLayerInfo.getString(i, "dsl_id");
                        if (!dslIds.contains(dslId)) {
                            dslIds.add(dslId);
                        }
                    }
                }
                result.setObject(j, "dsl_id", dslIds);
            }
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getStorageLayerDestById(long ocs_id) {
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        return Dbo.queryResult("select * from " + ObjectCollectTask.TableName + " oct left join " + DtabRelationStore.TableName + " drs on oct.ocs_id=drs.tab_id " + " where ocs_id=? and drs.data_source=?", ocs_id, StoreLayerDataSource.OBJ.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getStorageLayerAttrById(long dsl_id) {
        CommonUtils.isDataStoreLayerExist(dsl_id);
        return Dbo.queryResult("select * from " + DataStoreLayerAttr.TableName + " where dsl_id=?", dsl_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getColumnStorageLayerInfo(long dsl_id, long ocs_id) {
        CommonUtils.isDataStoreLayerExist(dsl_id);
        CommonUtils.isObjectCollectTaskExist(ocs_id);
        Result objCollectStructResult = Dbo.queryResult("select ocs_id,struct_id,column_name,data_desc from " + ObjectCollectStruct.TableName + " where ocs_id=?", ocs_id);
        Result columnStorageLayerInfo = Dbo.queryResult("select t1.struct_id,t1.column_name,t1.data_desc,t2.*,t3.dsla_storelayer from " + ObjectCollectStruct.TableName + " t1 left join " + DcolRelationStore.TableName + " t2 on t1.struct_id=t2.col_id left join " + DataStoreLayerAdded.TableName + " t3 on t2.dslad_id=t3.dslad_id " + " where t1.ocs_id=? and t3.dsl_id=? and t2.data_source=?", ocs_id, dsl_id, StoreLayerDataSource.OBJ.getCode());
        List<Map<String, Object>> dslaStorelayerList = Dbo.queryList("select t1.dsla_storelayer,t1.dslad_id from " + DataStoreLayerAdded.TableName + " t1 join " + DataStoreLayer.TableName + " t2 on t1.dsl_id = t2.dsl_id where t2.dsl_id = ?", dsl_id);
        objCollectStructResult.setObject(0, "dslaStorelayerList", dslaStorelayerList);
        if (!columnStorageLayerInfo.isEmpty()) {
            for (int j = 0; j < objCollectStructResult.getRowCount(); j++) {
                long struct_id = objCollectStructResult.getLong(j, "struct_id");
                Long csi_number = null;
                List<String> dsla_storelayers = new ArrayList<>();
                for (int i = 0; i < columnStorageLayerInfo.getRowCount(); i++) {
                    long col_id = columnStorageLayerInfo.getLong(i, "col_id");
                    if (col_id == struct_id) {
                        String dsla_storelayer = columnStorageLayerInfo.getString(i, "dsla_storelayer");
                        if (!dsla_storelayers.contains(dsla_storelayer)) {
                            dsla_storelayers.add(dsla_storelayer);
                        }
                        csi_number = columnStorageLayerInfo.getLong(i, "csi_number");
                    }
                }
                objCollectStructResult.setObject(j, "dsla_storelayer", dsla_storelayers);
                objCollectStructResult.setObject(j, "csi_number", csi_number);
            }
        }
        return objCollectStructResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "colStoParams", desc = "", range = "", isBean = true)
    public void saveColRelationStoreInfo(long ocs_id, ColStoParam[] colStoParams) {
        if (colStoParams != null && colStoParams.length != 0) {
            Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id in " + "(select struct_id from " + ObjectCollectStruct.TableName + " where ocs_id = ?)" + " AND data_source = ?", ocs_id, StoreLayerDataSource.OBJ.getCode());
            for (ColStoParam colStoParam : colStoParams) {
                Long[] dsladIds = colStoParam.getDsladIds();
                Validator.notNull(colStoParam.getColumnId(), "结构信息ID不能为空");
                for (Long dsladId : dsladIds) {
                    Validator.notNull(dsladId, "附加信息ID不能为空");
                    DcolRelationStore dcol_relation_store = new DcolRelationStore();
                    dcol_relation_store.setDslad_id(dsladId);
                    if (colStoParam.getCsiNumber() != null) {
                        dcol_relation_store.setCsi_number(colStoParam.getCsiNumber());
                    }
                    dcol_relation_store.setCol_id(colStoParam.getColumnId());
                    dcol_relation_store.setData_source(StoreLayerDataSource.OBJ.getCode());
                    dcol_relation_store.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objectCollectStructs", desc = "", range = "", isBean = true)
    public void updateColumnZhName(ObjectCollectStruct[] objectCollectStructs) {
        if (objectCollectStructs == null || objectCollectStructs.length == 0) {
            throw new BusinessException("获取字段信息失败");
        }
        for (ObjectCollectStruct objectCollectStruct : objectCollectStructs) {
            DboExecute.updatesOrThrow("更新字段" + objectCollectStruct.getColumn_name() + "的中文名失败", "update " + ObjectCollectStruct.TableName + " set data_desc=? where struct_id=?", objectCollectStruct.getData_desc(), objectCollectStruct.getStruct_id());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ocs_id", desc = "", range = "")
    @Param(name = "dslId", desc = "", range = "")
    private void addDtabRelationStore(long ocs_id, long dslId) {
        DtabRelationStore dtabRelationStore = new DtabRelationStore();
        dtabRelationStore.setData_source(StoreLayerDataSource.OBJ.getCode());
        dtabRelationStore.setIs_successful(JobExecuteState.DengDai.getCode());
        dtabRelationStore.setTab_id(ocs_id);
        dtabRelationStore.setDsl_id(dslId);
        dtabRelationStore.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "dataStoRelaParams", desc = "", range = "", isBean = true)
    public void batchSaveDtabRelationStoreInfo(long odc_id, DataStoRelaParam[] dataStoRelaParams) {
        CommonUtils.isObjectCollectExist(odc_id);
        if (dataStoRelaParams == null || dataStoRelaParams.length == 0) {
            throw new BusinessException("未获取到表存储目的地信息");
        }
        Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id in" + " (select ocs_id from " + ObjectCollectTask.TableName + " where odc_id = ?)" + " AND data_source = ?", odc_id, StoreLayerDataSource.OBJ.getCode());
        for (DataStoRelaParam dataStoRelaParam : dataStoRelaParams) {
            Validator.notNull(dataStoRelaParam.getTableId(), "对象采集任务编号不能为空");
            Long[] dslIds = dataStoRelaParam.getDslIds();
            if (dslIds == null || dslIds.length == 0) {
                throw new BusinessException("ocs_id=" + dataStoRelaParam.getTableId() + "对应表未选择存储层,请检查");
            }
            for (long dslId : dslIds) {
                addDtabRelationStore(dataStoRelaParam.getTableId(), dslId);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objectCollectTasks", desc = "", range = "", isBean = true)
    public void updateTableZhName(ObjectCollectTask[] objectCollectTasks) {
        if (objectCollectTasks == null || objectCollectTasks.length == 0) {
            throw new BusinessException("获取表信息失败");
        }
        for (int i = 0; i < objectCollectTasks.length; i++) {
            ObjectCollectTask object_collect_task = objectCollectTasks[i];
            Validator.notNull(object_collect_task.getOcs_id(), "保存第" + (i + 1) + "张表的任务编号不能为空");
            Validator.notBlank(object_collect_task.getEn_name(), "保存第" + (i + 1) + "张表的表英文名必须填写");
            Validator.notBlank(object_collect_task.getZh_name(), "保存第" + (i + 1) + "张表的表中文名必须填写");
            DboExecute.updatesOrThrow("保存第" + (i + 1) + "张表名称信息失败", "update " + ObjectCollectTask.TableName + " set zh_name = ? " + " where ocs_id = ? and en_name=?", object_collect_task.getZh_name(), object_collect_task.getOcs_id(), object_collect_task.getEn_name());
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchDataStore() {
        return Dbo.queryResult("select * from " + DataStoreLayer.TableName);
    }
}
