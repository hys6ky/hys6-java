package hyren.serv6.f.dataRegister.sourceList;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class SourceListService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getSourceInfoList(Integer pageNum, Integer pageSize) {
        Map<String, Object> result = new HashMap<>();
        Page page = new DefaultPageImpl(pageNum, pageSize);
        List<Map<String, Object>> list = Dbo.queryPagedList(page, "select ds.* " + "from " + DataSource.TableName + " ds left join " + SourceRelationDep.TableName + " srd " + "on ds.source_id = srd.source_id " + "left join " + DatabaseSet.TableName + " dbs " + "on ds.source_id = dbs.source_id  where srd.dep_id = ? " + "group by ds.source_id order by ds.datasource_name", UserUtil.getUser().getDepId());
        list.forEach(action -> {
            String sourceId = action.get("source_id").toString();
            Map<String, Object> map = Dbo.queryOneObject("select count(*) as tasknum from " + DatabaseSet.TableName + " where source_id = ? and collect_type = ? AND is_sendok = ?", Long.parseLong(sourceId), CollectType.TieYuanDengJi.getCode(), IsFlag.Shi.getCode());
            action.putAll(map);
        });
        result.put("datasourceList", list);
        result.put("totalSize", page.getTotalSize());
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Param(name = "agentId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getTaskInfo(long sourceId) {
        String sqlStr = " SELECT DATABASE_ID ID,task_name task_name," + " source_id source_id,collect_type" + " FROM " + DatabaseSet.TableName + " where is_sendok = ?  AND collect_type = ? AND source_id = ? ORDER BY task_name";
        return Dbo.queryResult(sqlStr, IsFlag.Shi.getCode(), CollectType.TieYuanDengJi.getCode(), sourceId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectSetId", desc = "", range = "")
    public void deleteDBTask(long collectSetId) {
        long val = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + DatabaseSet.TableName + " dbs" + " on ds.source_id = dbs.source_id " + " where dbs.database_id = ?", collectSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("要删除的数据库直连采集任务不存在");
        }
        DboExecute.deletesOrThrow("删除数据库直连采集任务异常!", "delete from " + DatabaseSet.TableName + " where database_id = ? ", collectSetId);
        List<Object> tableIds = Dbo.queryOneColumnList("select table_id from " + TableInfo.TableName + " where database_id = ?", collectSetId);
        if (!tableIds.isEmpty()) {
            for (Object tableId : tableIds) {
                deleteDirtyDataOfTb((long) tableId);
            }
        }
        Dbo.execute("delete from " + TableInfo.TableName + " where database_id = ? ", collectSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    private void deleteDirtyDataOfTb(long tableId) {
        List<Object> columnIds = Dbo.queryOneColumnList("select column_id from " + TableColumn.TableName + " WHERE table_id = ?", tableId);
        if (!columnIds.isEmpty()) {
            for (Object columnId : columnIds) {
                deleteDirtyDataOfCol((long) columnId);
            }
        }
        Dbo.execute(" DELETE FROM " + TableColumn.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + DataExtractionDef.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + DtabRelationStore.TableName + " WHERE tab_id = " + "(SELECT storage_id FROM " + TableStorageInfo.TableName + " WHERE table_id = ?) AND data_source = ? ", tableId, StoreLayerDataSource.DB.getCode());
        Dbo.execute(" DELETE FROM " + TableStorageInfo.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + ColumnMerge.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + TableClean.TableName + " WHERE table_id = ? ", tableId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    private void deleteDirtyDataOfCol(long columnId) {
        Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ? AND data_source = ?", columnId, StoreLayerDataSource.DB.getCode());
        Dbo.execute("delete from " + ColumnClean.TableName + " where column_id = ?", columnId);
        Dbo.execute("delete from " + ColumnSplit.TableName + " where column_id = ?", columnId);
    }
}
