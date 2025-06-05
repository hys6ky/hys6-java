package hyren.serv6.r.synch.service.impl;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.User;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.ProjectInfo;
import hyren.serv6.r.synch.service.SyncService;
import hyren.serv6.r.util.CreateTable;
import hyren.serv6.r.util.TempTableConf;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static hyren.daos.bizpot.commons.Dbo.queryOneObject;

@Service
public class SyncServiceImpl implements SyncService {

    private static final Logger log = LoggerFactory.getLogger(SyncServiceImpl.class);

    private static final String TEMP = "temp_";

    @Override
    public Boolean sync(Long applyTabId) {
        DfTableApply tableApply = getTableApply(applyTabId);
        if (tableApply == null) {
            throw new BusinessException("未找到申请表信息");
        }
        if (IsFlag.Shi.getCode().equals(tableApply.getIs_sync())) {
            throw new BusinessException("该数据表已同步");
        }
        Long dslId = null;
        try {
            dslId = getDslIdByProId(tableApply.getDf_pid());
            if (dslId == null) {
                throw new BusinessException("未找到存储库");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("未找到存储库");
        }
        try (DatabaseWrapper dw = ConnectionTool.getDBWrapper(Dbo.db(), dslId)) {
            TableStorageInfo tableInfo = getTableStorageInfoById(tableApply.getTable_id());
            if (tableInfo == null) {
                throw new BusinessException("未找到操作表");
            }
            List<DfTableColumn> primaryColumnList = this.getTempTablePrimaryColumnByApplyId(applyTabId);
            List<DfTableColumn> noPrimaryColumnList = this.getTempTableNoPrimaryColumnByApplyId(applyTabId);
            if (CollectionUtils.isEmpty(primaryColumnList) && CollectionUtils.isEmpty(noPrimaryColumnList)) {
                throw new BusinessException("未找到临时表字段");
            }
            LayerBean layerBean = SqlOperator.queryOneObject(Dbo.db(), LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dslId).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
            this.backupTableData(dw, layerBean, tableInfo);
            Boolean b = this.sync(dw, tableInfo.getHyren_name(), tableApply.getDsl_table_name_id(), primaryColumnList, noPrimaryColumnList);
            if (!b) {
                throw new BusinessException("同步失败");
            }
            String updateApplyTable = "UPDATE " + DfTableApply.TableName + " SET sync_date='" + DateUtil.getSysDate() + "',sync_time = '" + DateUtil.getSysTime() + "', is_sync = '" + IsFlag.Shi.getCode() + "' WHERE apply_tab_id = ?";
            int execute = SqlOperator.execute(Dbo.db(), updateApplyTable, applyTabId);
            if (execute != 1) {
                throw new BusinessException("同步状态失败");
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void backupTableData(DatabaseWrapper dw, LayerBean layerBean, TableStorageInfo tableInfo) {
        String backupTableName = TEMP + tableInfo.getHyren_name().toLowerCase();
        if (dw.isExistTable(backupTableName)) {
            String deleteStr = " drop table " + backupTableName;
            dw.ExecDDL(deleteStr);
            DqTableInfo dqTableInfo = queryOneObject(Dbo.db(), DqTableInfo.class, "select * from " + DqTableInfo.TableName + " where table_name = ?", backupTableName).get();
            List<DfTableColumn> dfTableColumns = SqlOperator.queryList(Dbo.db(), DfTableColumn.class, "select * from " + DfTableColumn.TableName + " where apply_tab_id = ?", dqTableInfo.getTable_id());
            try {
                CreateTable.createDataTableByStorageLayer(dw, layerBean, dqTableInfo, dfTableColumns);
                String selectColName = dfTableColumns.stream().map(column -> (column.getCol_name())).collect(Collectors.joining(","));
                String sqlStr = "insert into " + backupTableName + "(" + selectColName + ")" + " select " + selectColName + " from " + tableInfo.getHyren_name();
                int execute = SqlOperator.execute(dw, sqlStr);
                if (execute < 0) {
                    throw new BusinessException("备份表数据执行失败");
                }
            } catch (Exception e) {
                log.info("关系型数据库创建表时或保存临时表数据时发生异常,回滚此次存储层的db操作!");
                dw.execute(" drop table " + backupTableName);
                e.printStackTrace();
                throw new BusinessException("创建存储层数表或者保存临时表数据时发生异常!" + e.getMessage());
            }
        } else {
            DqTableInfo dqTableInfo = new DqTableInfo();
            dqTableInfo.setTable_id(PrimayKeyGener.getNextId());
            dqTableInfo.setTable_name(backupTableName);
            dqTableInfo.setCreate_date(DateUtil.getSysDate());
            dqTableInfo.setEnd_date(Constant.MAXDATE);
            User user = UserUtil.getUser();
            dqTableInfo.setCreate_id(user.getUserId());
            dqTableInfo.setIs_trace(IsFlag.Fou.getCode());
            dqTableInfo.setTable_space("");
            dqTableInfo.add(Dbo.db());
            List<Map<String, Object>> query_list = Dbo.queryList("SELECT " + " tc.table_id, " + " tc.column_name, " + " tc.column_ch_name," + " tc.is_primary_key," + " tsm.column_tar_type as column_type " + " FROM " + " table_column tc " + " left JOIN tbcol_srctgt_map tsm ON tc.column_id = tsm.column_id " + " where " + " tc.table_id = ? ", tableInfo.getTable_id());
            List<DfTableColumn> dfTableColumns = new ArrayList<DfTableColumn>();
            query_list.forEach(map -> {
                DfTableColumn dfTableColumn = new DfTableColumn();
                String column_type = StringUtil.EMPTY;
                if (map.get("column_type") == null) {
                    Map<String, Object> column_name = queryOneObject("select column_type from " + TableColumn.TableName + " where" + " table_id = ? and column_name = ?", tableInfo.getTable_id(), map.get("column_name"));
                    column_type = column_name.get("column_type").toString();
                } else {
                    column_type = map.get("column_type").toString();
                }
                dfTableColumn.setApply_col_id(PrimayKeyGener.getNextId());
                dfTableColumn.setApply_tab_id(dqTableInfo.getTable_id());
                dfTableColumn.setCol_name((String) map.get("column_name"));
                dfTableColumn.setCol_ch_name((String) map.get("column_ch_name"));
                dfTableColumn.setCol_type(column_type);
                dfTableColumn.setIs_primarykey((String) map.get("is_primary_key"));
                dfTableColumns.add(dfTableColumn);
            });
            dfTableColumns.forEach(dfColumn -> dfColumn.add(Dbo.db()));
            try {
                CreateTable.createDataTableByStorageLayer(dw, layerBean, dqTableInfo, dfTableColumns);
                String selectColName = dfTableColumns.stream().map(column -> (column.getCol_name())).collect(Collectors.joining(","));
                String sqlStr = "insert into " + backupTableName + "(" + selectColName + ")" + " select " + selectColName + " from " + tableInfo.getHyren_name();
                int execute = SqlOperator.execute(dw, sqlStr);
                if (execute < 0) {
                    throw new BusinessException("备份表数据执行失败");
                }
            } catch (Exception e) {
                log.info("关系型数据库创建表时或保存临时表数据时发生异常,回滚此次存储层的db操作!");
                dw.execute(" drop table " + backupTableName);
                e.printStackTrace();
                throw new BusinessException("创建存储层数表或者保存临时表数据时发生异常!" + e.getMessage());
            }
        }
    }

    private List<List<String>> objToArray(List<Map<String, Object>> data) {
        if (data.isEmpty() || data == null) {
            throw new BusinessException("数据有误，请检查");
        }
        List<List<String>> resultList = new ArrayList<List<String>>();
        List<String> colNameList = new ArrayList<String>(data.get(0).keySet());
        resultList.add(colNameList);
        data.forEach(map -> {
            List<String> valList = new ArrayList<String>();
            colNameList.forEach(col -> {
                if (ObjectUtils.isEmpty(map.get(col))) {
                    valList.add("");
                } else {
                    valList.add((String) map.get(col));
                }
            });
            resultList.add(valList);
        });
        return resultList;
    }

    private Boolean sync(DatabaseWrapper dw, String tableName, String tempTableName, List<DfTableColumn> primaryColumnList, List<DfTableColumn> noPrimaryColumnList) {
        if (CollectionUtils.isEmpty(primaryColumnList)) {
            primaryColumnList = noPrimaryColumnList;
            noPrimaryColumnList = new ArrayList<>();
        }
        String onSelectString = primaryColumnList.stream().map(t -> ("t1." + t.getCol_name() + "=t2." + t.getCol_name())).collect(Collectors.joining(" and "));
        String onWhereString = primaryColumnList.stream().map(t -> (tempTableName + "." + t.getCol_name() + " = t1." + t.getCol_name())).collect(Collectors.joining(" and "));
        String innerJoinSql = "select 1 from " + tempTableName + " t1 inner join " + tableName + " t2 on " + onSelectString + " where " + onWhereString;
        String tempUpdateSql = "update " + tempTableName + " set " + TempTableConf.OPERATION_COLUMN_NAME + "=" + TempTableConf.TempTableOperation.update.getCode() + " where exists( " + innerJoinSql + " )";
        String tempInsertSql = "update " + tempTableName + " set " + TempTableConf.OPERATION_COLUMN_NAME + "=" + TempTableConf.TempTableOperation.insert.getCode() + " where not exists( " + innerJoinSql + " )";
        SqlOperator.execute(dw, tempUpdateSql, new Object[0]);
        SqlOperator.execute(dw, tempInsertSql, new Object[0]);
        dw.commit();
        if (!CollectionUtils.isEmpty(noPrimaryColumnList)) {
            String deleteWhereStr = primaryColumnList.stream().map(t -> (t.getCol_name() + " in ( select " + t.getCol_name() + " from " + tempTableName + " where " + TempTableConf.OPERATION_COLUMN_NAME + "= '" + TempTableConf.TempTableOperation.update.getCode() + "' )")).collect(Collectors.joining(" and "));
            String deleteData = "delete from " + tableName + " where " + deleteWhereStr;
            SqlOperator.execute(dw, deleteData, new Object[0]);
            dw.commit();
            String insertCols = Stream.concat(primaryColumnList.stream(), noPrimaryColumnList.stream()).map(t -> t.getCol_name()).collect(Collectors.joining(","));
            String insertVals = "select " + insertCols + " from " + tempTableName + " where " + TempTableConf.OPERATION_COLUMN_NAME + "='" + TempTableConf.TempTableOperation.update.getCode() + "'";
            String insertSql = "insert into " + tableName + " ( " + insertCols + " ) " + insertVals;
            SqlOperator.execute(dw, insertSql, new Object[0]);
        }
        String insertInto = Stream.concat(primaryColumnList.stream(), noPrimaryColumnList.stream()).map(t -> t.getCol_name()).collect(Collectors.joining(","));
        String selectString = Stream.concat(primaryColumnList.stream(), noPrimaryColumnList.stream()).map(t -> ("tt." + t.getCol_name())).collect(Collectors.joining(","));
        String insertSql = "INSERT INTO " + tableName + " ( " + insertInto + " ) " + " (SELECT " + selectString + " FROM " + tempTableName + " tt where tt." + TempTableConf.OPERATION_COLUMN_NAME + "='" + TempTableConf.TempTableOperation.insert.getCode() + "' )";
        SqlOperator.execute(dw, insertSql, new Object[0]);
        dw.commit();
        return true;
    }

    @Override
    public Boolean rollback(Long applyTabId) {
        DfTableApply tableApply = getTableApply(applyTabId);
        if (tableApply == null) {
            throw new BusinessException("未找到申请表信息");
        }
        if (IsFlag.Fou.getCode().equals(tableApply.getIs_sync())) {
            throw new BusinessException("该数据表已同步");
        }
        Long dslId = null;
        try {
            dslId = getDslIdByProId(tableApply.getDf_pid());
            if (dslId == null) {
                throw new BusinessException("未找到存储库");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("未找到存储库");
        }
        try (DatabaseWrapper dw = ConnectionTool.getDBWrapper(Dbo.db(), dslId)) {
            TableStorageInfo tableInfo = getTableStorageInfoById(tableApply.getTable_id());
            if (tableInfo == null) {
                throw new BusinessException("未找到操作表");
            }
            List<DfTableColumn> primaryColumnList = this.getTempTablePrimaryColumnByApplyId(applyTabId);
            List<DfTableColumn> noPrimaryColumnList = this.getTempTableNoPrimaryColumnByApplyId(applyTabId);
            if (CollectionUtils.isEmpty(primaryColumnList) && CollectionUtils.isEmpty(noPrimaryColumnList)) {
                throw new BusinessException("未找到临时表字段");
            }
            Boolean b = this.rollback(dw, tableInfo);
            dw.commit();
            if (!b) {
                throw new BusinessException("回滚失败");
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public PageDTO<ProjectInfo> page(List<String> stateList, List<String> noStateList, String fuzzyName, String dfType, List<String> appStateList, List<String> noAppStateList, Integer startDate, Integer endDate, Page page) {
        List<Object> params = new ArrayList<>();
        SqlOperator.Assembler dataSql = SqlOperator.Assembler.newInstance();
        dataSql.addSql("SELECT df_pid, pro_name, df_type, user_id, submit_user, submit_date, submit_time, submit_state, dsl_id, df_remarks, (SELECT count(1) FROM " + DfTableApply.TableName + " tab WHERE tab.df_pid = pro.df_pid) as \"table_count\" FROM " + DfProInfo.TableName + " pro ");
        boolean hasWhere = false;
        boolean needAnd = false;
        if (!CollectionUtils.isEmpty(stateList)) {
            String strC = stateList.stream().map(t -> "?").collect(Collectors.joining(","));
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" submit_state in(" + strC + ") ");
            stateList.stream().forEach((Object t) -> dataSql.addParam(t));
        }
        if (!CollectionUtils.isEmpty(noStateList)) {
            String strC = noStateList.stream().map(t -> "?").collect(Collectors.joining(","));
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" submit_state not in(" + strC + ") ");
            noStateList.stream().forEach((Object t) -> dataSql.addParam(t));
        }
        if (StringUtil.isNotBlank(fuzzyName)) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" pro_name like ? ");
            dataSql.addParam("%" + fuzzyName + "%");
        }
        if (StringUtil.isNotBlank(dfType)) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" df_type = ? ");
            dataSql.addParam(dfType);
        }
        if (!CollectionUtils.isEmpty(appStateList)) {
            String strC = appStateList.stream().map(t -> "?").collect(Collectors.joining(","));
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" submit_state in (" + strC + ")");
            appStateList.stream().forEach((Object t) -> dataSql.addParam(t));
        }
        if (!CollectionUtils.isEmpty(noAppStateList)) {
            String strC = noAppStateList.stream().map(t -> "?").collect(Collectors.joining(","));
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" submit_state not in (" + strC + ")");
            noAppStateList.stream().forEach((Object t) -> dataSql.addParam(t));
        }
        if (startDate != null) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" submit_date::integer >= ? ");
            dataSql.addParam(startDate);
        }
        if (endDate != null) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" submit_date::integer < ? ");
            dataSql.addParam(endDate);
        }
        try {
            List<ProjectInfo> queryPagedList = SqlOperator.queryPagedList(Dbo.db(), ProjectInfo.class, page, dataSql.sql(), dataSql.params().toArray());
            return new PageDTO<ProjectInfo>(queryPagedList, page.getTotalSize());
        } catch (Exception e) {
            throw new BusinessException("查询失败");
        }
    }

    private Boolean rollback(DatabaseWrapper dw, TableStorageInfo tableInfo) {
        try {
            String deleteStr = "delete from " + tableInfo.getHyren_name();
            int execute = dw.execute(deleteStr);
            if (execute < 0) {
                throw new BusinessException("回滚表数据执行失败");
            }
            String tableName = TEMP + tableInfo.getHyren_name().toLowerCase();
            DqTableInfo dqTableInfo = queryOneObject(Dbo.db(), DqTableInfo.class, "select * from " + DqTableInfo.TableName + " where table_name = ?", tableName).get();
            List<DfTableColumn> dfTableColumns = SqlOperator.queryList(Dbo.db(), DfTableColumn.class, "select * from " + DfTableColumn.TableName + " where apply_tab_id = ?", dqTableInfo.getTable_id());
            String selectColName = dfTableColumns.stream().map(column -> (column.getCol_name())).collect(Collectors.joining(","));
            String sqlStr = "insert into " + tableInfo.getHyren_name() + "(" + selectColName + ")" + " select " + selectColName + " from " + tableName;
            int execute1 = SqlOperator.execute(dw, sqlStr);
            if (execute1 < 0) {
                throw new BusinessException("回滚表数据执行失败");
            }
        } catch (Exception e) {
            dw.rollback();
        }
        return true;
    }

    private List<DfTableColumn> getTempTablePrimaryColumnByApplyId(Long applyId) {
        String sql = "SELECT * FROM " + DfTableColumn.TableName + " where is_primarykey = '" + IsFlag.Shi.getCode() + "' and apply_tab_id = ? ";
        return SqlOperator.queryList(Dbo.db(), DfTableColumn.class, sql, applyId);
    }

    private List<DfTableColumn> getTempTableNoPrimaryColumnByApplyId(Long applyId) {
        String sql = "SELECT * FROM " + DfTableColumn.TableName + " where is_primarykey = '" + IsFlag.Fou.getCode() + "' and col_name !='" + TempTableConf.OPERATION_COLUMN_NAME.toLowerCase() + "' and apply_tab_id = ? ";
        return SqlOperator.queryList(Dbo.db(), DfTableColumn.class, sql, applyId);
    }

    private TableStorageInfo getTableStorageInfoById(Long tableId) {
        String sql = "SELECT tsi.storage_id,tsi.file_format,tsi.storage_type,tsi.is_zipper,tsi.storage_time,dsr.original_name AS hyren_name,\n" + " tsi.is_prefix,tsi.table_id,tsi.is_md5 FROM table_storage_info tsi\n" + " JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + " JOIN database_set dbs ON dsr.database_id = dbs.database_id \n" + " WHERE tsi.table_id = ? AND dbs.collect_type = '1' \n" + " AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' ) UNION\n" + " SELECT tsi.storage_id,tsi.file_format,tsi.storage_type,tsi.is_zipper,tsi.storage_time,tsi.hyren_name,tsi.is_prefix,\n" + " tsi.table_id,tsi.is_md5 FROM table_storage_info tsi\n" + " JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + " JOIN database_set dbs ON dsr.database_id = dbs.database_id \n" + " WHERE tsi.table_id = ? AND dbs.collect_type != '1' AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' )";
        Optional<TableStorageInfo> optional = SqlOperator.queryOneObject(Dbo.db(), TableStorageInfo.class, sql, tableId, tableId);
        return optional.get();
    }

    private DfTableApply getTableApply(Long applyTabId) {
        String sql = "SELECT apply_tab_id, table_id, df_pid, dep_id, create_user_id, create_date, create_time, update_date, update_time, dta_remarks, dsl_table_name_id, is_sync, is_rec " + " FROM " + DfTableApply.TableName + " where apply_tab_id = ?";
        Optional<DfTableApply> optional = SqlOperator.queryOneObject(Dbo.db(), DfTableApply.class, sql, applyTabId);
        return optional.get();
    }

    private Long getDslIdByProId(Long df_pid) {
        String proSql = "SELECT df_pid, pro_name, df_type, user_id, submit_user, submit_date, submit_time, submit_state, dsl_id, df_remarks " + " FROM " + DfProInfo.TableName + " where df_pid = ?";
        Optional<DfProInfo> proOptional = SqlOperator.queryOneObject(Dbo.db(), DfProInfo.class, proSql, df_pid);
        if (proOptional == null || proOptional.get() == null) {
            return null;
        }
        Long dsl_id = proOptional.get().getDsl_id();
        String dslSql = "SELECT dsl_id, dsl_name, store_type, is_hadoopclient, dsl_remark, database_name " + " FROM " + DataStoreLayer.TableName + " where dsl_id = ? ";
        Optional<DataStoreLayer> dslOptional = SqlOperator.queryOneObject(Dbo.db(), DataStoreLayer.class, dslSql, dsl_id);
        return dslOptional.get().getDsl_id();
    }
}
