package hyren.serv6.r.api.service;

import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.base.entity.DfTableColumn;
import hyren.serv6.base.entity.TableColumn;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class ApiDfProInfoApiServiceImpl {

    public Result searchDfProInfoApi(String sql, Object... params) {
        return Dbo.queryResult(sql, params);
    }

    public List<Map<String, Object>> queryDataBasedOnTableId(Long tableId) {
        return Dbo.queryList("SELECT column_name as dda_col,column_type as data_type,column_ch_name,is_primary_key as is_primarykey,tc_remark as dda_remarks FROM " + TableColumn.TableName + " WHERE table_id=? ", tableId);
    }

    public List<Map<String, Object>> queryPrimaryKeyOnTableName(Long applyTabId) {
        return Dbo.queryList("SELECT col_name as dda_col FROM " + DfTableColumn.TableName + " WHERE is_primarykey=?  and apply_tab_id =?", IsFlag.Shi.getCode(), applyTabId);
    }

    public Map<String, Object> queryTableByTableId(String sql, Object... params) {
        return Dbo.queryResult(sql, params).toList().get(0);
    }

    public Map<String, Object> queryDfTableApplyById(String sql, Object... params) {
        return Dbo.queryOneObject(sql, params);
    }

    public Optional<DfProInfo> queryDfProInfoById(String sql, Object... params) {
        return Dbo.queryOneObject(DfProInfo.class, sql, params);
    }

    public Result getDfProInfoListByDfType(String sql, Object... params) {
        Object[] filteredParams = Arrays.stream(params).filter(Objects::nonNull).toArray();
        return Dbo.queryResult(sql, filteredParams);
    }
}
