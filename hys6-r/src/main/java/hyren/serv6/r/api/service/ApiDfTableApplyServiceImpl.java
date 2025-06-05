package hyren.serv6.r.api.service;

import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DfTableApply;
import hyren.serv6.base.exception.BusinessException;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class ApiDfTableApplyServiceImpl {

    public Result getDfTableApplyList(String sql, Object... params) {
        return Dbo.queryResult(Dbo.db(), sql, params);
    }

    public Optional<DfTableApply> getTableId(String sql, Object... params) {
        return Dbo.queryOneObject(DfTableApply.class, sql, params);
    }

    public DfTableApply getDfTableApplyById(Long tableId, Long dfPid) {
        return this.getTableId("select * from " + DfTableApply.TableName + " WHERE table_id=? and df_pid=?", tableId, dfPid).orElse(new DfTableApply());
    }

    public DfTableApply getDfTableApplyByApiId(Long apply_tab_id) {
        return this.getTableId("select * from " + DfTableApply.TableName + " WHERE apply_tab_id=?", apply_tab_id).orElseThrow(() -> new BusinessException("未查询到DfTableApply信息"));
    }

    public List<Map<String, String>> arrayToObj(List<List<String>> data) {
        List<Map<String, String>> result = new ArrayList<>();
        if (data.isEmpty()) {
            throw new BusinessException("数据有误，请检查");
        }
        List<String> column = data.get(0);
        for (int j = 0; j < 1; j++) {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < column.size(); i++) {
                map.put(column.get(i), null);
            }
            result.add(map);
        }
        return result;
    }
}
