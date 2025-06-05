package hyren.serv6.h.websqlquery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class WebsqlqueryServiceImpl {

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTableColumnInfoBySql(String sql) {
        Validator.notBlank(sql, "解析sql不能为空");
        List<Map<String, Object>> tableColumnInfos = new ArrayList<>();
        List<String> table_name_s = new ArrayList<>();
        try {
            table_name_s = DruidParseQuerySql.parseSqlTableToList(sql);
        } catch (Exception e) {
            log.error("请输入合法的sql! " + sql);
        }
        if (!table_name_s.isEmpty()) {
            table_name_s.forEach(table_name -> {
                Map<String, Object> map = new HashMap<>();
                map.put("table_name", table_name);
                map.put("column_info", DataTableUtil.getColumnByTableName(Dbo.db(), table_name));
                tableColumnInfos.add(map);
            });
        }
        return tableColumnInfos;
    }
}
