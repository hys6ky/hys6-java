package hyren.serv6.commons.sqlanalysis;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.utils.DruidParseQuerySql;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.*;

@Slf4j
@Api(tags = "")
@RestController()
@RequestMapping("/sqlanalysis")
@Configuration
public class AnalysisAction {

    @ApiImplicitParams({ @ApiImplicitParam(name = "sql", value = "", example = ""), @ApiImplicitParam(name = "dbtype", value = "", example = "") })
    @ApiResponse(code = 200, message = "")
    @PostMapping("/analysisSqlData")
    public Map<String, Object> analysisSqlData(String sql, String dbtype) {
        Map<String, Object> tableMap = DruidParseQuerySql.analysisTableRelation(sql, dbtype);
        Map<String, Object> jsonObject = JsonUtil.toObject(JsonUtil.toJson(tableMap), new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> targetTableField = (Map<String, Object>) jsonObject.get("targetTableField");
        if (targetTableField != null) {
            targetTableField.forEach((k, v) -> {
                Set<Object> set = new HashSet<>((Collection) targetTableField.get(k));
                targetTableField.put(k, set);
            });
        }
        tableMap.put("condition", DruidParseQuerySql.getSqlConditions(sql, dbtype));
        tableMap.put("relation", DruidParseQuerySql.getRelationships(sql, dbtype));
        Map<String, List<String>> tableColumns = DruidParseQuerySql.getTableColumns(sql, dbtype);
        tableColumns.remove(tableMap.get("tableName").toString());
        tableMap.put("tableColumn", tableColumns);
        return tableMap;
    }
}
