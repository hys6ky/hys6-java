package hyren.serv6.q.websqlquery;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@Api("WebSql查询处理类")
@RequestMapping("/sqlConsole/websqlquery")
@Validated
public class WebsqlqueryController {

    @Autowired
    WebsqlqueryServiceImpl websqlqueryService;

    @RequestMapping("/getTableInfoByTableName_cache")
    @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class)
    public Object getTableInfoByTableName_cache(String table_name) {
        return websqlqueryService.getTableInfoByTableName_cache(table_name);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/queryDataBasedOnTableName")
    @ApiImplicitParams({ @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "begin", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "end", value = "", dataTypeClass = Integer.class) })
    public List<Map<String, Object>> queryDataBasedOnTableName(String table_name, @RequestParam(defaultValue = "1") int begin, @RequestParam(defaultValue = "10") int end) {
        String sql = "select * from " + table_name;
        List<Map<String, Object>> query_list = new ArrayList<>();
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            new ProcessingData() {

                @Override
                public void dealLine(Map<String, Object> map) {
                    query_list.add(map);
                }
            }.getPageDataLayer(sql, db, begin, end);
        }
        return query_list;
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/queryDataBasedOnSql")
    @ApiImplicitParams({ @ApiImplicitParam(name = "querySQL", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "begin", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "end", value = "", dataTypeClass = Integer.class) })
    public List<Map<String, Object>> queryDataBasedOnSql(String querySQL, @RequestParam(defaultValue = "1") int begin, @RequestParam(defaultValue = "10") int end) {
        return websqlqueryService.queryDataBasedOnSql(querySQL, begin, end);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getWebSQLTreeData")
    public List<Node> getWebSQLTreeData() {
        return websqlqueryService.getWebSQLTreeData();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getAllTableNameByPlatform")
    public List<String> getAllTableNameByPlatform() {
        return DataTableUtil.getAllTableNameByPlatform(Dbo.db());
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getColumnsByTableName")
    @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class)
    public List<Map<String, Object>> getColumnsByTableName(String table_name) {
        Validator.notBlank(table_name, "查询表名不能为空!");
        return DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getTableColumnInfoBySql")
    @ApiImplicitParam(name = "sql", value = "", dataTypeClass = String.class)
    public Object getTableColumnInfoBySql(String sql) {
        return websqlqueryService.getTableColumnInfoBySql(sql);
    }
}
