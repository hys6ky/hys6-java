package hyren.serv6.b.batchcollection.agent.tableconf;

import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.entity.TableCycle;
import hyren.serv6.base.entity.TableInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RestController
@Api("定义表抽取属性")
@Validated
@RequestMapping("/dataCollectionO/agent/isolate")
public class CollTbConfStepController {

    @Autowired
    CollTbConfStepService collTbConfStepService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableInfoString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "collTbConfParamString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "delTbString", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableCycles", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveCollTbInfo")
    public long saveCollTbInfo(@RequestParam(defaultValue = "") String tableInfoString, @NotNull Long colSetId, @RequestParam(defaultValue = "") String collTbConfParamString, String delTbString, String tableCycles) {
        return collTbConfStepService.saveCollTbInfo(tableInfoString, colSetId, collTbConfParamString, delTbString, tableCycles);
    }

    @RequestMapping("/getInitInfo")
    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public List<Map<String, Object>> getInitInfo(@NotNull Long colSetId) {
        return collTbConfStepService.getInitInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "table_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getTableCycle")
    public Map<String, Object> getTableCycle(@NotNull Long table_id) {
        return Dbo.queryOneObject("SELECT * FROM " + TableCycle.TableName + " WHERE table_id = ?", table_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "inputString", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getTableInfo")
    public List<Map<String, Object>> getTableInfo(@NotNull Long colSetId, @NotNull String inputString) {
        return collTbConfStepService.getTableInfo(colSetId, inputString);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getAllTableInfo")
    public List<Map<String, Object>> getAllTableInfo(@NotNull Long colSetId) {
        return collTbConfStepService.getAllTableInfo(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getPageSQL")
    public String getPageSQL(@NotNull Long tableId) {
        return collTbConfStepService.getPageSQL(tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableInfoArray", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableColumn", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableCycles", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveAllSQL")
    public long saveAllSQL(@RequestBody Map<String, Object> params) {
        String tableInfoArray = String.valueOf(params.get("tableInfoArray"));
        Long colSetId = Long.parseLong(params.get("colSetId").toString());
        String tableColumn = String.valueOf(params.get("tableColumn"));
        String tableCycles = String.valueOf(params.get("tableCycles"));
        return collTbConfStepService.saveAllSQL(tableInfoArray, colSetId, tableColumn, tableCycles);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "unloadType", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sql", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getSqlColumnData")
    public Set<TableColumn> getSqlColumnData(@NotNull Long colSetId, String unloadType, String sql, @RequestParam(defaultValue = "0") Long tableId, String tableName) {
        return collTbConfStepService.getSqlColumnData(colSetId, unloadType, sql, tableId, tableName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableId", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getColumnInfo")
    public Map<String, Object> getColumnInfo(String tableName, @NotNull Long colSetId, @RequestParam(defaultValue = "999999") Long tableId) {
        return collTbConfStepService.getColumnInfo(tableName, colSetId, tableId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pageSql", value = "", dataTypeClass = String.class) })
    @RequestMapping("/testParallelExtraction")
    public void testParallelExtraction(@NotNull Long colSetId, String pageSql) {
        collTbConfStepService.testParallelExtraction(colSetId, pageSql);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sql", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getTableDataCount")
    public long getTableDataCount(@NotNull Long colSetId, @RequestParam(defaultValue = "") String tableName, @RequestParam(defaultValue = "") String sql) {
        return collTbConfStepService.getTableDataCount(colSetId, tableName, sql);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getAllSQLs")
    public String getAllSQLs(@NotNull Long colSetId) {
        return collTbConfStepService.getAllSQLs(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getSingleTableSQL")
    public Result getSingleTableSQL(@NotNull Long colSetId, @NotNull String tableName) {
        return Dbo.queryResult("SELECT * " + " FROM " + TableInfo.TableName + " WHERE database_id = ? AND table_name = ? ", colSetId, tableName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getSQLInfoByColSetId")
    public List<Map<String, Object>> getSQLInfoByColSetId(@NotNull Long colSetId) {
        return collTbConfStepService.getSQLInfoByColSetId(colSetId).toList();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getColumnInfoByColSetId")
    public Map<String, List<Map<String, Object>>> getColumnInfoByColSetId(@NotNull Long colSetId) {
        return collTbConfStepService.getColumnInfoByColSetId(colSetId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "table_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/getTableSetUnloadData")
    public Optional<TableInfo> getTableSetUnloadData(@NotNull Long table_id) {
        return Dbo.queryOneObject(TableInfo.class, "SELECT * FROM " + TableInfo.TableName + " WHERE table_id = ?", table_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableNames", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableIds", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/checkTablePrimary")
    public Map<String, Boolean> checkTablePrimary(@NotNull long colSetId, @NotNull String tableNames, @NotNull String tableIds) {
        String[] tableNamesResult = tableNames.split(",");
        return collTbConfStepService.checkTablePrimary(colSetId, tableNamesResult, tableIds);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "table_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class) })
    @RequestMapping("/deleteTableSql")
    public void deleteTableSql(@NotNull Long database_id, @NotNull Long table_id, @NotNull String table_name) {
        collTbConfStepService.deleteTableSql(database_id, table_id, table_name);
    }
}
