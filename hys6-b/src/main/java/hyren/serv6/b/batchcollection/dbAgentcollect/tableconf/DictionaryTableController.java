package hyren.serv6.b.batchcollection.dbAgentcollect.tableconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.b.batchcollection.dbAgentcollect.tableconf.req.TableDataReq;
import hyren.serv6.base.entity.TableColumn;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataCollectionO/dbAgentcollect/tableconf")
@Slf4j
@Api("数据文件采集表配置")
@Validated
public class DictionaryTableController {

    @Autowired
    DictionaryTableService dictionaryTableService;

    @RequestMapping("/getTableData")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public Map<String, Object> getTableData(@NotNull Long colSetId) {
        return dictionaryTableService.getTableData(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "database_type", value = "", dataTypeClass = String.class) })
    @RequestMapping("/saveDatabaseType")
    public void saveDatabaseType(@NotNull Long database_id, @NotNull String database_type) {
        dictionaryTableService.saveDatabaseType(database_id, database_type);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "table_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/getTableColumnByTableId")
    public List<TableColumn> getTableColumnByTableId(@NotNull Long colSetId, @NotNull Long table_id) {
        return dictionaryTableService.getTableColumnByTableId(colSetId, table_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "table_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "table_name", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getTableColumnByTableName")
    public List<TableColumn> getTableColumnByTableName(@NotNull Long colSetId, String table_name) {
        return dictionaryTableService.getTableColumnByTableName(colSetId, table_name);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "table_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableColumns", value = "", dataTypeClass = TableColumn[].class) })
    @RequestMapping("/updateColumnByTableId")
    public void updateColumnByTableId(@RequestBody Map<String, Object> req) {
        long table_id = ReqDataUtils.getLongData(req, "table_id");
        String tableColumns = ReqDataUtils.getStringData(req, "tableColumns");
        TableColumn[] tableColumns1 = JsonUtil.toObject(tableColumns, new TypeReference<TableColumn[]>() {
        });
        dictionaryTableService.updateColumnByTableId(table_id, tableColumns1);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableInfos", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "tableColumns", value = "", dataTypeClass = String.class, example = "") })
    @Return(desc = "", range = "")
    @RequestMapping("/saveTableData")
    public long saveTableData(@RequestBody TableDataReq req) {
        return dictionaryTableService.saveTableData(req.getColSetId(), req.getTableInfos(), req.getTableColumns());
    }
}
