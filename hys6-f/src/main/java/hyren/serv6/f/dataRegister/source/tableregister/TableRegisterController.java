package hyren.serv6.f.dataRegister.source.tableregister;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.entity.TableInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.f.dataRegister.source.tableregister.req.TableDataReq;
import hyren.serv6.f.source.tools.ReqDataUtils;
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
import java.util.Objects;

@RestController
@Slf4j
@RequestMapping("/dataRegister/agent/tableregister")
@Api(value = "", tags = "")
@Validated
public class TableRegisterController {

    @Autowired
    public TableRegisterService registerService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getTableData")
    @ApiImplicitParam(name = "databaseId", value = "", dataTypeClass = Long.class)
    public List<TableDataReq> getTableData(@NotNull Long databaseId) {
        return registerService.getTableData(databaseId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveTableData")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "databaseId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableInfos", value = "", dataTypeClass = TableInfo[].class), @ApiImplicitParam(name = "tableColumns", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class) })
    public void saveTableData(@RequestBody Map<String, Object> req) {
        Object source_id = req.get("source_id");
        Object database_id = req.get("databaseId");
        Object tableColumns = req.get("tableColumns");
        Object dsl_id = req.get("dsl_id");
        Object tableInfos = req.get("tableInfos");
        if (Objects.isNull(tableInfos) || Objects.isNull(tableColumns)) {
            throw new BusinessException("tableInfos is null");
        }
        long sourceId = 0;
        long databaseId = 0;
        long dslId = 0;
        try {
            sourceId = Long.parseLong(source_id.toString());
            databaseId = Long.parseLong(database_id.toString());
            dslId = Long.parseLong(dsl_id.toString());
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new BusinessException("req format failed");
        }
        TableDataReq[] tableInfosResult = JsonUtil.toObject(tableInfos.toString(), new TypeReference<TableDataReq[]>() {
        });
        registerService.saveTableData(sourceId, databaseId, tableInfosResult, tableColumns.toString(), dslId);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateTableData")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "databaseId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableInfos", value = "", dataTypeClass = TableInfo[].class), @ApiImplicitParam(name = "tableColumns", value = "", dataTypeClass = String.class) })
    public void updateTableData(@RequestBody Map<String, Object> req) {
        long source_id = ReqDataUtils.getLongData(req, "source_id");
        long databaseId = ReqDataUtils.getLongData(req, "databaseId");
        long dsl_id = ReqDataUtils.getLongData(req, "dsl_id");
        String tableInfos = ReqDataUtils.getStringData(req, "tableInfos");
        String tableColumns = ReqDataUtils.getStringData(req, "tableColumns");
        TableDataReq[] tableInfosResult = JsonUtil.toObject(tableInfos, new TypeReference<TableDataReq[]>() {
        });
        registerService.updateTableData(source_id, databaseId, dsl_id, tableInfosResult, tableColumns);
    }
}
