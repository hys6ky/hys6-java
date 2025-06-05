package hyren.serv6.b.batchcollection.dbAgentcollect.datatransfer;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.entity.TableInfo;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Api("数据转存配置管理")
@RestController
@RequestMapping("/dataCollectionO/dbAgentcollect/dataTransfer")
@Validated
public class DataTransferController {

    @Autowired
    DataTransferService dataTransferService;

    @RequestMapping("/getInitDataTransfer")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class)
    public List<Map<String, Object>> getInitDataTransfer(@NotNull Long colSetId) {
        return dataTransferService.getInitDataTransfer(colSetId);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "colSetId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "dataExtractionDefs", value = "", example = "", dataTypeClass = DataExtractionDef[].class), @ApiImplicitParam(name = "tableInfos", value = "", example = "", dataTypeClass = TableInfo[].class) })
    @RequestMapping("/saveDataTransferData")
    public long saveDataTransferData(@RequestBody Map<String, Object> req) {
        long colSetId = 0;
        String dataExtractionDefs = null;
        String tableInfos = null;
        try {
            colSetId = Long.parseLong(req.get("colSetId").toString());
            dataExtractionDefs = req.get("dataExtractionDefs").toString();
            tableInfos = req.get("tableInfos").toString();
        } catch (Exception exception) {
            exception.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        DataExtractionDef[] dataExtractionDefsresult = JsonUtil.toObject(dataExtractionDefs, new TypeReference<DataExtractionDef[]>() {
        });
        TableInfo[] tableInfosresult = JsonUtil.toObject(tableInfos, new TypeReference<TableInfo[]>() {
        });
        return dataTransferService.saveDataTransferData(colSetId, dataExtractionDefsresult, tableInfosresult);
    }
}
