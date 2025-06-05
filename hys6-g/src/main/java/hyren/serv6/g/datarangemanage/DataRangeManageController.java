package hyren.serv6.g.datarangemanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.g.bean.TableDataInfo;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/interfaceManagement/dataRangeManage")
public class DataRangeManageController {

    @Autowired
    private DataRangeManageService dataRangeService;

    @ApiOperation(value = "", notes = "")
    @ApiResponse(code = 200, message = "")
    @PostMapping("/searchDataUsageRangeInfoToTreeData")
    public List<Node> searchDataUsageRangeInfoToTreeData() {
        return dataRangeService.searchDataUsageRangeInfoToTreeData();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file_id", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "data_layer", value = "", example = "", dataTypeClass = String.class) })
    @ApiResponse(code = 200, message = "")
    @GetMapping("/searchFieldById")
    public Map<String, Object> searchFieldById(String file_id, String data_layer) {
        return dataRangeService.searchFieldById(file_id, data_layer);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableDataInfos", value = "", example = "", dataTypeClass = TableDataInfo[].class), @ApiImplicitParam(name = "table_note", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "data_layer", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = long[].class) })
    @PostMapping("/saveTableData")
    public void saveTableData(@RequestBody Map<String, Object> params) {
        String tableDataInfos = params.get("tableDataInfos").toString();
        String table_note = "";
        String data_layer = "";
        if (params.get("table_note") != null) {
            table_note = params.get("table_note").toString();
        }
        if (params.get("data_layer") != null) {
            data_layer = params.get("data_layer").toString();
        }
        List<Integer> userId = (List<Integer>) params.get("user_id");
        Long[] user_id = new Long[userId.size()];
        for (int i = 0; i < userId.size(); i++) {
            user_id[i] = Long.parseLong(String.valueOf(userId.get(i)));
        }
        TableDataInfo[] tableInfoArray = JsonUtil.toObject(tableDataInfos, new TypeReference<TableDataInfo[]>() {
        });
        dataRangeService.saveTableData(tableInfoArray, table_note, data_layer, user_id);
    }
}
