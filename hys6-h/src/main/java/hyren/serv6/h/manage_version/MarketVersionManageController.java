package hyren.serv6.h.manage_version;

import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.h.manage_version.dto.GetDataTableMappingInfosDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@Api(tags = "")
@RestController
@RequestMapping("marketVersionManage")
public class MarketVersionManageController {

    @Autowired
    private MarketVersionManageService service;

    @PostMapping("getDataTableMappingInfos")
    @ApiOperation(value = "", notes = "")
    public Map<String, List<Map<String, Map<String, Object>>>> getDataTableMappingInfos(@RequestBody GetDataTableMappingInfosDTO dto) {
        return service.getDataTableMappingInfos(dto.getDatatable_id(), dto.getVersion_date_s());
    }

    @Deprecated
    @PostMapping("getDataTableStructureInfos")
    @ApiOperation(value = "", notes = "")
    public Map<String, Object> getDataTableStructureInfos(@RequestBody GetDataTableMappingInfosDTO dto) {
        return service.getDataTableStructureInfos(dto.getDatatable_id(), dto.getVersion_date_s());
    }

    @PostMapping("getMarketVerManageTreeData")
    @ApiOperation(value = "", notes = "")
    public List<Node> getMarketVerManageTreeData() {
        return service.getMarketVerManageTreeData();
    }
}
