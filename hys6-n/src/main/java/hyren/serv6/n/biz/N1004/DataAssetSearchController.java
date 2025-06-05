package hyren.serv6.n.biz.N1004;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashMap;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/N1004")
@Validated
public class DataAssetSearchController {

    DataAssetSearchService dataAssetSearchService;

    DataAssetSearchController(DataAssetSearchService dataAssetSearchService) {
        this.dataAssetSearchService = dataAssetSearchService;
    }

    @ApiOperation("检索资产")
    @PostMapping("/searchDataAsset")
    public Map<String, Object> searchDataAsset(@ApiParam(name = "searchText", value = "", required = false) String searchText, @ApiParam(name = "assetType", value = "", required = false) String assetType, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetSearchService.searchDataAsset(searchText, assetType, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }
}
