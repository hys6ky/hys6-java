package hyren.serv6.b.fulltextsearch;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/fullTextSearch")
@RestController
public class FullTextSearchController {

    @Autowired
    FullTextSearchService fullTextSearchService;

    @RequestMapping("/getCollectFiles")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "queryNum", value = "", dataTypeClass = Integer.class)
    public List<Map<String, Object>> getCollectFiles(@RequestParam(defaultValue = "9") int queryNum) {
        return fullTextSearchService.getCollectFiles(queryNum);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/fullTextSearchMethod")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fullTextSearchMethod", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "queryKeyword", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    public Map<String, Object> fullTextSearch(@RequestParam(defaultValue = "unstructuredFileSearch") String fullTextSearchMethod, String queryKeyword, @RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        return fullTextSearchService.fullTextSearch(fullTextSearchMethod, queryKeyword, currPage, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchByMap")
    @ApiImplicitParam(name = "imageAddress", value = "", dataTypeClass = String.class)
    public Map<String, Object> searchByMap(String imageAddress) {
        return fullTextSearchService.searchByMap(imageAddress);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/articleSimilarityQuery")
    @ApiImplicitParams({ @ApiImplicitParam(name = "docAddress", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "similarityRate", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "searchWay", value = "", dataTypeClass = String.class) })
    public Map<String, Object> articleSimilarityQuery(String docAddress, @RequestParam(defaultValue = "0") String similarityRate, String searchWay) {
        return fullTextSearchService.articleSimilarityQuery(docAddress, similarityRate, searchWay);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/fileNameSearch")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class) })
    public Map<String, Object> fileNameSearch(@RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize, String fileName) {
        return fullTextSearchService.fileNameSearch(currPage, pageSize, fileName);
    }
}
