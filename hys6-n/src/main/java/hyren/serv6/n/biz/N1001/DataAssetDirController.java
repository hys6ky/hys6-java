package hyren.serv6.n.biz.N1001;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.n.bean.DataAssetCatalogVo;
import hyren.serv6.n.bean.DataAssetDirDto;
import hyren.serv6.n.bean.DataAssetDirTreeDto;
import hyren.serv6.n.bean.DataAssetDirVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/N1001")
@Validated
public class DataAssetDirController {

    DataAssetDirService dataAssetDirService;

    DataAssetDirController(DataAssetDirService dataAssetDirService) {
        this.dataAssetDirService = dataAssetDirService;
    }

    @ApiOperation("新增或修改目录")
    @PostMapping("/addOrUpdateDir")
    public void addOrUpdateDir(@RequestBody @Validated DataAssetDirVo dataAssetDirVo) {
        dataAssetDirService.addOrUpdateDir(dataAssetDirVo);
    }

    @ApiOperation("删除目录")
    @PostMapping("/deleteDir")
    public void deleteDir(@ApiParam(name = "dirId", value = "", required = false) Long dirId, @ApiParam(name = "isDelCoding", value = "", required = false) String isDelCoding) {
        dataAssetDirService.deleteDir(dirId, isDelCoding);
    }

    @ApiOperation("查询下级目录")
    @PostMapping("/queryDirByParentId")
    public List<DataAssetDirDto> queryDirByParentId(@ApiParam(name = "parentId", value = "", required = false) Long parentId) {
        return dataAssetDirService.queryDirByParentId(parentId);
    }

    @ApiOperation("根据id查询目录")
    @PostMapping("/queryDirById")
    public List<DataAssetDirDto> queryDirById(@RequestBody @ApiParam(name = "dirIds", value = "", required = false) Long[] dirIds) {
        return dataAssetDirService.queryDirById(dirIds);
    }

    @ApiOperation("查询该目录下的目录以及资产")
    @PostMapping("/queryByParentId")
    public List<DataAssetDirTreeDto> queryByParentId(@ApiParam(name = "parentId", value = "", required = false) Long parentId, @ApiParam(name = "catalogId", value = "", required = false) Long catalogId, @ApiParam(name = "catalogStatus", value = "", required = false) String catalogStatus) {
        return dataAssetDirService.queryByParentId(parentId, catalogId, catalogStatus);
    }

    @ApiOperation("新增或修改编目")
    @PostMapping("/addOrUpdateCatalog")
    public void addOrUpdateCatalog(@RequestBody @Validated DataAssetCatalogVo dataAssetCatalogVo) {
        dataAssetDirService.addOrUpdateCatalog(dataAssetCatalogVo);
    }

    @ApiOperation("删除编目")
    @PostMapping("/deleteCatalog")
    public void deleteCatalog(@ApiParam(name = "catalogId", value = "", required = false) Long catalogId) {
        dataAssetDirService.deleteCatalog(catalogId);
    }

    @ApiOperation("发布编目")
    @PostMapping("/publishCatalog")
    public void publishCatalog(@ApiParam(name = "catalogId", value = "", required = false) Long catalogId) {
        dataAssetDirService.publishCatalog(catalogId);
    }

    @ApiOperation("查询编目列表")
    @PostMapping("/findCatalog")
    public Map<String, Object> findCatalog(@ApiParam(name = "catalogName", value = "", required = false) String catalogName, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetDirService.findCatalog(catalogName, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }
}
