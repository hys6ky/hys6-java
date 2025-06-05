package hyren.serv6.n.biz.N1002;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.n.bean.*;
import hyren.serv6.n.entity.DataAssetColumn;
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
@RequestMapping("/N1002")
@Validated
public class DataAssetRegistController {

    DataAssetRegistService dataAssetRegistService;

    DataAssetRegistController(DataAssetRegistService dataAssetRegistService) {
        this.dataAssetRegistService = dataAssetRegistService;
    }

    @ApiOperation("新增目录与资产的关系")
    @PostMapping("/addDirAndAssetRel")
    public void addDirAndAssetRel(@RequestBody @Validated DataAssetDirRelBean dataAssetDirRelBean) {
        dataAssetRegistService.addDirAndAssetRel(dataAssetDirRelBean);
    }

    @ApiOperation("修改目录与资产的关系")
    @PostMapping("/updateDirAndAssetRel")
    public void updateDirAndAssetRel(@RequestBody @Validated DataAssetDirRelBean dataAssetDirRelBean) {
        dataAssetRegistService.updateDirAndAssetRel(dataAssetDirRelBean);
    }

    @ApiOperation("查询元数据数据源列表")
    @PostMapping("/findMetaDataSource")
    @Deprecated
    public List<MetaDataSourceDto> findMetaDataSource() {
        return dataAssetRegistService.findMetaDataSource();
    }

    @ApiOperation("查询元数据对象")
    @PostMapping("/findMetaDataObj")
    @Deprecated
    public List<MetaDataObjDto> findMetaDataObj(@ApiParam(name = "sourceId", value = "", required = false) long sourceId, @ApiParam(name = "type", value = "", required = false) String type) {
        return dataAssetRegistService.findMetaDataObj(sourceId, type);
    }

    @ApiOperation("查询元数据对象字段")
    @PostMapping("/findMetaDataColumn")
    public List<MetaDataColumnDto> findMetaDataColumn(@ApiParam(name = "objId", value = "", required = true) String objId) {
        return dataAssetRegistService.findMetaDataColumn(objId);
    }

    @ApiOperation("登记资产")
    @PostMapping("/registDataAsset")
    public void registDataAsset(@RequestBody @Validated DataAssetRegistBean dataAssetRegistBean) {
        dataAssetRegistService.registDataAsset(dataAssetRegistBean);
    }

    @ApiOperation("删除资产")
    @PostMapping("/deleteDataAsset")
    public void deleteDataAsset(@RequestBody @ApiParam(name = "assetIds", value = "", required = true) Long[] assetIds) {
        dataAssetRegistService.deleteDataAsset(assetIds);
    }

    @ApiOperation("查询该目录下的资产")
    @PostMapping("/queryByDirId")
    public List<DataAssetRegistVo> queryByDirId(@ApiParam(name = "dirId", value = "", required = true) Long dirId) {
        return dataAssetRegistService.queryByDirId(dirId);
    }

    @ApiOperation("根据元数据id资产")
    @PostMapping("/queryByMdataId")
    public List<DataAssetRegistVo> queryByMdataId(@RequestBody @ApiParam(name = "mDataIds", value = "", required = true) Long[] mDataIds) {
        return dataAssetRegistService.queryByMdataId(mDataIds);
    }

    @ApiOperation("查询资产详细信息")
    @PostMapping("/findDataAsset")
    public Map<String, Object> findDataAsset(@ApiParam(name = "assetId", value = "", required = false) Long assetId, @ApiParam(name = "assetCode", value = "", required = false) String assetCode, @ApiParam(name = "assetName", value = "", required = false) String assetName, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAsset(assetId, assetCode, assetName, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产中的归属部门列表")
    @PostMapping("/findDataAssetDepart")
    public List<DataAssetDepartDto> findDataAssetDepart() {
        return dataAssetRegistService.findDataAssetDepart();
    }

    @ApiOperation("根据部门查询资产详细信息")
    @PostMapping("/findDataAssetByDepart")
    public Map<String, Object> findDataAssetByDepart(@ApiParam(name = "depId", value = "", required = false) Long depId, @ApiParam(name = "catalogId", value = "", required = false) Long catalogId, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetByDepart(depId, catalogId, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产详细历史信息")
    @PostMapping("/findDataAssetHis")
    public Map<String, Object> findDataAssetHis(@ApiParam(name = "assetId", value = "", required = false) Long assetId, @ApiParam(name = "assetCode", value = "", required = false) String assetCode, @ApiParam(name = "assetName", value = "", required = false) String assetName, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetHis(assetId, assetCode, assetName, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产字段详细信息")
    @PostMapping("/findDataAssetColumn")
    public Map<String, Object> findDataAssetColumn(@ApiParam(name = "assetId", value = "", required = false) Long assetId, @ApiParam(name = "colName", value = "", required = false) String colName, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetColumn(assetId, colName, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产字段详细信息")
    @PostMapping("/findDataAssetColumnById")
    public DataAssetColumn findDataAssetColumnById(@ApiParam(name = "colId", value = "", required = false) Long colId) {
        return dataAssetRegistService.findDataAssetColumnById(colId);
    }

    @ApiOperation("查询资产字段详细历史信息")
    @PostMapping("/findDataAssetColumnHis")
    public Map<String, Object> findDataAssetColumnHis(@ApiParam(name = "assetId", value = "", required = false) Long assetId, @ApiParam(name = "colName", value = "", required = false) String colName, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetColumnHis(assetId, colName, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产码值列表信息")
    @PostMapping("/findDataAssetEnum")
    public Map<String, Object> findDataAssetEnum(@ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetEnum(page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产码值列表历史信息")
    @PostMapping("/findDataAssetEnumHis")
    public Map<String, Object> findDataAssetEnumHis(@ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetEnumHis(page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产码值详细信息")
    @PostMapping("/findDataAssetEnumDetail")
    public Map<String, Object> findDataAssetEnumDetail(@ApiParam(name = "enumEname", value = "", required = false) String enumEname, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetEnumDetail(enumEname, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("查询资产码值详细历史信息")
    @PostMapping("/findDataAssetEnumDetailHis")
    public Map<String, Object> findDataAssetEnumDetailHis(@ApiParam(name = "enumEname", value = "", required = false) String enumEname, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetRegistService.findDataAssetEnumDetailHis(enumEname, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }
}
