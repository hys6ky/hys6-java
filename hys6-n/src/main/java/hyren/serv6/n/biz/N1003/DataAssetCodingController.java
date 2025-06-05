package hyren.serv6.n.biz.N1003;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.n.bean.DataAssetCodingVo;
import hyren.serv6.n.entity.DataAssetCoding;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("/N1003")
@Validated
public class DataAssetCodingController {

    DataAssetCodingService dataAssetCodingService;

    DataAssetCodingController(DataAssetCodingService dataAssetCodingService) {
        this.dataAssetCodingService = dataAssetCodingService;
    }

    @ApiOperation("新增或修改编码规则")
    @PostMapping("/addOrUpdateCoding")
    public void addOrUpdateCoding(@RequestBody @Validated DataAssetCodingVo dataAssetCodingVo) {
        dataAssetCodingService.addOrUpdateCoding(dataAssetCodingVo);
    }

    @ApiOperation("删除规则")
    @PostMapping("/deleteCodingById")
    public void deleteCodingById(@ApiParam(name = "codingId", value = "", required = false) Long codingId) {
        dataAssetCodingService.deleteCodingById(codingId);
    }

    @ApiOperation("查询编码规则列表")
    @PostMapping("/queryCoding")
    public Map<String, Object> queryCoding(@ApiParam(name = "codingId", value = "", required = false) Long codingId, @ApiParam(name = "currPage", value = "", required = false) int currPage, @ApiParam(name = "pageSize", value = "", required = false) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("pageList", dataAssetCodingService.queryCoding(codingId, page));
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    @ApiOperation("根据目录id查询编码规则列表")
    @PostMapping("/queryCodingByDirId")
    public List<DataAssetCoding> queryCodingByDirId(@ApiParam(name = "dirId", value = "", required = false) Long dirId) {
        return dataAssetCodingService.queryCodingByDirId(dirId);
    }
}
