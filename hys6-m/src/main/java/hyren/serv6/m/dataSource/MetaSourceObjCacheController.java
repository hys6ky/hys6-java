package hyren.serv6.m.dataSource;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.m.entity.MetaSourceObjCache;
import hyren.serv6.m.vo.query.MetaSourceObjCacheQueryVo;
import hyren.serv6.m.vo.save.MetaSourceObjCacheSaveVo;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("源表查询缓存管理")
@RestController
@RequestMapping("/metaTask/metaSourceObjCache")
public class MetaSourceObjCacheController {

    @Resource
    private MetaSourceObjCacheService metaSourceObjCacheService;

    @ApiOperation("源表查询缓存列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "MetaSourceObjCache", value = "") MetaSourceObjCacheQueryVo metaSourceObjCacheQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaSourceObjCacheQueryVo> pageList = this.metaSourceObjCacheService.queryByPage(metaSourceObjCacheQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("源表查询缓存详情")
    @GetMapping("{id}")
    public MetaSourceObjCacheQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaSourceObjCacheService.queryById(id);
    }

    @ApiOperation("源表查询缓存新增")
    @PostMapping
    public MetaSourceObjCache add(@ApiParam(name = "MetaSourceObjCache", value = "", required = true) @RequestBody MetaSourceObjCacheSaveVo metaSourceObjCacheSaveVo) {
        return this.metaSourceObjCacheService.insert(metaSourceObjCacheSaveVo);
    }

    @ApiOperation("源表查询缓存编辑")
    @PutMapping
    public MetaSourceObjCache edit(@ApiParam(name = "MetaSourceObjCache", value = "", required = true) @RequestBody MetaSourceObjCacheSaveVo metaSourceObjCacheSaveVo) {
        return this.metaSourceObjCacheService.update(metaSourceObjCacheSaveVo);
    }

    @ApiOperation("源表查询缓存删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaSourceObjCacheService.deleteById(id);
    }

    @ApiOperation("根据IS_COL分类查询源表缓存列表")
    @PostMapping("/getPageByIsCol")
    public Map<String, Object> getPageByIsCol(@ApiParam(name = "MetaSourceObjCache", value = "") MetaSourceObjCacheQueryVo metaSourceObjCacheQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaSourceObjCacheQueryVo> pageList = this.metaSourceObjCacheService.getPageByIsCol(metaSourceObjCacheQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }
}
