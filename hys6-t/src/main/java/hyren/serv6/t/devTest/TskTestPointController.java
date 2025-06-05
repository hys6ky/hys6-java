package hyren.serv6.t.devTest;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.t.entity.TskTestPoint;
import hyren.serv6.t.vo.query.TskTestPointQueryVo;
import hyren.serv6.t.vo.save.TskTestPointSaveVo;
import io.swagger.annotations.ApiImplicitParam;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("测试要点表管理")
@RestController
@RequestMapping("tskTestPoint")
public class TskTestPointController {

    @Resource
    private TskTestPointService tskTestPointService;

    @ApiOperation("测试要点表列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "TskTestPoint", value = "") TskTestPointQueryVo tskTestPointQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<TskTestPointQueryVo> pageList = this.tskTestPointService.queryByPage(tskTestPointQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("测试要点表详情")
    @GetMapping("{id}")
    public TskTestPointQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskTestPointService.queryById(id);
    }

    @ApiOperation("测试要点表新增")
    @PostMapping
    public TskTestPoint add(@ApiParam(name = "TskTestPoint", value = "", required = true) @Valid @RequestBody TskTestPointSaveVo tskTestPointSaveVo) {
        return this.tskTestPointService.insert(tskTestPointSaveVo);
    }

    @ApiOperation("测试要点表编辑")
    @PutMapping
    public TskTestPoint edit(@ApiParam(name = "TskTestPoint", value = "", required = true) @Valid @RequestBody TskTestPointSaveVo tskTestPointSaveVo) {
        return this.tskTestPointService.update(tskTestPointSaveVo);
    }

    @ApiOperation("测试要点表删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskTestPointService.deleteById(id);
    }

    @ApiOperation("测试要点表批量删除")
    @PostMapping("/batchDelete")
    public void batchDelete(@RequestParam("pointIds") Long[] pointIds) {
        tskTestPointService.batchDelete(pointIds);
    }
}
