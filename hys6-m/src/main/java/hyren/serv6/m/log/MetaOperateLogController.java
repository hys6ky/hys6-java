package hyren.serv6.m.log;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.m.entity.MetaOperateLog;
import hyren.serv6.m.vo.query.MetaOperateLogQueryVo;
import hyren.serv6.m.vo.save.MetaOperateLogSaveVo;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("元数据操作历史记录管理")
@RestController
@RequestMapping("metaOperateLog")
public class MetaOperateLogController {

    @Resource
    private MetaOperateLogService metaOperateLogService;

    @ApiOperation("元数据操作历史记录列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "MetaOperateLogQueryVo", value = "") MetaOperateLogQueryVo metaOperateLogQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaOperateLogQueryVo> pageList = this.metaOperateLogService.queryByPage(metaOperateLogQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("元数据操作历史记录详情")
    @GetMapping("{id}")
    public MetaOperateLogQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaOperateLogService.queryById(id);
    }

    @ApiOperation("元数据操作历史记录新增")
    @PostMapping
    public MetaOperateLog add(@ApiParam(name = "MetaOperateLogSaveVo", value = "", required = true) @RequestBody MetaOperateLogSaveVo metaOperateLogSaveVo) {
        return this.metaOperateLogService.insert(metaOperateLogSaveVo);
    }

    @ApiOperation("元数据操作历史记录编辑")
    @PutMapping
    public MetaOperateLog edit(@ApiParam(name = "MetaOperateLogSaveVo", value = "", required = true) @RequestBody MetaOperateLogSaveVo metaOperateLogSaveVo) {
        return this.metaOperateLogService.update(metaOperateLogSaveVo);
    }

    @ApiOperation("元数据操作历史记录删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaOperateLogService.deleteById(id);
    }
}
