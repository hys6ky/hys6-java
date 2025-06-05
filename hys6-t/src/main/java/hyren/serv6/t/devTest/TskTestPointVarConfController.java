package hyren.serv6.t.devTest;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.t.entity.TskTestPointVarConf;
import hyren.serv6.t.vo.query.TskTestPointVarConfQueryVo;
import hyren.serv6.t.vo.save.TskTestPointVarConfSaveVo;
import hyren.serv6.t.vo.save.VarConfigUpdateVo;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("测试要点变量配置表管理")
@RestController
@RequestMapping("tskTestPointVarConf")
public class TskTestPointVarConfController {

    @Resource
    private TskTestPointVarConfService tskTestPointVarConfService;

    @ApiOperation("测试要点变量配置表列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "TskTestPointVarConf", value = "") TskTestPointVarConfQueryVo tskTestPointVarConfQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<TskTestPointVarConfQueryVo> pageList = this.tskTestPointVarConfService.queryByPage(tskTestPointVarConfQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("测试要点变量配置表详情")
    @GetMapping("{id}")
    public TskTestPointVarConfQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskTestPointVarConfService.queryById(id);
    }

    @ApiOperation("测试要点变量配置表新增")
    @PostMapping
    public TskTestPointVarConf add(@ApiParam(name = "TskTestPointVarConf", value = "", required = true) @RequestBody TskTestPointVarConfSaveVo tskTestPointVarConfSaveVo) {
        return this.tskTestPointVarConfService.insert(tskTestPointVarConfSaveVo);
    }

    @ApiOperation("测试要点变量配置表编辑")
    @PutMapping
    public TskTestPointVarConf edit(@ApiParam(name = "TskTestPointVarConf", value = "", required = true) @RequestBody TskTestPointVarConfSaveVo tskTestPointVarConfSaveVo) {
        return this.tskTestPointVarConfService.update(tskTestPointVarConfSaveVo);
    }

    @ApiOperation("测试要点变量配置表删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskTestPointVarConfService.deleteById(id);
    }

    @ApiOperation("更新测试要点配置信息")
    @PostMapping("/updateVarConfig")
    public void updateVarConfig(@ApiParam(name = "VarConfigUpdateVo", value = "", required = true) @RequestBody VarConfigUpdateVo varConfigUpdateVo) {
        tskTestPointVarConfService.updateVarConfig(varConfigUpdateVo);
    }

    @ApiOperation("根据关联Id查询配置信息")
    @PostMapping("/queryByRelId")
    public List<TskTestPointVarConf> queryByRelId(@ApiParam(name = "rel_id", value = "", required = true) Long rel_id) {
        return tskTestPointVarConfService.queryByRelId(rel_id);
    }
}
