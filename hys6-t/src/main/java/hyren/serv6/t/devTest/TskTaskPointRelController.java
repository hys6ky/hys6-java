package hyren.serv6.t.devTest;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.t.entity.TskTestPoint;
import hyren.serv6.t.vo.query.TskTaskPointRelQueryVo;
import hyren.serv6.t.vo.query.TskTaskTestQueryVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("任务与要点关联表管理")
@RestController
@RequestMapping("tskTaskPointRel")
public class TskTaskPointRelController {

    @Resource
    @Autowired
    private TskTaskPointRelService tskTaskPointRelService;

    @ApiOperation("分页查询")
    @PostMapping("/queryByPage")
    public Map<String, Object> queryByPage(@ApiParam(name = "tskTaskTestQueryVo", value = "") @RequestBody TskTaskTestQueryVo tskTaskTestQueryVo) {
        return tskTaskPointRelService.queryByPage(tskTaskTestQueryVo);
    }

    @ApiOperation("任务与要点关联表详情")
    @GetMapping("{task_id}")
    public Map<String, Object> queryById(@ApiParam(name = "task_id", value = "", required = true) @PathVariable("task_id") Long task_id) {
        return this.tskTaskPointRelService.queryById(task_id);
    }

    @ApiOperation("添加要点-获取任务类型一致的要点")
    @PostMapping("/selectPointByTaskCategory")
    public Map<String, Object> selectPointByTaskCategory(@ApiParam(name = "taskCategory", value = "") @RequestParam String taskCategory, @ApiParam(name = "currPage", value = "", required = true) @RequestParam(defaultValue = "1") Integer currPage, @ApiParam(name = "pageSize", value = "", required = true) @RequestParam(defaultValue = "10") Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<TskTestPoint> pageList = this.tskTaskPointRelService.selectPointByTaskCategory(taskCategory, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("添加要点-批量新增任务要点关系表")
    @PostMapping("/bachInsertRel")
    public void insertRel(@ApiParam(name = "task_id", value = "", required = true) @RequestParam("task_id") Long task_id, @ApiParam(name = "ids", value = "") @RequestParam("ids") Long[] ids) {
        tskTaskPointRelService.insertRel(task_id, ids);
    }

    @ApiOperation("根据任务要点关系Id删除任务要点关系")
    @PostMapping("/deleteByRelId")
    public boolean deleteByRelId(@ApiParam(name = "rel_id", value = "") @RequestParam("rel_id") Long rel_id) {
        return tskTaskPointRelService.deleteByRelId(rel_id);
    }

    @ApiOperation("根据任务要点关系Id获取要点配置信息")
    @PostMapping("/queryConfigByRelId")
    public Map<String, Object> queryConfigByRelId(@ApiParam(name = "rel_id", value = "") @RequestParam("rel_id") Long rel_id) {
        return tskTaskPointRelService.queryConfigByRelId(rel_id);
    }

    @ApiOperation("提交测试")
    @PostMapping("/commitTest")
    public void commitTest(@ApiParam(name = "task_id", value = "") @RequestParam("task_id") Long task_id) {
        tskTaskPointRelService.commitTest(task_id);
    }

    @ApiOperation("测试反馈-获取测试要点等数据")
    @PostMapping("/testBack")
    public List<TskTaskPointRelQueryVo> testBack(@ApiParam(name = "task_id", value = "") @RequestParam("task_id") Long task_id) {
        return tskTaskPointRelService.testBack(task_id);
    }

    @ApiOperation("测试反馈-测试通过")
    @PostMapping("/testPass")
    public void testPass(@ApiParam(name = "task_id", value = "") @RequestParam("task_id") Long task_id, @ApiParam(name = "test_note", value = "") @RequestParam("test_note") String test_note) {
        tskTaskPointRelService.testPass(task_id, test_note);
    }

    @ApiOperation("测试反馈-测试驳回")
    @PostMapping("/testNoPass")
    public void testNoPass(@ApiParam(name = "task_id", value = "") @RequestParam("task_id") Long task_id, @ApiParam(name = "test_note", value = "") @RequestParam("test_note") String test_note) {
        tskTaskPointRelService.testNoPass(task_id, test_note);
    }
}
