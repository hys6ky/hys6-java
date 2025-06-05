package hyren.serv6.t.taskDev;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.t.contants.ReqCategoryEnum;
import hyren.serv6.t.dataReq.TskDataReqService;
import hyren.serv6.t.entity.TskTaskData;
import hyren.serv6.t.entity.TskTaskDev;
import hyren.serv6.t.vo.query.TskTaskDevQueryVo;
import hyren.serv6.t.vo.save.TskTaskDevUpdateVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Resource;
import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("任务开发信息表管理")
@RestController
@RequestMapping("taskDev")
public class TskTaskDevController {

    @Resource
    private TskTaskDevService tskTaskDevService;

    @Resource
    private TskDataReqService tskDataReqService;

    @Resource
    private TskTaskDataService tskTaskDataService;

    @ApiOperation("列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "taskDevQueryVo", value = "") TskTaskDevQueryVo taskDevQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<TskTaskDevQueryVo> pageList = this.tskTaskDevService.queryByPage(taskDevQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("详情")
    @GetMapping("{id}")
    public TskTaskDevQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskTaskDevService.queryById(id);
    }

    @ApiOperation("新增")
    @PostMapping
    public TskTaskDev add(@ApiParam(name = "TskTaskDev", value = "", required = true) @Valid @RequestBody TskTaskDev tskTaskDev) {
        return this.tskTaskDevService.insert(tskTaskDev);
    }

    @ApiOperation("编辑")
    @PutMapping
    public TskTaskDev edit(@ApiParam(name = "TskTaskDevUpdateVo", value = "", required = true) @Valid @RequestBody TskTaskDevUpdateVo taskDevUpdateVo) {
        return this.tskTaskDevService.update(taskDevUpdateVo);
    }

    @ApiOperation("删除")
    @DeleteMapping("{id}")
    public void deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        this.tskTaskDevService.deleteById(id);
        this.tskTaskDataService.deleteByTaskId(id);
    }

    @ApiOperation("批量删除")
    @DeleteMapping("/batch")
    public void batchDeleteByIds(@ApiParam(name = "ids", value = "", required = true) @RequestParam String ids) {
        this.tskTaskDevService.batchDeleteByIds(ids);
        for (String taskId : ids.split(",")) {
            this.tskTaskDataService.deleteByTaskId(Long.valueOf(taskId));
        }
    }

    @ApiOperation("更改任务状态")
    @PutMapping("/status")
    public void updateTaskStatus(@ApiParam(name = "id", value = "", required = true) @RequestParam Long id, @ApiParam(name = "taskStatus", value = "", required = true) @RequestParam String taskStatus) {
        this.tskTaskDevService.updateTaskStatus(id, taskStatus);
    }

    @ApiOperation("获取业务数据所分配的表")
    @GetMapping("/assign/treeTable")
    public List<Node> treeTable(Long id) {
        return tskDataReqService.getDateReqTreeTable(id, ReqCategoryEnum.DATA);
    }

    @ApiOperation("开发任务与数据关联表新增")
    @PostMapping("data")
    public TskTaskData add(@ApiParam(name = "TskTaskData", value = "", required = true) @RequestBody TskTaskData tskTaskData) {
        return this.tskTaskDataService.insert(tskTaskData);
    }

    @ApiOperation("开发任务与数据关联表详情")
    @GetMapping("data")
    public TskTaskData queryByTaskIdAntCategory(@ApiParam(name = "taskId", value = "", required = true) @RequestParam Long taskId, @ApiParam(name = "taskCategory", value = "", required = true) @RequestParam String taskCategory) {
        return this.tskTaskDataService.queryByTaskIdAntCategory(taskId, taskCategory);
    }

    @ApiOperation("开发任务与数据关联表删除")
    @DeleteMapping("data")
    public Boolean deleteByDataIdAndTaskCategory(@ApiParam(name = "dataId", value = "", required = true) @RequestParam String dataId, @ApiParam(name = "taskCategory", value = "", required = true) @RequestParam String taskCategory) {
        return this.tskTaskDataService.deleteByDataIdAndTaskCategory(dataId, taskCategory);
    }
}
