package hyren.serv6.m.task;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.m.dataSource.MetaDataSourceService;
import hyren.serv6.m.entity.MetaTask;
import hyren.serv6.m.entity.MetaTaskObj;
import hyren.serv6.m.vo.query.MetaTaskObjQueryVo;
import hyren.serv6.m.vo.save.MetaTaskObjSaveVo;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("采集任务与采集对象关联表管理")
@RestController
@RequestMapping("metaTask/metaTaskObj")
public class MetaTaskObjController {

    @Resource
    private MetaTaskObjService metaTaskObjService;

    @Resource
    private MetaTaskService metaTaskService;

    @Resource
    private MetaDataSourceService metaDataSourceService;

    @ApiOperation("采集任务与采集对象关联表列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "MetaTaskObj", value = "") MetaTaskObjQueryVo metaTaskObjQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaTaskObjQueryVo> pageList = this.metaTaskObjService.queryByPage(metaTaskObjQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("采集任务与采集对象关联表详情")
    @GetMapping("{id}")
    public MetaTaskObjQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaTaskObjService.queryById(id);
    }

    @ApiOperation("采集任务与采集对象关联表新增")
    @PostMapping
    public void add(@ApiParam(name = "MetaTaskObjSaveVo", value = "", required = true) @RequestBody MetaTaskObjSaveVo metaTaskObjSaveVo) {
        this.metaTaskObjService.insert(metaTaskObjSaveVo);
        metaDataSourceService.updateCacheObjNum(metaTaskService.queryById(metaTaskObjSaveVo.getTask_id()).getSource_id());
    }

    @ApiOperation("修改任务名称")
    @PostMapping("/updateTask")
    public void saupdateTaskve(@RequestBody MetaTask metaTask) {
        metaTaskService.updateTask(metaTask);
    }

    @ApiOperation("采集任务与采集对象关联表删除")
    @DeleteMapping("{task_id}")
    public void batchDel(@ApiParam(name = "task_id", required = true) @PathVariable Long task_id, @RequestBody List<Long> ids) {
        Validator.notNull(task_id, "任务ID不能为空！");
        Validator.notNull(ids, "表ID不能为空！");
        if (metaTaskObjService.getJobStatus(task_id) == null) {
            this.metaTaskObjService.batchDel(ids);
        } else {
            throw new BusinessException("任务已经生成作业不能进行删除操作！");
        }
    }
}
