package hyren.serv6.m.task;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.entity.MetaTask;
import hyren.serv6.m.vo.query.MetaTaskQueryVo;
import hyren.serv6.m.vo.save.MetaTaskSaveVo;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Api("采集任务表管理")
@RestController
@RequestMapping("metaTask")
public class MetaTaskController {

    @Resource
    private MetaTaskService metaTaskService;

    @Resource
    private MetaTaskObjService metaTaskObjService;

    @ApiOperation("采集任务表列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "MetaTask", value = "") MetaTaskQueryVo metaTaskQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaTaskQueryVo> pageList = this.metaTaskService.queryByPage(metaTaskQueryVo, page);
        Map<Long, Map<String, String>> taskEtlMap = getTaskEtlInfo(pageList);
        for (MetaTaskQueryVo queryVo : pageList) {
            if (taskEtlMap.get(queryVo.getTask_id()) != null) {
                queryVo.setLastExecTime(taskEtlMap.get(queryVo.getTask_id()).get("etl_time"));
                queryVo.setEtlStatus(taskEtlMap.get(queryVo.getTask_id()).get("etl_status"));
            } else {
                queryVo.setEtlStatus(IsFlag.Fou.getCode());
            }
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    private Map<Long, Map<String, String>> getTaskEtlInfo(List<MetaTaskQueryVo> pageList) {
        List<Long> taskIds = pageList.stream().map(MetaTaskQueryVo::getTask_id).collect(Collectors.toList());
        Map<Long, Map<String, String>> jobMap = new HashMap<>();
        taskIds.forEach(id -> {
            jobMap.put(id, metaTaskObjService.getJobStatus(id));
        });
        return jobMap;
    }

    @ApiOperation("采集任务表详情")
    @GetMapping("{id}")
    public MetaTaskQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaTaskService.queryById(id);
    }

    @ApiOperation("采集任务表新增")
    @PostMapping
    public MetaTask add(@ApiParam(name = "MetaTask", value = "", required = true) @RequestBody MetaTaskSaveVo metaTaskSaveVo) {
        MetaObjTypeEnum.ofEnumByCode(metaTaskSaveVo.getTask_type());
        return this.metaTaskService.insert(metaTaskSaveVo);
    }

    @ApiOperation("采集任务表编辑")
    @PutMapping
    public MetaTask edit(@ApiParam(name = "MetaTask", value = "", required = true) @RequestBody MetaTaskSaveVo metaTaskSaveVo) {
        return this.metaTaskService.update(metaTaskSaveVo);
    }

    @ApiOperation("采集任务表删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        if (metaTaskObjService.getJobStatus(id) == null) {
            return this.metaTaskService.deleteById(id);
        } else {
            throw new BusinessException("任务已经生成作业不能进行删除操作! ");
        }
    }

    @ApiOperation("生成调度任务")
    @PostMapping("/gen/etl")
    public void genEtl(@ApiParam(name = "task_id", value = "", required = true) Long task_id) {
        this.metaTaskService.genEtl(task_id);
    }

    @ApiOperation("生成调度任务")
    @GetMapping("/etl/conf")
    public void etlConf(@ApiParam(name = "task_id", value = "", required = true) Long task_id) {
    }
}
