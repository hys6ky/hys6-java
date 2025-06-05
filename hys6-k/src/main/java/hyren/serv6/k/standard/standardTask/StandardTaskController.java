package hyren.serv6.k.standard.standardTask;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.k.entity.StandardTask;
import hyren.serv6.k.standard.standardTask.entityVo.TaskVo;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/detection")
public class StandardTaskController {

    private StandardTaskService standardTaskService;

    public StandardTaskController(StandardTaskService standardTaskService) {
        this.standardTaskService = standardTaskService;
    }

    @GetMapping("/getTabel")
    @ApiOperation("获取所有的表信息")
    public Map<String, Object> queryTableByPage(@ApiParam(name = "source_id", value = "", required = true) Long source_id, @ApiParam(name = "table_name", value = "") String table_name, @ApiParam(name = "isAll", value = "", required = true) String isAll, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Validator.notNull(source_id, "元系统ID不能为空！");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> metaTable = standardTaskService.getMetaTable(source_id, table_name, isAll, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", metaTable);
        return resultMap;
    }

    @GetMapping("/getVime")
    @ApiOperation("获取所有的表信息")
    public Map<String, Object> queryTableByPage(@ApiParam(name = "task_id", value = "", required = true) Long task_id) {
        Validator.notNull(task_id, "任务ID不能为空！");
        return standardTaskService.getVime(task_id);
    }

    @GetMapping
    @ApiOperation("获取所有任务信息")
    public Map<String, Object> queryTask(@ApiParam(name = "source_name", value = "") String source_name, @ApiParam(name = "start_date", value = "") String start_date, @ApiParam(name = "task_id", value = "", required = false) Long task_id, @ApiParam(name = "end_date", value = "") String end_date, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> standardTasks = standardTaskService.queryTask(source_name, start_date, end_date, task_id, page);
        standardTasks.forEach(standardTask -> {
            EtlJobDef jobData = standardTaskService.getJobData((Long) standardTask.get("task_id"), (String) standardTask.get("task_name"));
            if (jobData != null) {
                standardTask.put("etl_time", jobData.getLast_exe_time());
                standardTask.put("etl_status", IsFlag.Shi.getCode());
            } else {
                standardTask.put("etl_time", null);
                standardTask.put("etl_status", IsFlag.Fou.getCode());
            }
        });
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", standardTasks);
        return resultMap;
    }

    @PostMapping
    @ApiOperation("新增任务")
    public void saveTask(@ApiParam(name = "taskVo", value = "") @RequestBody TaskVo taskVo) {
        standardTaskService.saveTask(taskVo);
    }

    @PutMapping
    @ApiOperation("修改任务信息")
    public void updateTask(@ApiParam(name = "taskVo", value = "") @RequestBody TaskVo taskVo) {
        standardTaskService.updateTask(taskVo);
    }

    @DeleteMapping
    @ApiOperation("删除任务")
    public void delTask(@ApiParam(name = "taskVo", value = "") @RequestBody List<Long> ids) {
        Validator.notNull(ids, "删除信息不能为空");
        standardTaskService.delTask(ids);
    }

    @GetMapping("/standardBatch")
    @ApiOperation("批量标准检测")
    public void standardBatch(@ApiParam(name = "taskId", value = "", required = true) long taskId) {
        standardTaskService.standardBatch(taskId, Dbo.db());
    }

    @GetMapping("/isRun")
    @ApiOperation("批量标准检测")
    public void isRun(@ApiParam(name = "taskId", value = "", required = true) long taskId) {
        standardTaskService.standardBatch(taskId, Dbo.db());
    }

    @ApiOperation("获取系统配置信息")
    @GetMapping("/getSysPara")
    public Map<String, String> getSysPara(@ApiParam(name = "paraType", value = "", required = true) String paraType) {
        Validator.notNull(paraType);
        return standardTaskService.getSysPara(paraType);
    }
}
