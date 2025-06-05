package hyren.serv6.b.datareceive;

import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.datadistribution.bean.DistributeJobBean;
import hyren.serv6.b.datareceive.req.BatchAnals;
import hyren.serv6.b.datareceive.req.ReqTaskInfo;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.user.UserUtil;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataReception")
@Slf4j
public class DataReceiveController {

    @Autowired
    private DataReceiveService dataReceiveService;

    @PostMapping("/ObtainSampleData")
    public String obtainSampleData(@RequestBody ReqTaskInfo taskInfo) {
        return dataReceiveService.obtainSampleData(taskInfo);
    }

    @PostMapping("/saveTask")
    public Long saveTask(@RequestBody ReqTaskInfo taskInfo) {
        return dataReceiveService.saveTask(taskInfo);
    }

    @PostMapping("/queryReceiveTask")
    public Map<String, Object> queryReceiveTask(@RequestParam(defaultValue = "1") Integer pageNum, @RequestParam(defaultValue = "10") Integer pageSize) {
        return dataReceiveService.queryReceiveTask(pageNum, pageSize, UserUtil.getUserId());
    }

    @PostMapping("/queryTaskAndFile")
    public Map<String, Object> queryTaskAndFileById(@NotNull Long dr_task_id) {
        return dataReceiveService.queryTaskAndFileById(dr_task_id);
    }

    @PostMapping("/updateExecuteTask")
    public Long updateReceiveTask(@RequestBody ReqTaskInfo taskInfo) {
        return dataReceiveService.updateReceiveTask(taskInfo);
    }

    @PostMapping("/deleteReceiveTask")
    public void deleteReceiveTask(@NotNull Long dr_task_id) {
        dataReceiveService.deleteReceiveTask(dr_task_id);
    }

    @PostMapping("/unloadAnalData")
    public void unloadAnalData(Long dr_task_id, String curr_bath_date, String drParams) {
        dataReceiveService.unloadAnalData(dr_task_id, curr_bath_date, drParams);
    }

    @PostMapping("/batchAnalData")
    public void unloadAnalData(@RequestBody List<BatchAnals> BatchAnals) {
        dataReceiveService.unloadAnalData(BatchAnals);
    }

    @RequestMapping("/getCategoryItems")
    public Result getCategoryItems(String category) {
        return dataReceiveService.getCategoryItems(category);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "task_ids", value = "", dataTypeClass = Long[].class), @ApiImplicitParam(name = "currPage", value = "", example = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", dataTypeClass = Integer.class) })
    @PostMapping("/getIsReleaseData")
    public Map<String, Object> getIsReleaseData(@RequestBody Map<String, Object> params) {
        List<String> task_idsL = new ArrayList<>();
        Object obj = params.get("task_ids");
        if (obj instanceof List) {
            task_idsL = (List<String>) obj;
        } else if (obj instanceof String) {
            String dd_idsStr = (String) obj;
            task_idsL.add(dd_idsStr);
        }
        Long[] task_ids = new Long[task_idsL.size()];
        for (int i = 0; i < task_ids.length; i++) {
            task_ids[i] = Long.parseLong(task_idsL.get(i));
        }
        int currPage = 1;
        if (params.get("currPage") != null) {
            currPage = Integer.parseInt(params.get("currPage").toString());
        }
        int pageSize = 10;
        if (params.get("pageSize") != null) {
            pageSize = Integer.parseInt(params.get("pageSize").toString());
        }
        return dataReceiveService.getIsReleaseData(task_ids, currPage, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "job_defs", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "task_id_list", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etlPre_job", value = "", dataTypeClass = String.class) })
    @PostMapping("/saveDataReceiveJobRelation")
    public void saveDataReceiveJobRelation(@RequestBody DistributeJobBean distributeJobBean) {
        List<List<String>> pre_etl_job_ids = distributeJobBean.getPreEtlJobIdList();
        List<Map<String, String>> task_ids = distributeJobBean.getDdIds();
        List<EtlJobDef> relation = distributeJobBean.getEtlJobDefList();
        dataReceiveService.saveDataReceiveJobRelation(pre_etl_job_ids, task_ids, relation);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getJobMsg")
    @ApiImplicitParam(name = "dd_ids", value = "", dataTypeClass = Long[].class)
    @PostMapping("/getJobMsg")
    public List<List<Map<String, Object>>> getJobMsg(@RequestBody Map<String, Object> params) {
        List<String> task_idsL = new ArrayList<>();
        Object obj = params.get("dd_ids");
        if (obj instanceof List) {
            task_idsL = (List<String>) obj;
        } else if (obj instanceof String) {
            String dd_idsStr = (String) obj;
            task_idsL.add(dd_idsStr);
        }
        Long[] task_ids = new Long[task_idsL.size()];
        for (int i = 0; i < task_ids.length; i++) {
            task_ids[i] = Long.parseLong(task_idsL.get(i));
        }
        return dataReceiveService.getJobMsg(task_ids);
    }

    @PostMapping("/testData")
    public Object test() {
        List<Object> objects = new ArrayList<>();
        Map<String, Object> test1 = new HashMap<>();
        test1.put("id", "1");
        test1.put("name", "张三");
        test1.put("age", 27);
        Map<String, Object> test2 = new HashMap<>();
        test2.put("sport", "篮球");
        test2.put("books", "西游记");
        test1.put("like", test2);
        objects.add(test1);
        return test1;
    }

    @RequestMapping("/test3")
    public Object test3() {
        List<Object> list = new ArrayList<>();
        Map<String, Object> test2 = new HashMap<>();
        test2.put("sport", "篮球");
        Map<String, Object> test3 = new HashMap<>();
        test3.put("sport", "足球");
        list.add(test2);
        list.add(test3);
        Map<String, Object> test1 = new HashMap<>();
        test1.put("name", "张三");
        test1.put("like", list);
        return test1;
    }

    @PostMapping("/testTable")
    public Object test1() {
        return SqlOperator.queryList(Dbo.db(), "select * from auto_label");
    }
}
