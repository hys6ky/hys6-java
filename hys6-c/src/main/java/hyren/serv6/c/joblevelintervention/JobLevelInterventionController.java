package hyren.serv6.c.joblevelintervention;

import hyren.serv6.base.user.UserUtil;
import hyren.serv6.c.joblevelintervention.dto.BatchJobLevelInterventionOperateDTO;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("etlMage/joblevelintervention")
@Slf4j
public class JobLevelInterventionController {

    @Autowired
    private JobLevelInterventionService service;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "jobHandBeans", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "job_priority", value = "", required = true, dataTypeClass = Integer.class) })
    @PostMapping("batchJobLevelInterventionOperate")
    public void batchJobLevelInterventionOperate(@RequestBody BatchJobLevelInterventionOperateDTO dto) {
        service.batchJobLevelInterventionOperate(dto, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_hand_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "job_priority", value = "", required = true, dataTypeClass = Integer.class) })
    @PostMapping("jobLevelInterventionOperate")
    public void jobLevelInterventionOperate(Long etl_sys_id, Long etl_job_id, String etl_hand_type, String curr_bath_date, Integer job_priority) {
        service.jobLevelInterventionOperate(etl_sys_id, etl_job_id, etl_hand_type, curr_bath_date, job_priority, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "5", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchJobLevelCurrInterventionByPage")
    public Map<String, Object> searchJobLevelCurrInterventionByPage(Long etl_sys_id, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "5") Integer pageSize) {
        return service.searchJobLevelCurrInterventionByPage(etl_sys_id, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "sub_sys_desc", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "job_status", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "5", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchJobLevelIntervention")
    public Map<String, Object> searchJobLevelIntervention(Long etl_sys_id, String etl_job, String sub_sys_desc, String job_status, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "5") Integer pageSize) {
        return service.searchJobLevelIntervention(etl_sys_id, etl_job, sub_sys_desc, job_status, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchJobLeverHisInterventionByPage")
    public Map<String, Object> searchJobLeverHisInterventionByPage(Long etl_sys_id, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchJobLeverHisInterventionByPage(etl_sys_id, currPage, pageSize, UserUtil.getUserId());
    }
}
