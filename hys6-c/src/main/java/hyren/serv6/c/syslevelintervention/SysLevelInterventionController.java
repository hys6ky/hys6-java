package hyren.serv6.c.syslevelintervention;

import hyren.serv6.base.user.UserUtil;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("etlMage/syslevelintervention")
@Slf4j
public class SysLevelInterventionController {

    @Autowired
    private SysLevelInterventionService service;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchSysLevelCurrInterventionInfo")
    public List<Map<String, Object>> searchSysLevelCurrInterventionInfo(Long etl_sys_id) {
        return service.searchSysLevelCurrInterventionInfo(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchSysLeverHisInterventionByPage")
    public Map<String, Object> searchSysLeverHisInterventionByPage(Long etl_sys_id, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchSysLeverHisInterventionByPage(etl_sys_id, currPage, pageSize);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchSystemBatchConditions")
    public Map<String, Object> searchSystemBatchConditions(Long etl_sys_id) {
        return service.searchSystemBatchConditions(etl_sys_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_hand_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class) })
    @PostMapping("sysLevelInterventionOperate")
    public void sysLevelInterventionOperate(Long etl_sys_id, String etl_hand_type, String curr_bath_date) {
        service.sysLevelInterventionOperate(etl_sys_id, etl_hand_type, curr_bath_date, UserUtil.getUserId());
    }
}
