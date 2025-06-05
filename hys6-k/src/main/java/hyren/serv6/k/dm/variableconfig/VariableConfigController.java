package hyren.serv6.k.dm.variableconfig;

import hyren.serv6.k.entity.DqSysCfg;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Nullable;
import java.util.List;

@Api(tags = "")
@RestController()
@RequestMapping("/dm/variableconfig")
public class VariableConfigController {

    @Autowired
    VariableConfigService variableConfigService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_sys_cfg", value = "", dataTypeClass = DqSysCfg.class, example = "")
    @PostMapping("/addVariableConfigDat")
    public void addVariableConfigDat(DqSysCfg dq_sys_cfg) {
        variableConfigService.addVariableConfigDat(dq_sys_cfg);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sys_var_id_s", value = "", dataTypeClass = Long[].class, example = "")
    @PostMapping("/deleteVariableConfigData")
    public void deleteVariableConfigData(Long[] sys_var_id_s) {
        variableConfigService.deleteVariableConfigData(sys_var_id_s);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dq_sys_cfg", value = "", dataTypeClass = DqSysCfg.class, example = "")
    @PostMapping("/updateVariableConfigData")
    public void updateVariableConfigData(DqSysCfg dq_sys_cfg) {
        variableConfigService.updateVariableConfigData(dq_sys_cfg);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getVariableConfigDataInfos")
    public List<DqSysCfg> getVariableConfigDataInfos() {
        return variableConfigService.getVariableConfigDataInfos();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sys_var_id", value = "", dataTypeClass = long.class, example = "")
    @PostMapping("/getVariableConfigDataInfo")
    public DqSysCfg getVariableConfigDataInfo(long sys_var_id) {
        return variableConfigService.getVariableConfigDataInfo(sys_var_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "var_name", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "var_value", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "start_date", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "end_date", value = "", dataTypeClass = String.class, example = "") })
    @PostMapping("/searchVariableConfigData")
    public List<DqSysCfg> searchVariableConfigData(@Nullable String var_name, @Nullable String var_value, @Nullable String start_date, @Nullable String end_date) {
        return variableConfigService.searchVariableConfigData(var_name, var_value, start_date, end_date);
    }
}
