package hyren.serv6.a.syspara;

import hyren.serv6.base.entity.SysPara;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Api(tags = "")
@RestController()
@RequestMapping("/systemParameters")
public class SysParaController {

    @Autowired
    SysParaService sysParaService;

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", required = true, dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", required = true, dataTypeClass = Integer.class), @ApiImplicitParam(name = "paraName", value = "", example = "", required = true, dataTypeClass = String.class) })
    @PostMapping("/getSysPara")
    public Map<String, Object> getSysPara(int currPage, int pageSize, String paraName) {
        return sysParaService.getSysPara(currPage, pageSize, paraName);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/deleteSysPara")
    public void deleteSysPara(@NotNull(message = "") long para_id, @NotNull(message = "") String para_name) {
        sysParaService.deleteSysPara(para_id, para_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sys_para", value = "", example = "", dataTypeClass = SysPara.class)
    @PostMapping("/addSysPara")
    public void addSysPara(SysPara sysPara) {
        sysParaService.addSysPara(sysPara);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sys_para", value = "", example = "", dataTypeClass = SysPara.class)
    @PostMapping("/updateSysPara")
    public void updateSysPara(SysPara sys_para) {
        sysParaService.updateSysPara(sys_para);
    }
}
