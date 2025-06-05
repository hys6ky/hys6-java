package hyren.serv6.f.dataRegister.sourceList.etljobs;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@Slf4j
@RequestMapping("/dataRegister/agentList/etljobs")
@RestController
@Validated
public class EtlJobController {

    @Autowired
    public EtlJobService etlJobService;

    @RequestMapping("/saveEtlJobs")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "database_id", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sub_sys_id", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_type", value = "", dataTypeClass = String.class) })
    public void saveEtlJobs(@NotNull String database_id, @NotNull Long etl_sys_id, @NotNull Long sub_sys_id, @NotNull String agent_type) {
        etlJobService.saveEtlJobs(database_id, etl_sys_id, sub_sys_id, agent_type);
    }
}
