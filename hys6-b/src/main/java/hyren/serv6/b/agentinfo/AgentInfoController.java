package hyren.serv6.b.agentinfo;

import hyren.serv6.base.entity.AgentInfo;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Slf4j
@RequestMapping("/dataCollectionM/agentInfo")
@RestController
@Validated
public class AgentInfoController {

    @Autowired
    AgentInfoService agentInfoService;

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "source_id", value = "", example = "", dataTypeClass = Long.class)
    @RequestMapping("/searchDatasourceAndAgentInfo")
    public Map<String, Object> searchDatasourceAndAgentInfo(@NotNull Long source_id) {
        return agentInfoService.searchDatasourceAndAgentInfo(source_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteAgent")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_type", value = "", example = "", dataTypeClass = Long.class) })
    public void deleteAgent(@NotNull Long source_id, @NotNull Long agent_id, @NotNull String agent_type) {
        agentInfoService.deleteAgent(source_id, agent_id, agent_type);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchAgent")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_type", value = "", example = "", dataTypeClass = String.class) })
    public Map<String, Object> searchAgent(@NotNull Long agent_id, String agent_type) {
        return agentInfoService.searchAgent(agent_id, agent_type);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateAgent")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_type", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_name", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_ip", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_port", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "source_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "user_id", value = "", example = "", dataTypeClass = Long.class) })
    public void updateAgent(@NotNull Long agent_id, String agent_name, String agent_type, String agent_ip, String agent_port, @NotNull Long source_id, @NotNull Long user_id) {
        agentInfoService.updateAgent(agent_id, agent_name, agent_type, agent_ip, agent_port, source_id, user_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveAgent")
    @ApiImplicitParam(name = "agentInfo", example = "", value = "", dataTypeClass = AgentInfo.class)
    public void saveAgent(AgentInfo agentInfo) {
        agentInfoService.saveAgent(agentInfo);
    }
}
