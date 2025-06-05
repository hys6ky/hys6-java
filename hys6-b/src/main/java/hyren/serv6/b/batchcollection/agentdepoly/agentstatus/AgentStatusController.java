package hyren.serv6.b.batchcollection.agentdepoly.agentstatus;

import hyren.serv6.base.entity.AgentDownInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@Api("/Agent运行状态管理")
@RequestMapping("/dataCollectionO/agentdeploy/agentstatus")
@Validated
public class AgentStatusController {

    @Autowired
    AgentStatusService agentStatusService;

    @RequestMapping("/agentInfo")
    public List<Map<String, Object>> agentInfo() {
        return agentStatusService.agentInfo();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/restartAgent")
    @ApiImplicitParam(name = "agent_down_info", value = "", dataTypeClass = AgentDownInfo.class)
    public void restartAgent(@NotNull AgentDownInfo agent_down_info) {
        agentStatusService.restartAgent(agent_down_info);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/stopAgent")
    @ApiImplicitParam(name = "agent_down_info", value = "", dataTypeClass = AgentDownInfo.class)
    public void stopAgent(@NotNull AgentDownInfo agent_down_info) {
        agentStatusService.stopAgent(agent_down_info);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/startAgent")
    @ApiImplicitParam(name = "agent_down_info", value = "", dataTypeClass = AgentDownInfo.class)
    public void startAgent(@NotNull AgentDownInfo agent_down_info) {
        agentStatusService.startAgent(agent_down_info);
    }
}
