package hyren.serv6.b.batchcollection.agentdepoly;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.AgentDownInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/dataCollectionO/agentdeploy")
@RestController
@Validated
@Api(value = "", tags = "")
public class AgentDeployController {

    @Autowired
    AgentDeployService agentDeployService;

    @RequestMapping("/getDataSourceInfo")
    @ApiOperation(value = "", tags = "")
    public Result getDataSourceInfo() {
        return agentDeployService.getDataSourceInfo();
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", example = "", required = true, dataTypeClass = Long.class), @ApiImplicitParam(name = "agent_type", value = "", example = "", required = true, dataTypeClass = String.class) })
    @RequestMapping("/getAgentInfo")
    public List<Map<String, Object>> getAgentInfo(@NotNull Long source_id, @NotNull String agent_type) {
        return agentDeployService.getAgentInfo(source_id, agent_type);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "agent_id", value = "", example = "", required = true, dataTypeClass = Long.class)
    @RequestMapping("/getAgentDownInfo")
    public Map<String, Object> getAgentDownInfo(@NotNull Long agent_id) {
        return agentDeployService.getAgentDownInfo(agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_down_info", value = "", example = "", required = true, dataTypeClass = AgentDownInfo.class), @ApiImplicitParam(name = "customPath", value = "", example = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "oldAgentDir", value = "", example = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "oldLogPath", value = "", example = "", required = true, dataTypeClass = String.class) })
    @RequestMapping("/saveAgentDownInfo")
    public void saveAgentDownInfo(@NotNull AgentDownInfo agent_down_info, @RequestParam(defaultValue = "0") String customPath, String oldAgentDir, String oldLogPath) {
        agentDeployService.saveAgentDownInfo(agent_down_info, customPath, oldAgentDir, oldLogPath);
    }

    @ApiOperation(value = "", tags = "")
    @PostMapping("/downloadAgentConf")
    @ApiImplicitParam(name = "down_id", value = "", example = "", required = true, dataTypeClass = Long.class)
    public String downloadAgentConf(@NotNull Long down_id) {
        return agentDeployService.downloadAgentConf(down_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/downloadFile")
    @ApiImplicitParam(name = "fileName", value = "", example = "", dataTypeClass = String.class)
    public void downloadFile(String fileName) {
        agentDeployService.downloadFile(fileName);
    }
}
