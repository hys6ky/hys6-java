package hyren.serv6.b.realtimecollection.agentdeploy;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.AgentDownInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Api("/Agent部署管理")
@Slf4j
@RequestMapping("/dataCollectionO/agentdeploy")
@RestController
@Validated
public class SdmAgentDeployController {

    @Autowired
    SdmAgentDeployService sdmAgentDeployService;

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getSdmDataSourceInfo")
    public Result getSdmDataSourceInfo() {
        return sdmAgentDeployService.getSdmDataSourceInfo();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_type", value = "", dataTypeClass = String.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/getSdmAgentInfo")
    public List<Map<String, Object>> getSdmAgentInfo(long source_id, String sdm_agent_type) {
        return sdmAgentDeployService.getSdmAgentInfo(source_id, sdm_agent_type);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getBrokerServer")
    public String getBrokerServer() {
        return sdmAgentDeployService.getBrokerServer();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getSdmAgentDownInfo")
    public Map<String, Object> getSdmAgentDownInfo(@NotNull long agent_id) {
        return sdmAgentDeployService.getSdmAgentDownInfo(agent_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "req", value = "", dataTypeClass = Map.class)
    @RequestMapping("/saveSdmAgentDownInfo")
    public AgentDownInfo saveSdmAgentDownInfo(@RequestBody Map<String, Object> req) {
        AgentDownInfo agentDownInfo = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<AgentDownInfo>() {
        });
        String customPath = ReqDataUtils.getStringData(req, "customPath");
        String oldAgentDir = ReqDataUtils.getStringData(req, "oldAgentDir");
        String oldLogPath = ReqDataUtils.getStringData(req, "oldLogPath");
        return sdmAgentDeployService.saveSdmAgentDownInfo(agentDownInfo, customPath, oldAgentDir, oldLogPath);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "req", value = "", dataTypeClass = Map.class)
    @RequestMapping("/updateSdmAgentDownInfo")
    public void updateSdmAgentDownInfo(@RequestBody Map<String, Object> req) {
        AgentDownInfo agentDownInfo = JsonUtil.toObject(JsonUtil.toJson(req), new TypeReference<AgentDownInfo>() {
        });
        String customPath = ReqDataUtils.getStringData(req, "customPath");
        String oldAgentDir = ReqDataUtils.getStringData(req, "oldAgentDir");
        String oldLogPath = ReqDataUtils.getStringData(req, "oldLogPath");
        sdmAgentDeployService.updateSdmAgentDownInfo(agentDownInfo, customPath, oldAgentDir, oldLogPath);
    }

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getAgentNumAndSourceNum")
    public Map<String, Object> getAgentNumAndSourceNum() {
        return sdmAgentDeployService.getAgentNumAndSourceNum();
    }
}
