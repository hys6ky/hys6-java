package hyren.serv6.b.realtimecollection.realtimeCollectManagement.agent;

import fd.ng.core.annotation.Return;
import hyren.serv6.base.entity.AgentInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("/dataCollectionM/agent")
@Api
public class SdmAgentInfoController {

    @Autowired
    SdmAgentInfoService sdmAgentInfoService;

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/searchSdmDatasourceAndSdmAgentInfo")
    public Map<String, Object> searchSdmDatasourceAndSdmAgentInfo(long sdm_source_id) {
        return sdmAgentInfoService.searchSdmDatasourceAndSdmAgentInfo(sdm_source_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_type", value = "", dataTypeClass = String.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/searchSdmAgent")
    public Map<String, Object> searchSdmAgent(long sdm_agent_id, String sdm_agent_type) {
        return sdmAgentInfoService.searchSdmAgent(sdm_agent_id, sdm_agent_type);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdmAgentInfo", value = "", dataTypeClass = AgentInfo.class)
    @RequestMapping("/saveSdmAgentInfo")
    public void saveSdmAgentInfo(AgentInfo sdmAgentInfo) {
        sdmAgentInfoService.saveSdmAgentInfo(sdmAgentInfo);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_type", value = "", dataTypeClass = String.class) })
    @RequestMapping("/deleteSdmAgent")
    public void deleteSdmAgent(long sdm_source_id, long sdm_agent_id, String sdm_agent_type) {
        sdmAgentInfoService.deleteSdmAgent(sdm_source_id, sdm_agent_id, sdm_agent_type);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_agent_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_agent_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_agent_ip", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_agent_port", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "user_id", value = "", dataTypeClass = Long.class) })
    @RequestMapping("/updateSdmAgent")
    public void updateSdmAgent(long sdm_agent_id, String sdm_agent_name, String sdm_agent_type, String sdm_agent_ip, String sdm_agent_port, long sdm_source_id, long user_id) {
        sdmAgentInfoService.updateSdmAgent(sdm_agent_id, sdm_agent_name, sdm_agent_type, sdm_agent_ip, sdm_agent_port, sdm_source_id, user_id);
    }
}
