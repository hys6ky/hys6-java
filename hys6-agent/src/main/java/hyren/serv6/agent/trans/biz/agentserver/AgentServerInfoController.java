package hyren.serv6.agent.trans.biz.agentserver;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Api("获取当前程序所在的服务器信息的接口类")
@RestController
@RequestMapping("/agentserver")
public class AgentServerInfoController {

    @Autowired
    public AgentServerInfoService serverInfo;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getServerInfo")
    public Map<String, Object> getServerInfo() {
        return serverInfo.getServerInfo();
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "pathVal", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "isFile", value = "", dataTypeClass = String.class) })
    @RequestMapping("/getSystemFileInfo")
    public List<Map<String, String>> getSystemFileInfo(String pathVal, @RequestParam(defaultValue = "false") String isFile) {
        return serverInfo.getSystemFileInfo(pathVal, isFile);
    }
}
