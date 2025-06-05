package hyren.serv6.commons.utils.agentmonitor;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-05-15 10:20")
@Slf4j
public class AgentMonitorUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isPortOccupied(String agent_ip, String agent_port) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(agent_ip, Integer.parseInt(agent_port)));
        } catch (Exception e) {
            return false;
        } finally {
            try {
                socket.close();
            } catch (Exception e) {
                log.info("关闭socket连接失败", e);
            }
        }
        return true;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "host", desc = "", range = "")
    @Param(name = "port", desc = "", range = "")
    @Param(name = "startTime", desc = "", range = "", nullable = true, valueIfNull = "0")
    @Param(name = "endTime", desc = "", range = "", nullable = true, valueIfNull = "0")
    @Return(desc = "", range = "")
    public static ActionResult agentResourceInfo(AgentDownInfo agent_down_info, long startTime, long endTime) {
        HttpClient httpClient = new HttpClient();
        String url = "http://".concat(agent_down_info.getAgent_ip()).concat(":").concat(agent_down_info.getAgent_port()).concat(agent_down_info.getAgent_context()).concat(StringUtil.replace(agent_down_info.getAgent_pattern(), "/*", "")).concat(AgentActionUtil.COMPUTERRESOURCE);
        HttpClient.ResponseValue resVal = httpClient.addData("startTime", startTime).addData("endTime", endTime).post(url);
        ActionResult ar = null;
        if (StringUtil.isNotBlank(resVal.getBodyString())) {
            ar = ActionResult.toActionResult(resVal.getBodyString());
        }
        return ar;
    }
}
