package hyren.serv6.b.realtimecollection.util;

import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.exception.BusinessException;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Component
public class SdmAgentActionUtil {

    private static final List<String> list;

    public static final String KAFKARESTSTARTINFO = "/agent/datamessagestream/execute";

    public static final String KAFKAFILECATALOGUE = "/agent/systemfile/getSystemFileInfo";

    public static final String KAFKADATADICTIONARY = "/agent/streamdatadictionary/readDataDictionary";

    public static final String KAFKAWENBENSTARTINFO = "/agent/filecontentstream/execute";

    static {
        list = new ArrayList<>();
        list.add(KAFKARESTSTARTINFO);
        list.add(KAFKAFILECATALOGUE);
        list.add(KAFKADATADICTIONARY);
        list.add(KAFKAWENBENSTARTINFO);
    }

    private SdmAgentActionUtil() {
    }

    public static String getUrl(long agent_id, long user_id, String methodName) {
        if (!list.contains(methodName)) {
            throw new BusinessException("Sdm被调用的agent接口" + methodName + "没有登记");
        }
        AgentDownInfo agent_down_info = Dbo.queryOneObject(AgentDownInfo.class, "SELECT distinct t1.agent_ip,t1.agent_port,t1.agent_context,t1.agent_pattern " + "FROM " + AgentDownInfo.TableName + " t1 join " + AgentInfo.TableName + " t2 on t1.agent_ip = t2.agent_ip and t1.agent_port=" + "t2.agent_port where  t2.agent_id= ? and t2.user_id = ?", agent_id, user_id).orElseThrow(() -> new BusinessException("根据sdm_agent_id:" + agent_id + "查询不到部署信息"));
        return "http://" + agent_down_info.getAgent_ip() + ":" + agent_down_info.getAgent_port() + methodName;
    }
}
