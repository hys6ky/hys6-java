package hyren.serv6.agent.trans.biz.cdccollect.req;

import java.util.List;
import org.apache.flink.util.CollectionUtil;
import lombok.Data;

@Data
public class TaskInfo {

    private Long taskId;

    private List<String> tableNames;

    private String scriptPath;
}
