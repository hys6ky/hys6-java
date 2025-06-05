package hyren.serv6.b.batchcollection.cdcCollect.req;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class CDCTaskRunStatus {

    private List<Status> collectTaskList;

    private List<Status> syncTaskList;

    public CDCTaskRunStatus() {
        this.collectTaskList = new ArrayList<CDCTaskRunStatus.Status>();
        this.syncTaskList = new ArrayList<CDCTaskRunStatus.Status>();
    }

    @Data
    @AllArgsConstructor
    public static class Status {

        private Long pId;

        private String tableName;

        private Boolean status;
    }
}
