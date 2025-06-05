package hyren.serv6.r.record.Req;

import fd.ng.db.jdbc.DefaultPageImpl;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class SelectData extends DefaultPageImpl {

    private String tableName;

    private String createUserId;

    private String createDate;

    private String updateDate;

    private Long dfPid;

    private Long applyTabId;

    private Long targetTableId;

    private List<Map<String, String>> data;
}
