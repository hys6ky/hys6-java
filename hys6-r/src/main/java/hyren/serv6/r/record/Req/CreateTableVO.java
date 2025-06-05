package hyren.serv6.r.record.Req;

import lombok.Data;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class CreateTableVO implements Serializable {

    private static final long serialVersionUID = -1732209152756256636L;

    List<String> keyData;

    List<Map<String, String>> data;

    Long dfPid;

    String tableName;

    Long tableId;
}
