package hyren.serv6.b.batchcollection.dbAgentcollect.tableconf.req;

import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.entity.TableInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableDataReq implements Serializable {

    private static final long serialVersionUID = -8168213648990237087L;

    private Long colSetId;

    private TableInfo[] tableInfos;

    private Map<String, List<TableColumn>> tableColumns;
}
