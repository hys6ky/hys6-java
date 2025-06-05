package hyren.serv6.b.agent.bean;

import fd.ng.core.annotation.DocClass;
import lombok.Data;

@Data
@DocClass(desc = "", author = "WangZhengcheng")
public class TableCleanParam {

    private Long tableId;

    private String tableName;

    private boolean complementFlag;

    private boolean replaceFlag;

    private boolean trimFlag;
}
