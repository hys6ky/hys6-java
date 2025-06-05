package hyren.serv6.b.agent.bean;

import fd.ng.core.annotation.DocClass;
import lombok.Data;

@Data
@DocClass(desc = "", author = "WangZhengcheng")
public class ColumnCleanParam {

    private Long columnId;

    private boolean complementFlag;

    private boolean replaceFlag;

    private boolean formatFlag;

    private boolean conversionFlag;

    private boolean spiltFlag;

    private boolean trimFlag;
}
