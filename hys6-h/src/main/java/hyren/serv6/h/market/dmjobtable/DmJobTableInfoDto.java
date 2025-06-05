package hyren.serv6.h.market.dmjobtable;

import hyren.serv6.base.entity.DmJobTableInfo;
import lombok.Data;

@Data
public class DmJobTableInfoDto extends DmJobTableInfo {

    private String task_name;

    private String task_number;
}
