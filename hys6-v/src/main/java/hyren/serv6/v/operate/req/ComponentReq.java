package hyren.serv6.v.operate.req;

import hyren.serv6.base.entity.AutoCompCond;
import hyren.serv6.base.entity.AutoCompDataSum;
import hyren.serv6.base.entity.AutoCompGroup;
import hyren.serv6.v.bean.ComponentBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ComponentReq {

    private ComponentBean componentBean;

    private AutoCompCond[] autoCompConds;

    private AutoCompGroup[] autoCompGroups;

    private AutoCompDataSum[] autoCompDataSums;
}
