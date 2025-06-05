package hyren.serv6.t.vo.save;

import hyren.serv6.t.entity.TskTestPointVarConf;
import lombok.Data;
import java.util.List;

@Data
public class VarConfigUpdateVo {

    private Long rel_id;

    private List<TskTestPointVarConf> varConfs;
}
