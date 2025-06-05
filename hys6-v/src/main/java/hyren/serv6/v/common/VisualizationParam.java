package hyren.serv6.v.common;

import hyren.serv6.base.entity.AutoTpCondInfo;
import hyren.serv6.base.entity.AutoTpResSet;
import lombok.Data;
import java.io.Serializable;
import java.util.List;

@Data
public class VisualizationParam implements Serializable {

    private List<AutoTpCondInfo> autoTpCondInfos;

    private List<AutoTpResSet> autoTpResSets;

    private String template_desc;

    private String data_source;

    private String template_name;

    private String template_sql;
}
