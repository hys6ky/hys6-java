package hyren.serv6.commons.jobUtil.beans;

import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.entity.EtlJobResourceRela;
import java.util.List;

public class EtlJobDefBean extends EtlJobDef {

    private List<EtlJobResourceRela> jobResources;

    public EtlJobDefBean() {
        super();
    }

    public List<EtlJobResourceRela> getJobResources() {
        return jobResources;
    }

    public void setJobResources(List<EtlJobResourceRela> jobResources) {
        this.jobResources = jobResources;
    }
}
