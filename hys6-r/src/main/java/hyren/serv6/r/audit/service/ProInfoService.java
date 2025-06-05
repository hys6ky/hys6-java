package hyren.serv6.r.audit.service;

import java.util.List;

public interface ProInfoService {

    public Boolean passList(List<Long> dfPids);

    public Boolean refuseList(List<Long> df_pids, String audit_opinion, String remarks);
}
