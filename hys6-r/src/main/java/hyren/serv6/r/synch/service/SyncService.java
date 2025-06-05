package hyren.serv6.r.synch.service;

import fd.ng.db.jdbc.Page;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.ProjectInfo;
import java.util.List;

public interface SyncService {

    public Boolean sync(Long applyTabId);

    public Boolean rollback(Long applyTabId);

    public PageDTO<ProjectInfo> page(List<String> statesList, List<String> noStatesList, String name, String dfType, List<String> dfAppStateList, List<String> noDfAppStateList, Integer startDate, Integer endDate, Page page);
}
