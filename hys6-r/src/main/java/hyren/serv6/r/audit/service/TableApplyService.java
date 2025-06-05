package hyren.serv6.r.audit.service;

import fd.ng.db.jdbc.Page;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.TableApplySyncInfo;

public interface TableApplyService {

    public PageDTO<TableApplySyncInfo> page(Long dfPid, String fuzzyName, String status, Page page);
}
