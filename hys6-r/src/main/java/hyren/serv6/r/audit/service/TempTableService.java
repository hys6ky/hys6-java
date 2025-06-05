package hyren.serv6.r.audit.service;

import java.util.List;
import java.util.Map;

public interface TempTableService {

    public List<Map<String, Object>> queryData(long applyTableId);
}
