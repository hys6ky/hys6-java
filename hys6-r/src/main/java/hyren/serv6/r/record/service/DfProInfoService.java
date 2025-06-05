package hyren.serv6.r.record.service;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.DfProInfo;
import java.util.List;
import java.util.Map;

public interface DfProInfoService {

    void saveInfo(DfProInfo dfProInfo);

    void updateInfo(DfProInfo dfProInfo);

    void updateSubmitStateById(Long dfPid, String submitState);

    Map<String, Object> queryDfProInfo(Integer currPage, Integer pageSize);

    Map<String, Object> queryListByNameAOrType(Integer currPage, Integer pageSize, String proName, String dfType);

    List<Map<String, Object>> queryAllDataLayer();

    Result getCategoryItems(String category);

    void deleteInfoByPid(Long dfPid);
}
