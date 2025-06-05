package hyren.serv6.k.dbm.tree;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.k.dbm.tree.query.DbmDataQuery;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class DbmTreeInfoService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDbmSortInfoTreeData() {
        Map<String, Object> dbmSortInfoTreeDataMap = new HashMap<>();
        Result dbmSortInfoRs = DbmDataQuery.getDbmSortInfos(UserUtil.getUser());
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (int i = 0; i < dbmSortInfoRs.getRowCount(); i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dbmSortInfoRs.getString(i, "sort_id"));
            map.put("label", dbmSortInfoRs.getString(i, "sort_name"));
            map.put("parent_id", dbmSortInfoRs.getString(i, "parent_id"));
            map.put("description", dbmSortInfoRs.getString(i, "sort_name"));
            dataList.add(map);
        }
        List<Node> dbmSortInfoTreeDataList = NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
        String str = dbmSortInfoTreeDataList.toString();
        dbmSortInfoTreeDataMap.put("dbmSortInfoTreeDataList", JsonUtil.toObjectSafety(str, List.class).orElse(Collections.emptyList()));
        return dbmSortInfoTreeDataMap;
    }
}
