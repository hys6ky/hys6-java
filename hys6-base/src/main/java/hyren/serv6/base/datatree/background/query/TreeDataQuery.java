package hyren.serv6.base.datatree.background.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.tree.TreePageSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2019/12/24 0024 上午 10:26")
public class TreeDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "tree_source", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getSourceTreeInfos(String tree_source) {
        List<Map<String, Object>> sourceTreeMenuInfos = new ArrayList<>();
        DataSourceType[] dataSourceTypes = TreePageSource.TREE_SOURCE.get(tree_source);
        for (DataSourceType dataSourceType : dataSourceTypes) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dataSourceType.getCode());
            map.put("label", dataSourceType.getValue());
            map.put("parent_id", "0");
            map.put("description", dataSourceType.getValue());
            sourceTreeMenuInfos.add(map);
        }
        return sourceTreeMenuInfos;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataLayers", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getSourceTreeInfos(DataSourceType[] dataSourceTypes) {
        List<Map<String, Object>> sourceTreeMenuInfos = new ArrayList<>();
        for (DataSourceType dataSourceType : dataSourceTypes) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dataSourceType.getCode());
            map.put("label", dataSourceType.getValue());
            map.put("parent_id", "0");
            map.put("description", dataSourceType.getValue());
            sourceTreeMenuInfos.add(map);
        }
        return sourceTreeMenuInfos;
    }
}
