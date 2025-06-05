package hyren.serv6.base.datatree.background;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.background.query.TreeDataQuery;
import hyren.serv6.base.datatree.background.utils.TreeNodeDataQuery;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/13 0013 上午 10:24")
public class TreeNodeInfo {

    @Method(desc = "", logicStep = "")
    @Param(name = "tree_source", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getTreeNodeInfo(String tree_source, User user, TreeConf treeConf) {
        List<Map<String, Object>> sourceTreeInfos = TreeDataQuery.getSourceTreeInfos(tree_source);
        return getDataList(sourceTreeInfos, user, treeConf);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataLayers", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getTreeNodeInfo(DataSourceType[] dataLayers, User user, TreeConf treeConf) {
        List<Map<String, Object>> sourceTreeInfos = TreeDataQuery.getSourceTreeInfos(dataLayers);
        return getDataList(sourceTreeInfos, user, treeConf);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceTreeInfos", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<Map<String, Object>> getDataList(List<Map<String, Object>> sourceTreeInfos, User user, TreeConf treeConf) {
        List<Map<String, Object>> dataList = new ArrayList<>(sourceTreeInfos);
        sourceTreeInfos.forEach(sourceTreeInfo -> {
            DataSourceType dataSourceType = DataSourceType.ofEnumByCode(sourceTreeInfo.get("id").toString());
            if (dataSourceType == DataSourceType.ISL) {
                TreeNodeDataQuery.getISLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DCL) {
                TreeNodeDataQuery.getDCLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DPL) {
                TreeNodeDataQuery.getDPLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DML) {
                TreeNodeDataQuery.getDMLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.SFL) {
                TreeNodeDataQuery.getSFLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.AML) {
                TreeNodeDataQuery.getAMLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DQC) {
                TreeNodeDataQuery.getDQCDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.UDL) {
                TreeNodeDataQuery.getUDLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.KFK) {
                TreeNodeDataQuery.getKFKDataList(user, dataList, treeConf);
            } else {
                throw new BusinessException("未找到匹配的数据层!" + sourceTreeInfo.get("id").toString());
            }
        });
        return dataList;
    }
}
