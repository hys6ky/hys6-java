package hyren.serv6.k.dm.metadatamanage.drbtree;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.background.query.TreeDataQuery;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import hyren.serv6.k.dm.metadatamanage.drbtree.utils.DRBTreeNodeDataQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/30 0030 上午 09:25")
public class DRBTreeNodeInfo {

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
    @Param(name = "sourceTreeInfos", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Param(name = "treeConf", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<Map<String, Object>> getDataList(List<Map<String, Object>> sourceTreeInfos, User user, TreeConf treeConf) {
        List<Map<String, Object>> dataList = new ArrayList<>(sourceTreeInfos);
        sourceTreeInfos.forEach(sourceTreeInfo -> {
            DataSourceType dataSourceType = DataSourceType.ofEnumByCode(sourceTreeInfo.get("id").toString());
            if (dataSourceType == DataSourceType.ISL) {
                DRBTreeNodeDataQuery.getISLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DCL) {
                DRBTreeNodeDataQuery.getDCLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DPL) {
                DRBTreeNodeDataQuery.getDPLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DML) {
                DRBTreeNodeDataQuery.getDMLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.SFL) {
                DRBTreeNodeDataQuery.getSFLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.AML) {
                DRBTreeNodeDataQuery.getAMLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.DQC) {
                DRBTreeNodeDataQuery.getDQCDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.UDL) {
                DRBTreeNodeDataQuery.getUDLDataList(user, dataList, treeConf);
            } else if (dataSourceType == DataSourceType.KFK) {
                DRBTreeNodeDataQuery.getKFKDataList(user, dataList, treeConf);
            } else {
                throw new BusinessException("未找到匹配的数据层!" + sourceTreeInfo.get("id").toString());
            }
        });
        return dataList;
    }
}
