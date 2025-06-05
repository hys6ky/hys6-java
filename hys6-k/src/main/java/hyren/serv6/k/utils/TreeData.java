package hyren.serv6.k.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.User;
import java.util.List;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/5/27 14:58")
public class TreeData {

    @Method(desc = "", logicStep = "")
    @Param(name = "tree_source", desc = "", range = "")
    @Param(name = "user", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Node> initTreeData(String tree_source, TreeConf treeConf, User user) {
        if (!TreePageSource.treeSourceList.contains(tree_source)) {
            throw new BusinessException("tree_source=" + tree_source + "不合法，请检查！");
        }
        return NodeDataConvertedTreeList.dataConversionTreeInfo(TreeNodeInfo.getTreeNodeInfo(tree_source, user, treeConf));
    }
}
