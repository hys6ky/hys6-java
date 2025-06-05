package hyren.serv6.v.manage;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.user.UserUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ManageService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> getAutoAnalysisTreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(TreePageSource.WEB_SQL, UserUtil.getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }
}
