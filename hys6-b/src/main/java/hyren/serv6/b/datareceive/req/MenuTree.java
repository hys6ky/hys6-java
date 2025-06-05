package hyren.serv6.b.datareceive.req;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MenuTree {

    public List<TreeNode> menuList;

    public List<Object> treeMenus = new ArrayList<Object>();

    public MenuTree(List<TreeNode> menu) {
        this.menuList = menu;
    }

    public List<Object> buildTree() {
        for (TreeNode node : menuList) {
            if (node.getPId().equals(0L)) {
                Map<String, Object> treeRoot = new LinkedHashMap<String, Object>();
                treeRoot.put("id", node.getId());
                treeRoot.put("label", node.getLabel());
                treeRoot.put("pid", node.getPId());
                treeRoot.put("children", buildChildTree(node.getId()));
                treeMenus.add(treeRoot);
            }
        }
        return treeMenus;
    }

    public List<?> buildChildTree(Long id) {
        List<Object> childMenus = new ArrayList<Object>();
        for (TreeNode node : menuList) {
            if (node.getPId().equals(id)) {
                Map<String, Object> treeChild = new LinkedHashMap<String, Object>();
                treeChild.put("id", node.getId());
                treeChild.put("label", node.getLabel());
                treeChild.put("pid", node.getPId());
                treeChild.put("children", buildChildTree(node.getId()));
                childMenus.add(treeChild);
            }
        }
        return childMenus;
    }
}
