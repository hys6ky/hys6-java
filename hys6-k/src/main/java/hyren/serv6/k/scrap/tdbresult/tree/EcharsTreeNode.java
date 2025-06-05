package hyren.serv6.k.scrap.tdbresult.tree;

import java.util.ArrayList;
import java.util.List;

public class EcharsTreeNode {

    private String id;

    private String name;

    private String value;

    private String parent_id;

    private List<EcharsTreeNode> children = new ArrayList<>();

    public String toString() {
        String str = "{" + "name : '" + name + "'" + ", value : '" + value + "'" + ", parent_id : '" + parent_id + "'";
        if (children != null && children.size() != 0) {
            str += ", children : " + children.toString();
        }
        return str + "}";
    }

    void addChild(EcharsTreeNode echarsTreeNode) {
        this.children.add(echarsTreeNode);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getParent_id() {
        return parent_id;
    }

    public void setParent_id(String parent_id) {
        this.parent_id = parent_id;
    }

    public List<EcharsTreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<EcharsTreeNode> children) {
        this.children = children;
    }
}
