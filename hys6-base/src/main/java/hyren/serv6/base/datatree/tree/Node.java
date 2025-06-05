package hyren.serv6.base.datatree.tree;

import fd.ng.core.annotation.DocClass;
import lombok.Getter;
import lombok.Setter;
import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@DocClass(desc = "", author = "BY-HLL", createdate = "2020/2/20 0020 下午 07:02")
public class Node {

    private String id = "";

    private String label = "";

    private String parent_id = "";

    private String description = "";

    private String data_layer = "";

    private String data_own_type = "";

    private String dsl_id = "";

    private String dsl_store_type = "";

    private String data_source_id = "";

    private String agent_id = "";

    private String classify_id = "";

    private String file_id = "";

    private String table_name = "";

    private String original_name = "";

    private String hyren_name = "";

    private String tree_page_source = "";

    private String type = "text";

    private List<Node> children = new ArrayList<>();

    public String toString() {
        String str = "{" + "\"id\" : \"" + id + "\"" + ", \"label\" : \"" + label + "\"" + ", \"parent_id\" : \"" + parent_id + "\"" + ", \"description\" : \"" + description + "\"" + ", \"data_layer\" : \"" + data_layer + "\"" + ", \"dsl_id\" : \"" + dsl_id + "\"" + ", \"data_own_type\" : \"" + data_own_type + "\"" + ", \"data_source_id\" : \"" + data_source_id + "\"" + ", \"agent_id\" : \"" + agent_id + "\"" + ", \"classify_id\" : \"" + classify_id + "\"" + ", \"file_id\" : \"" + file_id + "\"" + ", \"table_name\" : \"" + table_name + "\"" + ", \"original_name\" : \"" + original_name + "\"" + ", \"hyren_name\" : \"" + hyren_name + "\"" + ", \"type\" : \"" + type + "\"" + ", \"tree_page_source\" : \"" + tree_page_source + "\"";
        if (children != null && !children.isEmpty()) {
            str += ", \"children\" : " + children;
        } else {
            str += ", \"leaf\" : false";
        }
        return str + "}";
    }

    void sortChildren() {
        if (children != null && !children.isEmpty()) {
            children.sort(new NodeIDComparator());
            children.forEach(Node::sortChildren);
        }
    }

    void addChild(Node node) {
        this.children.add(node);
    }
}
