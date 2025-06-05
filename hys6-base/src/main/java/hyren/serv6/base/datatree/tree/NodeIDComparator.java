package hyren.serv6.base.datatree.tree;

import fd.ng.core.annotation.DocClass;
import java.util.Comparator;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/2/20 0020 下午 07:06")
public class NodeIDComparator implements Comparator<Node> {

    public int compare(Node n1, Node n2) {
        return -n1.getId().compareTo(n2.getId());
    }
}
