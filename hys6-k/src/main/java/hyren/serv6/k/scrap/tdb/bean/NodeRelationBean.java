package hyren.serv6.k.scrap.tdb.bean;

import lombok.Data;
import java.util.Map;

@Data
public class NodeRelationBean {

    private Map<Long, Map<String, Object>> leftNode;

    private Map<Long, Map<String, Object>> rightNode;

    private String relationType;

    private Long relationId;

    public Map<Long, Map<String, Object>> getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(Map<Long, Map<String, Object>> leftNode) {
        this.leftNode = leftNode;
    }

    public Map<Long, Map<String, Object>> getRightNode() {
        return rightNode;
    }

    @Override
    public String toString() {
        return "NodeRelationBean{" + "leftNode=" + leftNode + ", rightNode=" + rightNode + ", relationType='" + relationType + '\'' + ", relationId='" + relationId + '\'' + '}';
    }
}
