package hyren.serv6.k.scrap.tdb.bean;

import java.util.Map;

public class AdaptRelationBean {

    private Map<Long, Map<String, Object>> nodeCollection;

    private Map<Long, Map<String, Object>> relationCollection;

    public Map<Long, Map<String, Object>> getNodeCollection() {
        return nodeCollection;
    }

    public void setNodeCollection(Map<Long, Map<String, Object>> nodeCollection) {
        this.nodeCollection = nodeCollection;
    }

    public Map<Long, Map<String, Object>> getRelationCollection() {
        return relationCollection;
    }

    public void setRelationCollection(Map<Long, Map<String, Object>> relationCollection) {
        this.relationCollection = relationCollection;
    }
}
