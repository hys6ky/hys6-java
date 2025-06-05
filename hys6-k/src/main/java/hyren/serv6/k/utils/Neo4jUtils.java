package hyren.serv6.k.utils;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.k.scrap.tdb.bean.AdaptRelationBean;
import hyren.serv6.k.scrap.tdb.bean.NodeRelationBean;
import org.neo4j.driver.*;
import java.io.Closeable;
import java.util.List;
import java.util.Map;

public class Neo4jUtils implements Closeable {

    Driver driver;

    Session session;

    public Neo4jUtils() {
        String neo4j_uri = PropertyParaValue.getString("neo4jUri", "bolt://172.168.0.60:7687");
        String neo4j_user = PropertyParaValue.getString("neo4j_user", "neo4j");
        String neo4j_password = PropertyParaValue.getString("neo4j_password", "hrsdxg");
        driver = GraphDatabase.driver(neo4j_uri, AuthTokens.basic(neo4j_user, neo4j_password));
        session = driver.session();
    }

    public Neo4jUtils(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        session = driver.session();
    }

    private void addDataToNeo4j(String execute, Map<String, Object> parameters) {
        session.writeTransaction(tx -> tx.run(execute, parameters));
    }

    public Result queryNeo4j(String query) {
        return session.run(query);
    }

    public void executeNeo4j(String execute) {
        session.writeTransaction(tx -> tx.run(execute));
    }

    @Override
    public void close() {
        session.close();
        driver.close();
    }

    public List<NodeRelationBean> searchFromNeo4j(String cypher) {
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public Map<Long, Map<String, Object>> searchAllTableOfNodes(String limitNum) {
        String cypher = "MATCH (n:Table) RETURN n";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getNodeInfo(session.run(cypher));
    }

    public Map<Long, Map<String, Object>> searchAllColumnOfNodes(String limitNum) {
        String cypher = "MATCH (n:Column) RETURN n";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getNodeInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchColumnOfBfdRelation(String limitNum) {
        String cypher = "MATCH p=()-[r:BFD]->() RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchColumnOfIncludeRelation(String limitNum) {
        String cypher = "MATCH p=()-[r:INCLUDE]->() RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchColumnOfFkRelation(String limitNum) {
        String cypher = "MATCH p=()-[r:INCLUDE]->() RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchColumnOfFdRelation(String limitNum) {
        String cypher = "MATCH p=()-[r:FD]->() RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchColumnOfEqualsRelation(String limitNum) {
        String cypher = "MATCH p=()-[r:EQUALS]->() RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchColumnOfSameRelation(String limitNum) {
        String cypher = "MATCH p=()-[r:SAME]->() RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<Map<String, Object>> searchLabelPropagation(String relationship, int iterations, String limitNum) {
        String cypher = "CALL algo.labelPropagation.stream('Column', '" + relationship + "',{direction: 'OUTGOING', iterations: " + iterations + "})" + " yield  nodeId,label" + " return algo.getNodeById(nodeId).name as name ,label";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getLabelPropagationResult(session.run(cypher));
    }

    public List<Map<String, Object>> searchLouVain(String relationship, int iterations, String limitNum) {
        String cypher = "call algo.louvain.stream('Column','" + relationship + "', {iterations:" + iterations + "})" + " YIELD nodeId,community" + " return algo.getNodeById(nodeId).name as name ,community";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getLouVainResult(session.run(cypher));
    }

    public List<AdaptRelationBean> searchAllShortPath(String columnNodeName1, String columnNodeName2, int level, String limitNum) {
        String cypher = "MATCH (p1:Column {name:'" + columnNodeName1 + "'})," + " (p2:Column {name:'" + columnNodeName2 + "'})," + " p=allShortestPaths((p1)-[*.." + level + "]-(p2))" + " RETURN p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getAdaptRelationInfo(session.run(cypher));
    }

    public List<AdaptRelationBean> searchLongestPath(String columnNodeName1, String columnNodeName2, int level, String limitNum) {
        String cypher = "MATCH (a:Column {name:'" + columnNodeName1 + "'})," + " (b:Column {name:'" + columnNodeName2 + "'}),\n" + " p=(a)-[*.." + level + "]-(b)\n" + " RETURN p, length(p) ORDER BY length(p) DESC";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getAdaptRelationInfo(session.run(cypher));
    }

    public List<NodeRelationBean> searchNeighbors(String columnNodeName, int level, String limitNum) {
        String cypher = "MATCH p=(:Column {name:'" + columnNodeName + "'})-[*.." + level + "]-() return p";
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getRelationInfo(session.run(cypher));
    }

    public List<AdaptRelationBean> searchTriangleRelation(String relationship, String limitNum) {
        String cypher = "";
        if (!StringUtil.isEmpty(relationship)) {
            cypher = "match b=(a)-[:" + relationship + "]-()-[:" + relationship + "]-()-[:" + relationship + "]-(a) return b";
        } else {
            cypher = "match b=(a)-[]-()-[]-()-[]-(a) return b";
        }
        if (!StringUtil.isEmpty(limitNum)) {
            cypher += " LIMIT " + limitNum;
        }
        return StandardGraphUtils.getAdaptRelationInfo(session.run(cypher));
    }

    public static void main(String... args) {
        try (Neo4jUtils neo4jUtils = new Neo4jUtils()) {
            System.out.println(neo4jUtils.searchAllShortPath("S10_I_CHOU_ACCT_CAT_HOU_KIND", "S10_I_CHOU_ACCT_CAT_OWN_NAME", 5, "10"));
        }
    }
}
