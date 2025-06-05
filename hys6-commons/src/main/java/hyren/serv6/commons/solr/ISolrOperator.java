package hyren.serv6.commons.solr;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.IsFlag;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/9 0009 上午 11:34")
public interface ISolrOperator extends Closeable {

    void testConnectSolr();

    SolrClient getSolrClient();

    List<String> getAnalysis(String sentence);

    List<Map<String, Object>> querySolr(String searchCondition, int start, int num);

    List<Map<String, Object>> querySolrPlus(Map<String, String> params, int start, int num, IsFlag is_return_file_text);

    void deleteIndexById(String row_key);

    void deleteIndexByQuery(String query);

    void deleteIndexAll();

    List<Map<String, Object>> requestHandler(String... handler);

    List<Map<String, Object>> requestHandler(boolean is, Map<String, Object> parameters, String... temp);

    SolrDocumentList queryByField(String fieldName, String value, String responseColumns, int rows);

    void parseResultToSolr(Result result, Set<String> columnNames) throws SolrServerException, IOException;
}
