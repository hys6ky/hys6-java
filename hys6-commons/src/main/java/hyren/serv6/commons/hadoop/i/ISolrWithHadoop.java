package hyren.serv6.commons.hadoop.i;

import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import org.apache.solr.client.solrj.SolrClient;
import java.util.List;

public interface ISolrWithHadoop {

    Long readFileToSolr(CollectTableBean collectTableBean, TableBean tableBean, String fileAbsPath, SolrClient solrClient, List<String> columnList, List<String> typeList);
}
