package hyren.serv6.commons.solr.param;

import fd.ng.core.annotation.DocClass;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/1/9 0009 上午 11:26")
public class SolrParam {

    private String solrZkUrl;

    private String collection;

    private String principle_name = "hyshf@beyondsoft.com";

    public String getSolrZkUrl() {
        return solrZkUrl;
    }

    public void setSolrZkUrl(String solrUrl) {
        this.solrZkUrl = solrUrl;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getPrinciple_name() {
        return principle_name;
    }

    public void setPrinciple_name(String principle_name) {
        this.principle_name = principle_name;
    }
}
