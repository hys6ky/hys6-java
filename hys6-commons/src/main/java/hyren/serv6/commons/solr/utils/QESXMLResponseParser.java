package hyren.serv6.commons.solr.utils;

import fd.ng.core.annotation.DocClass;
import org.apache.solr.client.solrj.impl.XMLResponseParser;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/1/14 0014 下午 03:14")
public class QESXMLResponseParser extends XMLResponseParser {

    public QESXMLResponseParser() {
        super();
    }

    @Override
    public String getContentType() {
        return "application/xml; charset=UTF-8";
    }
}
