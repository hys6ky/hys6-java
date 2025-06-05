package hyren.serv6.commons.hadoop.hbaseindexer.type;

import hyren.serv6.commons.hadoop.hbaseindexer.bean.HbaseSolrField;

public interface FieldNameMapper {

    void map(HbaseSolrField hsf);
}
