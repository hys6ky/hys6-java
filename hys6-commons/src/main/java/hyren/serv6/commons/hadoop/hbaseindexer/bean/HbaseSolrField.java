package hyren.serv6.commons.hadoop.hbaseindexer.bean;

import hyren.serv6.commons.hadoop.hbaseindexer.type.DataTypeTransformSolr;
import hyren.serv6.commons.hadoop.hbaseindexer.type.FieldNameMapper;
import hyren.serv6.commons.hadoop.hbaseindexer.type.TypeFieldNameMapper;
import java.io.Serializable;

public class HbaseSolrField implements Serializable {

    private static final FieldNameMapper FIELD_NAME_MAPPER = new TypeFieldNameMapper();

    private String solrColumnName = "";

    private String hbaseColumnName = "";

    private String type = "string";

    public String getSolrColumnName() {
        return solrColumnName;
    }

    public void setSolrColumnName(String solrColumnName) {
        this.solrColumnName = solrColumnName;
    }

    public String getHbaseColumnName() {
        return hbaseColumnName;
    }

    public void setHbaseColumnName(String hbaseColumnName) {
        this.hbaseColumnName = hbaseColumnName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = DataTypeTransformSolr.transform(type);
        FIELD_NAME_MAPPER.map(this);
    }

    @Override
    public String toString() {
        return "HbaseSolrField [solrColumnName=" + solrColumnName + ", hbaseColumnName=" + hbaseColumnName + ", type=" + type + "]";
    }
}
