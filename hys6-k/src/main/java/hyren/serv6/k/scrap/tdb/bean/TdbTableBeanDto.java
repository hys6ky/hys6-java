package hyren.serv6.k.scrap.tdb.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.Data;

@Table(tableName = "tdb_table_bean")
@Data
public class TdbTableBeanDto {

    public static final String TableName = "tdb_table_bean";

    private static final long serialVersionUID = -380754015950555099L;

    private String dsl_id;

    private String agent_id;

    private String[] children;

    private String classify_id;

    private String data_own_type;

    private String data_source_id;

    private String dsl_store_type;

    private String description;

    private String tree_page_source;

    private String id;

    private String label;

    private String parent_id;

    private String table_name;

    @DocBean(name = "file_id", value = "", dataType = String.class)
    private String file_id;

    @DocBean(name = "data_layer", value = "", dataType = String.class)
    private String data_layer;

    @DocBean(name = "hyren_name", value = "", dataType = String.class)
    private String hyren_name;

    @DocBean(name = "table_cn_name", value = "", dataType = String.class)
    private String table_cn_name;

    @DocBean(name = "original_name", value = "", dataType = String.class)
    private String original_name;
}
