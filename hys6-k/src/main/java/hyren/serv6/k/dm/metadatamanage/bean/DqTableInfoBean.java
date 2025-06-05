package hyren.serv6.k.dm.metadatamanage.bean;

import fd.ng.core.annotation.DocBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DqTableInfoBean {

    public static final String TableName = "dq_table_info_bean";

    private static final long serialVersionUID = 3054079742119923746L;

    @DocBean(name = "table_space", value = "", dataType = String.class)
    private String table_space;

    @DocBean(name = "table_name", value = "", dataType = String.class)
    private String table_name;

    @DocBean(name = "ch_name", value = "", dataType = String.class)
    private String ch_name;

    @DocBean(name = "is_trace", value = "", dataType = String.class)
    private String is_trace;

    @DocBean(name = "dq_remark", value = "", dataType = String.class)
    private String dq_remark;

    @DocBean(name = "hbase_sort_columns", value = "", dataType = String[].class)
    private String[] hbase_sort_columns;

    @DocBean(name = "is_external", value = "", dataType = String.class)
    private String is_external;

    @DocBean(name = "storage_path", value = "", dataType = String.class)
    private String storage_path;

    @DocBean(name = "storage_type", value = "", dataType = String.class)
    private String storage_type;

    @DocBean(name = "line_separator", value = "", dataType = String.class)
    private String line_separator;

    @DocBean(name = "column_separator", value = "", dataType = String.class)
    private String column_separator;

    @DocBean(name = "escape_character", value = "", dataType = String.class)
    private String escape_character;

    @DocBean(name = "is_header", value = "", dataType = String.class)
    private String is_header;

    @DocBean(name = "column_familie_s", value = "", dataType = String.class)
    private String column_familie_s;

    @DocBean(name = "is_use_bloom_filter", value = "", dataType = String.class)
    private String is_use_bloom_filter;

    @DocBean(name = "bloom_filter_type", value = "", dataType = String.class)
    private String bloom_filter_type;

    @DocBean(name = "is_compress", value = "", dataType = String.class)
    private String is_compress;

    @DocBean(name = "block_size", value = "", dataType = String.class)
    private String block_size;

    @DocBean(name = "data_block_encoding", value = "", dataType = String.class)
    private String data_block_encoding;

    @DocBean(name = "max_version", value = "", dataType = String.class)
    private String max_version;

    @DocBean(name = "pre_split", value = "", dataType = String.class)
    private String pre_split;

    @DocBean(name = "pre_parm", value = "", dataType = String.class)
    private String pre_parm;
}
