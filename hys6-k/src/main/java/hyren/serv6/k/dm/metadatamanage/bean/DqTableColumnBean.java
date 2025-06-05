package hyren.serv6.k.dm.metadatamanage.bean;

import fd.ng.core.annotation.DocBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DqTableColumnBean {

    public static final String TableName = "dq_table_column_bean";

    private static final long serialVersionUID = -8972830752961712691L;

    @DocBean(name = "field_ch_name", value = "", dataType = String.class)
    private String field_ch_name;

    @DocBean(name = "column_name", value = "", dataType = String.class)
    private String column_name;

    @DocBean(name = "column_type", value = "", dataType = String.class)
    private String column_type;

    @DocBean(name = "column_length", value = "", dataType = String.class)
    private String column_length;

    @DocBean(name = "is_null", value = "", dataType = String.class)
    private String is_null;

    @DocBean(name = "colsourcetab", value = "", dataType = String.class)
    private String colsourcetab;

    @DocBean(name = "colsourcecol", value = "", dataType = String.class)
    private String colsourcecol;

    @DocBean(name = "dq_remark", value = "", dataType = String.class)
    private String dq_remark;

    @DocBean(name = "dslad_id_s", value = "", dataType = Long[].class)
    private Long[] dslad_id_s;
}
