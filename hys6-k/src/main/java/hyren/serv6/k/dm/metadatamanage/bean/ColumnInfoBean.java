package hyren.serv6.k.dm.metadatamanage.bean;

import fd.ng.core.annotation.DocBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnInfoBean {

    public static final String TableName = "column_info_bean";

    private static final long serialVersionUID = -6173825527384242061L;

    @DocBean(name = "column_id", value = "", dataType = Long.class)
    private Long column_id;

    @DocBean(name = "column_name", value = "", dataType = String.class)
    private String column_name;

    @DocBean(name = "column_ch_name", value = "", dataType = String.class, required = false)
    private String column_ch_name;

    @DocBean(name = "data_type", value = "", dataType = String.class, required = false)
    private String data_type;

    @DocBean(name = "data_len", value = "", dataType = String.class, required = false)
    private String data_len;

    @DocBean(name = "decimal_point", value = "", dataType = String.class, required = false)
    private String decimal_point;

    @DocBean(name = "is_primary_key", value = "", dataType = String.class, required = false)
    private String is_primary_key;
}
