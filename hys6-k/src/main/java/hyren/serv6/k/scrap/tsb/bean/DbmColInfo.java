package hyren.serv6.k.scrap.tsb.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "dbm_col_info")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DbmColInfo {

    private static final long serialVersionUID = 321561870187364L;

    public static final String TableName = "dbm_col_info";

    @DocBean(name = "column_id", value = "", dataType = Long.class)
    private Long column_id;

    @DocBean(name = "column_name", value = "", dataType = String.class)
    private String column_name;

    @DocBean(name = "column_ch_name", value = "", dataType = String.class)
    private String column_ch_name;

    @DocBean(name = "remark", value = "", dataType = String.class)
    private String remark;

    @DocBean(name = "column_type", value = "", dataType = String.class)
    private String column_type;

    @DocBean(name = "is_primary_key", value = "", dataType = String.class)
    private String is_primary_key;

    @DocBean(name = "default_value", value = "", dataType = String.class)
    private String default_value;
}
