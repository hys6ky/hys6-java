package hyren.serv6.n.bean;

import lombok.Data;

@Data
public class MetaDataColumnDto {

    private String mdata_col_id;

    private String col_cname;

    private String col_ename;

    private String col_type;

    private int col_len;

    private int col_prec;

    private String col_business;

    private Integer col_order;

    private String is_pri_key;

    private String is_null;
}
