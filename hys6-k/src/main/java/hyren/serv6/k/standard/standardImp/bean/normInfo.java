package hyren.serv6.k.standard.standardImp.bean;

import lombok.Data;

@Data
public class normInfo {

    private Long basic_id;

    private String norm_cname;

    private String norm_ename;

    private String data_type;

    private String data_type_name;

    private Long col_len;

    private Long DECIMAL_POINT;

    private double point;

    private Long code_type_id;

    private String code_type_name;
}
