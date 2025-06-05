package hyren.serv6.k.standard.standardImp.bean;

import io.swagger.models.auth.In;
import lombok.Data;

@Data
public class StandardImpVo {

    private Long source_id;

    private String source_name;

    private String schema_cname;

    private Long obj_id;

    private String table_cname;

    private String table_ename;

    private Long dtl_id;

    private String col_cname;

    private String col_ename;

    private String col_type;

    private Integer col_len;

    private Integer col_prec;

    private Long basic_id;

    private String norm_cname;

    private Long imp_id;

    private String imp_result;

    private String imp_detail;
}
