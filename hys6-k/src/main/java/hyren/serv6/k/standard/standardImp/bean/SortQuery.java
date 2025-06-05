package hyren.serv6.k.standard.standardImp.bean;

import fd.ng.db.jdbc.DefaultPageImpl;
import lombok.Data;
import javax.validation.constraints.NotNull;

@Data
public class SortQuery {

    private String src_col_cname;

    private String src_col_ename;

    private Long sort_id;

    private String search_cond;
}
