package hyren.serv6.k.scrap.tsb.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "tsb_result_bean")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TSBResultBean {

    private static final long serialVersionUID = -2279717234760388402L;

    public static final String TableName = "tsb_result_bean";

    @DocBean(name = "col_id", value = "", dataType = String.class)
    private String col_id;

    @DocBean(name = "result_id", value = "", dataType = String.class)
    private String result_id;

    @DocBean(name = "is_artificial", value = "", dataType = String.class)
    private String is_artificial;

    @DocBean(name = "col_ename", value = "", dataType = String.class)
    private String col_ename;
}
