package hyren.serv6.k.scrap.tdbresult.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "search_table_func_dep_bean")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class SearchTableFuncDepResultBean {

    private static final long serialVersionUID = -4784392934005447805L;

    public static final String TableName = "search_table_func_dep_bean";

    @DocBean(name = "table_name", value = "", dataType = String.class, required = false)
    private String table_name;
}
