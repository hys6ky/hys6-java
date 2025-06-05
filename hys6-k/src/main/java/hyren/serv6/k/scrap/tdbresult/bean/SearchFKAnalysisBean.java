package hyren.serv6.k.scrap.tdbresult.bean;

import fd.ng.core.annotation.DocBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchFKAnalysisBean {

    private static final long serialVersionUID = -2727689833798758656L;

    public static final String TableName = "search_fk_analysis_bean";

    @DocBean(name = "table_name", value = "", dataType = String.class, required = false)
    private String table_name;

    @DocBean(name = "table_field_name", value = "", dataType = String.class, required = false)
    private String table_field_name;

    @DocBean(name = "fk_table_name", value = "", dataType = String.class, required = false)
    private String fk_table_name;

    @DocBean(name = "fk_table_field_name", value = "", dataType = String.class, required = false)
    private String fk_table_field_name;
}
