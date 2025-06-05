package hyren.serv6.k.scrap.tdbresult.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "search_join_pk_analysis_bean")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class SearchJoinPKAnalysisBean {

    private static final long serialVersionUID = -3741603791275809710L;

    public static final String TableName = "search_join_pk_analysis_bean";

    @DocBean(name = "table_name", value = "", dataType = String.class, required = false)
    private String table_name;
}
