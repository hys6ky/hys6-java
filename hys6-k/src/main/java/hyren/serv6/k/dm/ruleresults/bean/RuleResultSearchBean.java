package hyren.serv6.k.dm.ruleresults.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "rule_result_search_bean")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RuleResultSearchBean {

    private static final long serialVersionUID = -4272971336553207489L;

    public static final String TableName = "rule_result_search_bean";

    @DocBean(name = "verify_date", value = "", dataType = String.class, required = false)
    private String verify_date;

    @DocBean(name = "start_date", value = "", dataType = String.class, required = false)
    private String start_date;

    @DocBean(name = "rule_src", value = "", dataType = String.class, required = false)
    private String rule_src;

    @DocBean(name = "rule_tag", value = "", dataType = String.class, required = false)
    private String rule_tag;

    @DocBean(name = "reg_name", value = "", dataType = String.class, required = false)
    private String reg_name;

    @DocBean(name = "reg_num", value = "", dataType = String.class, required = false)
    private String reg_num;

    @DocBean(name = "exec_mode", value = "", dataType = String.class, required = false)
    private String[] exec_mode;

    @DocBean(name = "verify_result", value = "", dataType = String.class, required = false)
    private String[] verify_result;

    @DocBean(name = "case_type", value = "", dataType = String.class, required = false)
    private String[] case_type;
}
