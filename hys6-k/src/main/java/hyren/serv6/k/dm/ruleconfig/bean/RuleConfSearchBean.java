package hyren.serv6.k.dm.ruleconfig.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "rule_conf_search_bean")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RuleConfSearchBean {

    private static final long serialVersionUID = 3923798876127740050L;

    private static final String TableName = "rule_conf_search_bean";

    @DocBean(name = "reg_num", value = "", dataType = String.class, required = false)
    private String reg_num;

    @DocBean(name = "target_tab", value = "", dataType = String.class, required = false)
    private String target_tab;

    @DocBean(name = "rule_tag", value = "", dataType = String.class, required = false)
    private String rule_tag;

    @DocBean(name = "reg_name", value = "", dataType = String.class, required = false)
    private String reg_name;

    @DocBean(name = "rule_src", value = "", dataType = String.class, required = false)
    private String rule_src;

    @DocBean(name = "case_type", value = "", dataType = String.class, required = false)
    private String[] case_type;

    @DocBean(name = "job_status", value = "", dataType = String.class, required = false)
    private String[] job_status;
}
