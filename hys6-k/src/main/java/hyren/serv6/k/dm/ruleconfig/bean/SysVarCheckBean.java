package hyren.serv6.k.dm.ruleconfig.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "sys_var_check_bean")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class SysVarCheckBean {

    private static final long serialVersionUID = -6775651305689004626L;

    public static final String TableName = "sys_var_check_bean";

    @DocBean(name = "name", value = "", dataType = String.class, required = false)
    private String name;

    @DocBean(name = "value", value = "", dataType = String.class, required = false)
    private String value;

    @DocBean(name = "isEff", value = "", dataType = String.class, required = false)
    private String isEff;
}
