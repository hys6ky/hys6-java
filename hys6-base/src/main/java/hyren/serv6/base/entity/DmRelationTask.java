package hyren.serv6.base.entity;

import io.swagger.annotations.ApiModel;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import fd.ng.db.entity.anno.Table;
import io.swagger.annotations.ApiModelProperty;
import hyren.serv6.base.entity.fdentity.ProEntity;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

@Data
@ApiModel("数据加工和项目任务关系表-")
@Table(tableName = "dm_relation_task")
public class DmRelationTask extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_relation_task";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("module_table_id");
        __tmpPKS.add("task_uuid");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long module_table_id;

    @ApiModelProperty(value = "", required = true)
    protected String task_uuid;

    @ApiModelProperty(value = "", required = true)
    protected String task_category;

    public void setModule_table_id(String module_table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(module_table_id)) {
            this.module_table_id = new Long(module_table_id);
        }
    }

    public void setModule_table_id(Long module_table_id) {
        this.module_table_id = module_table_id;
    }
}
