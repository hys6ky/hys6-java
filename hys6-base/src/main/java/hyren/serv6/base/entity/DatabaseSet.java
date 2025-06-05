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
@ApiModel("源系统数据库设置")
@Table(tableName = "database_set")
public class DatabaseSet extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "database_set";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("database_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = false)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long database_id;

    @ApiModelProperty(value = "", required = false)
    protected String host_name;

    @ApiModelProperty(value = "", required = false)
    protected String database_number;

    @ApiModelProperty(value = "", required = false)
    protected String system_type;

    @ApiModelProperty(value = "", required = false)
    protected String task_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String db_agent;

    @ApiModelProperty(value = "", required = false)
    protected String database_separatorr;

    @ApiModelProperty(value = "", required = false)
    protected String row_separator;

    @ApiModelProperty(value = "", required = false)
    protected String plane_url;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_sendok;

    @ApiModelProperty(value = "", required = false)
    protected String cp_or;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String collect_type;

    @ApiModelProperty(value = "", required = false)
    protected Integer fetch_size;

    @ApiModelProperty(value = "", required = false)
    protected Long dsl_id;

    @ApiModelProperty(value = "", required = true)
    protected Long classify_id;

    @ApiModelProperty(value = "", required = false)
    protected Long source_id;

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public void setDatabase_id(String database_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(database_id)) {
            this.database_id = new Long(database_id);
        }
    }

    public void setDatabase_id(Long database_id) {
        this.database_id = database_id;
    }

    public void setFetch_size(String fetch_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fetch_size)) {
            this.fetch_size = new Integer(fetch_size);
        }
    }

    public void setFetch_size(Integer fetch_size) {
        this.fetch_size = fetch_size;
    }

    public void setDsl_id(String dsl_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setClassify_id(String classify_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(classify_id)) {
            this.classify_id = new Long(classify_id);
        }
    }

    public void setClassify_id(Long classify_id) {
        this.classify_id = classify_id;
    }

    public void setSource_id(String source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(source_id)) {
            this.source_id = new Long(source_id);
        }
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }
}
