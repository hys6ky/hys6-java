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
@ApiModel("系统备份信息表")
@Table(tableName = "sys_dump")
public class SysDump extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "sys_dump";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dump_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dump_id;

    @ApiModelProperty(value = "", required = true)
    protected String bak_date;

    @ApiModelProperty(value = "", required = true)
    protected String bak_time;

    @ApiModelProperty(value = "", required = true)
    protected String file_size;

    @ApiModelProperty(value = "", required = true)
    protected String file_name;

    @ApiModelProperty(value = "", required = true)
    protected String hdfs_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 10, message = "")
    @NotBlank(message = "")
    protected String length;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setDump_id(String dump_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dump_id)) {
            this.dump_id = new Long(dump_id);
        }
    }

    public void setDump_id(Long dump_id) {
        this.dump_id = dump_id;
    }
}
