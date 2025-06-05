package hyren.serv6.base.entity;

import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@ApiModel("数据文件定义表-")
@Table(tableName = "dr_file_def")
public class DrFileDef extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dr_file_def";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dr_file_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long dr_file_id;

    @ApiModelProperty(value = "", required = true)
    protected Long dr_task_id;

    @ApiModelProperty(value = "", required = true)
    protected String is_header;

    @ApiModelProperty(value = "", required = true)
    private String dr_is_flag;

    @ApiModelProperty(value = "", required = true)
    protected String dr_database_code;

    @ApiModelProperty(value = "", required = false)
    protected String dr_row_separator;

    @ApiModelProperty(value = "", required = false)
    protected String dr_database_separator;

    @ApiModelProperty(value = "", required = true)
    protected String dbfile_format;

    @ApiModelProperty(value = "", required = true)
    protected String dr_plane_url;

    @ApiModelProperty(value = "", required = true)
    protected String dr_file_name;

    @ApiModelProperty(value = "", required = false)
    protected String dr_file_suffix;

    @ApiModelProperty(value = "", required = false)
    protected String df_remark;

    public void setDr_file_id(String dr_file_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dr_file_id)) {
            this.dr_file_id = new Long(dr_file_id);
        }
    }

    public void setDr_file_id(Long dr_file_id) {
        this.dr_file_id = dr_file_id;
    }

    public void setDr_task_id(String dr_task_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(dr_task_id)) {
            this.dr_task_id = new Long(dr_task_id);
        }
    }

    public void setDr_task_id(Long dr_task_id) {
        this.dr_task_id = dr_task_id;
    }
}
