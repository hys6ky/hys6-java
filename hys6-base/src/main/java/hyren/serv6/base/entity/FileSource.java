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
@ApiModel("文件源设置")
@Table(tableName = "file_source")
public class FileSource extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "file_source";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("file_source_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long file_source_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String file_source_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_pdf;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_office;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_text;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_video;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_audio;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_image;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_compress;

    @ApiModelProperty(value = "", required = false)
    protected String custom_suffix;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String is_other;

    @ApiModelProperty(value = "", required = false)
    protected String file_remark;

    @ApiModelProperty(value = "", required = true)
    protected Long fcs_id;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    public void setFile_source_id(String file_source_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(file_source_id)) {
            this.file_source_id = new Long(file_source_id);
        }
    }

    public void setFile_source_id(Long file_source_id) {
        this.file_source_id = file_source_id;
    }

    public void setFcs_id(String fcs_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(fcs_id)) {
            this.fcs_id = new Long(fcs_id);
        }
    }

    public void setFcs_id(Long fcs_id) {
        this.fcs_id = fcs_id;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }
}
