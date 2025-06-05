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
@ApiModel("源文件夹属性表")
@Table(tableName = "source_folder_attribute")
public class SourceFolderAttribute extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "source_folder_attribute";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("folder_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long folder_id;

    @ApiModelProperty(value = "", required = false)
    protected Long super_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String folder_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String original_create_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String original_create_time;

    @ApiModelProperty(value = "", required = true)
    protected BigDecimal folder_size;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String storage_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String storage_time;

    @ApiModelProperty(value = "", required = true)
    protected Long folders_in_no;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String location_in_hdfs;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    public void setFolder_id(String folder_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(folder_id)) {
            this.folder_id = new Long(folder_id);
        }
    }

    public void setFolder_id(Long folder_id) {
        this.folder_id = folder_id;
    }

    public void setSuper_id(String super_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(super_id)) {
            this.super_id = new Long(super_id);
        }
    }

    public void setSuper_id(Long super_id) {
        this.super_id = super_id;
    }

    public void setFolder_size(String folder_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(folder_size)) {
            this.folder_size = new BigDecimal(folder_size);
        }
    }

    public void setFolder_size(BigDecimal folder_size) {
        this.folder_size = folder_size;
    }

    public void setFolders_in_no(String folders_in_no) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(folders_in_no)) {
            this.folders_in_no = new Long(folders_in_no);
        }
    }

    public void setFolders_in_no(Long folders_in_no) {
        this.folders_in_no = folders_in_no;
    }

    public void setAgent_id(String agent_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(agent_id)) {
            this.agent_id = new Long(agent_id);
        }
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
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
