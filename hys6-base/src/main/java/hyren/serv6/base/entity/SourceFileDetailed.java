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
@ApiModel("源文件属性清册")
@Table(tableName = "source_file_detailed")
public class SourceFileDetailed extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "source_file_detailed";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("sfd_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String sfd_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 40, message = "")
    @NotBlank(message = "")
    protected String file_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String original_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String original_update_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String original_update_time;

    @ApiModelProperty(value = "", required = false)
    protected String table_name;

    @ApiModelProperty(value = "", required = false)
    protected String meta_info;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String hbase_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 8, message = "")
    @NotBlank(message = "")
    protected String storage_date;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 6, message = "")
    @NotBlank(message = "")
    protected String storage_time;

    @ApiModelProperty(value = "", required = true)
    protected Long file_size;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String file_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String file_suffix;

    @ApiModelProperty(value = "", required = false)
    protected String hdfs_storage_path;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String source_path;

    @ApiModelProperty(value = "", required = false)
    protected String file_md5;

    @ApiModelProperty(value = "", required = false)
    protected String file_avro_path;

    @ApiModelProperty(value = "", required = false)
    protected Long file_avro_block;

    @ApiModelProperty(value = "", required = false)
    protected String is_big_file;

    @ApiModelProperty(value = "", required = true)
    protected Long folder_id;

    @ApiModelProperty(value = "", required = true)
    protected Long agent_id;

    @ApiModelProperty(value = "", required = true)
    protected Long source_id;

    @ApiModelProperty(value = "", required = true)
    protected Long collect_set_id;

    public void setFile_size(String file_size) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(file_size)) {
            this.file_size = new Long(file_size);
        }
    }

    public void setFile_size(Long file_size) {
        this.file_size = file_size;
    }

    public void setFile_avro_block(String file_avro_block) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(file_avro_block)) {
            this.file_avro_block = new Long(file_avro_block);
        }
    }

    public void setFile_avro_block(Long file_avro_block) {
        this.file_avro_block = file_avro_block;
    }

    public void setFolder_id(String folder_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(folder_id)) {
            this.folder_id = new Long(folder_id);
        }
    }

    public void setFolder_id(Long folder_id) {
        this.folder_id = folder_id;
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

    public void setCollect_set_id(String collect_set_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(collect_set_id)) {
            this.collect_set_id = new Long(collect_set_id);
        }
    }

    public void setCollect_set_id(Long collect_set_id) {
        this.collect_set_id = collect_set_id;
    }
}
