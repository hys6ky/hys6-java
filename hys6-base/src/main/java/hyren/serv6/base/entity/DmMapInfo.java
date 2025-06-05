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
@ApiModel("结果映射信息表")
@Table(tableName = "dm_map_info")
public class DmMapInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "dm_map_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("map_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long own_source_table_id;

    @ApiModelProperty(value = "", required = true)
    protected Long jobtab_id;

    @ApiModelProperty(value = "", required = true)
    protected Long map_id;

    @ApiModelProperty(value = "", required = false)
    protected String tar_field_name;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 512, message = "")
    @NotBlank(message = "")
    protected String src_fields_name;

    @ApiModelProperty(value = "", required = false)
    protected String remark;

    public void setOwn_source_table_id(String own_source_table_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(own_source_table_id)) {
            this.own_source_table_id = new Long(own_source_table_id);
        }
    }

    public void setOwn_source_table_id(Long own_source_table_id) {
        this.own_source_table_id = own_source_table_id;
    }

    public void setJobtab_id(String jobtab_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(jobtab_id)) {
            this.jobtab_id = new Long(jobtab_id);
        }
    }

    public void setJobtab_id(Long jobtab_id) {
        this.jobtab_id = jobtab_id;
    }

    public void setMap_id(String map_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(map_id)) {
            this.map_id = new Long(map_id);
        }
    }

    public void setMap_id(Long map_id) {
        this.map_id = map_id;
    }
}
