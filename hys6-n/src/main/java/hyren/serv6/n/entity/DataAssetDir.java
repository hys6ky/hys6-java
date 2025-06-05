package hyren.serv6.n.entity;

import fd.ng.db.entity.TableEntity;
import fd.ng.db.entity.anno.Table;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "data_asset_dir")
public class DataAssetDir extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321565870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_asset_dir";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("dir_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "目录id", notes = "", dataType = "long", required = true)
    @NotBlank(message = "")
    private long dir_id;

    @ApiModelProperty(name = "目录名称", notes = "", dataType = "String", required = true)
    @Size(min = 1, max = 255, message = "")
    @NotBlank(message = "")
    private String dir_name;

    @ApiModelProperty(name = "目录代码", notes = "", dataType = "String", required = false)
    private String dir_code;

    @ApiModelProperty(name = "编目id", notes = "", dataType = "long", required = false)
    private long catalog_id;

    @ApiModelProperty(name = "上级目录id", notes = "", dataType = "long", required = false)
    private long parent_id;

    @ApiModelProperty(name = "创建人", notes = "", dataType = "String", required = false)
    private String create_by;

    @ApiModelProperty(name = "创建日期", notes = "", dataType = "String", required = false)
    private String create_date;

    @ApiModelProperty(name = "创建时间", notes = "", dataType = "String", required = false)
    private String create_time;
}
