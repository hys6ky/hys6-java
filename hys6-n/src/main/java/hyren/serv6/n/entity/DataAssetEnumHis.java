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
@Table(tableName = "data_asset_enum_his")
public class DataAssetEnumHis extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321566880187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_asset_enum_his";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("enum_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "码值id", notes = "", dataType = "long", required = true)
    @NotBlank(message = "")
    private long enum_id;

    @ApiModelProperty(name = "码值中文名", notes = "", dataType = "String", required = false)
    private String enum_cname;

    @ApiModelProperty(name = "码值英文名", notes = "", dataType = "String", required = false)
    private String enum_ename;

    @ApiModelProperty(name = "码值项中文名", notes = "", dataType = "String", required = false)
    private String item_cname;

    @ApiModelProperty(name = "码值项英文名", notes = "", dataType = "String", required = false)
    private String item_ename;

    @ApiModelProperty(name = "码值项值", notes = "", dataType = "String", required = false)
    private String item_value;
}
