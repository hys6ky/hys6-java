package hyren.serv6.n.entity;

import fd.ng.db.entity.TableEntity;
import fd.ng.db.entity.anno.Table;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Data
@ApiModel(value = "", description = "")
@Table(tableName = "data_asset_dir_rel")
public class DataAssetDirRel extends TableEntity implements Serializable, Cloneable {

    private static final long serialVersionUID = 321565870187114L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "data_asset_dir_rel";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("rel_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(name = "关系id", notes = "", dataType = "long", required = false)
    private long rel_id;

    @ApiModelProperty(name = "资产id", notes = "", dataType = "long", required = false)
    private long asset_id;

    @ApiModelProperty(name = "目录id", notes = "", dataType = "long", required = false)
    private long dir_id;

    @ApiModelProperty(name = "任务id", notes = "", dataType = "long", required = false)
    private long task_id;
}
