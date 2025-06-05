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
@ApiModel("轴配置信息表")
@Table(tableName = "auto_axis_info")
public class AutoAxisInfo extends ProEntity {

    private static final long serialVersionUID = 321566870187324L;

    private transient static final Set<String> __PrimaryKeys;

    public static final String TableName = "auto_axis_info";

    public static boolean isPrimaryKey(String name) {
        return __PrimaryKeys.contains(name);
    }

    public static Set<String> getPrimaryKeyNames() {
        return __PrimaryKeys;
    }

    static {
        Set<String> __tmpPKS = new HashSet<>();
        __tmpPKS.add("axis_id");
        __PrimaryKeys = Collections.unmodifiableSet(__tmpPKS);
    }

    @ApiModelProperty(value = "", required = true)
    protected Long axis_id;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String axis_type;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String show;

    @ApiModelProperty(value = "", required = false)
    protected String position;

    @ApiModelProperty(value = "", required = false)
    protected Long axisoffset;

    @ApiModelProperty(value = "", required = false)
    protected String name;

    @ApiModelProperty(value = "", required = false)
    protected String namelocation;

    @ApiModelProperty(value = "", required = false)
    protected Long namegap;

    @ApiModelProperty(value = "", required = false)
    protected Long namerotate;

    @ApiModelProperty(value = "", required = false)
    protected Long min;

    @ApiModelProperty(value = "", required = false)
    protected Long max;

    @ApiModelProperty(value = "", required = true)
    @Size(min = 1, max = 1, message = "")
    @NotBlank(message = "")
    protected String silent;

    @ApiModelProperty(value = "", required = true)
    protected Long component_id;

    public void setAxis_id(String axis_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(axis_id)) {
            this.axis_id = new Long(axis_id);
        }
    }

    public void setAxis_id(Long axis_id) {
        this.axis_id = axis_id;
    }

    public void setAxisoffset(String axisoffset) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(axisoffset)) {
            this.axisoffset = new Long(axisoffset);
        }
    }

    public void setAxisoffset(Long axisoffset) {
        this.axisoffset = axisoffset;
    }

    public void setNamegap(String namegap) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(namegap)) {
            this.namegap = new Long(namegap);
        }
    }

    public void setNamegap(Long namegap) {
        this.namegap = namegap;
    }

    public void setNamerotate(String namerotate) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(namerotate)) {
            this.namerotate = new Long(namerotate);
        }
    }

    public void setNamerotate(Long namerotate) {
        this.namerotate = namerotate;
    }

    public void setMin(String min) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(min)) {
            this.min = new Long(min);
        }
    }

    public void setMin(Long min) {
        this.min = min;
    }

    public void setMax(String max) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(max)) {
            this.max = new Long(max);
        }
    }

    public void setMax(Long max) {
        this.max = max;
    }

    public void setComponent_id(String component_id) {
        if (fd.ng.core.utils.StringUtil.isNotBlank(component_id)) {
            this.component_id = new Long(component_id);
        }
    }

    public void setComponent_id(Long component_id) {
        this.component_id = component_id;
    }
}
