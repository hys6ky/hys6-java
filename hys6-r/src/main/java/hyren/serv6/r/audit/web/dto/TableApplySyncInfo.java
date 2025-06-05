package hyren.serv6.r.audit.web.dto;

import hyren.serv6.base.entity.DfTableApply;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@ToString
public class TableApplySyncInfo extends DfTableApply {

    private static final long serialVersionUID = 7159388431075288193L;

    @ApiModelProperty(name = "hyren_name", value = "", dataType = "String", required = false)
    private String hyren_name;

    @ApiModelProperty(name = "table_ch_name", value = "", dataType = "String", required = false)
    private String table_ch_name;

    @ApiModelProperty(name = "user_name", value = "", dataType = "String", required = false)
    private String user_name;

    @ApiModelProperty(name = "dep_name", value = "", dataType = "String", required = false)
    private String dep_name;
}
