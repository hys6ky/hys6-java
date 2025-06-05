package hyren.serv6.r.record.Res;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class DfProInfoRes {

    @ApiModelProperty(name = "df_pid", value = "", dataType = "Long", required = true)
    private Long df_pid;

    @ApiModelProperty(name = "pro_name", value = "", dataType = "String", required = true)
    private String pro_name;

    @ApiModelProperty(name = "df_type", value = "", dataType = "String", required = false)
    private String df_type;

    @ApiModelProperty(name = "create_user_id", value = "", dataType = "Long", required = false)
    private Long create_user_id;

    @ApiModelProperty(name = "user_id", value = "", dataType = "Long", required = false)
    private Long user_id;

    @ApiModelProperty(name = "submit_user", value = "", dataType = "String", required = false)
    private String submit_user;

    @ApiModelProperty(name = "submit_date", value = "", dataType = "String", required = false)
    private String submit_date;

    @ApiModelProperty(name = "submit_time", value = "", dataType = "String", required = false)
    private String submit_time;

    @ApiModelProperty(name = "submit_state", value = "", dataType = "String", required = false)
    private String submit_state;

    @ApiModelProperty(name = "dsl_id", value = "", dataType = "Long", required = false)
    private Long dsl_id;

    @ApiModelProperty(name = "df_remarks", value = "", dataType = "String", required = false)
    private String df_remarks;

    private Long number;
}
