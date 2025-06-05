package hyren.serv6.k.dbm.codetypeinfo.bean;

import fd.ng.db.jdbc.DefaultPageImpl;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel(value = "", description = "")
@Data
public class DbmCodeTypeQueryVo extends DefaultPageImpl {

    @ApiModelProperty(value = "", name = "代码类名:", dataType = "String", required = true)
    private String code_type_name;

    @ApiModelProperty(value = "", name = "代码编码:", dataType = "String", required = false)
    private String code_encode;

    @ApiModelProperty(value = "", name = "代码状态（是否发布）:(是否标识)", dataType = "String", required = true)
    private String code_status;
}
