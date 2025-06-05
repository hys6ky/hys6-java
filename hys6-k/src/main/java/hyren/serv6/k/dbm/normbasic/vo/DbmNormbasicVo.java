package hyren.serv6.k.dbm.normbasic.vo;

import hyren.serv6.k.entity.DbmNormbasic;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel("数据对标元管理标准元表视图")
public class DbmNormbasicVo extends DbmNormbasic {

    @ApiModelProperty(value = "", name = "代码类名:", dataType = "String")
    String code_type_name;

    @ApiModelProperty(value = "", name = "分类名称:", dataType = "String")
    String sort_name;
}
