package hyren.serv6.k.standard.standardTask.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class StandardInfoVo {

    @ApiModelProperty(name = "原始字段英文名称", notes = "", dataType = "String", required = false)
    private String src_col_ename;

    @ApiModelProperty(name = "原始字段中文名称", notes = "", dataType = "String", required = false)
    private String src_col_cname;

    @ApiModelProperty(name = "原始字段类型", notes = "", dataType = "String", required = false)
    private String src_col_type;

    @ApiModelProperty(name = "原始字段长度", notes = "", dataType = "Integer", required = false)
    private Integer src_col_len;

    @ApiModelProperty(name = "原始字段精度", notes = "", dataType = "Integer", required = false)
    private Integer src_col_preci;

    @ApiModelProperty(name = "标准元主键", notes = "", dataType = "Long", required = false)
    private Long basic_id;

    @ApiModelProperty(name = "代码类主键（标准代码）", notes = "", dataType = "Long", required = false)
    private Long code_type_id;
}
