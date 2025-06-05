package hyren.serv6.n.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@Data
public class DataAssetDirVo {

    @ApiModelProperty(name = "目录id", notes = "", dataType = "long", required = false)
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
}
