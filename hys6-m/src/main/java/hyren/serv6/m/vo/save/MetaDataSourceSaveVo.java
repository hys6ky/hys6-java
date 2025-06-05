package hyren.serv6.m.vo.save;

import hyren.serv6.m.entity.MetaDataSource;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@ApiModel(value = "", description = "")
@Data
public class MetaDataSourceSaveVo {

    @ApiModelProperty(name = "主键", notes = "", dataType = "Long", required = true)
    private Long source_id;

    @ApiModelProperty(name = "数据源名称", notes = "", dataType = "String", required = true)
    @Size(min = 1, max = 32, message = "")
    @NotBlank(message = "")
    private String source_name;

    @ApiModelProperty(name = "描述", notes = "", dataType = "String", required = false)
    private String mds_desc;

    @ApiModelProperty(name = "元数据源来源", notes = "", dataType = "String", required = false)
    @NotBlank(message = "")
    private String ds_type;

    @ApiModelProperty(name = "存储层配置ID", notes = "", dataType = "Long", required = false)
    private Long dsl_id;
}
