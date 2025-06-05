package hyren.serv6.b.datasource.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UpdateSourceRelationDepDTO {

    @ApiModelProperty(value = "", name = "data_source表主键ID:", dataType = "Long", required = true)
    Long source_id;

    @ApiModelProperty(value = "", name = "部门ID", dataType = "Long", required = true)
    Long[] dep_id;
}
