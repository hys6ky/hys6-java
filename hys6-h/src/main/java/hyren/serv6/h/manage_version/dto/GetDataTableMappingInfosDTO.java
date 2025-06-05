package hyren.serv6.h.manage_version.dto;

import java.util.List;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("获取集市数据表Mapping的版本信息列表")
public class GetDataTableMappingInfosDTO {

    @ApiModelProperty(name = "datatable_id", value = "")
    private Long datatable_id;

    @ApiModelProperty(name = "version_date_s", value = "")
    private List<String> version_date_s;
}
