package hyren.serv6.c.jobconfig.dto;

import java.util.List;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchDeleteEtlParaDTO {

    @ApiModelProperty(value = "", name = "工程主键:", dataType = "Long", required = true)
    private Long etl_sys_id;

    @ApiModelProperty(value = "", name = "变量名称的数组", dataType = "Array", required = true)
    private List<String> para_cd;
}
