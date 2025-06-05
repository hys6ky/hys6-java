package hyren.serv6.c.etlsys.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class EtlSysAddDTO {

    @ApiModelProperty(value = "", name = "工程主键:", dataType = "Long", required = true)
    private Long etl_sys_id;

    @ApiModelProperty(value = "", name = "工程代码:", dataType = "String", required = true)
    private String etl_sys_cd;

    @ApiModelProperty(value = "", name = "工程名称:", dataType = "String", required = true)
    private String etl_sys_name;

    @ApiModelProperty(value = "", name = "备注信息:", dataType = "String", required = false)
    private String comments;

    @ApiModelProperty(value = "", name = "上游系统主键数组:", dataType = "Array", required = false)
    private Long[] pre_etl_sys_ids;

    @ApiModelProperty(value = "", name = "状态:(ETL状态)", dataType = "String", required = true)
    private String status;
}
