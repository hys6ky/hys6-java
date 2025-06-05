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
public class EtlSysDepSaveDTO {

    @ApiModelProperty(value = "", name = "工程代码:", dataType = "String", required = true)
    private String etl_sys_cd;

    @ApiModelProperty(value = "", name = "上游工程编号数组:", dataType = "Array", required = false)
    private String[] pre_etl_sys_cds;

    @ApiModelProperty(value = "", name = "状态:(ETL状态)", dataType = "String", required = true)
    private String status;
}
