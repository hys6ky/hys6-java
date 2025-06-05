package hyren.serv6.c.joblevelintervention.dto;

import java.util.List;
import hyren.serv6.c.entity.JobHandBean;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchJobLevelInterventionOperateDTO {

    @ApiModelProperty(value = "", name = "作业优先级（无限制）", dataType = "Integer", required = true)
    private Integer job_priority;

    @ApiModelProperty(value = "", name = "作业干预自定义实体对象", required = true)
    private List<JobHandBean> jobHandBeans;
}
