package hyren.serv6.c.jobconfig.dto;

import hyren.serv6.base.entity.EtlDependency;
import hyren.serv6.base.entity.EtlJobDef;
import lombok.*;

@Data
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class EtlJobDefUpdateDTO {

    private EtlJobDef etl_job_def;

    private EtlDependency etl_dependency;

    private Long[] pre_etl_job_ids;

    private String old_disp_freq;

    private Long[] old_pre_etl_job_ids;

    private String old_dispatch_type;
}
