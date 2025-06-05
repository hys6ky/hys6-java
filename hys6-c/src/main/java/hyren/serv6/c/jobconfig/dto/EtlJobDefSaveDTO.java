package hyren.serv6.c.jobconfig.dto;

import hyren.serv6.base.entity.EtlDependency;
import hyren.serv6.base.entity.EtlJobDef;
import lombok.*;

@Data
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class EtlJobDefSaveDTO {

    private EtlJobDef etl_job_def;

    private EtlDependency etl_dependency;

    private Long[] pre_etl_job_ids;
}
