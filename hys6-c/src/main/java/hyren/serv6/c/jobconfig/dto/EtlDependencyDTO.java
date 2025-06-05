package hyren.serv6.c.jobconfig.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlDependencyDTO {

    private Long etl_sys_id;

    private Long[] etl_job_ids;
}
