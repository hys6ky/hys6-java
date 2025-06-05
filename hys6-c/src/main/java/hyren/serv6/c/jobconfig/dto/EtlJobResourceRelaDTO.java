package hyren.serv6.c.jobconfig.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EtlJobResourceRelaDTO {

    private Long etl_sys_id;

    private String etl_job;

    private String pageType;

    private Integer currPage;

    private Integer pageSize;

    private String resource_type;
}
