package hyren.serv6.c.entity;

import fd.ng.core.annotation.DocBean;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(tableName = "job_hand")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobHandBean extends ProEntity {

    private static final long serialVersionUID = -3688380256036681819L;

    @DocBean(name = "etl_job_id", value = "", dataType = Long.class, required = false)
    private Long etl_job_id;

    @DocBean(name = "etl_sys_id", value = "", dataType = Long.class, required = false)
    private Long etl_sys_id;

    @DocBean(value = "", name = "作业名称", dataType = String.class, required = true)
    private String etl_job;

    @DocBean(name = "etl_hand_type", value = "", dataType = String.class, required = false)
    private String etl_hand_type;

    @DocBean(name = "curr_bath_date", value = "", dataType = String.class, required = false)
    private String curr_bath_date;
}
