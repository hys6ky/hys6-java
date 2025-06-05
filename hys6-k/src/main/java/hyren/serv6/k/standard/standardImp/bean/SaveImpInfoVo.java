package hyren.serv6.k.standard.standardImp.bean;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class SaveImpInfoVo {

    private Long imp_id;

    private Long obj_id;

    private Long dtl_id;

    private Long basic_id;

    private String imp_result;

    private String imp_detail;
}
