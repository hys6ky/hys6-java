package hyren.serv6.m.vo.query;

import hyren.serv6.m.entity.MetaObjInfoHis;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import java.util.List;

@ApiModel(value = "", description = "")
@Data
public class MetaObjInfoHisQueryVo extends MetaObjInfoHis {

    @ApiModelProperty(value = "", notes = "", dataType = "String", required = true)
    private String source_name;

    @ApiModelProperty(value = "", notes = "", dataType = "List", required = true)
    private List<MetaObjTblColHisQueryVo> colQueryVoList;

    @ApiModelProperty(name = "函数对象", notes = "", dataType = "MetaObjFuncQueryVo", required = true)
    private MetaObjFuncHisQueryVo funcQueryVo;
}
