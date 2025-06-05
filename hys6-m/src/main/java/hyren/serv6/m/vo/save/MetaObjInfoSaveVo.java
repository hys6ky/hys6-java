package hyren.serv6.m.vo.save;

import hyren.serv6.m.entity.MetaObjInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import java.util.List;

@ApiModel(value = "", description = "")
@Data
public class MetaObjInfoSaveVo extends MetaObjInfo {

    @ApiModelProperty(value = "", notes = "", dataType = "List<MetaObjTblColSaveVo>", required = true)
    private List<MetaObjTblColSaveVo> tblColSaveVoList;

    @ApiModelProperty(name = "函数对象", notes = "", dataType = "MetaObjFuncSaveVo", required = true)
    private MetaObjFuncSaveVo funcSaveVo;

    public interface Default {
    }

    public interface Edit {
    }
}
