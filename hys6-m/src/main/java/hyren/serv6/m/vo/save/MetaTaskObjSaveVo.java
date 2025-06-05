package hyren.serv6.m.vo.save;

import hyren.serv6.m.entity.MetaTaskObj;
import io.swagger.annotations.ApiModel;
import lombok.Data;
import java.util.List;

@ApiModel(value = "", description = "")
@Data
public class MetaTaskObjSaveVo {

    private Long task_id;

    private List<MetaSourceObjCacheSaveVo> objList;
}
