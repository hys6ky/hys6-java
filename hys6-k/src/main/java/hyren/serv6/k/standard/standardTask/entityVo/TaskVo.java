package hyren.serv6.k.standard.standardTask.entityVo;

import hyren.serv6.k.entity.StandardTask;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import java.util.List;

@Data
public class TaskVo extends StandardTask {

    private List<Long> objIds;

    public List<Long> getObjIds() {
        return objIds;
    }

    public void setObjIds(List<Long> objIds) {
        this.objIds = objIds;
    }
}
