package hyren.serv6.n.bean;

import lombok.Data;
import java.util.List;

@Data
public class DataAssetRegistBean {

    private DataAssetRegistVo dataAssetRegistVo;

    private List<DataAssetColumnVo> dataAssetColumnVo;

    private List<DataAssetEnumVo> dataAssetEnumVoList;

    private long taskId;
}
