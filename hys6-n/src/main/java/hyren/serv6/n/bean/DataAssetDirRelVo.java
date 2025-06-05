package hyren.serv6.n.bean;

import lombok.Data;
import java.util.List;

@Data
public class DataAssetDirRelVo {

    private String dirId;

    private List<DataAssetRegistVo> dataAssetRegistVoList;
}
