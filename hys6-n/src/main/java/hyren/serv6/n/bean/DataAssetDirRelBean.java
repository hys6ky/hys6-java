package hyren.serv6.n.bean;

import lombok.Data;
import java.util.List;

@Data
public class DataAssetDirRelBean {

    private long dirId;

    private List<DataAssetRegistVo> dataAssetRegistVoList;
}
