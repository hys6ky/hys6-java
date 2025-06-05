package hyren.serv6.b.datareceive.req;

import hyren.serv6.base.entity.DrAnalysis;
import hyren.serv6.base.entity.DrFileDef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReqFileInfo {

    private DrFileDef fileDef;

    private List<DrAnalysis> drAnalyses;
}
