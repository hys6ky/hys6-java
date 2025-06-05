package hyren.serv6.b.datareceive.req;

import hyren.serv6.base.entity.DrAnalysis;
import hyren.serv6.base.entity.DrFileDef;
import hyren.serv6.base.entity.DrParamsDef;
import hyren.serv6.base.entity.DrTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReqTaskInfo {

    private DrTask drTask;

    private List<DrParamsDef> drParamsDefList;

    private DrFileDef fileDef;

    private List<DrAnalysis> drAnalyses;

    private String ParamValues;
}
