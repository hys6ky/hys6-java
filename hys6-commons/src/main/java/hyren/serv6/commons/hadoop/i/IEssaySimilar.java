package hyren.serv6.commons.hadoop.i;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.IsFlag;
import java.util.List;
import java.util.Map;

public interface IEssaySimilar {

    @Method(desc = "", logicStep = "")
    @Param(name = "filePath", desc = "", range = "")
    @Param(name = "similarityRate", desc = "", range = "", valueIfNull = "1")
    @Param(name = "searchWayFlag", desc = "", range = "")
    @Return(desc = "", range = "")
    List<Map<String, String>> getDocumentSimilarFromSolr(String filePath, String similarityRate, IsFlag searchWayFlag);

    @Method(desc = "", logicStep = "")
    @Param(name = "filePath", desc = "", range = "")
    @Param(name = "blockId", desc = "", range = "")
    @Param(name = "fileId", desc = "", range = "")
    @Return(desc = "", range = "")
    String getFileSummaryFromAvro(String filePath, String blockId, String fileId);
}
