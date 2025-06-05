package hyren.serv6.r.record.service;

import hyren.serv6.r.record.Req.SelectData;
import hyren.serv6.r.record.Res.ResData;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;
import java.util.Map;

public interface DfTableApplyService {

    ResData findAll(SelectData page);

    ResData findByRecordList(SelectData page);

    ResData findRecordListByName(SelectData page);

    ResData findAllListByName(SelectData page);

    ResData findCursorList(SelectData dfId);

    boolean updateStatus(Long dfId);

    ResData importExcel(MultipartFile excel);

    void recordExportExcel(List<Map<String, String>> data);

    void exportCursor(SelectData dfId);

    ResData findList(SelectData dfId, long targetTableId);

    void exportDslTable(Long dfAppId);

    boolean updateCursor(Long applyId, List<Map<String, String>> data, Long targetTableId, Long dfId);

    void createTempTable(List<String> keyData, List<Map<String, String>> data, Long dfPid, Long tableId, String tableName);
}
