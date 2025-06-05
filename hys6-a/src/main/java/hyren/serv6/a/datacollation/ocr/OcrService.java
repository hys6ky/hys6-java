package hyren.serv6.a.datacollation.ocr;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.FileNameUtils;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("ocrService")
public class OcrService {

    public List<Map<String, Object>> getFileCollectionDataSources() {
        return Dbo.queryList(" select ds.source_id,ds.datasource_name" + " from " + SourceRelationDep.TableName + " srd" + " join " + DataSource.TableName + " ds on srd.source_id = ds.source_id" + " join " + AgentInfo.TableName + " ai on ds.source_id = ai.source_id" + " where ai.agent_type = ?" + " GROUP BY ds.source_id,ds.datasource_name", AgentType.WenJianXiTong.getCode());
    }

    public List<Map<String, Object>> getFileCollectionTasks(long sourceId) {
        return Dbo.queryList("select * from " + FileCollectSet.TableName + " fc" + " join " + AgentInfo.TableName + " ai on fc.agent_id = ai.agent_id" + " where ai.source_id = ? AND ai.agent_type = ? AND fc.is_sendok = ? ", sourceId, AgentType.WenJianXiTong.getCode(), IsFlag.Shi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fcs_id", desc = "", range = "")
    public void startOcrRunBatch(long fcs_id) {
        List<Map<String, Object>> avroPaths = Dbo.queryList("SELECT DISTINCT file_avro_path,storage_date FROM " + SourceFileAttribute.TableName + " WHERE collect_set_id = ? AND collect_type = ? ORDER BY storage_date", fcs_id, AgentType.WenJianXiTong.getCode());
        List<String> fcsPathList = new ArrayList<>();
        for (Map<String, Object> avroPath : avroPaths) {
            String file_avro_path = FileNameUtils.getFullPath(avroPath.get("file_avro_path").toString());
            if (!fcsPathList.contains(file_avro_path)) {
                fcsPathList.add(file_avro_path);
            }
        }
        try {
            ClassBase.ocrInstance().OCR2AvroAndSolr(fcsPathList);
        } catch (Exception e) {
            throw new BusinessException(String.format("Failed to excute ocr job! exception: [ %s ]", e));
        }
    }
}
