package hyren.serv6.r.record.web;

import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DfTableApply;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.r.record.Req.CreateTableVO;
import hyren.serv6.r.record.Req.SelectData;
import hyren.serv6.r.record.Res.ResData;
import hyren.serv6.r.record.service.DfTableApplyService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("dataSupplementEntry/dfTableApply")
@RestController
public class DfTableApplyController {

    @Autowired
    DfTableApplyService dfTableApplyService;

    @RequestMapping("/findAll")
    public ResData findAll(@RequestBody SelectData page) {
        return dfTableApplyService.findAll(page);
    }

    @RequestMapping("/findByRecordList")
    public ResData findByRecordList(@RequestBody SelectData page) {
        return dfTableApplyService.findByRecordList(page);
    }

    @RequestMapping("/findRecordListByName")
    public ResData findRecordListByName(@RequestBody SelectData page) {
        return dfTableApplyService.findRecordListByName(page);
    }

    @RequestMapping("/findAllListByName")
    public ResData findAllListByName(@RequestBody SelectData page) {
        return dfTableApplyService.findAllListByName(page);
    }

    @RequestMapping("/findCursorList")
    public ResData findCursorList(@RequestBody SelectData dfId) {
        return dfTableApplyService.findCursorList(dfId);
    }

    @RequestMapping("/findList/{targetTableId}")
    public ResData findList(@RequestBody SelectData data, @PathVariable long targetTableId) {
        return dfTableApplyService.findList(data, targetTableId);
    }

    @RequestMapping("/updateStatus")
    public boolean updateStatus(Long dfId) {
        return dfTableApplyService.updateStatus(dfId);
    }

    @RequestMapping("/importExcel")
    public ResData importExcel(@RequestParam("file") MultipartFile excel) {
        return dfTableApplyService.importExcel(excel);
    }

    @RequestMapping("/updateCursor")
    public boolean updateCursor(@RequestBody SelectData reqData) {
        return dfTableApplyService.updateCursor(reqData.getApplyTabId(), reqData.getData(), reqData.getTargetTableId(), reqData.getDfPid());
    }

    @RequestMapping("/recordExportExcel")
    public void recordExportExcel(@RequestBody List<Map<String, String>> data) {
        dfTableApplyService.recordExportExcel(data);
    }

    @RequestMapping("/exportCursor")
    public void exportCursor(@RequestBody SelectData dfId) {
        dfTableApplyService.exportCursor(dfId);
    }

    @RequestMapping("/createTempTable")
    public void createTempTable(@RequestBody CreateTableVO createTableVO) {
        List<Map<String, Object>> queryList = Dbo.queryList("SELECT * FROM " + DfTableApply.TableName + " WHERE df_pid=? AND table_id=? AND is_rec=?", createTableVO.getDfPid(), createTableVO.getTableId(), IsFlag.Shi.getCode());
        if (queryList.isEmpty()) {
            dfTableApplyService.createTempTable(createTableVO.getKeyData(), createTableVO.getData(), createTableVO.getDfPid(), createTableVO.getTableId(), createTableVO.getTableName());
        } else {
            throw new BusinessException("此项目下 " + createTableVO.getTableName() + " 表进行补录过,无法二次补录");
        }
    }

    @RequestMapping("/exportDslTable")
    public void exportDslTable(Long dfAppId) {
        dfTableApplyService.exportDslTable(dfAppId);
    }
}
