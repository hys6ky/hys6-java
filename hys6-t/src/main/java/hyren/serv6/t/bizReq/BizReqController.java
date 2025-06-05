package hyren.serv6.t.bizReq;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.user.User;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.t.contants.DateConstants;
import hyren.serv6.t.contants.TaskStatusEnum;
import hyren.serv6.t.contants.ReqCategoryEnum;
import hyren.serv6.t.entity.TskBizReq;
import hyren.serv6.t.entity.TskTblAssign;
import hyren.serv6.t.tableAssign.TskTableAssignServe;
import hyren.serv6.t.util.IdGenerator;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/bizReq")
@RestController
public class BizReqController {

    private BizReqServeImpl bizReqServeImpl;

    private TskTableAssignServe tskTableAssignServe;

    public BizReqController(BizReqServeImpl bizReqServeImpl, TskTableAssignServe tskTableAssignServe) {
        this.bizReqServeImpl = bizReqServeImpl;
        this.tskTableAssignServe = tskTableAssignServe;
    }

    @GetMapping("/queryBizList")
    @ApiModelProperty(value = "")
    public Map<String, Object> queryBizList(@Validated TskBizReq tskBizReq, int currPage, int pageSize) {
        Validator.notNull(currPage, "分页信息不能为空");
        Validator.notNull(pageSize, "分页信息不能为空");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> pageList = bizReqServeImpl.queryBizList(tskBizReq, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @GetMapping("/queryOneBiz")
    @ApiModelProperty(value = "")
    public TskBizReq queryOneBizList(Long biz_id) {
        return bizReqServeImpl.queryOneBizList(biz_id);
    }

    @PostMapping("/stopState")
    @ApiModelProperty(value = "")
    public void stopState(Long biz_id) {
        Validator.notNull(biz_id, "业务ID不能为空");
        bizReqServeImpl.stopState(biz_id);
    }

    @PostMapping("/saveBizReq")
    @ApiModelProperty(value = "")
    @ResponseBody
    public void saveBizReq(@Validated TskBizReq tskBizReq, MultipartFile file) {
        Validator.notNull(tskBizReq, "业务信息不能为空");
        Validator.notNull(tskBizReq.getBiz_name(), "业务名称不能为空");
        Validator.notNull(tskBizReq.getBiz_desc(), "业务描述不能为空");
        Validator.notNull(tskBizReq.getOwner_name(), "业务需求提出人不能为空");
        Validator.notNull(tskBizReq.getOnline_date(), "期望上线日期不能为空");
        Validator.notNull(tskBizReq.getDept(), "业务需求提出部门不能为空");
        bizReqServeImpl.bizNameCheck(tskBizReq.getBiz_name());
        tskBizReq.setBiz_id(IdGenerator.nextId());
        tskBizReq.setCreated_time(DateFormatUtils.format(new Date(), DateConstants.FORMAT_DATE_TIME));
        tskBizReq.setCreated_id(ContextDataHolder.getUserInfo(User.class).getUserId());
        tskBizReq.setCreated_by(ContextDataHolder.getUserInfo(User.class).getUsername());
        tskBizReq.setBiz_status(TaskStatusEnum.TO_BE_DEV.getCode());
        if (file != null && !file.isEmpty()) {
            tskBizReq.setAtt_name(file.getOriginalFilename());
            String fileName = bizReqServeImpl.uploadFile(file);
            tskBizReq.setAtt_path(fileName);
        }
        bizReqServeImpl.saveBizReq(tskBizReq);
    }

    @PostMapping("/updateBizReq")
    @ApiModelProperty(value = "")
    @ResponseBody
    public void updateBizReq(@Validated TskBizReq tskBizReq, MultipartFile file) {
        Validator.notNull(tskBizReq, "业务信息不能为空");
        Validator.notNull(tskBizReq.getBiz_id(), "业务ID不能为空");
        Validator.notNull(tskBizReq.getBiz_name(), "业务名称不能为空");
        Validator.notNull(tskBizReq.getBiz_desc(), "业务描述不能为空");
        Validator.notNull(tskBizReq.getOwner_name(), "业务需求提出人不能为空");
        Validator.notNull(tskBizReq.getOnline_date(), "期望上线日期不能为空");
        Validator.notNull(tskBizReq.getDept(), "业务需求提出部门不能为空");
        bizReqServeImpl.bizNameCheck(tskBizReq.getBiz_name(), tskBizReq.getBiz_id());
        tskBizReq.setUpdated_id(ContextDataHolder.getUserInfo(User.class).getUserId());
        tskBizReq.setUpdated_by(ContextDataHolder.getUserInfo(User.class).getUsername());
        tskBizReq.setUpdated_time(DateFormatUtils.format(new Date(), DateConstants.FORMAT_DATE_TIME));
        if (file != null && !file.isEmpty()) {
            tskBizReq.setAtt_name(file.getOriginalFilename());
            String fileName = bizReqServeImpl.uploadFile(file);
            tskBizReq.setAtt_path(fileName);
        }
        bizReqServeImpl.updateBizReq(tskBizReq);
    }

    @DeleteMapping("/deleteBizReq")
    @ApiModelProperty(value = "")
    public void deleteBizReq(Long biz_id) {
        Validator.notNull(biz_id, "业务id不能为空");
        bizReqServeImpl.deleteBizReq(biz_id);
    }

    @PostMapping("/tableBizReq/{data_type}")
    @ApiModelProperty(value = "")
    public void tableBizReq(@RequestBody List<TskTblAssign> tskTblAssigns, @ApiParam(name = "data_type", value = "", required = true) @PathVariable("data_type") String data_type) {
        Validator.notNull(data_type, "业务表信息资源来源不能为空！");
        bizReqServeImpl.relationTable(tskTblAssigns, data_type);
    }

    @GetMapping("/getAssignTable")
    @ApiModelProperty(value = "")
    public List<TskTblAssign> getAssignTable(Long id) {
        return tskTableAssignServe.queryList(id, ReqCategoryEnum.BIZ);
    }

    @DeleteMapping("/batchDelete")
    @ApiModelProperty(value = "")
    public void tableBizReq(String ids) {
        bizReqServeImpl.tableBizReq(ids);
    }

    @GetMapping("/download")
    @ApiModelProperty(value = "")
    public void downloadBizReq(Long biz_id) {
        TskBizReq tskBizReq = bizReqServeImpl.queryOneBizList(biz_id);
        FileDownloadUtil.downloadFile(tskBizReq.getAtt_path());
    }

    @GetMapping("/getTreeDataInfo")
    @ApiModelProperty(value = "")
    public List<Node> getTreeDataInfo() {
        return bizReqServeImpl.getTreeDataInfo();
    }

    @GetMapping("/getMetaTreeDataInfo")
    @ApiModelProperty(value = "")
    public List<Node> getMetaTreeDataInfo(String isPrco) {
        return bizReqServeImpl.getMetaTreeDataInfo(isPrco);
    }
}
