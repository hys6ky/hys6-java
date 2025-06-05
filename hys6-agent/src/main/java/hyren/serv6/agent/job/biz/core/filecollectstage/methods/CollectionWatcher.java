package hyren.serv6.agent.job.biz.core.filecollectstage.methods;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.utils.CommunicationUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.ExecuteState;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.CollectCase;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import java.util.UUID;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/4 17:20")
public class CollectionWatcher {

    private final FileCollectParamBean fileCollectParamBean;

    private final CollectCase collectCase = new CollectCase();

    private String job_rs_id;

    private String excuteLength;

    private Long collect_total;

    public void setCollect_total(Long collect_total) {
        this.collect_total = collect_total;
    }

    public CollectCase getCollectCase() {
        return collectCase;
    }

    public void setExcuteLength(String excuteLength) {
        this.excuteLength = excuteLength;
    }

    public CollectionWatcher(FileCollectParamBean fileCollectParamBean) {
        this.fileCollectParamBean = fileCollectParamBean;
        initJobInfo();
    }

    public String getJob_rs_id() {
        return job_rs_id;
    }

    private void initJobInfo() {
        collectCase.setJob_rs_id(UUID.randomUUID().toString());
        job_rs_id = collectCase.getJob_rs_id();
        collectCase.setAgent_id(fileCollectParamBean.getAgent_id());
        collectCase.setSource_id(Long.parseLong(fileCollectParamBean.getFcs_id()));
        collectCase.setCollect_set_id(Long.parseLong(fileCollectParamBean.getFile_source_id()));
        collectCase.setEtl_date(fileCollectParamBean.getSysDate());
        collectCase.setTask_classify(fileCollectParamBean.getFile_source_path());
        collectCase.setCc_remark("");
        collectCase.setJob_group("");
        collectCase.setCollect_type(AgentType.WenJianXiTong.getCode());
        collectCase.setJob_type(AgentType.WenJianXiTong.getCode());
        collectCase.setCollet_database_size("");
        collectCase.setIs_again(IsFlag.Fou.getCode());
        startJob();
    }

    private void startJob() {
        collectCase.setCollect_s_date(DateUtil.getSysDate());
        collectCase.setCollect_s_time(DateUtil.getSysTime());
        collectCase.setCollect_e_date("-");
        collectCase.setCollect_e_time("-");
        collectCase.setExecute_state(ExecuteState.KaiShiYunXing.getCode());
        collectCase.setExecute_length("-");
        collectCase.setCollect_total(collect_total);
    }

    public void endJob(String loadMessage) {
        collectCase.setCollect_e_date(DateUtil.getSysDate());
        collectCase.setCollect_e_time(DateUtil.getSysTime());
        collectCase.setExecute_length(excuteLength);
        collectCase.setCollect_total(collect_total);
        if (!StringUtil.isBlank(loadMessage)) {
            collectCase.setExecute_state(ExecuteState.TongZhiChengGong.getCode());
        } else {
            collectCase.setExecute_state(ExecuteState.TongZhiShiBai.getCode());
        }
        CommunicationUtil.saveCollectCase(collectCase, loadMessage);
    }
}
