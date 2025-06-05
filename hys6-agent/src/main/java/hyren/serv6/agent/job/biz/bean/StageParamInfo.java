package hyren.serv6.agent.job.biz.bean;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import java.io.Serializable;

@DocClass(desc = "")
public class StageParamInfo implements Serializable {

    private static final long serialVersionUID = 7280781296966361533L;

    private long rowCount = 0;

    private TableBean tableBean;

    private long fileSize = 0;

    private String[] fileArr;

    private String[] fileNameArr;

    private StageStatusInfo statusInfo;

    private String taskClassify;

    private String etlDate;

    private Long agentId;

    private Long collectSetId;

    private Long sourceId;

    private String collectType;

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public TableBean getTableBean() {
        return tableBean;
    }

    public void setTableBean(TableBean tableBean) {
        this.tableBean = tableBean;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String[] getFileArr() {
        return fileArr;
    }

    public void setFileArr(String[] fileArr) {
        this.fileArr = fileArr;
    }

    public StageStatusInfo getStatusInfo() {
        return statusInfo;
    }

    public void setStatusInfo(StageStatusInfo statusInfo) {
        this.statusInfo = statusInfo;
    }

    public String getTaskClassify() {
        return taskClassify;
    }

    public void setTaskClassify(String taskClassify) {
        this.taskClassify = taskClassify;
    }

    public String getEtlDate() {
        return etlDate;
    }

    public void setEtlDate(String etlDate) {
        this.etlDate = etlDate;
    }

    public Long getAgentId() {
        return agentId;
    }

    public void setAgentId(Long agentId) {
        this.agentId = agentId;
    }

    public Long getCollectSetId() {
        return collectSetId;
    }

    public void setCollectSetId(Long collectSetId) {
        this.collectSetId = collectSetId;
    }

    public Long getSourceId() {
        return sourceId;
    }

    public void setSourceId(Long sourceId) {
        this.sourceId = sourceId;
    }

    public String getCollectType() {
        return collectType;
    }

    public void setCollectType(String collectType) {
        this.collectType = collectType;
    }

    public String[] getFileNameArr() {
        return fileNameArr;
    }

    public void setFileNameArr(String[] fileNameArr) {
        this.fileNameArr = fileNameArr;
    }
}
