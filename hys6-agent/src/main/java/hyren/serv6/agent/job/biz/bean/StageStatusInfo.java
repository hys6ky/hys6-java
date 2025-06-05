package hyren.serv6.agent.job.biz.bean;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.codes.IsFlag;
import java.io.Serializable;

@DocClass(desc = "")
public class StageStatusInfo implements Serializable {

    private static final long serialVersionUID = 2931307277043396275L;

    private String jobId;

    private int stageNameCode;

    private int statusCode;

    private String startDate;

    private String startTime;

    private String endDate;

    private String endTime;

    private String message;

    private String isAgain;

    private Integer againNum;

    public StageStatusInfo() {
        this.isAgain = IsFlag.Fou.getCode();
        this.againNum = 0;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getStageNameCode() {
        return stageNameCode;
    }

    public void setStageNameCode(int stageNameCode) {
        this.stageNameCode = stageNameCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getIsAgain() {
        return isAgain;
    }

    public void setIsAgain(String isAgain) {
        this.isAgain = isAgain;
    }

    public Integer getAgainNum() {
        return againNum;
    }

    public void setAgainNum(Integer againNum) {
        this.againNum = againNum;
    }
}
