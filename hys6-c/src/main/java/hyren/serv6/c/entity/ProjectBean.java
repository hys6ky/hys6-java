package hyren.serv6.c.entity;

import fd.ng.core.annotation.DocClass;

@DocClass(desc = "", author = "yanerchuan")
public class ProjectBean {

    private String taskNum;

    private String jobNum;

    private String curr_st_time;

    private String curr_end_time;

    private String projectConsumeTime;

    private String projectConsumeAveTime;

    public String getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(String taskNum) {
        this.taskNum = taskNum;
    }

    public String getJobNum() {
        return jobNum;
    }

    public void setJobNum(String jobNum) {
        this.jobNum = jobNum;
    }

    public String getCurr_st_time() {
        return curr_st_time;
    }

    public void setCurr_st_time(String curr_st_time) {
        this.curr_st_time = curr_st_time;
    }

    public String getCurr_end_time() {
        return curr_end_time;
    }

    public void setCurr_end_time(String curr_end_time) {
        this.curr_end_time = curr_end_time;
    }

    public String getProjectConsumeTime() {
        return projectConsumeTime;
    }

    public void setProjectConsumeTime(String projectConsumeTime) {
        this.projectConsumeTime = projectConsumeTime;
    }

    public String getProjectConsumeAveTime() {
        return projectConsumeAveTime;
    }

    public void setProjectConsumeAveTime(String projectConsumeAveTime) {
        this.projectConsumeAveTime = projectConsumeAveTime;
    }
}
