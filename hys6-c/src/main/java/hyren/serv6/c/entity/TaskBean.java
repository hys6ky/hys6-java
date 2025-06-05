package hyren.serv6.c.entity;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "yanerchuan")
public class TaskBean extends ProEntity {

    private String sub_sys_desc;

    private String jobNum;

    private String curr_st_time;

    private String curr_end_time;

    private String taskConsumeTime;

    private String taskConsumeAveTime;

    public String getSub_sys_desc() {
        return sub_sys_desc;
    }

    public void setSub_sys_desc(String sub_sys_desc) {
        this.sub_sys_desc = sub_sys_desc;
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

    public String getTaskConsumeTime() {
        return taskConsumeTime;
    }

    public void setTaskConsumeTime(String taskConsumeTime) {
        this.taskConsumeTime = taskConsumeTime;
    }

    public String getTaskConsumeAveTime() {
        return taskConsumeAveTime;
    }

    public void setTaskConsumeAveTime(String taskConsumeAveTime) {
        this.taskConsumeAveTime = taskConsumeAveTime;
    }
}
