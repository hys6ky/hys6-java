package hyren.serv6.c.entity;

import fd.ng.core.annotation.DocClass;

@DocClass(desc = "", author = "yanerchuan")
public class JobBean {

    private String etl_job;

    private String etl_job_desc;

    private String curr_st_time;

    private String curr_end_time;

    private String jobTime;

    public String getEtl_job() {
        return etl_job;
    }

    public void setEtl_job(String etl_job) {
        this.etl_job = etl_job;
    }

    public String getEtl_job_desc() {
        return etl_job_desc;
    }

    public void setEtl_job_desc(String etl_job_desc) {
        this.etl_job_desc = etl_job_desc;
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

    public String getJobTime() {
        return jobTime;
    }

    public void setJobTime(String jobTime) {
        this.jobTime = jobTime;
    }
}
