package hyren.serv6.commons.jobUtil.beans;

import hyren.serv6.base.entity.EtlJobCur;

public class EtlJobBean extends EtlJobCur implements Comparable<EtlJobBean> {

    private String strNextDate;

    private boolean preDateFlag;

    private boolean dependencyFlag;

    private int DoneDependencyJobCount;

    private long executeTime;

    private long jobStartTime;

    public EtlJobBean() {
        super();
    }

    public String getStrNextDate() {
        return strNextDate;
    }

    public void setStrNextDate(String strNextDate) {
        this.strNextDate = strNextDate;
    }

    public boolean isPreDateFlag() {
        return preDateFlag;
    }

    public void setPreDateFlag(boolean preDateFlag) {
        this.preDateFlag = preDateFlag;
    }

    public boolean isDependencyFlag() {
        return dependencyFlag;
    }

    public void setDependencyFlag(boolean dependencyFlag) {
        this.dependencyFlag = dependencyFlag;
    }

    public int getDoneDependencyJobCount() {
        return DoneDependencyJobCount;
    }

    public void setDoneDependencyJobCount(int doneDependencyJobCount) {
        DoneDependencyJobCount = doneDependencyJobCount;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public long getJobStartTime() {
        return jobStartTime;
    }

    public void setJobStartTime(long jobStartTime) {
        this.jobStartTime = jobStartTime;
    }

    @Override
    public int compareTo(EtlJobBean otherEtlJob) {
        if (null == otherEtlJob) {
            return 0;
        }
        return otherEtlJob.getJob_priority_curr().compareTo(super.getJob_priority_curr());
    }
}
