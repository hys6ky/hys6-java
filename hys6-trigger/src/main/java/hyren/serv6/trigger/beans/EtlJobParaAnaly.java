package hyren.serv6.trigger.beans;

import hyren.serv6.base.entity.EtlJobCur;

public class EtlJobParaAnaly {

    private EtlJobCur etlJobCur;

    private boolean hasHandle;

    private boolean hasEtlJob;

    public EtlJobCur getEtlJobCur() {
        return etlJobCur;
    }

    public void setEtlJobCur(EtlJobCur etlJobCur) {
        this.etlJobCur = etlJobCur;
    }

    public boolean isHasHandle() {
        return hasHandle;
    }

    public void setHasHandle(boolean hasHandle) {
        this.hasHandle = hasHandle;
    }

    public boolean isHasEtlJob() {
        return hasEtlJob;
    }

    public void setHasEtlJob(boolean hasEtlJob) {
        this.hasEtlJob = hasEtlJob;
    }
}
