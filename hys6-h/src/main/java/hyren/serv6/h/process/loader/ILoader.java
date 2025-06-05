package hyren.serv6.h.process.loader;

import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import java.io.Closeable;

public interface ILoader extends Closeable {

    void init();

    ProcessJobTableConfBean getProcessJobTableConfBean();

    void ensureVersionRelation();

    void restore();

    void append();

    void replace();

    void upSert();

    void historyZipperFullLoading();

    void historyZipperIncrementLoading();

    void incrementalDataZipper();

    void handleException();

    void clean();

    void close();

    ProcessJobRunStatusEnum getJobRunStatus();
}
