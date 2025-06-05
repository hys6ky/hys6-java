package hyren.serv6.k.dm.metadatamanage.transctrl;

import fd.ng.db.resultset.Result;
import java.util.Map;

public class MutexException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private Result rs;

    private Map<String, Object> ctrlTable;

    public Result getMutexList() {
        return rs;
    }

    public Map<String, Object> getCtrlTable() {
        return ctrlTable;
    }

    public MutexException(String msg, Result rsMutex) {
        super(msg);
        rs = rsMutex;
        this.ctrlTable = null;
    }

    public MutexException(String msg, Map<String, Object> ctrlTable) {
        super(msg);
        rs = null;
        this.ctrlTable = ctrlTable;
    }

    public MutexException(String msg, Result rsMutex, Map<String, Object> ctrlTable) {
        super(msg);
        rs = rsMutex;
        this.ctrlTable = ctrlTable;
    }

    public MutexException(String msg, Result rsMutex, Throwable ex) {
        super(msg, ex);
        this.rs = rsMutex;
        this.ctrlTable = null;
    }
}
