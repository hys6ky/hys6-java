package hyren.serv6.agent.resourceused;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-09-28 15:55")
public class TimerManager {

    private Timer timer = null;

    private static final long PERIOD_DAY = 60 * 1000;

    public TimerManager() {
        timer = new Timer();
    }

    public void autoWriterResourceInfo() {
        Date date = getDate();
        WriterResource resource = new WriterResource();
        timer.schedule(resource, date, PERIOD_DAY);
    }

    private Date getDate() {
        Date date;
        String TimePoint = DateUtil.getSysDate() + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime());
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
        try {
            date = format.parse(TimePoint);
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw (BusinessException) e;
            } else {
                throw new AppSystemException(e);
            }
        }
        return date;
    }
}
