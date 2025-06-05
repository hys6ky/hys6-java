package hyren.serv6.base.utils.jsch;

import com.jcraft.jsch.SftpProgressMonitor;
import lombok.extern.slf4j.Slf4j;
import java.text.DecimalFormat;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class FileProgressMonitor extends TimerTask implements SftpProgressMonitor {

    private boolean isEnd = false;

    private long transfered;

    private long fileSize;

    private Timer timer;

    private boolean isScheduled = false;

    public FileProgressMonitor(long fileSize) {
        this.fileSize = fileSize;
    }

    @Override
    public void run() {
        if (!isEnd()) {
            long transfered = getTransfered();
            if (transfered != fileSize) {
                log.info("Current transfered: " + transfered + " bytes");
                sendProgressMessage(transfered);
            } else {
                setEnd();
            }
        } else {
            stop();
        }
    }

    public void stop() {
        if (timer != null) {
            timer.cancel();
            timer.purge();
            timer = null;
            isScheduled = false;
        }
    }

    public void start() {
        if (timer == null) {
            timer = new Timer();
        }
        timer.schedule(this, 1000, 5 * 1000);
        isScheduled = true;
    }

    private void sendProgressMessage(long transfered) {
        if (fileSize != 0) {
            double d = ((double) transfered * 100) / (double) fileSize;
            DecimalFormat df = new DecimalFormat("#.##");
            log.info("Sending progress message: " + df.format(d) + "%");
        } else {
            log.info("Sending progress message: " + transfered);
        }
    }

    @Override
    public boolean count(long count) {
        if (isEnd()) {
            return false;
        }
        if (!isScheduled) {
            start();
        }
        add(count);
        return true;
    }

    @Override
    public void end() {
        setEnd();
    }

    private synchronized void add(long count) {
        transfered = transfered + count;
    }

    private synchronized long getTransfered() {
        return transfered;
    }

    public synchronized void setTransfered(long transfered) {
        this.transfered = transfered;
    }

    private synchronized void setEnd() {
        this.isEnd = true;
    }

    private synchronized boolean isEnd() {
        return isEnd;
    }

    @Override
    public void init(int op, String src, String dest, long max) {
    }
}
