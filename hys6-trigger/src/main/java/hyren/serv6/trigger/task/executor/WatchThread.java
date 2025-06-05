package hyren.serv6.trigger.task.executor;

import lombok.extern.slf4j.Slf4j;
import java.io.*;

@Slf4j
class WatchThread extends Thread {

    private static final String ENCODING = "UTF-8";

    private InputStream inputStream;

    private String logDirc;

    WatchThread(InputStream inputStream, String logDirc) {
        this.inputStream = inputStream;
        this.logDirc = logDirc;
    }

    @Override
    public void run() {
        File file = new File(logDirc);
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(inputStreamReader);
            OutputStream outputStream = new FileOutputStream(file, true);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, ENCODING);
            PrintWriter pw = new PrintWriter(outputStreamWriter, true)) {
            if (!file.exists() && !file.createNewFile()) {
                log.warn("日志文件创建失败 {}", logDirc);
            }
            String line;
            while ((line = br.readLine()) != null) {
                pw.write(line + "\r\n");
                pw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
