package hyren.serv6.agent.job.biz.utils;

import lombok.extern.slf4j.Slf4j;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class WriterFile implements Closeable {

    private final String filePath;

    private BufferedWriter bufferedWriter = null;

    private BufferedWriter incrementBufferedWriter = null;

    private CsvListWriter csvWriter = null;

    public WriterFile(String filePath) {
        this.filePath = filePath;
        File parentFile = new File(filePath).getParentFile();
        if (!parentFile.exists()) {
            boolean mkdirs = parentFile.mkdirs();
            log.info("创建文件夹..." + parentFile.getAbsolutePath() + "..." + mkdirs);
        }
    }

    public CsvListWriter getCsvWriter(String charset) {
        try {
            csvWriter = new CsvListWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(filePath)), charset), CsvPreference.EXCEL_PREFERENCE);
        } catch (IOException e) {
            log.error("获取csv文件流失败：", e);
        }
        return csvWriter;
    }

    public CsvListWriter getIncrementCsvWriter(String charset) {
        try {
            csvWriter = new CsvListWriter(new OutputStreamWriter(new FileOutputStream(filePath, true), charset), CsvPreference.EXCEL_PREFERENCE);
        } catch (IOException e) {
            log.error("获取csv文件流失败:", e);
        }
        return csvWriter;
    }

    public BufferedWriter getBufferedWriter(String charset) {
        try {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(filePath)), charset));
        } catch (IOException e) {
            log.error("获取BufferedWriter失败：", e);
        }
        return bufferedWriter;
    }

    public BufferedWriter getIncrementBufferedWriter(String charset) {
        try {
            incrementBufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, true), charset));
        } catch (IOException e) {
            log.error("获取BufferedWriter失败：", e);
        }
        return incrementBufferedWriter;
    }

    public void bufferedWriterClose() {
        try {
            if (bufferedWriter != null)
                bufferedWriter.close();
        } catch (IOException e) {
            log.error("bufferedWriter文件关闭失败！！！", e);
        }
    }

    public void incrementBufferedWriterClose() {
        try {
            if (incrementBufferedWriter != null)
                incrementBufferedWriter.close();
        } catch (IOException e) {
            log.error("incrementBufferedWriter文件关闭失败！！！", e);
        }
    }

    public void csvClose() {
        try {
            if (csvWriter != null)
                csvWriter.close();
        } catch (IOException e) {
            log.error("csvWriter文件关闭失败！！！", e);
        }
    }

    public void close() {
        log.info("调用了关闭文件流");
        try {
            if (bufferedWriter != null)
                bufferedWriter.close();
            if (csvWriter != null)
                csvWriter.close();
        } catch (IOException e) {
            log.error("文件流关闭失败！！！", e);
        }
    }
}
