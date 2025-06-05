package hyren.serv6.stream.agent.producer.string.file.dirString;

import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.stream.agent.producer.commons.FileDataValidator;
import hyren.serv6.stream.agent.producer.commons.GetFileParams;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class ReaderFileString implements Runnable {

    private JobParamsEntity jobParams;

    private List<String> listColumn;

    private File file;

    private File fileRename;

    private String readMode;

    private ConcurrentMap<String, String> htMap;

    private String charset;

    private String beforeLine;

    private ConcurrentMap<String, String> htMapThread;

    private FileDataValidator fileDataValidator;

    private MapDBHelper mapDBHelper;

    private GetFileParams getFileParams = new GetFileParams();

    public ReaderFileString(MapDBHelper mapDBHelper, ConcurrentMap<String, String> htmap, ConcurrentMap<String, String> htMapThread, String readMode, JobParamsEntity jobParams, File file, File fileRename, String charset, String beforeLine, FileDataValidator fileDataValidator) {
        this.mapDBHelper = mapDBHelper;
        this.jobParams = jobParams;
        this.listColumn = jobParams.getListColumn();
        this.file = file;
        this.fileRename = fileRename;
        this.readMode = readMode;
        this.htMap = htmap;
        this.charset = charset;
        this.beforeLine = beforeLine;
        this.htMapThread = htMapThread;
        this.fileDataValidator = fileDataValidator;
    }

    @Override
    public void run() {
        Map<String, Object> json = getFileParams.getParmJson(listColumn, file);
        ReadFileProcessorString readFileProcessor = new ReadFileProcessorString();
        if (readMode.equals("1")) {
            readFileProcessor.lineProcessor(getFileParams, mapDBHelper, htMap, htMapThread, file, listColumn, jobParams, fileRename, beforeLine, json, fileDataValidator);
        } else {
            readFileProcessor.objectProcessor(mapDBHelper, htMap, htMapThread, file, listColumn, jobParams, fileRename, charset, json);
        }
    }
}
