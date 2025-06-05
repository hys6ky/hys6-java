package hyren.serv6.agent.job.biz.core.dfstage.service;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.agent.job.biz.core.dfstage.fileparser.FileParserFactory;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import java.util.concurrent.Callable;

@DocClass(desc = "", author = "zxz", createdate = "2020/04/21 16:19")
public class FileConversionThread implements Callable<String> {

    private final TableBean tableBean;

    private final CollectTableBean collectTableBean;

    private final String readFile;

    public FileConversionThread(TableBean tableBean, CollectTableBean collectTableBean, String readFile) {
        this.tableBean = tableBean;
        this.collectTableBean = collectTableBean;
        this.readFile = readFile;
    }

    @Override
    public String call() throws Exception {
        return FileParserFactory.getFileParserImpl(tableBean, collectTableBean, readFile);
    }
}
