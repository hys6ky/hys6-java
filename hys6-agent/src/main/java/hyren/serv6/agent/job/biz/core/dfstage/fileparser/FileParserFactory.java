package hyren.serv6.agent.job.biz.core.dfstage.fileparser;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.core.dfstage.fileparser.impl.CsvFileParserDeal;
import hyren.serv6.agent.job.biz.core.dfstage.fileparser.impl.FixedFileParserDeal;
import hyren.serv6.agent.job.biz.core.dfstage.fileparser.impl.NonFixedFileParserDeal;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.IHadoopFile;
import hyren.serv6.commons.hadoop.fileparser.FileParserInterface;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import java.io.IOException;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/6 17:09")
public class FileParserFactory {

    private FileParserFactory() {
    }

    public static String getFileParserImpl(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        String format = CollectTableBeanUtil.getSourceData_extraction_def(collectTableBean.getData_extraction_def_list()).getDbfile_format();
        FileParserInterface fileParserInterface;
        if (FileFormat.CSV.getCode().equals(format)) {
            fileParserInterface = new CsvFileParserDeal(tableBean, collectTableBean, readFile);
            return parseFile(fileParserInterface);
        } else if (FileFormat.ORC.getCode().equals(format)) {
            IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
            return iHadoopFile.parserOrcFile(tableBean, collectTableBean, readFile);
        } else if (FileFormat.PARQUET.getCode().equals(format)) {
            IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
            return iHadoopFile.parserParquetFile(tableBean, collectTableBean, readFile);
        } else if (FileFormat.SEQUENCEFILE.getCode().equals(format)) {
            IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
            return iHadoopFile.parserSequenceFile(tableBean, collectTableBean, readFile);
        } else if (FileFormat.DingChang.getCode().equals(format)) {
            if (!StringUtil.isEmpty(tableBean.getColumn_separator())) {
                fileParserInterface = new NonFixedFileParserDeal(tableBean, collectTableBean, readFile);
            } else {
                fileParserInterface = new FixedFileParserDeal(tableBean, collectTableBean, readFile);
            }
            return parseFile(fileParserInterface);
        } else if (FileFormat.FeiDingChang.getCode().equals(format)) {
            fileParserInterface = new NonFixedFileParserDeal(tableBean, collectTableBean, readFile);
            return parseFile(fileParserInterface);
        } else {
            throw new AppSystemException("系统仅支持落地CSV/PARQUET/ORC/SEQUENCE/定长/非定长数据文件");
        }
    }

    private static String parseFile(FileParserInterface fileParserInterface) throws IOException {
        String parseResult = fileParserInterface.parserFile();
        fileParserInterface.stopStream();
        return parseResult;
    }
}
