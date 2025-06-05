package hyren.serv6.agent.job.biz.core.dbstage.writer;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.agent.job.biz.core.dbstage.writer.impl.*;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.IHadoopFile;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.hadoop.writer.FileWriterInterface;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import java.sql.ResultSet;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/6 17:09")
public class FileWriterFactory {

    private FileWriterFactory() {
    }

    public static String getFileWriterImpl(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef data_extraction_def, boolean writeHeaderFlag) {
        UnloadType unload_type = UnloadType.ofEnumByCode(collectTableBean.getUnload_type());
        FileFormat format = FileFormat.ofEnumByCode(data_extraction_def.getDbfile_format());
        FileWriterInterface fileWriterInterface;
        if (UnloadType.ZengLiangXieShu == unload_type) {
            if (FileFormat.CSV == format) {
                fileWriterInterface = new JdbcToCsvIncrementFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def, writeHeaderFlag);
                return fileWriterInterface.writeFiles();
            } else if (FileFormat.DingChang == format) {
                fileWriterInterface = new JdbcToFixedIncrementFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def, writeHeaderFlag);
                return fileWriterInterface.writeFiles();
            } else if (FileFormat.FeiDingChang == format) {
                fileWriterInterface = new JdbcToNonFixedIncrementFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def, writeHeaderFlag);
                return fileWriterInterface.writeFiles();
            } else {
                throw new AppSystemException("增量数据库抽取落地平台仅支持落地CSV/定长/非定长数据文件");
            }
        } else if (UnloadType.QuanLiangXieShu == unload_type) {
            if (FileFormat.CSV == format) {
                fileWriterInterface = new JdbcToCsvFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def);
                return fileWriterInterface.writeFiles();
            } else if (FileFormat.ORC == format) {
                IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
                return iHadoopFile.orcFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def);
            } else if (FileFormat.PARQUET == format) {
                IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
                return iHadoopFile.parquetFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def);
            } else if (FileFormat.SEQUENCEFILE == format) {
                IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
                return iHadoopFile.sequenceFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def);
            } else if (FileFormat.DingChang == format) {
                fileWriterInterface = new JdbcToFixedFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def);
                return fileWriterInterface.writeFiles();
            } else if (FileFormat.FeiDingChang == format) {
                fileWriterInterface = new JdbcToNonFixedFileWriter(resultSet, collectTableBean, pageNum, tableBean, data_extraction_def);
                return fileWriterInterface.writeFiles();
            } else {
                throw new AppSystemException("全量数据库抽取落地平台" + "仅支持落地CSV/PARQUET/ORC/SEQUENCE/定长/非定长数据文件");
            }
        } else {
            throw new AppSystemException("数据库抽取方式参数不正确");
        }
    }
}
