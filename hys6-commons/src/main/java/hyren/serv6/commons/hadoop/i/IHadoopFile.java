package hyren.serv6.commons.hadoop.i;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;

public interface IHadoopFile {

    String parquetFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef);

    String sequenceFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef);

    String orcFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef);

    long readSequenceToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception;

    long readParquetToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception;

    long readOrcToDataBase(DatabaseWrapper db, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception;

    String parserParquetFile(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception;

    String parserSequenceFile(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception;

    String parserOrcFile(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception;

    List<String> readHdfsFile(String outPutFilePath) throws IOException;
}
