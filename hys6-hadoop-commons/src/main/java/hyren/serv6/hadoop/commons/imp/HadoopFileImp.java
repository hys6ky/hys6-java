package hyren.serv6.hadoop.commons.imp;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.commons.hadoop.i.IHadoopFile;
import hyren.serv6.hadoop.commons.hadoop_helper.ConfigurationOperator;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.hadoop.commons.imp.fileparser.impl.OrcFileParserDeal;
import hyren.serv6.hadoop.commons.imp.fileparser.impl.ParquetFileParserDeal;
import hyren.serv6.hadoop.commons.imp.fileparser.impl.SequenceFileParserDeal;
import hyren.serv6.hadoop.commons.imp.readfile.ReadHadoopFileToDataBase;
import hyren.serv6.hadoop.commons.imp.writer.impl.JdbcToOrcFileWriter;
import hyren.serv6.hadoop.commons.imp.writer.impl.JdbcToParquetFileWriter;
import hyren.serv6.hadoop.commons.imp.writer.impl.JdbcToSequenceFileWriter;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class HadoopFileImp implements IHadoopFile {

    @Override
    public String parquetFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        JdbcToParquetFileWriter jdbcToParquetFileWriter = new JdbcToParquetFileWriter(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
        return jdbcToParquetFileWriter.writeFiles();
    }

    @Override
    public String sequenceFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        JdbcToSequenceFileWriter jdbcToSequenceFileWriter = new JdbcToSequenceFileWriter(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
        return jdbcToSequenceFileWriter.writeFiles();
    }

    @Override
    public String orcFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        JdbcToOrcFileWriter jdbcToOrcFileWriter = new JdbcToOrcFileWriter(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
        return jdbcToOrcFileWriter.writeFiles();
    }

    @Override
    public long readSequenceToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception {
        return ReadHadoopFileToDataBase.readSequenceToDataBase(db, columnList, typeList, batchSql, fileAbsolutePath, tableBean);
    }

    @Override
    public long readParquetToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception {
        return ReadHadoopFileToDataBase.readParquetToDataBase(db, columnList, typeList, batchSql, fileAbsolutePath, tableBean);
    }

    @Override
    public long readOrcToDataBase(DatabaseWrapper db, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception {
        return ReadHadoopFileToDataBase.readOrcToDataBase(db, typeList, batchSql, fileAbsolutePath, tableBean);
    }

    @Override
    public String parserParquetFile(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        ParquetFileParserDeal parquetFileParserDeal = new ParquetFileParserDeal(tableBean, collectTableBean, readFile);
        return parquetFileParserDeal.parserFile();
    }

    @Override
    public String parserSequenceFile(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        SequenceFileParserDeal sequenceFileParserDeal = new SequenceFileParserDeal(tableBean, collectTableBean, readFile);
        return sequenceFileParserDeal.parserFile();
    }

    @Override
    public String parserOrcFile(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        OrcFileParserDeal orcFileParserDeal = new OrcFileParserDeal(tableBean, collectTableBean, readFile);
        return orcFileParserDeal.parserFile();
    }

    @Override
    public List<String> readHdfsFile(String outPutFilePath) throws IOException {
        List<String> strings = new ArrayList<>();
        String outPath = outPutFilePath + Constant.HYFD_RESULT_PATH_NAME + "/part-00000";
        FileSystem fs;
        if (CommonVariables.FILE_COLLECTION_IS_WRITE_HADOOP) {
            HdfsOperator hdfsOperator = new HdfsOperator(System.getProperty("user.dir") + File.separator + "conf" + File.separator, HdfsOperator.PlatformType.cdh5.name());
            fs = FileSystem.get(URI.create(outPath), hdfsOperator.conf);
        } else {
            fs = FileSystem.get(URI.create(outPath), new ConfigurationOperator().getConfiguration());
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(outPath)), StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            strings.add(line);
        }
        return strings;
    }
}
