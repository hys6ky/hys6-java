package hyren.serv6.hadoop.commons.imp;

import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.ISolrWithHadoop;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.hadoop.commons.imp.readfile.ReadHadoopFileToSolr;
import org.apache.solr.client.solrj.SolrClient;
import java.util.List;

public class SolrWithHadoopImp implements ISolrWithHadoop {

    @Override
    public Long readFileToSolr(CollectTableBean collectTableBean, TableBean tableBean, String fileAbsPath, SolrClient solrClient, List<String> columnList, List<String> typeList) {
        long count = 0L;
        String fileFormat = tableBean.getFile_format();
        boolean isMd5 = !columnList.contains(Constant._HYREN_MD5_VAL);
        ReadHadoopFileToSolr readHadoopFileToSolr = new ReadHadoopFileToSolr(collectTableBean, fileAbsPath);
        if (FileFormat.PARQUET.getCode().equals(fileFormat)) {
            count = readHadoopFileToSolr.readParquetToSolr(solrClient, columnList, typeList, isMd5);
        } else if (FileFormat.ORC.getCode().equals(fileFormat)) {
            count = readHadoopFileToSolr.readOrcToSolr(solrClient, columnList, typeList, isMd5);
        } else if (FileFormat.SEQUENCEFILE.getCode().equals(fileFormat)) {
            count = readHadoopFileToSolr.readSequenceToSolr(solrClient, columnList, typeList, isMd5);
        } else {
            throw new AppSystemException(String.format("读取Hadoop文件入Solr, 不支持的文件格式! FileFormat: %s", fileFormat));
        }
        return count;
    }
}
