package hyren.serv6.commons.hadoop.util;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClassBase {

    private final static String HadoopImp = "hyren.serv6.hadoop.commons.imp.HadoopImp";

    private final static String HadoopFileImp = "hyren.serv6.hadoop.commons.imp.HadoopFileImp";

    private final static String YarnImp = "hyren.serv6.hadoop.commons.imp.YarnImp";

    private final static String AvroRecord = "hyren.serv6.hadoop.commons.imp.AvroRecordImp";

    private final static String SolrWithHadoopImpl = "hyren.serv6.hadoop.commons.imp.SolrWithHadoopImpl";

    private final static String OcrExtact = "hyren.serv6.hadoop.commons.imp.OcrExtactImp";

    private final static String EssaySimilar = "hyren.serv6.hadoop.commons.imp.EssaySimilarImp";

    private final static String HbaseImp = "hyren.serv6.hadoop.commons.imp.HbaseImp";

    private final static String JobIoUtilImp = "hyren.serv6.hadoop.commons.imp.JobIoUtilImp";

    public static IHadoop hadoopInstance() {
        return newInstance(HadoopImp);
    }

    public static IHadoopFile hadoopFileInstance() {
        return newInstance(HadoopFileImp);
    }

    public static IJobIoUtil jobIoUtilItance() {
        return newInstance(JobIoUtilImp);
    }

    public static IYarn yarnInstance() {
        return newInstance(YarnImp);
    }

    public static IAvroRecord avroPInstance() {
        return newInstance(AvroRecord);
    }

    public static ISolrWithHadoop solrWithHadoopInstance() {
        return newInstance(SolrWithHadoopImpl);
    }

    public static IOcrExtact ocrInstance() {
        return newInstance(OcrExtact);
    }

    public static IEssaySimilar essaySimilar() {
        return newInstance(EssaySimilar);
    }

    public static IHbase HbaseInstance() {
        return newInstance(HbaseImp);
    }

    @SuppressWarnings("unchecked")
    private static <T> T newInstance(String aclass) {
        try {
            return (T) Class.forName(aclass).newInstance();
        } catch (Exception e) {
            log.error("实体化失败", e);
            throw new AppSystemException(String.format("实例化 %s 失败", aclass));
        }
    }
}
