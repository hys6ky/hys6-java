package hyren.serv6.hadoop.commons.imp;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileUploadUtil;
import hyren.serv6.commons.hadoop.i.IEssaySimilar;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.utils.fileutil.read.ReadFileUtil;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.hadoop.commons.ocr.OcrExtractText;
import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EssaySimilarImp implements IEssaySimilar {

    @Override
    public List<Map<String, String>> getDocumentSimilarFromSolr(String filePath, String similarityRate, IsFlag searchWayFlag) {
        double dblRate = Double.parseDouble(similarityRate);
        if (Double.compare(dblRate, 1) == 1) {
            dblRate = 1;
        }
        if (Double.compare(dblRate, 0) == -1) {
            dblRate = 0;
        }
        List<Map<String, String>> values = new ArrayList<>();
        List<Keyword> keywords = new KeyWordComputer<>(20).computeArticleTfidf(ReadFileUtil.file2String(FileUploadUtil.getUploadedFile(filePath)));
        int keywords_size = keywords.size();
        if (searchWayFlag == IsFlag.Shi) {
            StringBuilder query = new StringBuilder();
            for (int i = 0; i < keywords_size; i++) {
                query.append(keywords.get(i).getName()).append("^").append((int) keywords.get(i).getScore());
                if (i < keywords_size - 1) {
                    query.append(" OR ");
                }
            }
            Map<String, String> params = new HashMap<>();
            params.put("q", query.toString());
            params.put("indent", "true");
            params.put("fl", "tf-file_summary,id,score");
            List<Map<String, Object>> querySolrPlusResult = SolrFactory.getSolrOperatorInstance().querySolrPlus(params, 0, 500, IsFlag.Fou);
            for (Map<String, Object> map : querySolrPlusResult) {
                Map<String, String> mapresult = new HashMap<>();
                double maxscore = Double.parseDouble(map.get("score").toString());
                if (Double.compare(maxscore, dblRate) == 1) {
                    mapresult.put("file_id", map.get("id").toString());
                    mapresult.put("rate", String.valueOf(maxscore));
                    mapresult.put("summary_content", map.get("file_summary").toString());
                    values.add(mapresult);
                }
            }
        } else if (searchWayFlag == IsFlag.Fou) {
            boolean end = true;
            Map<String, String> params = new HashMap<>();
            params.put("q", "avro_*");
            params.put("indent", "true");
            params.put("fl", "tf-file_text,tf-file_summary,id");
            int count = 0;
            while (end) {
                List<Map<String, Object>> querySolrPlusResult = SolrFactory.getSolrOperatorInstance().querySolrPlus(params, count, 50000, IsFlag.Shi);
                count += 50000;
                if (querySolrPlusResult.isEmpty()) {
                    end = false;
                }
                for (Map<String, Object> map : querySolrPlusResult) {
                    if (null != map.get("file_text") && StringUtil.isNotBlank(map.get("file_text").toString())) {
                        double result = 0;
                        List<Keyword> keywords_2 = new KeyWordComputer<>(20).computeArticleTfidf(map.get("file_text").toString());
                        for (Keyword keyword : keywords) {
                            if (keywords_2.contains(keyword)) {
                                result += 0.05;
                            }
                        }
                        result = (int) (result * 1000000) / (double) 1000000;
                        if (result >= dblRate) {
                            Map<String, String> mapresult = new HashMap<>();
                            mapresult.put("file_id", map.get("id").toString());
                            mapresult.put("rate", String.valueOf(result));
                            mapresult.put("summary_content", map.get("file_summary").toString());
                            values.add(mapresult);
                        }
                    }
                }
            }
        } else {
            throw new BusinessException("搜索方式类型不合法! searchWay=" + searchWayFlag.getCode());
        }
        values.sort((o1, o2) -> Double.compare(Double.parseDouble(o2.get("rate")), Double.parseDouble(o1.get("rate"))));
        return values;
    }

    @Override
    public String getFileSummaryFromAvro(String filePath, String blockId, String fileId) {
        String summary = "";
        if (!StringUtil.isBlank(filePath) && !StringUtil.isBlank(blockId) && !StringUtil.isBlank(fileId)) {
            try (HdfsOperator hdfsOperator = new HdfsOperator()) {
                Path path = new Path(filePath);
                SeekableInput in = new FsInput(path, hdfsOperator.conf);
                DatumReader<GenericRecord> reader = new GenericDatumReader<>();
                DataFileReader<GenericRecord> fileReader = new DataFileReader<>(in, reader);
                GenericRecord record = new GenericData.Record(fileReader.getSchema());
                fileReader.seek(Long.parseLong(blockId));
                if (fileReader.hasNext()) {
                    GenericRecord grnext = fileReader.next(record);
                    String name = grnext.get("file_name").toString();
                    if (OcrExtractText.isOcrFile(name)) {
                        String ocrpath = path.getParent() + "/ocravro/" + path.getName() + "_zw";
                        if (hdfsOperator.exists(ocrpath)) {
                            SeekableInput ocrin = new FsInput(new Path(ocrpath), hdfsOperator.conf);
                            DatumReader<GenericRecord> ocrreader = new GenericDatumReader<>();
                            DataFileReader<GenericRecord> ocrfileReader = new DataFileReader<>(ocrin, ocrreader);
                            GenericRecord ocrrecord = new GenericData.Record(ocrfileReader.getSchema());
                            while (ocrfileReader.hasNext()) {
                                GenericRecord ocrgrnext = ocrfileReader.next(ocrrecord);
                                if (ocrgrnext.get("uuid").toString().equals(fileId)) {
                                    summary = ocrgrnext.get("file_summary").toString();
                                    break;
                                }
                            }
                        }
                    } else {
                        summary = grnext.get("file_summary").toString();
                    }
                }
            } catch (IOException e) {
                throw new BusinessException("EssaySimilarImp获取文件摘要失败!" + e);
            }
        }
        return summary;
    }
}
