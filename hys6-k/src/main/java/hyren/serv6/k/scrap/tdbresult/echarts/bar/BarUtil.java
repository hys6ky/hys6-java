package hyren.serv6.k.scrap.tdbresult.echarts.bar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BarUtil {

    public static Map<String, Object> extractStatisticsChartData(List<Map<String, Object>> columnFeatureAnalysisResult) {
        List<Object> column_name_s = new ArrayList<>();
        List<Object> col_record_s = new ArrayList<>();
        List<Object> col_distinct_s = new ArrayList<>();
        List<Object> max_len_s = new ArrayList<>();
        List<Object> min_len_s = new ArrayList<>();
        List<Object> avg_len_s = new ArrayList<>();
        List<Object> skew_len_s = new ArrayList<>();
        List<Object> kurt_len_s = new ArrayList<>();
        List<Object> median_len_s = new ArrayList<>();
        List<Object> var_len_s = new ArrayList<>();
        List<Object> has_chinese_s = new ArrayList<>();
        List<Object> tech_cate_s = new ArrayList<>();
        columnFeatureAnalysisResult.forEach(columnFeatureInfo -> {
            column_name_s.add(columnFeatureInfo.get("col_code"));
            col_record_s.add(columnFeatureInfo.get("col_records"));
            col_distinct_s.add(columnFeatureInfo.get("col_distinct"));
            max_len_s.add(columnFeatureInfo.get("max_len"));
            min_len_s.add(columnFeatureInfo.get("min_len"));
            avg_len_s.add(columnFeatureInfo.get("avg_len"));
            skew_len_s.add(columnFeatureInfo.get("skew_len"));
            kurt_len_s.add(columnFeatureInfo.get("kurt_len"));
            median_len_s.add(columnFeatureInfo.get("median_len"));
            var_len_s.add(columnFeatureInfo.get("var_len"));
            has_chinese_s.add(columnFeatureInfo.get("has_chinese"));
            tech_cate_s.add(columnFeatureInfo.get("tech_cate"));
        });
        Map<String, Object> columnFeatureAnalysisResultMap = new HashMap<>();
        columnFeatureAnalysisResultMap.put("column_name_s", column_name_s);
        columnFeatureAnalysisResultMap.put("col_record_s", col_record_s);
        columnFeatureAnalysisResultMap.put("col_distinct_s", col_distinct_s);
        columnFeatureAnalysisResultMap.put("max_len_s", max_len_s);
        columnFeatureAnalysisResultMap.put("min_len_s", min_len_s);
        columnFeatureAnalysisResultMap.put("avg_len_s", avg_len_s);
        columnFeatureAnalysisResultMap.put("skew_len_s", skew_len_s);
        columnFeatureAnalysisResultMap.put("kurt_len_s", kurt_len_s);
        columnFeatureAnalysisResultMap.put("median_len_s", median_len_s);
        columnFeatureAnalysisResultMap.put("var_len_s", var_len_s);
        columnFeatureAnalysisResultMap.put("has_chinese_s", has_chinese_s);
        columnFeatureAnalysisResultMap.put("tech_cate_s", tech_cate_s);
        return columnFeatureAnalysisResultMap;
    }
}
