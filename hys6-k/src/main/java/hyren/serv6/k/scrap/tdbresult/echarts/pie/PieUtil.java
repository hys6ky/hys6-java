package hyren.serv6.k.scrap.tdbresult.echarts.pie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PieUtil {

    public static Map<String, Object> extractLegendDataAndSeriesData(List<Map<String, Object>> fieldSameStatisticalResult) {
        List<String> legendData = new ArrayList<>();
        List<Map<String, Object>> seriesData = new ArrayList<>();
        fieldSameStatisticalResult.forEach(item -> {
            String legendName = "";
            if (null != item.get("category_same")) {
                legendName = item.get("category_same").toString();
            }
            if (!legendData.contains(legendName)) {
                legendData.add(legendName);
            }
            Map<String, Object> map = new HashMap<>();
            map.put("name", item.get("category_same").toString());
            map.put("value", item.get("count").toString());
            seriesData.add(map);
        });
        Map<String, Object> fieldEqualityCategoryMap = new HashMap<>();
        fieldEqualityCategoryMap.put("legendData", legendData);
        fieldEqualityCategoryMap.put("seriesData", seriesData);
        return fieldEqualityCategoryMap;
    }
}
