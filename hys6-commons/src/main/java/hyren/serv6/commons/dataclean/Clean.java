package hyren.serv6.commons.dataclean;

import hyren.serv6.base.entity.ColumnSplit;
import java.util.List;
import java.util.Map;

public class Clean {

    private final DataCleanInterface allclean;

    private final Map<String, Map<String, String>> deleSpecialSpace;

    private final Map<String, Map<String, ColumnSplit>> splitIng;

    private final Map<String, String> strFilling;

    private final Map<String, String> dating;

    private final Map<String, String> codeIng;

    private final Map<String, String> Triming;

    Map<String, Map<Integer, String>> ordering;

    @SuppressWarnings("unchecked")
    public Clean(Map<String, Object> parseJson, DataCleanInterface allclean) {
        this.allclean = allclean;
        deleSpecialSpace = (Map<String, Map<String, String>>) parseJson.get("deleSpecialSpace");
        splitIng = (Map<String, Map<String, ColumnSplit>>) parseJson.get("splitIng");
        strFilling = (Map<String, String>) parseJson.get("strFilling");
        dating = (Map<String, String>) parseJson.get("dating");
        codeIng = (Map<String, String>) parseJson.get("codeIng");
        Triming = (Map<String, String>) parseJson.get("Triming");
        ordering = (Map<String, Map<Integer, String>>) parseJson.get("ordering");
    }

    public String cleanColumn(String columndata, String columnname, String type, String fileType, List<Object> list, String database_code, String database_separatorr) {
        if (!ordering.isEmpty()) {
            Map<Integer, String> colMap = ordering.get(columnname.toUpperCase());
            for (int i = 1; i <= colMap.size(); i++) {
                switch(colMap.get(i)) {
                    case "2":
                        columndata = allclean.replace(deleSpecialSpace, columndata, columnname.toUpperCase());
                        break;
                    case "1":
                        columndata = allclean.filling(strFilling, columndata, columnname.toUpperCase());
                        break;
                    case "4":
                        columndata = allclean.codeTrans(codeIng, columndata, columnname.toUpperCase());
                        break;
                    case "6":
                        columndata = allclean.split(splitIng, columndata, columnname.toUpperCase(), type, fileType, list, database_code, database_separatorr);
                        break;
                    case "7":
                        columndata = allclean.trim(Triming, columndata, columnname.toUpperCase());
                        break;
                    case "3":
                        columndata = allclean.dateing(dating, columndata, columnname.toUpperCase());
                        break;
                }
            }
        }
        return columndata;
    }
}
