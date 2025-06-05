package hyren.serv6.m.vo.excel.cover;

import com.alibaba.excel.converters.Converter;
import com.alibaba.excel.enums.CellDataTypeEnum;
import com.alibaba.excel.metadata.CellData;
import com.alibaba.excel.metadata.GlobalConfiguration;
import com.alibaba.excel.metadata.property.ExcelContentProperty;
import hyren.serv6.m.contants.MetaObjTypeEnum;

public class ObjTypeConverter implements Converter<String> {

    @Override
    public Class supportJavaTypeKey() {
        return null;
    }

    @Override
    public CellDataTypeEnum supportExcelTypeKey() {
        return null;
    }

    @Override
    public String convertToJavaData(CellData cellData, ExcelContentProperty excelContentProperty, GlobalConfiguration globalConfiguration) throws Exception {
        return MetaObjTypeEnum.getCodeByValue(cellData.getStringValue());
    }

    @Override
    public CellData convertToExcelData(String code, ExcelContentProperty excelContentProperty, GlobalConfiguration globalConfiguration) throws Exception {
        return new CellData(MetaObjTypeEnum.ofEnumByCode(code).getValue());
    }
}
