package hyren.serv6.m.vo.excel;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.serv6.m.util.easyexcel.ExcelValid;
import hyren.serv6.m.vo.excel.cover.ObjTypeConverter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetaObjInfoExcelVo {

    @ExcelValid
    @ExcelProperty(value = "", index = 0)
    private String en_name;

    @ExcelValid
    @ExcelProperty(value = "", index = 1)
    private String ch_name;

    @ExcelValid
    @ExcelProperty(value = "", index = 2, converter = ObjTypeConverter.class)
    private String type;
}
