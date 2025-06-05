package hyren.serv6.m.vo.excel;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.serv6.m.util.easyexcel.ExcelValid;
import hyren.serv6.m.vo.excel.cover.IsFlagConverter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExportDataVo {

    @ExcelProperty(value = "", index = 0)
    private String en_name;

    @ExcelProperty(value = "", index = 1)
    private String ch_name;

    @ExcelProperty(value = "", index = 2)
    private String col_en_name;

    @ExcelProperty(value = "", index = 3)
    private String col_ch_name;

    @ExcelProperty(value = "", index = 4)
    private String col_type;

    @ExcelProperty(value = "", index = 5)
    private Integer col_len;

    @ExcelProperty(value = "", index = 6)
    private Integer col_prec;

    @ExcelProperty(value = "", index = 7, converter = IsFlagConverter.class)
    private String is_pri_key;

    @ExcelProperty(value = "", index = 8, converter = IsFlagConverter.class)
    private String is_null;

    @ExcelProperty(value = "", index = 9)
    private String biz_desc;
}
