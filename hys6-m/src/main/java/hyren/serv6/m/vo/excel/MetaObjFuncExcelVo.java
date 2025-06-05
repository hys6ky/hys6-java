package hyren.serv6.m.vo.excel;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.serv6.m.util.easyexcel.ExcelValid;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetaObjFuncExcelVo {

    @ExcelValid
    @ExcelProperty(value = "", index = 0)
    private String obj_en_name;

    @ExcelValid
    @ExcelProperty(value = "", index = 1)
    private String ori_sql;

    @ExcelProperty(value = "", index = 2)
    private String biz_desc;

    @ExcelProperty(value = "", index = 3)
    private Long obj_id;
}
