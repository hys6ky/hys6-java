package hyren.serv6.k.dbm.dataimport.vo;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.validation.Valid;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DbmSortInfoExcelVo {

    @ExcelProperty(value = "", index = 0)
    private String sort_theme;

    @ExcelProperty(value = "", index = 1)
    private String sort_class;

    @ExcelProperty(value = "", index = 2)
    private String sort_subClass;

    @ExcelProperty(value = "", index = 3)
    private String sort_remark;
}
