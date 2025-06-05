package hyren.serv6.m.vo.excel;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.serv6.m.util.easyexcel.ExcelValid;
import hyren.serv6.m.vo.excel.cover.IsFlagConverter;
import hyren.serv6.m.vo.excel.cover.ObjTypeConverter;
import hyren.serv6.m.vo.save.MetaObjInfoSaveVo;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetaObjTblColExcelVo {

    @ExcelValid
    @ExcelProperty(value = "", index = 0)
    private String obj_en_name;

    @ExcelValid
    @ExcelProperty(value = "", index = 1)
    private String col_en_name;

    @ExcelProperty(value = "", index = 2)
    private String col_ch_name;

    @ExcelValid
    @ExcelProperty(value = "", index = 3)
    private String col_type;

    @ExcelProperty(value = "", index = 4)
    private Integer col_len;

    @ExcelProperty(value = "", index = 5)
    private Integer col_prec;

    @ExcelProperty(value = "", index = 6, converter = IsFlagConverter.class)
    private String is_pri_key;

    @ExcelProperty(value = "", index = 7, converter = IsFlagConverter.class)
    private String is_null;

    @ExcelProperty(value = "", index = 8)
    private String biz_desc;

    @ExcelProperty(value = "", index = 9)
    private Long obj_id;
}
