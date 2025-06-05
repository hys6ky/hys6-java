package hyren.serv6.k.dbm.dataimport.vo;

import lombok.Data;
import java.util.List;

@Data
public class ExportExcelVo {

    private List<DbmSortInfoExcelVo> dbmSortInfoExcelVos;

    private List<DbmNormExcelVo> dbmNormExcelVos;

    private List<DbmCodeTypeExcelVo> dbmCodeTypeExcelVos;
}
