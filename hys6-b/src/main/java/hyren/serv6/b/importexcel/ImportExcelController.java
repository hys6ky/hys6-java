package hyren.serv6.b.importexcel;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import java.util.Map;

@Slf4j
@RequestMapping("/dataCollectionM/importExcel")
@RestController
@Validated
public class ImportExcelController {

    @Autowired
    ImportExcelServiceImpl importExcelService;

    @RequestMapping("/importDatabaseByExcel")
    @ApiOperation(value = "", tags = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "upload", value = "", dataTypeClass = String.class) })
    public Map<Object, Object> importDatabaseByExcel(MultipartFile file, @RequestParam(defaultValue = "false") String upload) {
        return importExcelService.importDatabaseByExcel(file, upload);
    }

    @RequestMapping("/downloadExcel")
    @ApiOperation(value = "", tags = "")
    public void downloadExcel() {
        importExcelService.downloadExcel();
    }
}
