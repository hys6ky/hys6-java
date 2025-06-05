package hyren.serv6.k.dbm.dataimport;

import cn.hutool.core.io.file.FileNameUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.exception.ExcelAnalysisException;
import com.alibaba.excel.write.builder.ExcelWriterBuilder;
import com.alibaba.excel.write.builder.ExcelWriterSheetBuilder;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.excel.write.metadata.style.WriteCellStyle;
import com.alibaba.excel.write.style.HorizontalCellStyleStrategy;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.FileUtil;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileUploadUtil;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import hyren.serv6.k.constants.TemplateConstants;
import hyren.serv6.k.dbm.codetypeinfo.DbmCodeTypeInfoService;
import hyren.serv6.k.dbm.codetypeinfo.bean.DbmCodeTypeQueryVo;
import hyren.serv6.k.dbm.dataimport.commons.ImportData;
import hyren.serv6.k.dbm.dataimport.vo.DbmCodeTypeInfoExcelVo;
import hyren.serv6.k.dbm.dataimport.vo.DbmNormbasicExcelVo;
import hyren.serv6.k.dbm.dataimport.vo.ExcelErrVo;
import hyren.serv6.k.dbm.dataimport.vo.ExportExcelVo;
import hyren.serv6.k.dbm.normbasic.DbmNormbasicService;
import hyren.serv6.k.utils.FileDownLoadUtil;
import hyren.serv6.k.utils.ResourceUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.boot.autoconfigure.web.servlet.MultipartProperties;
import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import javax.annotation.Nullable;
import javax.annotation.Resource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Api(tags = "")
@RestController()
@RequestMapping("/dbm/dataimport")
@Slf4j
public class DbmDataImportController {

    @Resource
    DbmCodeTypeInfoService dbmCodeTypeInfoService;

    @Resource
    DbmNormbasicService dbmNormbasicService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "pathName", value = "", dataTypeClass = String.class, example = "")
    @PostMapping("/importExcelData")
    public void importExcelData(@RequestParam("pathName") MultipartFile file) {
        File destFileDir = new File(WebinfoProperties.FileUpload_SavedDirName);
        if (!destFileDir.exists() && !destFileDir.isDirectory()) {
            if (!destFileDir.mkdirs()) {
                throw new BusinessException("创建文件目录失败");
            }
        }
        String originalFileName = file.getOriginalFilename();
        String pathname = destFileDir.getPath() + File.separator + originalFileName;
        File destFile = new File(pathname);
        try {
            file.transferTo(destFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File excelFile = FileUploadUtil.getUploadedFile(pathname);
        if (!excelFile.exists()) {
            throw new BusinessException("excel文件不存在!");
        }
        Workbook workbook;
        try {
            workbook = ExcelUtil.getWorkbookFromExcel(excelFile);
        } catch (IOException e) {
            throw new BusinessException("获取excel数据失败!");
        }
        ImportData.importDbmSortInfoData(workbook, UserUtil.getUser());
        ImportData.importDbmCodeTypeInfoData(workbook, UserUtil.getUser());
        ImportData.importDbmCodeItemInfoData(workbook);
        ImportData.importDbmNormbasicData(workbook, UserUtil.getUser());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class, example = "", paramType = "query")
    @PostMapping("/importDataStandards")
    public Map<String, ExcelErrVo> importDataStandards(@RequestParam MultipartFile file) throws Throwable {
        try {
            return ImportData.importDataStandards(file);
        } catch (Exception e) {
            throw getLastException(e);
        }
    }

    private Throwable getLastException(Throwable throwable) {
        Throwable cause = throwable.getCause();
        if (cause == null) {
            return throwable;
        } else {
            return getLastException(cause);
        }
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class, example = "", paramType = "query")
    @PostMapping("/importTypeAndItem")
    public void importTypeAndItem(@RequestParam MultipartFile file) throws Throwable {
        try {
            ImportData.importTypeAndItem(file);
        } catch (Exception e) {
            throw getLastException(e);
        }
    }

    @ApiOperation(value = "", notes = "")
    @GetMapping("/getDataStandardsTemplate")
    public void getDataStandardsTemplate() throws FileNotFoundException {
        FileDownLoadUtil.exportToBrowser(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_NORMBASIC_CODETYPE_NAME), TemplateConstants.TMPL_NORMBASIC_NAME);
    }

    @ApiOperation(value = "", notes = "")
    @GetMapping("/getTypeItemTemplate")
    public void getTypeItemTemplate() throws FileNotFoundException {
        FileDownLoadUtil.exportToBrowser(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_CODETYPE_NAME), TemplateConstants.TMPL_CODETYPE_NAME);
    }

    @ApiOperation(value = "", notes = "")
    @GetMapping("/exportTypeItem")
    public void exportTypeItemTemplate(DbmCodeTypeQueryVo dbmCodeTypeQueryVo) throws IOException {
        File srcFile;
        File destFile;
        try {
            srcFile = new File(FileUtils.getTempDirectory().getAbsolutePath() + File.separator + new Date().getTime() + ".xlsx");
            FileUtils.copyInputStreamToFile(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_CODETYPE_NAME), srcFile);
            destFile = new File(FileUtils.getTempDirectory().getAbsolutePath() + File.separator + "标准代码信息.xlsx");
            FileUtil.copyFile(srcFile, destFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<DbmCodeTypeInfoExcelVo> excelVoList = dbmCodeTypeInfoService.getExportCodeTypeList(dbmCodeTypeQueryVo);
        EasyExcel.write(srcFile, DbmCodeTypeInfoExcelVo.class).needHead(false).withTemplate(srcFile).file(destFile).sheet().doWrite(excelVoList);
        FileDownLoadUtil.exportToBrowser(destFile);
    }

    @ApiOperation(value = "", notes = "")
    @GetMapping("/exportDataStandards")
    public void exportDataStandards(@RequestParam(defaultValue = "") String search_cond, @Nullable String status, @RequestParam(defaultValue = "") String sort_id, @Nullable Integer startDate, @Nullable Integer endDate, @RequestParam("basic_id_s") Long[] basic_id_s) throws FileNotFoundException {
        File srcFile;
        File destFile;
        try {
            srcFile = new File(FileUtils.getTempDirectory().getAbsolutePath() + File.separator + new Date().getTime() + ".xlsx");
            FileUtils.copyInputStreamToFile(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_NORMBASIC_CODETYPE_NAME), srcFile);
            destFile = new File(FileUtils.getTempDirectory().getAbsolutePath() + File.separator + "数据对标模板.xlsx");
            FileUtil.copyFile(srcFile, destFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ExportExcelVo standardSourceData = dbmNormbasicService.getStandardSourceData(search_cond, status, sort_id, startDate, endDate, basic_id_s);
        ExcelWriterBuilder writerBuilder = EasyExcel.write(srcFile).needHead(false).withTemplate(srcFile).file(destFile);
        ExcelWriter excelWriter = writerBuilder.build();
        ExcelWriterSheetBuilder sheet0 = writerBuilder.sheet(1);
        excelWriter.write(standardSourceData.getDbmSortInfoExcelVos(), sheet0.build());
        ExcelWriterSheetBuilder sheet1 = writerBuilder.sheet(3);
        excelWriter.write(standardSourceData.getDbmNormExcelVos(), sheet1.build());
        if (standardSourceData.getDbmCodeTypeExcelVos() != null && standardSourceData.getDbmCodeTypeExcelVos().size() != 0) {
            ExcelWriterSheetBuilder sheet2 = writerBuilder.sheet(4);
            excelWriter.write(standardSourceData.getDbmCodeTypeExcelVos(), sheet2.build());
            excelWriter.finish();
        }
        FileDownLoadUtil.exportToBrowser(destFile);
    }
}
