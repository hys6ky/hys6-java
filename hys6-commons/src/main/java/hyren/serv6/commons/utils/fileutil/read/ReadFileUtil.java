package hyren.serv6.commons.utils.fileutil.read;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.commons.ocr.PictureTextExtract;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import info.monitorenter.cpdetector.io.*;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import javax.imageio.ImageIO;
import java.awt.*;
import java.io.*;
import java.nio.charset.Charset;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/4 16:04")
@Slf4j
public class ReadFileUtil {

    private static final CodepageDetectorProxy detector = CodepageDetectorProxy.getInstance();

    static {
        detector.add(new ParsingDetector(false));
        detector.add(JChardetFacade.getInstance());
        detector.add(ASCIIDetector.getInstance());
        detector.add(UnicodeDetector.getInstance());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String detectCode(File file) {
        try {
            return detector.detectCodepage(file.toURI().toURL()).name();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return Charset.defaultCharset().name();
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String readExcel(File file) {
        StringBuilder sb = new StringBuilder(120);
        Sheet sheet;
        try (InputStream ins = new FileInputStream(file);
            Workbook wb = WorkbookFactory.create(ins)) {
            for (int i = 0; i < wb.getNumberOfSheets(); i++) {
                sheet = wb.getSheetAt(i);
                int trNumber = sheet.getLastRowNum() + 1;
                if (trNumber == 0)
                    System.exit(0);
                for (int j = 0; j < trNumber; j++) {
                    Row row1 = sheet.getRow(j);
                    if (row1 == null)
                        continue;
                    int tdNumber = row1.getLastCellNum();
                    for (int k = 0; k < tdNumber; k++) {
                        Cell cell1 = row1.getCell(k);
                        if (cell1 != null) {
                            cell1.setCellType(CellType.STRING);
                        }
                        sb.append(cell1);
                    }
                }
            }
            return sb.toString().replace("null", "");
        } catch (EncryptedDocumentException | InvalidFormatException | IOException e) {
            return "";
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String readHtml(File file) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), detectCode(file)))) {
            String temp;
            while ((temp = br.readLine()) != null) {
                sb.append(temp);
            }
            return sb.toString();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getPdf(File file) {
        boolean sort = true;
        int startPage = 1;
        int endPage = 10;
        try (PDDocument document = PDDocument.load(file)) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setSortByPosition(sort);
            stripper.setStartPage(startPage);
            stripper.setEndPage(endPage);
            return stripper.getText(document);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String readText(File file) {
        StringBuilder sub = new StringBuilder();
        String code = detectCode(file);
        if ("UTF-16LE".equals(code) || "UTF-18".equals(code)) {
            code = "GBK";
        }
        try (BufferedReader bufReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), code))) {
            String str;
            while ((str = bufReader.readLine()) != null) {
                sub.append(str);
            }
            return convertSymbol(sub.toString());
        } catch (Exception e) {
            return "";
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "docText", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String convertSymbol(String docText) {
        StringBuilder sub = new StringBuilder();
        char[] ch = docText.toCharArray();
        for (char buf : ch) {
            if (9 == buf || 10 == buf || 13 == buf || 32 <= buf && !Character.isISOControl(buf))
                sub.append(buf);
        }
        return sub.toString();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String readWord(File file) {
        String text = "";
        String fileName = file.getName().toLowerCase();
        try {
            FileInputStream in = new FileInputStream(file);
            if (fileName.endsWith(".doc")) {
                WordExtractor extractor = new WordExtractor(in);
                text = extractor.getText();
            }
            if (fileName.endsWith(".docx")) {
                XWPFWordExtractor docx = new XWPFWordExtractor(new XWPFDocument(in));
                text = docx.getText();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return text;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String readPdfOrPicture(File file) {
        try {
            ITesseract instance = new Tesseract();
            log.info(" tessdata 文件夹存放目录： " + System.getProperty("user.dir"));
            instance.setDatapath(System.getProperty("user.dir"));
            instance.setLanguage(PropertyParaValue.getString("language", "chi_sim"));
            return instance.doOCR(file);
        } catch (Exception e) {
            return "";
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String file2String(File file) {
        if (file.length() == 0) {
            return "";
        }
        try {
            String fileExtension = FilenameUtils.getExtension(file.getName());
            switch(fileExtension) {
                case "pdf":
                    return getPdf(file);
                case "docx":
                case "doc":
                    return readWord(file);
                case "xlsx":
                case "xls":
                    return readExcel(file);
                case "html":
                    return readHtml(file);
                case "txt":
                case "csv":
                case "log":
                    return readText(file);
                default:
                    return "";
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String file2String(String filePath) {
        File file = FileUtils.getFile(filePath);
        Image image;
        String readData = null;
        try {
            image = ImageIO.read(file);
            if (image != null) {
                readData = new PictureTextExtract().extractText(filePath);
                return readData;
            }
            readData = file2String(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return readData;
    }

    public static byte[] readBytesForPicture(String filePath) {
        File f = new File(filePath);
        return readBytesForPicture(f);
    }

    public static byte[] readBytesForPicture(File file) {
        BufferedInputStream bis = null;
        byte[] b = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(file));
            b = new byte[bis.available()];
            if (bis.read(b) == -1) {
                System.out.println("文件:" + file.getAbsolutePath() + " , 为空! 或者 文件损坏!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return b;
    }
}
