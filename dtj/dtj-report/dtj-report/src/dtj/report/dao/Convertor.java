package dtj.report.dao;

import com.documents4j.api.IConverter;
import com.documents4j.job.LocalConverter;
import org.apache.poi.ss.usermodel.PrintSetup;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static com.documents4j.api.DocumentType.PDF;
import static com.documents4j.api.DocumentType.XLSX;

public class Convertor {

    static void cnv2pdf(String src, String dst, boolean isLandscape) throws Exception {
        File tempExcelFile = null;
        try {
            // 1. Загружаем исходный Excel-файл через Apache POI
            FileInputStream fis = new FileInputStream(src);
            Workbook workbook = new XSSFWorkbook(fis);

            // 2. Настраиваем параметры печати для каждого листа
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                PrintSetup ps = sheet.getPrintSetup();

                // Устанавливаем ориентацию страницы на альбомную (опционально, если нужно больше ширины)
                if (isLandscape)
                    ps.setLandscape(true);

                // Самое главное: уместить все колонки на 1 страницу по ширине
                ps.setPaperSize((short) 9);
                sheet.setMargin((short) 0, 0.5);
                sheet.setMargin((short) 1, 0.5);
                sheet.setMargin((short) 2, 0.5);
                sheet.setMargin((short) 3, 0.5);
                //sheet.setAutobreaks(true);

                ps.setFitWidth((short) 1);
                ps.setFitHeight((short) 0); // Высота может занимать сколько угодно страниц
            }
            fis.close();

            // 3. Сохраняем измененный Excel-файл во временный файл
            tempExcelFile = File.createTempFile("temp", ".xlsx");
            FileOutputStream fos = new FileOutputStream(tempExcelFile);
            workbook.write(fos);
            fos.close();
            workbook.close();

            // 4. Используем ваш конвертер для конвертации временного файла
            try (InputStream docxInputStream = new FileInputStream(tempExcelFile);
                 OutputStream pdfOutputStream = new FileOutputStream(dst)) {

                IConverter converter = LocalConverter.builder()
                        .workerPool(20, 25, 2, TimeUnit.SECONDS)
                        .processTimeout(5, TimeUnit.SECONDS)
                        .build();

                converter.convert(docxInputStream).as(XLSX)
                        .to(pdfOutputStream).as(PDF)
                        .execute();

                converter.shutDown();
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            // 5. Обязательно удаляем временный файл
            if (tempExcelFile != null && tempExcelFile.exists()) {
                tempExcelFile.delete();
            }
        }
    }
}
