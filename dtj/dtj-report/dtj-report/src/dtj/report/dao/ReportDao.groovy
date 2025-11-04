package dtj.report.dao

import dtj.report.action.DownFile
import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.ss.usermodel.RangeCopier
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.XSSFRangeCopier
import org.apache.poi.xssf.usermodel.XSSFSheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import tofi.api.dta.ApiClientData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiRepairData
import tofi.api.dta.ApiResourceData
import tofi.api.dta.ApiUserData
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

import javax.print.DocFlavor

@CompileStatic
class ReportDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }
    ApinatorApi apiNSIData() {
        return app.bean(ApinatorService).getApi("nsidata")
    }
    ApinatorApi apiPersonnalData() {
        return app.bean(ApinatorService).getApi("personnaldata")
    }
    ApinatorApi apiOrgStructureData() {
        return app.bean(ApinatorService).getApi("orgstructuredata")
    }
    ApinatorApi apiObjectData() {
        return app.bean(ApinatorService).getApi("objectdata")
    }
    ApinatorApi apiPlanData() {
        return app.bean(ApinatorService).getApi("plandata")
    }
    ApinatorApi apiInspectionData() {
        return app.bean(ApinatorService).getApi("inspectiondata")
    }
    ApinatorApi apiClientData() {
        return app.bean(ApinatorService).getApi("clientdata")
    }
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }


    @DaoMethod
    void generateReport1(Map<String, Object> params) {
        String pathin = mdb.getApp().appdir + File.separator + "tml" + File.separator + "ПО-4.xlsx"
        String pathout = mdb.getApp().appdir + File.separator + "report" + File.separator + "ПО-4.xlsx"


        // 1. Загрузка исходной книги
        InputStream inputStream = new FileInputStream(pathin)
        XSSFWorkbook sourceWorkbook = new XSSFWorkbook(inputStream)


// 2. Создание целевой книги и листа
        XSSFWorkbook targetWorkbook = new XSSFWorkbook();

        XSSFSheet sourceSheet = sourceWorkbook.getSheetAt(0);
        XSSFSheet destSheet = targetWorkbook.createSheet("ПЧ")

        RangeCopier copier = new XSSFRangeCopier(sourceSheet, destSheet)

        // Например, копирование диапазона A1:C5
        CellRangeAddress sourceRange = new CellRangeAddress(0, 100, 0, 9)
        // Вставка в диапазон D1:F5
        CellRangeAddress destRange = new CellRangeAddress(0, 100, 0, 9)

        //
        copier.copyRange(sourceRange, destRange, true, true)
        //

        // Итерируем по всем столбцам от 0 до последнего используемого
        for (int i = 0; i <= 9; i++) {
            // Получаем ширину столбца из исходного листа
            int columnWidth = sourceSheet.getColumnWidth(i);

            // Устанавливаем ту же ширину для соответствующего столбца в целевом листе
            destSheet.setColumnWidth(i, columnWidth);
        }

        //




        OutputStream outputStream = new FileOutputStream(pathout)
        targetWorkbook.write(outputStream)
        outputStream.close()


    }



    @DaoMethod
    void generateReport(Map<String, Object> params) {
        String pathin = mdb.getApp().appdir+File.separator+"tml"+File.separator+"ПО-4.xlsx"
        String pathout = mdb.getApp().appdir+File.separator+"report"+File.separator+"ПО-4.xlsx"


        // 1. Загрузка исходной книги
        InputStream inputStream = new FileInputStream(pathin)
        XSSFWorkbook sourceWorkbook = new XSSFWorkbook(inputStream)
        XSSFSheet sourceSheet = sourceWorkbook.getSheetAt(0)

// 2. Создание целевой книги и листа
        XSSFWorkbook targetWorkbook = new XSSFWorkbook();
        XSSFSheet targetSheet = targetWorkbook.createSheet("ПЧ")

// 3. Копирование данных
        for (Row sourceRow : sourceSheet) {
            Row targetRow = targetSheet.createRow(sourceRow.getRowNum())
            for (Cell sourceCell : sourceRow) {
                Cell targetCell = targetRow.createCell(sourceCell.getColumnIndex(), sourceCell.cellType)

                // Копируем значение ячейки
                //targetCell.setCellValue(sourceCell.getStringCellValue())
                targetCell.setCellValue(getCellValueAsString(sourceCell))
                // И так далее для других типов ячеек (числовых, булевых и т.д.)

                // TODO: Добавить логику копирования стилей и форматирования
            }
        }

// 4. Сохранение целевой книги
        OutputStream outputStream = new FileOutputStream(pathout)
        targetWorkbook.write(outputStream)
        outputStream.close()

    }

    public static String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return null
        }
        switch (cell.getCellType()) {
            case CellType.STRING:
                return cell.getStringCellValue()
            case CellType.NUMERIC:
                // For numeric cells, including dates and times
                DataFormatter formatter = new DataFormatter()
                return formatter.formatCellValue(cell)
            case CellType.BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue())
            case CellType.FORMULA:
                // To get the evaluated value of a formula cell
                // You'll need a FormulaEvaluator instance
                // FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator()
                // CellValue cellValue = evaluator.evaluate(cell)
                // return getCellValueAsString(cellValue) // Recursively call for evaluated value
                return cell.getCellFormula() // Or just return the formula string
            case CellType.BLANK:
                return null
            case CellType.ERROR:
                return "ERROR" // Or handle error appropriately
            default:
                return null
        }
    }


    //-------------------------

    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }

    private Store loadSqlService(String sql, String domain, String model) {
        if (model.equalsIgnoreCase("userdata"))
            return apiUserData().get(ApiUserData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("nsidata"))
            return apiNSIData().get(ApiNSIData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("personnaldata"))
            return apiPersonnalData().get(ApiPersonnalData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("orgstructuredata"))
            return apiOrgStructureData().get(ApiOrgStructureData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("objectdata"))
            return apiObjectData().get(ApiObjectData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("inspectiondata"))
            return apiInspectionData().get(ApiInspectionData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSql(sql, domain)
        else
            throw new XError("Unknown model [${model}]")
    }


    private long getUser() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        long au = authSvc.getCurrentUser().getAttrs().getLong("id")
        if (au == 0)
            au = 1//throw new XError("notLogined")
        return au
    }


}
