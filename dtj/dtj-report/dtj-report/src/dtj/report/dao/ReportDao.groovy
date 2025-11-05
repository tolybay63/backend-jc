package dtj.report.dao


import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.RangeCopier
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.XSSFRangeCopier
import org.apache.poi.xssf.usermodel.XSSFSheet
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import tofi.api.dta.*
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.utils.UtPeriod
import tofi.api.mdl.utils.dimPeriod.PeriodGenerator
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

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
    ApinatorApi apiIncidentData() {
        return app.bean(ApinatorService).getApi("incidentdata")
    }
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }

    @DaoMethod
    void generateReport(Map<String, Object> params) {
        if (UtCnv.toString(params.get("tml")).equalsIgnoreCase("по-4"))
            generateReportPO_4(params)
        else
            throw new XError("Не известный шаблон")

    }


    void generateReportPO_4(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        String pathin = mdb.getApp().appdir + File.separator + "tml" + File.separator + "ПО-4.xlsx"
        String pathout = mdb.getApp().appdir + File.separator + "report" + File.separator + "ПО-4.xlsx"

        System.out.println(pathout)

        // 1. Загрузка исходной книги
        InputStream inputStream = new FileInputStream(pathin)
        XSSFWorkbook sourceWorkbook = new XSSFWorkbook(inputStream)


        // 2. Создание целевой книги и листа
        XSSFWorkbook targetWorkbook = new XSSFWorkbook()

        XSSFSheet sourceSheet = sourceWorkbook.getSheetAt(0)
        XSSFSheet destSheet = targetWorkbook.createSheet("Лист1")

        // Итерируем по всем столбцам от 0 до последнего используемого
        for (int i = 0; i < 10; i++) {
            // Получаем ширину столбца из исходного листа
            int columnWidth = sourceSheet.getColumnWidth(i)
            // Устанавливаем ту же ширину для соответствующего столбца в целевом листе
            destSheet.setColumnWidth(i, columnWidth)
        }


        RangeCopier copier = new XSSFRangeCopier(sourceSheet, destSheet)
        CellRangeAddress sourceRange = new CellRangeAddress(0, 71, 0, 9)
        CellRangeAddress destRange = new CellRangeAddress(0, 71, 0, 9)
        //
        copier.copyRange(sourceRange, destRange, true, true)
        //
        long pt = pms.getLong("periodType")
        String dte = pms.getString("dte")
        UtPeriod utPeriod = new UtPeriod()
        XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
        XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
        PeriodGenerator pg = new PeriodGenerator()
        String namePeriod = pg.getPeriodName(d1, d2, pt, 1)
        String h2=""
        Store stLocation = loadSqlService("""
            select v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${pms.getLong("objLocation")}
        """, "", "orgstructuredata")
        if (stLocation.size()>0)
            h2 = "за "+namePeriod+" по " + stLocation.get(0).getString("name").toLowerCase()


        //
        Row row = destSheet.getRow(5)
        Cell cell = row.getCell(0)
        cell.setCellValue(h2)
        //
        row = destSheet.getRow(67)
        cell = row.getCell(0)
        cell.setCellValue(pms.getString("nameDirectorPosition"))
        row = destSheet.getRow(68)
        cell = row.getCell(0)
        cell.setCellValue(pms.getString("nameDirectorLocation"))
        cell = row.getCell(8)
        cell.setCellValue(pms.getString("fullNameDirector"))
        //
        String isp = "Исп. "+pms.getString("nameUserPosition").toLowerCase()+" "+pms.getString("fulNameUser")
        String tel = "тел. "+pms.getString("UserPhone")
        row = destSheet.getRow(70)
        cell = row.getCell(0)
        cell.setCellValue(isp)
        row = destSheet.getRow(71)
        cell = row.getCell(0)
        cell.setCellValue(tel)
        //

        // Данные
        Map<String, Map<String, Long>> mapData =  loadDataPO_4(params)
        //
        row = destSheet.getRow(63)
        cell = row.getCell(8)
        cell.setCellValue(mapData.get("shtuka").get("shtuka"))

        //10
        row = destSheet.getRow(13)
        cell = row.getCell(4)
        cell.setCellValue(mapData.get("10").get("75c"))
        cell = row.getCell(5)
        cell.setCellValue(mapData.get("10").get("75z"))
        cell = row.getCell(6)
        cell.setCellValue(mapData.get("10").get("65c"))
        cell = row.getCell(7)
        cell.setCellValue(mapData.get("10").get("65z"))
        cell = row.getCell(8)
        cell.setCellValue(mapData.get("10").get("50"))
        cell = row.getCell(9)
        cell.setCellValue(mapData.get("10").get("43"))
        //11
        row = destSheet.getRow(14)
        cell = row.getCell(4)
        if (mapData.get("11") && mapData.get("11").get("75c"))
            cell.setCellValue(mapData.get("11").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("11") && mapData.get("11").get("75z"))
            cell.setCellValue(mapData.get("11").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("11") && mapData.get("11").get("65c"))
            cell.setCellValue(mapData.get("11").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("11") && mapData.get("11").get("65z"))
            cell.setCellValue(mapData.get("11").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("11") && mapData.get("11").get("50"))
            cell.setCellValue(mapData.get("11").get("50"))
        cell = row.getCell(9)
        if (mapData.get("11") && mapData.get("11").get("43"))
            cell.setCellValue(mapData.get("11").get("43"))
        //12
        row = destSheet.getRow(15)
        cell = row.getCell(4)
        if (mapData.get("12") && mapData.get("12").get("75c"))
            cell.setCellValue(mapData.get("12").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("12") && mapData.get("12").get("75z"))
            cell.setCellValue(mapData.get("12").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("12") && mapData.get("12").get("65c"))
            cell.setCellValue(mapData.get("12").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("12") && mapData.get("12").get("65z"))
            cell.setCellValue(mapData.get("12").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("12") && mapData.get("12").get("50"))
            cell.setCellValue(mapData.get("12").get("50"))
        cell = row.getCell(9)
        if (mapData.get("12") && mapData.get("12").get("43"))
            cell.setCellValue(mapData.get("12").get("43"))
        //13
        row = destSheet.getRow(16)
        cell = row.getCell(4)
        if (mapData.get("13") && mapData.get("13").get("75c"))
            cell.setCellValue(mapData.get("13").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("13") && mapData.get("13").get("75z"))
            cell.setCellValue(mapData.get("13").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("13") && mapData.get("13").get("65c"))
            cell.setCellValue(mapData.get("13").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("13") && mapData.get("13").get("65z"))
            cell.setCellValue(mapData.get("13").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("13") && mapData.get("13").get("50"))
            cell.setCellValue(mapData.get("13").get("50"))
        cell = row.getCell(9)
        if (mapData.get("13") && mapData.get("13").get("43"))
            cell.setCellValue(mapData.get("13").get("43"))
        //14
        row = destSheet.getRow(17)
        cell = row.getCell(4)
        if (mapData.get("14") && mapData.get("14").get("75c"))
            cell.setCellValue(mapData.get("14").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("14") && mapData.get("14").get("75z"))
            cell.setCellValue(mapData.get("14").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("14") && mapData.get("14").get("65c"))
            cell.setCellValue(mapData.get("14").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("14") && mapData.get("14").get("65z"))
            cell.setCellValue(mapData.get("14").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("14") && mapData.get("14").get("50"))
            cell.setCellValue(mapData.get("14").get("50"))
        cell = row.getCell(9)
        if (mapData.get("14") && mapData.get("14").get("43"))
            cell.setCellValue(mapData.get("14").get("43"))
        //18
        row = destSheet.getRow(18)
        cell = row.getCell(4)
        if (mapData.get("18") && mapData.get("18").get("75c"))
            cell.setCellValue(mapData.get("18").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("18") && mapData.get("18").get("75z"))
            cell.setCellValue(mapData.get("18").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("18") && mapData.get("18").get("65c"))
            cell.setCellValue(mapData.get("18").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("18") && mapData.get("18").get("65z"))
            cell.setCellValue(mapData.get("18").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("18") && mapData.get("18").get("50"))
            cell.setCellValue(mapData.get("18").get("50"))
        cell = row.getCell(9)
        if (mapData.get("18") && mapData.get("18").get("43"))
            cell.setCellValue(mapData.get("18").get("43"))
        //20
        row = destSheet.getRow(20)
        cell = row.getCell(4)
        if (mapData.get("20") && mapData.get("20").get("75c"))
            cell.setCellValue(mapData.get("20").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("20") && mapData.get("20").get("75z"))
            cell.setCellValue(mapData.get("20").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("20") && mapData.get("20").get("65c"))
            cell.setCellValue(mapData.get("20").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("20") && mapData.get("20").get("65z"))
            cell.setCellValue(mapData.get("20").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("20") && mapData.get("20").get("50"))
            cell.setCellValue(mapData.get("20").get("50"))
        cell = row.getCell(9)
        if (mapData.get("20") && mapData.get("20").get("43"))
            cell.setCellValue(mapData.get("20").get("43"))
        //
        //21
        row = destSheet.getRow(21)
        cell = row.getCell(4)
        if (mapData.get("21") && mapData.get("21").get("75c"))
            cell.setCellValue(mapData.get("21").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("21") && mapData.get("21").get("75z"))
            cell.setCellValue(mapData.get("21").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("21") && mapData.get("21").get("65c"))
            cell.setCellValue(mapData.get("21").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("21") && mapData.get("21").get("65z"))
            cell.setCellValue(mapData.get("21").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("21") && mapData.get("21").get("50"))
            cell.setCellValue(mapData.get("21").get("50"))
        cell = row.getCell(9)
        if (mapData.get("21") && mapData.get("21").get("43"))
            cell.setCellValue(mapData.get("21").get("43"))
        //24
        row = destSheet.getRow(22)
        cell = row.getCell(4)
        if (mapData.get("24") && mapData.get("24").get("75c"))
            cell.setCellValue(mapData.get("24").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("24") && mapData.get("24").get("75z"))
            cell.setCellValue(mapData.get("24").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("24") && mapData.get("24").get("65c"))
            cell.setCellValue(mapData.get("24").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("24") && mapData.get("24").get("65z"))
            cell.setCellValue(mapData.get("24").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("24") && mapData.get("24").get("50"))
            cell.setCellValue(mapData.get("24").get("50"))
        cell = row.getCell(9)
        if (mapData.get("24") && mapData.get("24").get("43"))
            cell.setCellValue(mapData.get("24").get("43"))
        //25
        row = destSheet.getRow(23)
        cell = row.getCell(4)
        if (mapData.get("25") && mapData.get("25").get("75c"))
            cell.setCellValue(mapData.get("25").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("25") && mapData.get("25").get("75z"))
            cell.setCellValue(mapData.get("25").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("25") && mapData.get("25").get("65c"))
            cell.setCellValue(mapData.get("25").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("25") && mapData.get("25").get("65z"))
            cell.setCellValue(mapData.get("25").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("25") && mapData.get("25").get("50"))
            cell.setCellValue(mapData.get("25").get("50"))
        cell = row.getCell(9)
        if (mapData.get("25") && mapData.get("25").get("43"))
            cell.setCellValue(mapData.get("25").get("43"))
        //26
        row = destSheet.getRow(24)
        cell = row.getCell(4)
        if (mapData.get("26") && mapData.get("26").get("75c"))
            cell.setCellValue(mapData.get("26").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("26") && mapData.get("26").get("75z"))
            cell.setCellValue(mapData.get("26").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("26") && mapData.get("26").get("65c"))
            cell.setCellValue(mapData.get("26").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("26") && mapData.get("26").get("65z"))
            cell.setCellValue(mapData.get("26").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("26") && mapData.get("26").get("50"))
            cell.setCellValue(mapData.get("26").get("50"))
        cell = row.getCell(9)
        if (mapData.get("26") && mapData.get("26").get("43"))
            cell.setCellValue(mapData.get("26").get("43"))
        //27
        row = destSheet.getRow(25)
        cell = row.getCell(4)
        if (mapData.get("27") && mapData.get("27").get("75c"))
            cell.setCellValue(mapData.get("27").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("27") && mapData.get("27").get("75z"))
            cell.setCellValue(mapData.get("27").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("27") && mapData.get("27").get("65c"))
            cell.setCellValue(mapData.get("27").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("27") && mapData.get("27").get("65z"))
            cell.setCellValue(mapData.get("27").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("27") && mapData.get("27").get("50"))
            cell.setCellValue(mapData.get("27").get("50"))
        cell = row.getCell(9)
        if (mapData.get("27") && mapData.get("27").get("43"))
            cell.setCellValue(mapData.get("27").get("43"))
        //31 (30 В)
        row = destSheet.getRow(28)
        cell = row.getCell(4)
        if (mapData.get("31") && mapData.get("31").get("75c"))
            cell.setCellValue(mapData.get("31").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("31") && mapData.get("31").get("75z"))
            cell.setCellValue(mapData.get("31").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("31") && mapData.get("31").get("65c"))
            cell.setCellValue(mapData.get("31").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("31") && mapData.get("31").get("65z"))
            cell.setCellValue(mapData.get("31").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("31") && mapData.get("31").get("50"))
            cell.setCellValue(mapData.get("31").get("50"))
        cell = row.getCell(9)
        if (mapData.get("31") && mapData.get("31").get("43"))
            cell.setCellValue(mapData.get("31").get("43"))
        //30 (30 Г)
        row = destSheet.getRow(27)
        cell = row.getCell(4)
        if (mapData.get("30") && mapData.get("30").get("75c"))
            cell.setCellValue(mapData.get("30").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("30") && mapData.get("30").get("75z"))
            cell.setCellValue(mapData.get("30").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("30") && mapData.get("30").get("65c"))
            cell.setCellValue(mapData.get("30").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("30") && mapData.get("30").get("65z"))
            cell.setCellValue(mapData.get("30").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("30") && mapData.get("30").get("50"))
            cell.setCellValue(mapData.get("30").get("50"))
        cell = row.getCell(9)
        if (mapData.get("30") && mapData.get("30").get("43"))
            cell.setCellValue(mapData.get("30").get("43"))
        //38
        row = destSheet.getRow(29)
        cell = row.getCell(4)
        if (mapData.get("38") && mapData.get("38").get("75c"))
            cell.setCellValue(mapData.get("38").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("38") && mapData.get("38").get("75z"))
            cell.setCellValue(mapData.get("38").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("38") && mapData.get("38").get("65c"))
            cell.setCellValue(mapData.get("38").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("38") && mapData.get("38").get("65z"))
            cell.setCellValue(mapData.get("38").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("38") && mapData.get("38").get("50"))
            cell.setCellValue(mapData.get("38").get("50"))
        cell = row.getCell(9)
        if (mapData.get("38") && mapData.get("38").get("43"))
            cell.setCellValue(mapData.get("38").get("43"))
        //40
        row = destSheet.getRow(31)
        cell = row.getCell(4)
        if (mapData.get("40") && mapData.get("40").get("75c"))
            cell.setCellValue(mapData.get("40").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("40") && mapData.get("40").get("75z"))
            cell.setCellValue(mapData.get("40").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("40") && mapData.get("40").get("65c"))
            cell.setCellValue(mapData.get("40").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("40") && mapData.get("40").get("65z"))
            cell.setCellValue(mapData.get("40").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("40") && mapData.get("40").get("50"))
            cell.setCellValue(mapData.get("40").get("50"))
        cell = row.getCell(9)
        if (mapData.get("40") && mapData.get("40").get("43"))
            cell.setCellValue(mapData.get("40").get("43"))
        //41
        row = destSheet.getRow(32)
        cell = row.getCell(4)
        if (mapData.get("41") && mapData.get("41").get("75c"))
            cell.setCellValue(mapData.get("41").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("41") && mapData.get("41").get("75z"))
            cell.setCellValue(mapData.get("41").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("41") && mapData.get("41").get("65c"))
            cell.setCellValue(mapData.get("41").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("41") && mapData.get("41").get("65z"))
            cell.setCellValue(mapData.get("41").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("41") && mapData.get("41").get("50"))
            cell.setCellValue(mapData.get("41").get("50"))
        cell = row.getCell(9)
        if (mapData.get("41") && mapData.get("41").get("43"))
            cell.setCellValue(mapData.get("41").get("43"))
        //43
        row = destSheet.getRow(33)
        cell = row.getCell(4)
        if (mapData.get("43") && mapData.get("43").get("75c"))
            cell.setCellValue(mapData.get("43").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("43") && mapData.get("43").get("75z"))
            cell.setCellValue(mapData.get("43").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("43") && mapData.get("43").get("65c"))
            cell.setCellValue(mapData.get("43").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("43") && mapData.get("43").get("65z"))
            cell.setCellValue(mapData.get("43").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("43") && mapData.get("43").get("50"))
            cell.setCellValue(mapData.get("43").get("50"))
        cell = row.getCell(9)
        if (mapData.get("43") && mapData.get("43").get("43"))
            cell.setCellValue(mapData.get("43").get("43"))
        //44
        row = destSheet.getRow(34)
        cell = row.getCell(4)
        if (mapData.get("44") && mapData.get("44").get("75c"))
            cell.setCellValue(mapData.get("44").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("44") && mapData.get("44").get("75z"))
            cell.setCellValue(mapData.get("44").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("44") && mapData.get("44").get("65c"))
            cell.setCellValue(mapData.get("44").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("44") && mapData.get("44").get("65z"))
            cell.setCellValue(mapData.get("44").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("44") && mapData.get("44").get("50"))
            cell.setCellValue(mapData.get("44").get("50"))
        cell = row.getCell(9)
        if (mapData.get("44") && mapData.get("44").get("44"))
            cell.setCellValue(mapData.get("44").get("44"))
        //46
        row = destSheet.getRow(35)
        cell = row.getCell(4)
        if (mapData.get("46") && mapData.get("46").get("75c"))
            cell.setCellValue(mapData.get("46").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("46") && mapData.get("46").get("75z"))
            cell.setCellValue(mapData.get("46").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("46") && mapData.get("46").get("65c"))
            cell.setCellValue(mapData.get("46").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("46") && mapData.get("46").get("65z"))
            cell.setCellValue(mapData.get("46").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("46") && mapData.get("46").get("50"))
            cell.setCellValue(mapData.get("46").get("50"))
        cell = row.getCell(9)
        if (mapData.get("46") && mapData.get("46").get("46"))
            cell.setCellValue(mapData.get("46").get("46"))
        //47
        row = destSheet.getRow(36)
        cell = row.getCell(4)
        if (mapData.get("47") && mapData.get("47").get("75c"))
            cell.setCellValue(mapData.get("47").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("47") && mapData.get("47").get("75z"))
            cell.setCellValue(mapData.get("47").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("47") && mapData.get("47").get("65c"))
            cell.setCellValue(mapData.get("47").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("47") && mapData.get("47").get("65z"))
            cell.setCellValue(mapData.get("47").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("47") && mapData.get("47").get("50"))
            cell.setCellValue(mapData.get("47").get("50"))
        cell = row.getCell(9)
        if (mapData.get("47") && mapData.get("47").get("47"))
            cell.setCellValue(mapData.get("47").get("47"))
        //49
        row = destSheet.getRow(37)
        cell = row.getCell(4)
        if (mapData.get("49") && mapData.get("49").get("75c"))
            cell.setCellValue(mapData.get("49").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("49") && mapData.get("49").get("75z"))
            cell.setCellValue(mapData.get("49").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("49") && mapData.get("49").get("65c"))
            cell.setCellValue(mapData.get("49").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("49") && mapData.get("49").get("65z"))
            cell.setCellValue(mapData.get("49").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("49") && mapData.get("49").get("50"))
            cell.setCellValue(mapData.get("49").get("50"))
        cell = row.getCell(9)
        if (mapData.get("49") && mapData.get("49").get("49"))
            cell.setCellValue(mapData.get("49").get("49"))
        //50
        row = destSheet.getRow(39)
        cell = row.getCell(4)
        if (mapData.get("50") && mapData.get("50").get("75c"))
            cell.setCellValue(mapData.get("50").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("50") && mapData.get("50").get("75z"))
            cell.setCellValue(mapData.get("50").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("50") && mapData.get("50").get("65c"))
            cell.setCellValue(mapData.get("50").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("50") && mapData.get("50").get("65z"))
            cell.setCellValue(mapData.get("50").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("50") && mapData.get("50").get("50"))
            cell.setCellValue(mapData.get("50").get("50"))
        cell = row.getCell(9)
        if (mapData.get("50") && mapData.get("50").get("50"))
            cell.setCellValue(mapData.get("50").get("50"))
        //52
        row = destSheet.getRow(40)
        cell = row.getCell(4)
        if (mapData.get("52") && mapData.get("52").get("75c"))
            cell.setCellValue(mapData.get("52").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("52") && mapData.get("52").get("75z"))
            cell.setCellValue(mapData.get("52").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("52") && mapData.get("52").get("65c"))
            cell.setCellValue(mapData.get("52").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("52") && mapData.get("52").get("65z"))
            cell.setCellValue(mapData.get("52").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("52") && mapData.get("52").get("52"))
            cell.setCellValue(mapData.get("52").get("52"))
        cell = row.getCell(9)
        if (mapData.get("52") && mapData.get("52").get("52"))
            cell.setCellValue(mapData.get("52").get("52"))
        //53
        row = destSheet.getRow(41)
        cell = row.getCell(4)
        if (mapData.get("53") && mapData.get("53").get("75c"))
            cell.setCellValue(mapData.get("53").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("53") && mapData.get("53").get("75z"))
            cell.setCellValue(mapData.get("53").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("53") && mapData.get("53").get("65c"))
            cell.setCellValue(mapData.get("53").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("53") && mapData.get("53").get("65z"))
            cell.setCellValue(mapData.get("53").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("53") && mapData.get("53").get("53"))
            cell.setCellValue(mapData.get("53").get("53"))
        cell = row.getCell(9)
        if (mapData.get("53") && mapData.get("53").get("53"))
            cell.setCellValue(mapData.get("53").get("53"))
        //55
        row = destSheet.getRow(42)
        cell = row.getCell(4)
        if (mapData.get("55") && mapData.get("55").get("75c"))
            cell.setCellValue(mapData.get("55").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("55") && mapData.get("55").get("75z"))
            cell.setCellValue(mapData.get("55").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("55") && mapData.get("55").get("65c"))
            cell.setCellValue(mapData.get("55").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("55") && mapData.get("55").get("65z"))
            cell.setCellValue(mapData.get("55").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("55") && mapData.get("55").get("55"))
            cell.setCellValue(mapData.get("55").get("55"))
        cell = row.getCell(9)
        if (mapData.get("55") && mapData.get("55").get("55"))
            cell.setCellValue(mapData.get("55").get("55"))
        //56
        row = destSheet.getRow(43)
        cell = row.getCell(4)
        if (mapData.get("56") && mapData.get("56").get("75c"))
            cell.setCellValue(mapData.get("56").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("56") && mapData.get("56").get("75z"))
            cell.setCellValue(mapData.get("56").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("56") && mapData.get("56").get("65c"))
            cell.setCellValue(mapData.get("56").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("56") && mapData.get("56").get("65z"))
            cell.setCellValue(mapData.get("56").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("56") && mapData.get("56").get("56"))
            cell.setCellValue(mapData.get("56").get("56"))
        cell = row.getCell(9)
        if (mapData.get("56") && mapData.get("56").get("56"))
            cell.setCellValue(mapData.get("56").get("56"))
        //59
        row = destSheet.getRow(44)
        cell = row.getCell(4)
        if (mapData.get("59") && mapData.get("59").get("75c"))
            cell.setCellValue(mapData.get("59").get("75c"))
        cell = row.getCell(5)
        if (mapData.get("59") && mapData.get("59").get("75z"))
            cell.setCellValue(mapData.get("59").get("75z"))
        cell = row.getCell(6)
        if (mapData.get("59") && mapData.get("59").get("65c"))
            cell.setCellValue(mapData.get("59").get("65c"))
        cell = row.getCell(7)
        if (mapData.get("59") && mapData.get("59").get("65z"))
            cell.setCellValue(mapData.get("59").get("65z"))
        cell = row.getCell(8)
        if (mapData.get("59") && mapData.get("59").get("59"))
            cell.setCellValue(mapData.get("59").get("59"))
        cell = row.getCell(9)
        if (mapData.get("59") && mapData.get("59").get("59"))
            cell.setCellValue(mapData.get("59").get("59"))

        //60


/*
        for (int idxRow=13; idxRow < 19; idxRow++) {
            Row row = destSheet.getRow(idxRow)
            for (int idxCol=4; idxCol < 10; idxCol++) {
                Cell cell = row.getCell(idxCol)
                double v = Math.random() * 10
                def v1 = UtCnv.toInt(v)
                if (v1 > 0)
                    cell.setCellValue(v1)
            }
        }
*/




        OutputStream outputStream = new FileOutputStream(pathout)
        targetWorkbook.write(outputStream)
        outputStream.close()


    }

    @DaoMethod
    Map<String, Map<String, Long>> loadDataPO_4(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_%")
        map.put("cls", map2.get("Cls_TaskLog"))
        map.put("Cls_RailwayStage", map2.get("Cls_RailwayStage"))
        map.put("Cls_RailwayStation", map2.get("Cls_RailwayStation"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
        map.put("FV_Fact", map2.get("FV_Fact"))
        //
        map.put("objClient", UtCnv.toLong(params.get("objClient")))
        //
        String dte = UtCnv.toString(params.get("dte"))
        UtPeriod utPeriod = new UtPeriod()
        long pt = UtCnv.toLong(params.get("periodType"))
        XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
        XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
        Store stTasks = loadSqlService("""
            select o.id
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and 
                (LOWER(v.name) like 'смена рельса р75 с%' or LOWER(v.name) like 'смена рельса р75 з%' or
                LOWER(v.name) like 'смена рельса р65 с%' or LOWER(v.name) like 'смена рельса р65 з%' or
                LOWER(v.name) like 'смена рельса р50%' or LOWER(v.name) like 'смена рельса р43%')
        """, "", "nsidata")
        Set<Object> idsTasks = stTasks.getUniqueValues("id")
        //
        Store st = loadSqlService("""
            select o.id, o.cls, v2.dateTimeVal as FactDateEnd, v3.obj as objTask, v1.numberVal as Value, 
                v4.obj as objWorkPlan, v5.obj as objLocation, v6.obj as objObject, null as nameObject
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Value")} and d1.status=${map.get("FV_Fact")}
                inner join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FactDateEnd")}
                inner join DataPropVal v2 on d2.id=v2.dataProp and v2.dateTimeVal between '${d1}' and '${d2}'
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_Task")}
                inner join DataPropVal v3 on d3.id=v3.dataProp and v3.obj in (0${idsTasks.join(",")})
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v4 on d4.id=v4.dataProp
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_LocationClsSection")}
                left join DataPropVal v5 on d5.id=v5.dataProp
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_Object")}
                left join DataPropVal v6 on d6.id=v6.dataProp                    
            where o.cls=${map.get("cls")}
        """, "Report.po_4", "repairdata")
        //
        if (st.size()==0)
            throw new XError("Нет данных")
        //

        //mdb.outTable(st)

        Set<Object> idsWorkPlan = st.getUniqueValues("objWorkPlan")
        //
        long objLocation = UtCnv.toLong(params.get("objLocation"))
        Store stLocation = loadSqlService("""
           WITH RECURSIVE r AS (
               SELECT o.id, v.objParent as parent
               FROM Obj o, ObjVer v
               WHERE o.id=v.ownerver and v.lastver=1 and v.objParent=${objLocation}
               UNION ALL
               SELECT t.*
               FROM ( SELECT o.id, v.objParent as parent
                      FROM Obj o, ObjVer v
                      WHERE o.id=v.ownerver and v.lastver=1
                    ) t
                  JOIN r
                      ON t.parent = r.id
           ),
           o as (
           SELECT o.id, v.objParent as parent
           FROM Obj o, ObjVer v
           WHERE o.id=v.ownerver and v.lastver=1 and o.id=${objLocation}
           )
           SELECT * FROM o
           UNION ALL
           SELECT * FROM r
           where 0=0
        """, "", "orgstructuredata")

        Set<Object> idsLocation = stLocation.getUniqueValues("id")
        //

        Store stWorkPlan = loadSqlService("""
            select o.id, o.cls,  
                v1.obj as objLocation, v2.obj as objObject, null as clsObject, null as nameObject, v3.obj as objIncident 
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_LocationClsSection")}
                inner join DataPropVal v1 on d1.id=v1.dataProp and v1.obj in (0${idsLocation.join(",")})
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                inner join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_Incident")}
                inner join DataPropVal v3 on d3.id=v3.dataProp
            where o.id in (0${idsWorkPlan.join(",")})
        """, "", "plandata")

        Set<Object> idsObject = stWorkPlan.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, o.cls, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObject = stObject.getIndex("id")
        for (StoreRecord r in stWorkPlan) {
            StoreRecord rec = indObject.get(r.getLong("objObject"))
            if (rec != null) {
                r.set("clsobject", rec.getLong("cls"))
                r.set("nameobject", rec.getString("name"))
            }
        }
        //
        StoreIndex indWorkPlan = stWorkPlan.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indWorkPlan.get(r.getLong("objWorkPlan"))
            if (rec != null) {
                r.set("objObject", rec.getLong("objObject"))
                r.set("clsObject", rec.getLong("clsobject"))
                r.set("nameObject", rec.getString("nameobject"))
                r.set("objIncident", rec.getLong("objIncident"))
            }
        }
        //
        stObject = loadSqlService("""
            select o.id, o.cls, v1.obj as objSection, ov1.objParent as parent
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        indObject = stObject.getIndex("id")

        for (StoreRecord r in st) {
            StoreRecord rec = indObject.get(r.getLong("objObject"))
            if (rec != null) {
                r.set("objSection", rec.getLong("objSection"))
                r.set("Parent", rec.getLong("parent"))
            }
        }
        //
        Set<Object> idsParent = stObject.getUniqueValues("parent")
        stObject = loadSqlService("""
            select o.id, v2.obj as objClient
            from Obj o
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Client")}
                inner join DataPropVal v2 on d2.id=v2.dataProp and v2.obj=${map.get("objClient")}
            where o.id in (0${idsParent.join(",")}) 
        """, "", "objectdata")
        indObject = stObject.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indObject.get(r.getLong("Parent"))
            if (rec != null) {
                r.set("objClient", rec.getLong("objClient"))
            }
        }
        //
        Set<Object> idsObjClient = st.getUniqueValues("objClient")
        if (!idsObjClient.contains(UtCnv.toLong(params.get("objClient"))))
            throw new XError("Нет данных")


        //
        Set<Object> idsIncident = stWorkPlan.getUniqueValues("objIncident")
        Store stIncident = loadSqlService("""
            select o.id, v1.obj as objFault
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Fault")}
                left join DataPropVal v1 on d1.id=v1.dataProp
            where o.id in (0${idsIncident.join(",")}) 
        """, "", "incidentdata")
        StoreIndex indIncident = stIncident.getIndex("id")
        Set<Object> idsFault = stIncident.getUniqueValues("objFault")
        //
        Store stInspection = loadSqlService("""
            select o.id, v1.obj as objDefect
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Defect")}
                left join DataPropVal v1 on d1.id=v1.dataProp
            where o.id in (0${idsFault.join(",")}) 
        """, "", "inspectiondata")
        StoreIndex indInspection = stInspection.getIndex("id")
        Set<Object> idsDefect = stInspection.getUniqueValues("objDefect")
        //
        Store stNsi = loadSqlService("""
            select o.id, v1.strVal as DefectsIndex
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_DefectsIndex")}
                left join DataPropVal v1 on d1.id=v1.dataProp
            where o.id in (0${idsDefect.join(",")}) 
        """, "", "nsidata")

        StoreIndex indNsi = stNsi.getIndex("id")
        //
        Set<Object> idsTask = st.getUniqueValues("objTask")
        Store stTask = loadSqlService("""
            select o.id, v.name 
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTask.join(",")})
        """, "", "nsidata")
        StoreIndex indTask = stTask.getIndex("id")
        //
        Store stRes = mdb.createStore("Report.po_4")
        for (StoreRecord r in st) {
            StoreRecord recTask = indTask.get(r.getLong("objTask"))
            if (recTask != null)
                r.set("nameTask", recTask.getString("name"))
            //
            StoreRecord recIncident = indIncident.get(r.getLong("objIncident"))
            if (recIncident != null) {
                r.set("objFault", recIncident.getLong("objFault"))
            }
            //
            StoreRecord recInspection = indInspection.get(r.getLong("objFault"))
            if (recInspection != null) {
                r.set("objDefect", recInspection.getLong("objDefect"))
            }
            //
            StoreRecord recNsi = indNsi.get(r.getLong("objDefect"))
            if (recNsi != null) {
                r.set("DefectsIndex", recNsi.getString("DefectsIndex"))
            }
            //
            if (r.getLong("objClient") > 0)
                stRes.add(r)
        }
        //
        Map<String, Map<String, Long>> mapNum = new HashMap<>()
        Map<String, Long> mapType10 = new HashMap<>()
        Map<String, Long> mapType11 = new HashMap<>()
        Map<String, Long> mapType12 = new HashMap<>()
        Map<String, Long> mapType13 = new HashMap<>()
        Map<String, Long> mapType14 = new HashMap<>()
        Map<String, Long> mapType18 = new HashMap<>()
        Map<String, Long> mapType20 = new HashMap<>()
        Map<String, Long> mapType21 = new HashMap<>()
        Map<String, Long> mapType24 = new HashMap<>()
        Map<String, Long> mapType25 = new HashMap<>()
        Map<String, Long> mapType26 = new HashMap<>()
        Map<String, Long> mapType27 = new HashMap<>()
        Map<String, Long> mapType30 = new HashMap<>()
        Map<String, Long> mapType31 = new HashMap<>()
        Map<String, Long> mapType38 = new HashMap<>()
        Map<String, Long> mapType40 = new HashMap<>()
        Map<String, Long> mapType41 = new HashMap<>()
        Map<String, Long> mapType43 = new HashMap<>()
        Map<String, Long> mapType44 = new HashMap<>()
        Map<String, Long> mapType46 = new HashMap<>()
        Map<String, Long> mapType47 = new HashMap<>()
        Map<String, Long> mapType49 = new HashMap<>()
        Map<String, Long> mapType50 = new HashMap<>()
        Map<String, Long> mapType52 = new HashMap<>()
        Map<String, Long> mapType53 = new HashMap<>()
        Map<String, Long> mapType55 = new HashMap<>()
        Map<String, Long> mapType56 = new HashMap<>()
        Map<String, Long> mapType59 = new HashMap<>()
        Map<String, Long> mapType60 = new HashMap<>()
        Map<String, Long> mapType62 = new HashMap<>()
        Map<String, Long> mapType65 = new HashMap<>()
        Map<String, Long> mapType66 = new HashMap<>()
        Map<String, Long> mapType69 = new HashMap<>()
        Map<String, Long> mapType70 = new HashMap<>()
        Map<String, Long> mapType74 = new HashMap<>()
        Map<String, Long> mapType79 = new HashMap<>()
        Map<String, Long> mapType85 = new HashMap<>()
        Map<String, Long> mapType86 = new HashMap<>()
        Map<String, Long> mapType99 = new HashMap<>()

        long shtuka = 0
        //10.
        for (StoreRecord r in stRes) {
            if (defectsIndex(r.getString("DefectsIndex"))) {
                if (r.getString("nameTask").toLowerCase().contains("смена рельса р75 с") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р75 з") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р65 с") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р65 з") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р50") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р43")) {

                    if (r.getLong("clsObject") == map.get("Cls_RailwayStage")) {

                        //10.
                        if (r.getString("DefectsIndex").startsWith("10.")) {
                            if (r.getString("nameTask").toLowerCase().contains("смена рельса р75 с"))
                                mapType10.put("75c", mapType10.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("смена рельса р75 з"))
                                mapType10.put("75z", mapType10.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType10.put("65c", mapType10.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType10.put("65z", mapType10.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType10.put("50", mapType10.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType10.put("43", mapType10.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("10", mapType10)
                        }
                        //11.
                        if (r.getString("DefectsIndex").startsWith("11.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType11.put("75c", mapType11.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType11.put("75z", mapType11.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType11.put("65c", mapType11.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType11.put("65z", mapType11.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType11.put("50", mapType11.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType11.put("43", mapType11.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("11", mapType11)
                        }
                        //12.
                        if (r.getString("DefectsIndex").startsWith("12.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType12.put("75c", mapType12.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType12.put("75z", mapType12.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType12.put("65c", mapType12.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType12.put("65z", mapType12.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType12.put("50", mapType12.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType12.put("43", mapType12.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("12", mapType12)
                        }
                        //13.
                        if (r.getString("DefectsIndex").startsWith("13.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType13.put("75c", mapType13.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType13.put("75z", mapType13.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType13.put("65c", mapType13.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType13.put("65z", mapType13.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType13.put("50", mapType13.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType13.put("43", mapType13.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("13", mapType13)
                        }
                        //14.
                        if (r.getString("DefectsIndex").startsWith("14.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType14.put("75c", mapType14.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType14.put("75z", mapType14.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType14.put("65c", mapType14.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType14.put("65z", mapType14.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType14.put("50", mapType14.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType14.put("43", mapType14.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("14", mapType14)
                        }
                        //18.
                        if (r.getString("DefectsIndex").startsWith("18.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType18.put("75c", mapType18.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType18.put("75z", mapType18.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType18.put("65c", mapType18.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType18.put("65z", mapType18.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType18.put("50", mapType18.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType18.put("43", mapType18.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("18", mapType18)
                        }
                        //20.
                        if (r.getString("DefectsIndex").startsWith("20.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType20.put("75c", mapType20.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType20.put("75z", mapType20.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType20.put("65c", mapType20.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType20.put("65z", mapType20.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType20.put("50", mapType20.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType20.put("43", mapType20.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("20", mapType20)
                        }
                        //21.
                        if (r.getString("DefectsIndex").startsWith("21.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType21.put("75c", mapType21.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType21.put("75z", mapType21.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType21.put("65c", mapType21.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType21.put("65z", mapType21.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType21.put("50", mapType21.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType21.put("43", mapType21.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("21", mapType21)
                        }
                        //24.
                        if (r.getString("DefectsIndex").startsWith("24.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType24.put("75c", mapType24.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType24.put("75z", mapType24.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType24.put("65c", mapType24.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType24.put("65z", mapType24.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType24.put("50", mapType24.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType24.put("43", mapType24.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("24", mapType24)
                        }
                        //25.
                        if (r.getString("DefectsIndex").startsWith("25.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType25.put("75c", mapType25.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType25.put("75z", mapType25.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType25.put("65c", mapType25.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType25.put("65z", mapType25.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType25.put("50", mapType25.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType25.put("43", mapType25.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("25", mapType25)
                        }
                        //26.
                        if (r.getString("DefectsIndex").startsWith("26.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType26.put("75c", mapType26.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType26.put("75z", mapType26.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType26.put("65c", mapType26.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType26.put("65z", mapType26.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType26.put("50", mapType26.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType26.put("43", mapType26.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("26", mapType26)
                        }
                        //27.
                        if (r.getString("DefectsIndex").startsWith("27.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType27.put("75c", mapType27.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType27.put("75z", mapType27.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType27.put("65c", mapType27.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType27.put("65z", mapType27.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType27.put("50", mapType27.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType27.put("43", mapType27.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("27", mapType27)
                        }
                        //30.
                        if (r.getString("DefectsIndex").startsWith("30.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType30.put("75c", mapType30.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType30.put("75z", mapType30.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType30.put("65c", mapType30.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType30.put("65z", mapType30.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType30.put("50", mapType30.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType30.put("43", mapType30.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("30", mapType30)
                        }
                        //31.
                        if (r.getString("DefectsIndex").startsWith("31.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType31.put("75c", mapType31.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType31.put("75z", mapType31.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType31.put("65c", mapType31.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType31.put("65z", mapType31.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType31.put("50", mapType31.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType31.put("43", mapType31.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("31", mapType31)
                        }
                        //38.
                        if (r.getString("DefectsIndex").startsWith("38.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType38.put("75c", mapType38.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType38.put("75z", mapType38.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType38.put("65c", mapType38.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType38.put("65z", mapType38.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType38.put("50", mapType38.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType38.put("43", mapType38.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("38", mapType38)
                        }
                        //40.
                        if (r.getString("DefectsIndex").startsWith("40.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType40.put("75c", mapType40.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType40.put("75z", mapType40.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType40.put("65c", mapType40.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType40.put("65z", mapType40.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType40.put("50", mapType40.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType40.put("43", mapType40.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("40", mapType40)
                        }
                        //41.
                        if (r.getString("DefectsIndex").startsWith("41.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType41.put("75c", mapType41.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType41.put("75z", mapType41.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType41.put("65c", mapType41.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType41.put("65z", mapType41.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType41.put("50", mapType41.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType41.put("43", mapType41.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("41", mapType41)
                        }
                        //43.
                        if (r.getString("DefectsIndex").startsWith("43.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType43.put("75c", mapType43.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType43.put("75z", mapType43.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType43.put("65c", mapType43.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType43.put("65z", mapType43.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType43.put("50", mapType43.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType43.put("43", mapType43.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("43", mapType43)
                        }
                        //44.
                        if (r.getString("DefectsIndex").startsWith("44.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType44.put("75c", mapType44.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType44.put("75z", mapType44.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType44.put("65c", mapType44.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType44.put("65z", mapType44.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType44.put("50", mapType44.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType44.put("43", mapType44.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("44", mapType44)
                        }
                        //46.
                        if (r.getString("DefectsIndex").startsWith("46.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType46.put("75c", mapType46.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType46.put("75z", mapType46.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType46.put("65c", mapType46.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType46.put("65z", mapType46.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType46.put("50", mapType46.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType46.put("43", mapType46.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("46", mapType46)
                        }
                        //47.
                        if (r.getString("DefectsIndex").startsWith("47.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType47.put("75c", mapType47.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType47.put("75z", mapType47.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType47.put("65c", mapType47.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType47.put("65z", mapType47.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType47.put("50", mapType47.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType47.put("43", mapType47.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("47", mapType47)
                        }
                        //49.
                        if (r.getString("DefectsIndex").startsWith("49.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType49.put("75c", mapType49.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType49.put("75z", mapType49.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType49.put("65c", mapType49.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType49.put("65z", mapType49.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType49.put("50", mapType49.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType49.put("43", mapType49.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("49", mapType49)
                        }
                        //50.
                        if (r.getString("DefectsIndex").startsWith("50.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType50.put("75c", mapType50.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType50.put("75z", mapType50.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType50.put("65c", mapType50.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType50.put("65z", mapType50.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType50.put("50", mapType50.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType50.put("43", mapType50.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("50", mapType50)
                        }
                        //52.
                        if (r.getString("DefectsIndex").startsWith("52.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType52.put("75c", mapType52.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType52.put("75z", mapType52.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType52.put("65c", mapType52.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType52.put("65z", mapType52.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType52.put("50", mapType52.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType52.put("43", mapType52.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("52", mapType52)
                        }
                        //53.
                        if (r.getString("DefectsIndex").startsWith("53.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType53.put("75c", mapType53.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType53.put("75z", mapType53.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType53.put("65c", mapType53.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType53.put("65z", mapType53.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType53.put("50", mapType53.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType53.put("43", mapType53.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("53", mapType53)
                        }
                        //55.
                        if (r.getString("DefectsIndex").startsWith("55.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType55.put("75c", mapType55.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType55.put("75z", mapType55.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType55.put("65c", mapType55.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType55.put("65z", mapType55.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType55.put("50", mapType55.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType55.put("43", mapType55.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("55", mapType55)
                        }
                        //56.
                        if (r.getString("DefectsIndex").startsWith("56.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType56.put("75c", mapType56.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType56.put("75z", mapType56.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType56.put("65c", mapType56.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType56.put("65z", mapType56.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType56.put("50", mapType56.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType56.put("43", mapType56.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("56", mapType56)
                        }
                        //59.
                        if (r.getString("DefectsIndex").startsWith("59.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType59.put("75c", mapType59.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType59.put("75z", mapType59.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType59.put("65c", mapType59.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType59.put("65z", mapType59.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType59.put("50", mapType59.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType59.put("43", mapType59.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("59", mapType59)
                        }
                        //60.
                        if (r.getString("DefectsIndex").startsWith("60.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType60.put("75c", mapType60.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType60.put("75z", mapType60.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType60.put("65c", mapType60.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType60.put("65z", mapType60.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType60.put("50", mapType60.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType60.put("43", mapType60.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("60", mapType60)
                        }
                        //62.
                        if (r.getString("DefectsIndex").startsWith("62.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType62.put("75c", mapType62.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType62.put("75z", mapType62.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType62.put("65c", mapType62.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType62.put("65z", mapType62.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType62.put("50", mapType62.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType62.put("43", mapType62.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("62", mapType62)
                        }
                        //65.
                        if (r.getString("DefectsIndex").startsWith("65.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType65.put("75c", mapType65.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType65.put("75z", mapType65.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType65.put("65c", mapType65.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType65.put("65z", mapType65.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType65.put("50", mapType65.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType65.put("43", mapType65.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("65", mapType65)
                        }
                        //66.
                        if (r.getString("DefectsIndex").startsWith("66.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType66.put("75c", mapType66.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType66.put("75z", mapType66.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType66.put("65c", mapType66.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType66.put("65z", mapType66.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType66.put("50", mapType66.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType66.put("43", mapType66.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("66", mapType66)
                        }
                        //69.
                        if (r.getString("DefectsIndex").startsWith("69.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType69.put("75c", mapType69.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType69.put("75z", mapType69.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType69.put("65c", mapType69.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType69.put("65z", mapType69.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType69.put("50", mapType69.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType69.put("43", mapType69.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("69", mapType69)
                        }
                        //70.
                        if (r.getString("DefectsIndex").startsWith("70.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType70.put("75c", mapType70.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType70.put("75z", mapType70.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType70.put("65c", mapType70.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType70.put("65z", mapType70.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType70.put("50", mapType70.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType70.put("43", mapType70.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("70", mapType70)
                        }
                        //74.
                        if (r.getString("DefectsIndex").startsWith("74.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType74.put("75c", mapType74.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType74.put("75z", mapType74.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType74.put("65c", mapType74.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType74.put("65z", mapType74.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType74.put("50", mapType74.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType74.put("43", mapType74.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("74", mapType74)
                        }
                        //79.
                        if (r.getString("DefectsIndex").startsWith("79.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType79.put("75c", mapType79.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType79.put("75z", mapType79.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType79.put("65c", mapType79.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType79.put("65z", mapType79.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType79.put("50", mapType79.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType79.put("43", mapType79.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("79", mapType79)
                        }
                        //85.
                        if (r.getString("DefectsIndex").startsWith("85.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType85.put("75c", mapType85.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType85.put("75z", mapType85.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType85.put("65c", mapType85.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType85.put("65z", mapType85.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType85.put("50", mapType85.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType85.put("43", mapType85.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("85", mapType85)
                        }
                        //86.
                        if (r.getString("DefectsIndex").startsWith("86.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType86.put("75c", mapType86.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType86.put("75z", mapType86.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType86.put("65c", mapType86.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType86.put("65z", mapType86.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType86.put("50", mapType86.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType86.put("43", mapType86.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("86", mapType86)
                        }
                        //99.
                        if (r.getString("DefectsIndex").startsWith("99.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType99.put("75c", mapType99.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType99.put("75z", mapType99.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType99.put("65c", mapType99.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType99.put("65z", mapType99.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType99.put("50", mapType99.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType99.put("43", mapType99.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("99", mapType99)
                        }
                    } else if (r.getLong("clsObject") == map.get("Cls_RailwayStation")) {
                        shtuka = shtuka + r.getLong("Value")
                    }
                }
            }
        }

        mapNum.put("shtuka", Map.of("shtuka", shtuka))
        mdb.outMap(mapNum)
        //
        return mapNum
    }

    static boolean defectsIndex(String index) {
        return index.startsWith("10.") || index.startsWith("11.") || index.startsWith("12.") || index.startsWith("13.") ||
                index.startsWith("14.") || index.startsWith("18.") || index.startsWith("20.") || index.startsWith("21.") ||
                index.startsWith("24.") || index.startsWith("25.") || index.startsWith("26.") || index.startsWith("27.") ||
                index.startsWith("30.") || index.startsWith("31.") || index.startsWith("38.") || index.startsWith("40.") ||
                index.startsWith("43.") || index.startsWith("44.") || index.startsWith("46.") || index.startsWith("47.") ||
                index.startsWith("49.") || index.startsWith("50.") || index.startsWith("52.") || index.startsWith("53.") ||
                index.startsWith("55.") || index.startsWith("56.") || index.startsWith("59.") || index.startsWith("60.") ||
                index.startsWith("62.") || index.startsWith("65.") || index.startsWith("66.") || index.startsWith("69.") ||
                index.startsWith("70.") || index.startsWith("74.") || index.startsWith("79.") || index.startsWith("85.") ||
                index.startsWith("86.") || index.startsWith("99.") ||  index.startsWith("41.")
    }

/*
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
        XSSFSheet targetSheet = targetWorkbook.createSheet()

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
*/


    //-------------------------
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
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
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
