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
    void generateReportPO_4(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        String pathin = mdb.getApp().appdir + File.separator + "tml" + File.separator + "ПО-4.xlsx"
        String pathout = mdb.getApp().appdir + File.separator + "report" + File.separator + "ПО-4.xlsx"


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
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        map.put("cls", map2.get("Cls_TaskLog"))
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
        Store st = loadSqlService("""
            select o.id, o.cls, v2.dateTimeVal as FactDateEnd, v3.obj as objTask, v1.numberVal as Value, 
                v4.obj as objWorkPlan, v5.obj as objLocation, v6.obj as objObject, null as nameObject
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Value")} and d1.status=${map.get("FV_Fact")}
                inner join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FactDateEnd")}
                inner join DataPropVal v2 on d2.id=v2.dataProp and v2.dateTimeVal between '${d1}' and '${d2}'
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_Task")}
                inner join DataPropVal v3 on d3.id=v3.dataProp
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v4 on d4.id=v4.dataProp
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_LocationClsSection")}
                left join DataPropVal v5 on d5.id=v5.dataProp
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_Object")}
                left join DataPropVal v6 on d6.id=v6.dataProp                    
            where o.cls=${map.get("cls")}
        """, "Report.po_4", "repairdata")

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
        mdb.outTable(stRes)
        //

        Map<String, Map<String, Long>> mapNum = new HashMap<>()
        Map<String, Long> mapType10 = new HashMap<>()
        Map<String, Long> mapType11 = new HashMap<>()
        Map<String, Long> mapType12 = new HashMap<>()
        Map<String, Long> mapType13 = new HashMap<>()
        Map<String, Long> mapType14 = new HashMap<>()
        Map<String, Long> mapType18 = new HashMap<>()
        Map<String, Long> mapType20 = new HashMap<>()
        //10.1
        for (StoreRecord r in stRes) {
            if (r.getString("DefectsIndex").startsWith("10.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType10.put("75c", mapType10.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType10.put("75z", mapType10.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType10.put("65c", mapType10.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType10.put("65z", mapType10.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType10.put("50", mapType10.get("50", 0L)+r.getLong("Value"))
                else
                    mapType10.put("43", mapType10.get("43", 0L)+r.getLong("Value"))
                mapNum.put("10", mapType10)
            }
            //11.1
            if (r.getString("DefectsIndex").startsWith("11.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType11.put("75c", mapType11.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType11.put("75z", mapType11.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType11.put("65c", mapType11.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType11.put("65z", mapType11.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType11.put("50", mapType11.get("50", 0L)+r.getLong("Value"))
                else
                    mapType11.put("43", mapType11.get("43", 0L)+r.getLong("Value"))
                mapNum.put("11", mapType11)
            }
            //12.1
            if (r.getString("DefectsIndex").startsWith("12.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType12.put("75c", mapType12.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType12.put("75z", mapType12.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType12.put("65c", mapType12.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType12.put("65z", mapType12.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType12.put("50", mapType12.get("50", 0L)+r.getLong("Value"))
                else
                    mapType12.put("43", mapType12.get("43", 0L)+r.getLong("Value"))
                mapNum.put("12", mapType12)
            }
            //13.1
            if (r.getString("DefectsIndex").startsWith("13.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType13.put("75c", mapType13.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType13.put("75z", mapType13.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType13.put("65c", mapType13.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType13.put("65z", mapType13.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType13.put("50", mapType13.get("50", 0L)+r.getLong("Value"))
                else
                    mapType13.put("43", mapType13.get("43", 0L)+r.getLong("Value"))
                mapNum.put("13", mapType13)
            }
            //14.1
            if (r.getString("DefectsIndex").startsWith("14.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType14.put("75c", mapType14.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType14.put("75z", mapType14.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType14.put("65c", mapType14.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType14.put("65z", mapType14.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType14.put("50", mapType14.get("50", 0L)+r.getLong("Value"))
                else
                    mapType14.put("43", mapType14.get("43", 0L)+r.getLong("Value"))
                mapNum.put("14", mapType14)
            }
            //18.1
            if (r.getString("DefectsIndex").startsWith("18.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType18.put("75c", mapType18.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType18.put("75z", mapType18.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType18.put("65c", mapType18.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType18.put("65z", mapType18.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType18.put("50", mapType18.get("50", 0L)+r.getLong("Value"))
                else
                    mapType18.put("43", mapType18.get("43", 0L)+r.getLong("Value"))
                mapNum.put("18", mapType18)
            }
            //20.1
            if (r.getString("DefectsIndex").startsWith("20.1")) {
                if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                    mapType20.put("75c", mapType20.get("75c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                    mapType20.put("75z", mapType20.get("75z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                    mapType20.put("65c", mapType20.get("65c", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                    mapType20.put("65z", mapType20.get("65z", 0L)+r.getLong("Value"))
                else if (r.getString("nameTask").toLowerCase().contains("р50"))
                    mapType20.put("50", mapType20.get("50", 0L)+r.getLong("Value"))
                else
                    mapType20.put("43", mapType20.get("43", 0L)+r.getLong("Value"))
                mapNum.put("20", mapType20)
            }


        }

        mdb.outMap(mapNum)
        //
        return mapNum
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
