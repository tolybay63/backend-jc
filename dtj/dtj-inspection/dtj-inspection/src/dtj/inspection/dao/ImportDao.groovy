package dtj.inspection.dao

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiPlanData
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.utils.UtPeriod
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService
import dtj.inspection.dao.DataDao

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory

class ImportDao extends BaseMdbUtils {
    ApinatorApi apiMeta() { return app.bean(ApinatorService).getApi("meta") }
    ApinatorApi apiPlanData() { return app.bean(ApinatorService).getApi("plandata") }
    ApinatorApi apiNSIData() { return app.bean(ApinatorService).getApi("nsidata") }
    ApinatorApi apiIncidentData() { return app.bean(ApinatorService).getApi("incidentdata") }
    ApinatorApi apiObjectData() { return app.bean(ApinatorService).getApi("objectdata") }

    def infoFile = [:]
    def errorImport = false
    def datetime_create = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)

    void saveLog(String filename, String datetime_create, int filled, String datetime_fill, String msg) {
        Store stLog = mdb.loadQuery("select * from _log where filename like '${filename}'")
        String sql = """
            INSERT INTO public._log (filename, datetime_create, filled, datetime_fill, msg)
            VALUES('${filename}', '${datetime_create}', ${filled}, ${datetime_fill}, '${msg}');
            COMMIT;
        """
        if (stLog.size() > 0) {
            sql = "UPDATE public._log SET msg='${msg}' where filename like '${filename}'; COMMIT;"
        }
        mdb.execQueryNative(sql)
    }

    @DaoMethod
    Store loadLog(String filename) {
        return mdb.loadQuery("select * from _log where filename like '${filename}'")
    }

    @DaoMethod
    Store loadTable(String table) {
        return mdb.loadQuery("select * from ${table} where 0=0")
    }

    @DaoMethod
    void analyze(File file, Map<String, Object> params) {
        try {
            //File inputFile = new File("C:\\jc-2\\_info\\xml\\G057_22042025_113706_64_1 1.xml")
            //File inputFile = new File("C:\\jc-2\\_info\\xml\\B057_22042025_113706_1 1.xml")
            String filename = UtCnv.toString(params.get("filename"))
            infoFile.put("filename", filename)
            Store stLog = mdb.loadQuery("select * from _log where filename like '${filename}'")
            boolean fileLoaded = false
            if (stLog.size()>0 && stLog.get(0).getInt("filled")==0)
                fileLoaded = true

            if (filename.startsWith("G")) {
                if (!fileLoaded) {
                    parseOtstup(file)
                    assignPropDefault("_otstup")
                }
                check("_otstup", params)
            } else if (filename.startsWith("B")) {
                if (!fileLoaded) {
                    parseBall(file)
                    assignPropDefault("_ball")
                }
                check("_ball", params)
            }
        } finally {
            if (!errorImport)
                saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null, '')
        }
    }

    void check(String domain, Map<String, Object> params) {
        DataDao dataDao = mdb.createDao(DataDao.class)
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_")
        //
        Store st = mdb.loadQuery("""
            select * from ${domain}
        """)
        if (st.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null, "Нет данных в [${domain}]")
            throw new XError("Нет данных в [${domain}]")
        }
        //Находим отступления
        Set<Object> kodsOtstup = st.getUniqueValues("kod_otstup")
        Store stOtstup = apiNSIData().get(ApiNSIData).loadSql("""
            select s.cod, c.entityid as id from syscod c, syscodingcod s
            where c.id=s.syscod and c.entitytype=2 
                and s.syscoding=1001
        ""","")
        StoreIndex indOtstup = stOtstup.getIndex("cod")
        Set<Object> idsRelobjComponentParams = new HashSet<>()
        for (cod in kodsOtstup) {
            StoreRecord r = indOtstup.get(UtCnv.toString(cod))
            if (r == null) {
                errorImport = true
                saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                        "Не найдено привязка [kod_otstup: ${cod}]")
                throw new XError("Не найдено привязка [kod_otstup: ${cod}]")
            } else
                idsRelobjComponentParams.add(r.getLong("id"))
        }
        // Находим направления
        Store stNapr = apiObjectData().get(ApiObjectData).loadSql("""
            select c.entityid as id from syscod c, syscodingcod s
            where c.id=s.syscod and c.entitytype=1 
                and s.syscoding=1001 and s.cod like 'kod_napr_${st.get(0).getString("kod_napr")}'
        """, "")
        if (stNapr.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    "Не найдено привязка [kod_napr: ${st.get(0).getString( "kod_napr")}]")
            throw new XError("Не найдено привязка [kod_napr: ${st.get(0).getString("kod_napr")}]")
        }
        // Находим участки направления
        Store stSection = apiObjectData().get(ApiObjectData).loadSql("""
            select o.id from Obj o, ObjVer v 
            where o.id=v.ownerver and v.lastver=1 and v.objparent=${stNapr.get(0).getLong("id")} 
                and o.cls in (0${mapCls.get("Cls_Station")}, 0${mapCls.get("Cls_Stage")})
        """, "")
        if (stSection.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    "Не найден Раздельный пункт и/или Перегон по направлению")
            throw new XError("Не найден Раздельный пункт и/или Перегон по направлению")
        }
        // Находим обслуживаемые объекты
        Store stObject = apiObjectData().get(ApiObjectData).loadSql("""
            select id from Obj
            where cls in (0${mapCls.get("Cls_RailwayStage")}, 0${mapCls.get("Cls_RailwayStation")})
        """, "")
        if (stObject.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    "Не найден обслуживаемый объект по направлению")
            throw new XError("Не найден обслуживаемый объект по направлению")
        }
        // Находим работу вагона-путеизмерителя - 3394
        Store stWork = apiNSIData().get(ApiNSIData).loadSqlWithParams("""
            select o.id from obj o, objVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls=${mapCls.get("Cls_WorkInspection")}
                and v.name like 'Работа вагона-путеизмерителя'
        """, null, "")
        if (stWork.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    'Не найдена "Работа вагона-путеизмерителя" в справочнике работ')
            throw new XError('Не найдена "Работа вагона-путеизмерителя" в справочнике работ')
        }
        String wheV11 = "and v11.obj=${stWork.get(0).getLong("id")}"
        // План работ (plandata)
        String whe = "o.cls=${mapCls.get("Cls_WorkPlanInspection")}"
        Set<Object> idsObject = stObject.getUniqueValues("id")
        String wheV2 = "and v2.obj in (0${idsObject.join(",")})"
        String wheV7
        if (domain == "_ball")
            wheV7 = "and v7.dateTimeVal='${st.get(0).getString("date_obn")}'"
        else
            wheV7 = "and v7.dateTimeVal='${st.get(0).getString("datetime_obn").split("T")[0]}'"
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        Store stPlan = apiPlanData().get(ApiPlanData).loadSqlWithParams("""
            select o.id, o.cls,
                v3.numberVal * 1000 + (v5.numberVal - 1) * 100 + v14.numberVal * 25 as beg,
                v4.numberVal * 1000 + (v6.numberVal - 1) * 100 + v15.numberVal * 25 as end,
                v1.propVal as pvLocationClsSection, v1.obj as objLocationClsSection,
                v2.propVal as pvObject, v2.obj as objObject,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as PlanDateEnd,
                --v8.propVal as pvUser, v8.obj as objUser,
                --v9.dateTimeVal as CreatedAt,
                --v10.dateTimeVal as UpdatedAt,
                v11.propVal as pvWork, v11.obj as objWork,
                --v12.dateTimeVal as FactDateEnd,
                --v13.propVal as pvIncident, v13.obj as objIncident,
                v14.numberVal as StartLink,
                v15.numberVal as FinishLink
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Object
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                inner join DataPropVal v3 on d3.id=v3.dataprop ${wheV2}
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                --left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_User
                --left join DataPropVal v8 on d8.id=v8.dataprop
                --left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_CreatedAt
                --left join DataPropVal v9 on d9.id=v9.dataprop
                --left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_UpdatedAt
                --left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_Work
                inner join DataPropVal v11 on d11.id=v11.dataprop ${wheV11}
                --left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                --left join DataPropVal v12 on d12.id=v12.dataprop
                --left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_Incident
                --left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_StartLink
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_FinishLink
                left join DataPropVal v15 on d15.id=v15.dataprop
            where ${whe}
        """, map, "Obj.plan")
        //
        if (stPlan.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    "Не найден план работ")
            throw new XError('Не найден план работ')
        }
        //Проверяем запланирована ли работа по километрам из импортируемого файла
        Boolean bool = false
        for (StoreRecord r1 in st) {
            for (StoreRecord r2 in stPlan) {
                if (r2.getLong("beg") <= (r1.getLong("km") + 1) * 1000 && (r1.getLong("km") + 1) * 1000 <= r2.getLong("end")) {
                    bool = true
                    break;
                }
            }
            if (bool) {
                bool = false
                break;
            } else {
                errorImport = true
                saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                        "Не найден план работ на ${r1.getLong('km') + 1} км")
                throw new XError("Не найден план работ на ${r1.getLong('km') + 1} км")
            }
        }
        //3 Журнал осмотров и проверок
        Store stInspection = mdb.createStore("Obj.inspection")
        whe = "o.cls=${mapCls.get("Cls_Inspection")}"
        Set<Object> idsPlan = stPlan.getUniqueValues("id")
        wheV2 = "and v2.obj in (0${idsPlan.join(",")})"
        mdb.loadQuery(stInspection,"""
            select o.id, o.cls,
                v1.propVal as pvLocationClsSection, v1.obj as objLocationClsSection,
                v2.propVal as pvWorkPlan, v2.obj as objWorkPlan, 
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as FactDateEnd,
                --v8.propVal as pvUser, v8.obj as objUser,
                --v9.dateTimeVal as CreatedAt,
                --v10.dateTimeVal as UpdatedAt,
                v11.propVal as pvFlagDefect, null as fvFlagDefect,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink,
                --v14.multiStrVal as ReasonDeviation,
                v15.propVal as pvFlagParameter, null as fvFlagParameter,
                v16.strVal as NumberTrack,
                v17.strVal as HeadTrack
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_WorkPlan
                inner join DataPropVal v2 on d2.id=v2.dataprop ${wheV2}
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_FactDateEnd
                left join DataPropVal v7 on d7.id=v7.dataprop
                --left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_User
                --left join DataPropVal v8 on d8.id=v8.dataprop
                --left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_CreatedAt
                --left join DataPropVal v9 on d9.id=v9.dataprop
                --left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_UpdatedAt
                --left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_FlagDefect
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                --left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_ReasonDeviation
                --left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_FlagParameter
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_NumberTrack
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_HeadTrack
                left join DataPropVal v17 on d17.id=v17.dataprop
            where ${whe}
        """, map)
        //
        StoreIndex indWorkPlan = stInspection.getIndex("objWorkPlan")
        if (stInspection.size() != stPlan.size()) {
            for (StoreRecord r in stPlan) {
                StoreRecord rPlan = indWorkPlan.get(r.getLong("id"))
                if (rPlan != null)
                    continue
                Map<String, Object> mapIns = r.getValues()
                mapIns.putAll(params)
                mapIns.put("name", r.getString("id") + "-" + r.getString("PlanDateEnd"))
                mapIns.put("objWorkPlan", r.getLong("id"))
                mapIns.put("FactDateEnd", r.getString("PlanDateEnd"))
                mapIns.put("NumberTrack", r.getLong("nomer_mdk"))
                mapIns.put("HeadTrack", r.getString("avtor"))
                mapIns.remove("id")
                mapIns.remove("cls")
                mapIns.remove("beg")
                mapIns.remove("end")
                dataDao.saveInspection("ins", mapIns)
                //Результаты saveInspection надо добавить stInspection
            }
        }
        //4 Журнал событий и запросов (incidentdata)
        long relobjComponentParams = 2525// Оценка состояния жд пути, балл
        String wheV8 = ""
        if (domain != "_ball") {
            whe = "o.cls=${mapCls.get("Cls_IncidentParameter")}"
            Map<String, Long> mapFV = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_StatusEliminated", "")
            long pv = apiMeta().get(ApiMeta).idPV("FactorVal", mapFV.get("FV_StatusEliminated"), "Prop_Status")
            String wheV6 = "and v6.propVal not in (${pv})"
            Store stIncident = apiIncidentData().get(ApiIncidentData).loadSqlWithParams("""
                select o.id, o.cls,
                    --v1.propVal as pvEvent, v1.obj as objEvent,
                    v2.propVal as pvObject, v2.obj as objObject,
                    --v3.propVal as pvUser, v3.obj as objUser,
                    v4.propVal as pvParameterLog, v4.obj as objParameterLog,
                    --v5.propVal as pvFault, v5.obj as objFault,
                    v6.propVal as pvStatus,
                    --v7.propVal as pvCriticality,
                    v8.numberVal as StartKm,
                    v9.numberVal as FinishKm,
                    v10.numberVal as StartPicket,
                    v11.numberVal as FinishPicket,
                    v12.numberVal as StartLink,
                    v13.numberVal as FinishLink,
                    --v14.multiStrVal as Description,
                    --v15.dateTimeVal as CreatedAt,
                    --v16.dateTimeVal as UpdatedAt,
                    v17.dateTimeVal as RegistrationDateTime,
                    --v18.strVal as InfoApplicant,
                    v19.propVal as pvLocationClsSection, v19.obj as objLocationClsSection
                    --21.dateTimeVal as AssignDateTime
                from Obj o
                    --left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Event
                    --left join DataPropVal v1 on d1.id=v1.dataprop
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Object
                    left join DataPropVal v2 on d2.id=v2.dataprop
                    --left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                    --left join DataPropVal v3 on d3.id=v3.dataprop
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_ParameterLog
                    left join DataPropVal v4 on d4.id=v4.dataprop
                    --left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Fault
                    --left join DataPropVal v5 on d5.id=v5.dataprop
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_Status
                    inner join DataPropVal v6 on d6.id=v6.dataprop ${wheV6}
                    --left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_Criticality
                    --left join DataPropVal v7 on d7.id=v7.dataprop
                    left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_StartKm
                    left join DataPropVal v8 on d8.id=v8.dataprop
                    left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_FinishKm
                    left join DataPropVal v9 on d9.id=v9.dataprop
                    left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_StartPicket
                    left join DataPropVal v10 on d10.id=v10.dataprop
                    left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_FinishPicket
                    left join DataPropVal v11 on d11.id=v11.dataprop
                    left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                    left join DataPropVal v12 on d12.id=v12.dataprop
                    left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                    left join DataPropVal v13 on d13.id=v13.dataprop
                    --left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Description
                    --left join DataPropVal v14 on d14.id=v14.dataprop
                    --left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_CreatedAt
                    --left join DataPropVal v15 on d15.id=v15.dataprop
                    --left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_UpdatedAt
                    --left join DataPropVal v16 on d16.id=v16.dataprop
                    left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_RegistrationDateTime
                    left join DataPropVal v17 on d17.id=v17.dataprop
                    --left join DataProp d18 on d18.objorrelobj=o.id and d18.prop=:Prop_InfoApplicant
                    --left join DataPropVal v18 on d18.id=v18.dataprop
                    left join DataProp d19 on d19.objorrelobj=o.id and d19.prop=:Prop_LocationClsSection
                    left join DataPropVal v19 on d19.id=v19.dataprop
                    --left join DataProp d21 on d21.objorrelobj=o.id and d21.prop=:Prop_AssignDateTime
                    --left join DataPropVal v21 on d21.id=v21.dataprop
                where ${whe}
            """, map, "")
            //
            Set<Object> idsParameterLog = stIncident.getUniqueValues("objParameterLog")
//            whe = "o.id in (0${idsParameterLog.join(",")})"
//            wheV2 = "left join DataPropVal v2 on d2.id=v2.dataprop"
//            wheV8 = "left join DataPropVal v8 on d8.id=v8.dataprop"
            whe = "o.cls=${mapCls.get("Cls_ParameterLog")}"
            Set<Object> idsInspection = stInspection.getUniqueValues("id")
            wheV2 = "inner join DataPropVal v2 on d2.id=v2.dataprop and v2.obj in (0${idsInspection.join(",")})"
            wheV8 = "inner join DataPropVal v8 on d8.id=v8.dataprop and v8.relobj in (0${idsRelobjComponentParams.join(",")})"
        } else {
            whe = "o.cls=${mapCls.get("Cls_ParameterLog")}"
            Set<Object> idsInspection = stInspection.getUniqueValues("id")
            wheV2 = "inner join DataPropVal v2 on d2.id=v2.dataprop and v2.obj in (0${idsInspection.join(",")})"
            wheV8 = "inner join DataPropVal v8 on d8.id=v8.dataprop and v8.relobj=${relobjComponentParams}"
        }
        //5 Журнал параметров
        Store stParameterLog = mdb.createStore("Obj.ParameterLog")
        mdb.loadQuery(stParameterLog, """
            select o.id, o.cls,
                v3.numberVal * 1000 + v22.numberVal as beg,
                v1.propVal as pvLocationClsSection, v1.obj as objLocationClsSection,
                v2.propVal as pvInspection, v2.obj as objInspection, 
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as CreationDateTime,
                v8.propVal as pvComponentParams, v8.relobj as relobjComponentParams,
                --v10.propVal as pvOutOfNorm,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink,
                --v14.multiStrVal as Description,
                v15.numberVal as ParamsLimit,
                v16.numberVal as ParamsLimitMax,
                v17.numberVal as ParamsLimitMin,
                --v18.propVal as pvUser, v18.obj as objUser,
                --v19.id as idCreatedAt, v19.dateTimeVal as CreatedAt,
                --v20.id as idUpdatedAt, v20.dateTimeVal as UpdatedAt,
                v21.numberVal as NumberRetreat,
                v22.numberVal as StartMeter,
                v23.numberVal as LengthRetreat,
                v24.numberVal as DepthRetreat,
                v25.numberVal as DegreeRetreat
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Inspection
                ${wheV2}
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_ComponentParams
                ${wheV8}
                --left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_OutOfNorm
                --left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                --left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Description
                --left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_ParamsLimit
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_ParamsLimitMax
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_ParamsLimitMin
                left join DataPropVal v17 on d17.id=v17.dataprop
                --left join DataProp d18 on d18.objorrelobj=o.id and d18.prop=:Prop_User
                --left join DataPropVal v18 on d18.id=v18.dataprop
                --left join DataProp d19 on d19.objorrelobj=o.id and d19.prop=:Prop_CreatedAt
                --left join DataPropVal v19 on d19.id=v19.dataprop
                --left join DataProp d20 on d20.objorrelobj=o.id and d20.prop=:Prop_UpdatedAt
                --left join DataPropVal v20 on d20.id=v20.dataprop
                left join DataProp d21 on d21.objorrelobj=o.id and d21.prop=:Prop_NumberRetreat
                left join DataPropVal v21 on d21.id=v21.dataprop
                left join DataProp d22 on d22.objorrelobj=o.id and d22.prop=:Prop_StartMeter
                left join DataPropVal v22 on d22.id=v22.dataprop
                left join DataProp d23 on d23.objorrelobj=o.id and d23.prop=:Prop_LengthRetreat
                left join DataPropVal v23 on d23.id=v23.dataprop
                left join DataProp d24 on d24.objorrelobj=o.id and d24.prop=:Prop_DepthRetreat
                left join DataPropVal v24 on d24.id=v24.dataprop
                left join DataProp d25 on d25.objorrelobj=o.id and d25.prop=:Prop_DegreeRetreat
                left join DataPropVal v25 on d25.id=v25.dataprop
            where ${whe}
            order by o.id
        """, map)
        //
        mdb.outTable(stParameterLog)
        Map<String, Long> mapRelCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_ParamsComponent", "")
        long pvComponentParams = apiMeta().get(ApiMeta).idPV("relcls", mapRelCls.get("RC_ParamsComponent"), "Prop_ComponentParams")
        if (domain == "_ball") {
            StoreIndex indStartKm = stParameterLog.getIndex("StartKm")
            bool = false
            for (StoreRecord r1 in st) {
                if (indStartKm.get(r1.getLong("km") + 1) != null)
                    continue
                for (StoreRecord r2 in stInspection) {
                    Long beg = r2.getLong("StartKm") * 1000 + (r2.getLong("StartPicket") - 1) * 100 + r2.getLong("StartLink") * 25
                    Long end = r2.getLong("FinishKm") * 1000 + (r2.getLong("FinishPicket") - 1) * 100 + r2.getLong("FinishLink") * 25
                    if (beg <= (r1.getLong("km") + 1) * 1000 && (r1.getLong("km") + 1) * 1000 <= end) {
                        Map<String, Object> mapIns = new HashMap<>(params)
                        mapIns.put("name", r2.getString("id") + "-" + r1.getString("date_obn"))
                        mapIns.put("relobjComponentParams", relobjComponentParams)
                        mapIns.put("pvComponentParams", pvComponentParams)
                        mapIns.put("objInspection", r2.getLong("id"))
                        mapIns.put("pvLocationClsSection", r2.getLong("pvLocationClsSection"))
                        mapIns.put("objLocationClsSection", r2.getLong("objLocationClsSection"))
                        mapIns.put("StartKm", r1.getLong("km") + 1)
                        mapIns.put("FinishKm", r1.getLong("km") + 1)
                        mapIns.put("StartPicket", 1)
                        mapIns.put("FinishPicket", 1)
                        mapIns.put("StartLink", 1)
                        mapIns.put("FinishLink", 1)
                        mapIns.put("ParamsLimit", r1.getLong("ballkm"))
                        mapIns.put("NumberRetreat", r1.getLong("kol_ots"))
                        mapIns.put("ParamsLimitMax", 999)
                        mapIns.put("ParamsLimitMin", 0)
                        mapIns.put("CreationDateTime",  r1.getString("date_obn") + "T01:00:00.000")
                        dataDao.saveParameterLog("ins", mapIns)
                        //
                        bool = true
                        break
                    }
                }
                if (bool) {
                    bool = false
                    //continue
                }
                else {
                    errorImport = true
                    saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                            "Не найден запись в Журнале осмотров и проверок на ${r1.getLong('km') + 1} км")
                    throw new XError("Не найден запись в Журнале осмотров и проверок на ${r1.getLong('km') + 1} км")
                }
            }
        } else if (domain == "_otstup") {
            StoreIndex indStartKm = stParameterLog.getIndex("beg")
            bool = false
            for (StoreRecord r1 in st) {
                Long metrOts = (r1.getLong("km") + 1) * 1000 + r1.getLong("metr")
                if (indStartKm.get(metrOts) != null)
                    continue
                for (StoreRecord r2 in stInspection) {
                    Long beg = r2.getLong("StartKm") * 1000 + (r2.getLong("StartPicket") - 1) * 100 + r2.getLong("StartLink") * 25
                    Long end = r2.getLong("FinishKm") * 1000 + (r2.getLong("FinishPicket") - 1) * 100 + r2.getLong("FinishLink") * 25
                    if (beg <= metrOts && metrOts <= end) {
                        Map<String, Object> mapIns = new HashMap<>(params)
                        mapIns.put("name", r2.getString("id") + "-" + r1.getString("datetime_obn"))
                        mapIns.put("relobjComponentParams", indOtstup.get(r1.getString("kod_otstup")).get("id"))
                        mapIns.put("pvComponentParams", pvComponentParams)
                        mapIns.put("objInspection", r2.getLong("id"))
                        mapIns.put("pvLocationClsSection", r2.getLong("pvLocationClsSection"))
                        mapIns.put("objLocationClsSection", r2.getLong("objLocationClsSection"))
                        mapIns.put("StartKm", r1.getLong("km") + 1)
                        mapIns.put("FinishKm", r1.getLong("km") + 1)
                        mapIns.put("StartPicket", r1.getLong("pk") + 1)
                        mapIns.put("FinishPicket", r1.getLong("pk") + 1)
                        mapIns.put("StartLink", Math.ceil(((metrOts - (r1.getLong("km") + 1) * 1000) % 100) / 25))
                        mapIns.put("FinishLink", Math.ceil(((metrOts - (r1.getLong("km") + 1) * 1000 + r1.getLong("dlina_ots")) % 100) / 25))
                        mapIns.put("ParamsLimit", r1.getLong("velich_ots"))
                        mapIns.put("ParamsLimitMax", 0)
                        mapIns.put("ParamsLimitMin", 0)
                        mapIns.put("NumberRetreat", r1.getLong("kol_ots"))
                        mapIns.put("StartMeter", r1.getLong("metr"))
                        mapIns.put("LengthRetreat", r1.getLong("dlina_ots"))
                        mapIns.put("DepthRetreat", r1.getLong("glub_ots"))
                        mapIns.put("DegreeRetreat", r1.getLong("stepen_ots"))
                        mapIns.put("CreationDateTime",  r1.getString("datetime_obn"))
                        mapIns.put("Description",  "Место - " + (r1.getLong("km") + 1) + " км " + r1.getLong("metr") +
                                " метр; длина - " + r1.getLong("dlina_ots") + "; глубина - " +
                                r1.getLong("glub_ots") + "; степень - " + r1.getLong("stepen_ots"))
                        mapIns.put("nameLocation",  "ПС №" + r1.getString("nomer_mdk"))
                        mapIns.put("fullNameUser",  r1.getString("avtor"))
                        dataDao.saveParameterLog("ins", mapIns)
                        //
                        bool = true
                        break
                    }
                }
                if (bool) {
                    bool = false
                    //continue
                } else {
                    errorImport = true
                    saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                            "Не найден запись в Журнале осмотров и проверок на ${r1.getLong('km') + 1} км")
                    throw new XError("Не найден запись в Журнале осмотров и проверок на ${r1.getLong('km') + 1} км")
                }
            }
        }
        //
        Long test = 0

        //
    }

    void assignPropDefault(String domain) {
        Map<String, String> mapCoding = new HashMap<>()
        if (domain == "_ball") {
            //
            mapCoding.put("date_obn", "Prop_FactDateEnd")
            mapCoding.put("nomer_mdk", "Prop_NumberTrack")
            mapCoding.put("avtor", "Prop_HeadTrack")
            mapCoding.put("km", "Prop_StartKm")
            mapCoding.put("pk", "Prop_StartPicket")
            mapCoding.put("ballkm", "Prop_ParamsLimit")
            mapCoding.put("kol_ots", "Prop_NumberRetreat")
            //
            long idSysCoding = 1000
            Store stTmp = apiMeta().get(ApiMeta).loadSql("""
                select sc.id, sc.cod, scc.syscod, scc.cod as codOther
                from syscoding sc
                    left join syscodingcod scc on scc.syscoding=sc.id
                where sc.id=${idSysCoding} and scc.cod in (${"'" + mapCoding.keySet().join("','") + "'"})
            """, "")
            Set<Object> codsOther = stTmp.getUniqueValues("codOther")
            Store stSysCod = apiMeta().get(ApiMeta).loadSql("""
                select id, cod from SysCod where cod in (${"'" + mapCoding.values().join("','") + "'"})
            """, "")
            StoreIndex indSysCod = stSysCod.getIndex("cod")
            for (Map.Entry entry : mapCoding) {
                if (!codsOther.contains(entry.key)) {
                    StoreRecord rec = mdb.createStoreRecord("SysCodingCod")
                    StoreRecord recInd = indSysCod.get(entry.value.toString())
                    if (recInd != null)
                        rec.set("syscod", recInd.getLong("id"))
                    rec.set("sysCoding", idSysCoding)
                    rec.set("cod", entry.key)
                    apiMeta().get(ApiMeta).insertRecToTable("SysCodingCod", rec.getValues(), true)
                }
            }
        }
    }


    void parseOtstup(File inputFile) {
        String filename = infoFile.get("filename")

        System.out.println("Импорт файла: ${filename}")

        try {
            mdb.startTran()
            mdb.execQuery("delete from _otstup")
            //
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")
            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String ind = rowElement.getElementsByTagName("REC").item(0).getTextContent()
                    String kod_otstup = rowElement.getElementsByTagName("kod_otstup").item(0).getTextContent()

                    String kod_napr_s = rowElement.getElementsByTagName("kod_napr").item(0).getTextContent()
                    Long kod_napr = null
                    if (!kod_napr_s.isEmpty())
                        kod_napr = UtCnv.toLong(kod_napr_s)

                    String prizn_most_s = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
                    Long prizn_most = null
                    if (!prizn_most_s.isEmpty())
                        prizn_most = UtCnv.toLong(prizn_most)
                    //
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    String time_obn = rowElement.getElementsByTagName("time_obn").item(0).getTextContent()
                    String y = date_obn.substring(6)
                    String m = date_obn.substring(3, 5)
                    String d = date_obn.substring(0, 2)
                    String hh = time_obn.substring(0, 2)
                    String mm = time_obn.substring(3, 5)
                    String ss = time_obn.substring(6)
                    String dte = XDateTime.create(UtCnv.toInt(y), UtCnv.toInt(m), UtCnv.toInt(d),
                            UtCnv.toInt(hh), UtCnv.toInt(mm), UtCnv.toInt(ss))
                    //
                    String nomer_mdk = rowElement.getElementsByTagName("nomer_mdk").item(0).getTextContent()
                    String avtor = rowElement.getElementsByTagName("avtor").item(0).getTextContent()
                    //
                    String km_s = UtCnv.toLong(rowElement.getElementsByTagName("km").item(0).getTextContent())
                    Long km = null
                    if (!km_s.isEmpty())
                        km = UtCnv.toLong(km_s)

                    String pk_s = UtCnv.toLong(rowElement.getElementsByTagName("pk").item(0).getTextContent())
                    Long pk = null
                    if (!pk_s.isEmpty())
                        pk = UtCnv.toLong(pk_s)

                    String metr_s = rowElement.getElementsByTagName("metr").item(0).getTextContent()
                    Long metr = null
                    if (!metr_s.isEmpty())
                        metr = UtCnv.toLong(metr_s)

                    String dlina_ots_s = rowElement.getElementsByTagName("dlina_ots").item(0).getTextContent()
                    Long dlina_ots = null
                    if (!dlina_ots_s.isEmpty())
                        dlina_ots = UtCnv.toLong(dlina_ots_s)

                    String velich_ots_s = rowElement.getElementsByTagName("velich_ots").item(0).getTextContent()
                    Long velich_ots = null
                    if (!velich_ots_s.isEmpty())
                        velich_ots = UtCnv.toLong(velich_ots_s)

                    String glub_ots_s = rowElement.getElementsByTagName("glub_ots").item(0).getTextContent()
                    Long glub_ots = null
                    if (!glub_ots_s.isEmpty())
                        glub_ots = UtCnv.toLong(glub_ots_s)

                    String stepen_ots_s = rowElement.getElementsByTagName("stepen_ots").item(0).getTextContent()
                    Long stepen_ots = null
                    if (!stepen_ots_s.isEmpty())
                        stepen_ots = UtCnv.toLong(stepen_ots_s)

                    String kol_ots_s = UtCnv.toLong(rowElement.getElementsByTagName("kol_ots").item(0).getTextContent())
                    Long kol_ots = null
                    if (!kol_ots_s.isEmpty())
                        kol_ots = UtCnv.toLong(kol_ots_s)
                    //
                    mdb.execQueryNative("""
                        INSERT INTO _otstup (rec,kod_otstup,kod_napr,prizn_most,datetime_obn,nomer_mdk,avtor,km,pk,metr,dlina_ots,velich_ots,glub_ots,stepen_ots,kol_ots)
                        VALUES ($ind,$kod_otstup,$kod_napr,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$metr, $dlina_ots,$velich_ots,$glub_ots,$stepen_ots,$kol_ots);
                    """)
                }
            }
/*            mdb.execQueryNative("""
                INSERT INTO public._log (filename, datetime_create, filled, datetime_fill)
                VALUES('${filename}', '${XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)}', 0, null);
            """)*/
            datetime_create = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        } catch (Exception e) {
            errorImport = true
            e.printStackTrace()
            mdb.rollback()
        } finally {
            if (!errorImport)
                mdb.commit()
            //Store st = mdb.loadQuery("select * from _otstup where 0=0")
            //mdb.outTable(st)
        }
    }

    void parseBall(File inputFile) {
        String filename = UtCnv.toString(infoFile.get("filename"))

        System.out.println("Импорт файла: ${filename}")

        try {
            mdb.startTran()
            mdb.execQuery("delete from _ball")
            //
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance()
            DocumentBuilder builder = factory.newDocumentBuilder()
            Document doc = builder.parse(inputFile)
            doc.getDocumentElement().normalize()
            System.out.println("Root element: " + doc.getDocumentElement().getNodeName())
            NodeList rowList = doc.getElementsByTagName("ROW")

            for (int i = 0; i < rowList.getLength(); i++) {
                Node rowNode = rowList.item(i)
                if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element rowElement = (Element) rowNode
                    String ind = UtCnv.toLong(rowElement.getElementsByTagName("REC").item(0).getTextContent())
                    String kod_napr_s = UtCnv.toLong(rowElement.getElementsByTagName("kod_napr").item(0).getTextContent())
                    Long kod_napr = null
                    if (!kod_napr_s.isEmpty())
                        kod_napr = UtCnv.toLong(kod_napr_s)


                    String prizn_most_s = rowElement.getElementsByTagName("prizn_most").item(0).getTextContent()
                    Long prizn_most = null
                    if (!prizn_most_s.isEmpty())
                        prizn_most = UtCnv.toLong(prizn_most)
                    String date_obn = rowElement.getElementsByTagName("date_obn").item(0).getTextContent()
                    //
                    String y = date_obn.substring(6)
                    String m = date_obn.substring(3, 5)
                    String d = date_obn.substring(0, 2)
                    String dte = XDate.create(UtCnv.toInt(y), UtCnv.toInt(m), UtCnv.toInt(d))
                    //
                    String nomer_mdk = rowElement.getElementsByTagName("nomer_mdk").item(0).getTextContent()
                    String avtor = rowElement.getElementsByTagName("avtor").item(0).getTextContent()
                    String km_s = UtCnv.toLong(rowElement.getElementsByTagName("km").item(0).getTextContent())
                    Long km = null
                    if (!km_s.isEmpty())
                        km = UtCnv.toLong(km_s)

                    String pk_s = UtCnv.toLong(rowElement.getElementsByTagName("pk").item(0).getTextContent())
                    Long pk = null
                    if (!pk_s.isEmpty())
                        pk = UtCnv.toLong(pk_s)

                    String ballkm_s = UtCnv.toLong(rowElement.getElementsByTagName("ballkm").item(0).getTextContent())
                    Long ballkm = null
                    if (!ballkm_s.isEmpty())
                        ballkm = UtCnv.toLong(ballkm_s)

                    String kol_ots_s = UtCnv.toLong(rowElement.getElementsByTagName("kol_ots").item(0).getTextContent())
                    Long kol_ots = null
                    if (!kol_ots_s.isEmpty())
                        kol_ots = UtCnv.toLong(kol_ots_s)
                    //
                    mdb.execQueryNative("""
                        INSERT INTO _ball (rec,kod_napr,prizn_most,date_obn,nomer_mdk,avtor,km,pk,ballkm,kol_ots)
                        VALUES ($ind,$kod_napr,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$ballkm,$kol_ots);
                    """)
                }
            }
/*            mdb.execQueryNative("""
                INSERT INTO public._log (filename, datetime_create, filled, datetime_fill)
                VALUES('${filename}', '${XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)}', 0, null);
            """)*/
            datetime_create = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        } catch (Exception e) {
            errorImport = true
            e.printStackTrace()
            mdb.rollback()
        } finally {
            if (!errorImport)
                mdb.commit()
            //Store st = mdb.loadQuery("select * from _ball where 0=0")
            //mdb.outTable(st)
        }

    }


}
