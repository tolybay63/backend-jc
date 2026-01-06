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
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

import javax.xml.parsers.DocumentBuilder
import javax.xml.parsers.DocumentBuilderFactory

class ImportXmlDao extends BaseMdbUtils {
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
    Store analyze(File file, Map<String, Object> params) {
        Store store = null
        try {
            String filename = UtCnv.toString(params.get("filename"))
            infoFile.put("filename", filename)
            Store stLog = mdb.loadQuery("select * from _log where filename like '${filename}'")
            boolean fileLoaded = false
            if (stLog.size()>0 && stLog.get(0).getInt("filled")==0)
                fileLoaded = true

            if (filename.startsWith("G")) {
                store = parseOtstup(file)
                if (!fileLoaded) {
                    assignPropDefault("Otstup")
                }
                check("Otstup", store)
            } else if (filename.startsWith("B")) {
                store = parseBall(file)
                if (!fileLoaded) {
                    assignPropDefault("Ball")
                }
                check("Ball", store)
            }
        } finally {
            if (!errorImport)
                saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null, '')
        }
        return store
    }

    void check(String domain, Store st) {
        if (st.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null, 'Файл пустой')
            return
        }
        // Проверка привязки
        Set<String> cods = new HashSet<>()

        Set<Object> setNapr = st.getUniqueValues("kod_napr")
        for (Object o : setNapr) {
            String cod = "kod_napr_"+UtCnv.toString(o)
            cods.add(cod)
        }
        Store stCod = apiObjectData().get(ApiObjectData).loadSql ("""
            select * from SysCodingCod where cod like 'kod_napr_%'
        """, "")

        if (domain == "Otstup") {
            Set<Object> setOtstup = st.getUniqueValues("kod_otstup")
            for (Object o : setOtstup) {
                String cod = "kod_otstup_"+UtCnv.toString(o)
                cods.add(cod)
            }

            Store stOts = apiNSIData().get(ApiNSIData).loadSql("""
                select * from SysCodingCod where cod like 'kod_otstup_%'
            """, "")

            stCod.add(stOts)
        }
        //
        Set<Object> codsOther = stCod.getUniqueValues("cod")
        Set<Object> codsErr = new HashSet<>()
        for (String cod : cods) {
            if (!codsOther.contains(cod))
                codsErr.add(cod)
        }
        //
        if (codsErr.size() > 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create, 0, null, "Нет привязки [" + codsErr.join(", ") + "]")
            return
        }
        // Далее проверяем данные в системе
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        // Находим направления
        Set<Object> idsNapr = new HashSet<>()
        Set<Object> kodsNapr = st.getUniqueValues("kod_napr")
        Store stNapr = apiObjectData().get(ApiObjectData).loadSql("""
            select s.cod, c.entityid as id from syscod c, syscodingcod s
            where c.id=s.syscod and c.entitytype=1 
                and s.syscoding=1001 and s.cod like 'kod_napr_%'
        """, "")
        StoreIndex indNapr = stNapr.getIndex("cod")
        for (cod in kodsNapr) {
            StoreRecord r = indNapr.get(UtCnv.toString("kod_napr_" + cod))
            if (r != null)
                idsNapr.add(r.getLong("id"))
        }
        // Находим участки направления
        Store stSection = apiObjectData().get(ApiObjectData).loadSql("""
            select o.id from Obj o, ObjVer v 
            where o.id=v.ownerver and v.lastver=1 and v.objparent in (0${idsNapr.join(",")})
                and o.cls in (0${mapCls.get("Cls_Station")}, 0${mapCls.get("Cls_Stage")})
        """, "")
        if (stSection.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    "Не найден Раздельный пункт и/или Перегон по направлению")
            return
        }
        // Находим обслуживаемые объекты
        Set<Object> idsSection = stSection.getUniqueValues("id")
        Store stObject = apiObjectData().get(ApiObjectData).loadSql("""
            select o.id 
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj in (0${idsSection.join(",")})
            where cls in (0${mapCls.get("Cls_RailwayStage")}, 0${mapCls.get("Cls_RailwayStation")})
        """, "")
        if (stObject.size() == 0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null,
                    "Не найден обслуживаемый объект по направлению")
            return
        }
        Set<Object> idsObject = stObject.getUniqueValues("id")
        // Находим параметр балл или отступления
        Set<Object> idsRelobjComponentParams = new HashSet<>()
        if (domain == "Otstup") {
            Set<Object> kodsOtstup = st.getUniqueValues("kod_otstup")
            Store stOtstup = apiNSIData().get(ApiNSIData).loadSql("""
                select s.cod, c.entityid as id from syscod c, syscodingcod s
                where c.id=s.syscod and c.entitytype=2 
                    and s.syscoding=1001 and s.cod like 'kod_otstup_%'
            """, "")
            StoreIndex indOtstup = stOtstup.getIndex("cod")
            for (cod in kodsOtstup) {
                StoreRecord r = indOtstup.get(UtCnv.toString("kod_otstup_" + cod))
                if (r != null)
                    idsRelobjComponentParams.add(r.getLong("id"))
            }
        } else {
            long ro = apiNSIData().get(ApiNSIData).loadSql("""
                select id from RelObj
                where cod like 'RelObj_Ball'
            """, "").get(0).getLong("id")
            idsRelobjComponentParams.add(ro)// Оценка состояния жд пути, балл - 2525
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
            return
        }
        // План работ (plandata)
        String whe = "o.cls=${mapCls.get("Cls_WorkPlanInspection")}"
        String wheV2 = "and v2.obj in (0${idsObject.join(",")})"
        String wheV7
        if (domain == "Ball")
            wheV7 = "and v7.dateTimeVal='${st.get(0).getString("date_obn")}'"
        else
            wheV7 = "and v7.dateTimeVal='${st.get(0).getString("datetime_obn").split("T")[0]}'"
        String wheV11 = "and v11.obj=${stWork.get(0).getLong("id")}"
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
                v11.propVal as pvWork, v11.obj as objWork,
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
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_Work
                inner join DataPropVal v11 on d11.id=v11.dataprop ${wheV11}
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
            return
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
                return
            }
        }
        Set<Object> idsPlan = stPlan.getUniqueValues("id")
        // Журнал осмотров и проверок
        Store stInspection = mdb.createStore("Obj.inspection")
        whe = "o.cls=${mapCls.get("Cls_Inspection")}"
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
                v11.propVal as pvFlagDefect, null as fvFlagDefect,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink,
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
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_FlagDefect
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_FlagParameter
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_NumberTrack
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_HeadTrack
                left join DataPropVal v17 on d17.id=v17.dataprop
            where ${whe}
        """, map)
        Set<Object> idsInspection = stInspection.getUniqueValues("id")
        // Журнал параметров
        Store stParameterLog = mdb.createStore("Obj.ParameterLog")
        Store stParameterLog2 = mdb.createStore("Obj.ParameterLog")
        if (stInspection.size() != 0) {
            whe = "o.cls=${mapCls.get("Cls_ParameterLog")}"
            wheV2 = "and v2.obj in (0${idsInspection.join(",")})"
            String wheV8 = "and v8.relobj in (0${idsRelobjComponentParams.join(",")})"
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
                    v12.numberVal as StartLink,
                    v13.numberVal as FinishLink,
                    v15.numberVal as ParamsLimit,
                    v16.numberVal as ParamsLimitMax,
                    v17.numberVal as ParamsLimitMin,
                    v21.numberVal as NumberRetreat,
                    v22.numberVal as StartMeter,
                    v23.numberVal as LengthRetreat,
                    v24.numberVal as DepthRetreat,
                    v25.numberVal as DegreeRetreat
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                    left join DataPropVal v1 on d1.id=v1.dataprop and v1.inputtype=3
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Inspection
                    inner join DataPropVal v2 on d2.id=v2.dataprop and v2.inputtype=3 ${wheV2}
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                    left join DataPropVal v3 on d3.id=v3.dataprop and v3.inputtype=3
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                    left join DataPropVal v4 on d4.id=v4.dataprop and v4.inputtype=3
                    left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                    left join DataPropVal v5 on d5.id=v5.dataprop and v5.inputtype=3
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                    left join DataPropVal v6 on d6.id=v6.dataprop and v6.inputtype=3
                    left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime
                    left join DataPropVal v7 on d7.id=v7.dataprop and v7.inputtype=3
                    left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_ComponentParams
                    inner join DataPropVal v8 on d8.id=v8.dataprop and v8.inputtype=3 ${wheV8}
                    left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                    left join DataPropVal v12 on d12.id=v12.dataprop and v12.inputtype=3
                    left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                    left join DataPropVal v13 on d13.id=v13.dataprop and v13.inputtype=3
                    left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_ParamsLimit
                    left join DataPropVal v15 on d15.id=v15.dataprop and v15.inputtype=3
                    left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_ParamsLimitMax
                    left join DataPropVal v16 on d16.id=v16.dataprop and v16.inputtype=3
                    left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_ParamsLimitMin
                    left join DataPropVal v17 on d17.id=v17.dataprop and v17.inputtype=3
                    left join DataProp d21 on d21.objorrelobj=o.id and d21.prop=:Prop_NumberRetreat
                    left join DataPropVal v21 on d21.id=v21.dataprop and v21.inputtype=3
                    left join DataProp d22 on d22.objorrelobj=o.id and d22.prop=:Prop_StartMeter
                    left join DataPropVal v22 on d22.id=v22.dataprop and v22.inputtype=3
                    left join DataProp d23 on d23.objorrelobj=o.id and d23.prop=:Prop_LengthRetreat
                    left join DataPropVal v23 on d23.id=v23.dataprop and v23.inputtype=3
                    left join DataProp d24 on d24.objorrelobj=o.id and d24.prop=:Prop_DepthRetreat
                    left join DataPropVal v24 on d24.id=v24.dataprop and v24.inputtype=3
                    left join DataProp d25 on d25.objorrelobj=o.id and d25.prop=:Prop_DegreeRetreat
                    left join DataPropVal v25 on d25.id=v25.dataprop and v25.inputtype=3
                where ${whe}
                order by o.id
            """, map)
            //mdb.outTable(stParameterLog)
        }
        // Журнал событий и запросов (incidentdata)
        if (domain != "Ball") {
            whe = "o.cls=${mapCls.get("Cls_IncidentParameter")}"
            Map<String, Long> mapFV = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_StatusEliminated", "")
            long pv = apiMeta().get(ApiMeta).idPV("FactorVal", mapFV.get("FV_StatusEliminated"), "Prop_Status")
            String wheV6 = "and v6.propVal not in (${pv})"
            Store stIncident = apiIncidentData().get(ApiIncidentData).loadSqlWithParams("""
                select o.id,
                    v1.propVal as pvParameterLog, v1.obj as objParameterLog,
                    v6.propVal as pvStatus
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ParameterLog
                    left join DataPropVal v1 on d1.id=v1.dataprop
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_Status
                    inner join DataPropVal v6 on d6.id=v6.dataprop ${wheV6}
                where ${whe}
            """, map, "")
            //
            Set<Object> idsParameterLog = stIncident.getUniqueValues("objParameterLog")
            if (stIncident.size() > 0) {
                whe = "o.id in (0${idsParameterLog.join(",")})"
                String wheV8 = "and v8.relobj in (0${idsRelobjComponentParams.join(",")})"
                mdb.loadQuery(stParameterLog2, """
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
                    v12.numberVal as StartLink,
                    v13.numberVal as FinishLink,
                    v15.numberVal as ParamsLimit,
                    v16.numberVal as ParamsLimitMax,
                    v17.numberVal as ParamsLimitMin,
                    v21.numberVal as NumberRetreat,
                    v22.numberVal as StartMeter,
                    v23.numberVal as LengthRetreat,
                    v24.numberVal as DepthRetreat,
                    v25.numberVal as DegreeRetreat
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                    left join DataPropVal v1 on d1.id=v1.dataprop and v1.inputtype=3
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Inspection
                    left join DataPropVal v2 on d2.id=v2.dataprop and v2.inputtype=3
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                    left join DataPropVal v3 on d3.id=v3.dataprop and v3.inputtype=3
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                    left join DataPropVal v4 on d4.id=v4.dataprop and v4.inputtype=3
                    left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                    left join DataPropVal v5 on d5.id=v5.dataprop and v5.inputtype=3
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                    left join DataPropVal v6 on d6.id=v6.dataprop and v6.inputtype=3
                    left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime
                    left join DataPropVal v7 on d7.id=v7.dataprop and v7.inputtype=3
                    left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_ComponentParams
                    inner join DataPropVal v8 on d8.id=v8.dataprop and v8.inputtype=3 ${wheV8}
                    left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                    left join DataPropVal v12 on d12.id=v12.dataprop and v12.inputtype=3
                    left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                    left join DataPropVal v13 on d13.id=v13.dataprop and v13.inputtype=3
                    left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_ParamsLimit
                    left join DataPropVal v15 on d15.id=v15.dataprop and v15.inputtype=3
                    left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_ParamsLimitMax
                    left join DataPropVal v16 on d16.id=v16.dataprop and v16.inputtype=3
                    left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_ParamsLimitMin
                    left join DataPropVal v17 on d17.id=v17.dataprop and v17.inputtype=3
                    left join DataProp d21 on d21.objorrelobj=o.id and d21.prop=:Prop_NumberRetreat
                    left join DataPropVal v21 on d21.id=v21.dataprop and v21.inputtype=3
                    left join DataProp d22 on d22.objorrelobj=o.id and d22.prop=:Prop_StartMeter
                    left join DataPropVal v22 on d22.id=v22.dataprop and v22.inputtype=3
                    left join DataProp d23 on d23.objorrelobj=o.id and d23.prop=:Prop_LengthRetreat
                    left join DataPropVal v23 on d23.id=v23.dataprop and v23.inputtype=3
                    left join DataProp d24 on d24.objorrelobj=o.id and d24.prop=:Prop_DepthRetreat
                    left join DataPropVal v24 on d24.id=v24.dataprop and v24.inputtype=3
                    left join DataProp d25 on d25.objorrelobj=o.id and d25.prop=:Prop_DegreeRetreat
                    left join DataPropVal v25 on d25.id=v25.dataprop and v25.inputtype=3
                where ${whe}
                order by o.id
            """, map)
            }
        }
        //
        if (domain == "Ball") {
            StoreIndex indStartKm = stParameterLog.getIndex("StartKm")
            StoreIndex indStartKm2 = stParameterLog2.getIndex("StartKm")
            for (StoreRecord r1 in st) {
                if (indStartKm.get(r1.getLong("km") + 1) != null || indStartKm2.get(r1.getLong("km") + 1) != null)
                    r1.set("import", 1)
                else
                    r1.set("import", 0)
            }
        } else if (domain == "Otstup") {
            StoreIndex indStartKm = stParameterLog.getIndex("beg")
            StoreIndex indStartKm2 = stParameterLog2.getIndex("beg")
            for (StoreRecord r1 in st) {
                Long metrOts = (r1.getLong("km") + 1) * 1000 + r1.getLong("metr")
                if (indStartKm.get(metrOts) != null || indStartKm2.get(metrOts) != null)
                    r1.set("import", 1)
                else
                    r1.set("import", 0)
            }
        }
    }

    void assignPropDefault(String domain) {
        Map<String, String> mapCoding = new HashMap<>()
        if (domain == "Ball") {
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

    Store parseOtstup(File inputFile) {
        String filename = infoFile.get("filename")
        System.out.println("Импорт файла: ${filename}")

        Store st = mdb.createStore("Otstup")
        try {
            //mdb.startTran()
            //mdb.execQuery("delete from Otstup")
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
                    String kod_otstup_s = rowElement.getElementsByTagName("kod_otstup").item(0).getTextContent()
                    Long kod_otstup = null
                    if (!kod_otstup_s.isEmpty())
                        kod_otstup = UtCnv.toLong(kod_otstup_s)

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
                    String km_s = rowElement.getElementsByTagName("km").item(0).getTextContent()
                    Long km = null
                    if (!km_s.isEmpty())
                        km = UtCnv.toLong(km_s)

                    String pk_s = rowElement.getElementsByTagName("pk").item(0).getTextContent()
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

                    String kol_ots_s = rowElement.getElementsByTagName("kol_ots").item(0).getTextContent()
                    Long kol_ots = null
                    if (!kol_ots_s.isEmpty())
                        kol_ots = UtCnv.toLong(kol_ots_s)
                    //
                    StoreRecord rec = mdb.createStoreRecord("Otstup")
                    rec.set("rec", ind)
                    rec.set("kod_otstup", kod_otstup)
                    rec.set("kod_napr", kod_napr)
                    rec.set("prizn_most", prizn_most)
                    rec.set("datetime_obn", dte)
                    rec.set("nomer_mdk", nomer_mdk)
                    rec.set("avtor", avtor)
                    rec.set("km", km)
                    rec.set("pk", pk)
                    rec.set("metr", metr)
                    rec.set("dlina_ots", dlina_ots)
                    rec.set("velich_ots", velich_ots)
                    rec.set("glub_ots", glub_ots)
                    rec.set("stepen_ots", stepen_ots)
                    rec.set("kol_ots", kol_ots)
                    st.add(rec)

/*
                    mdb.execQueryNative("""
                        INSERT INTO Otstup (rec,kod_otstup,kod_napr,prizn_most,datetime_obn,nomer_mdk,avtor,km,pk, metr,dlina_ots,velich_ots,glub_ots,stepen_ots,kol_ots)
                        VALUES ($ind,$kod_otstup,$kod_napr,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$metr, $dlina_ots,$velich_ots,$glub_ots,$stepen_ots,$kol_ots);
                    """)
*/
                }
            }

        } catch (Exception e) {
            errorImport = true
            e.printStackTrace()
            //mdb.rollback()
        } finally {
            //if (!errorImport) mdb.commit()
            datetime_create = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        }
        if (st.size()==0) {
            errorImport = true
            saveLog(UtCnv.toString(infoFile.get("filename")), datetime_create,0, null, 'Файл пустой')
        }
        return st
    }

    Store parseBall(File inputFile) {
        String filename = UtCnv.toString(infoFile.get("filename"))
        System.out.println("Импорт файла: ${filename}")

        Store st = mdb.createStore("Ball")

        try {
            //mdb.startTran()
            //mdb.execQuery("delete from Ball")
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
                    String kod_napr_s = rowElement.getElementsByTagName("kod_napr").item(0).getTextContent()
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
                    String km_s = rowElement.getElementsByTagName("km").item(0).getTextContent()
                    Long km = null
                    if (!km_s.isEmpty())
                        km = UtCnv.toLong(km_s)

                    String pk_s = rowElement.getElementsByTagName("pk").item(0).getTextContent()
                    Long pk = null
                    if (!pk_s.isEmpty())
                        pk = UtCnv.toLong(pk_s)

                    String ballkm_s = rowElement.getElementsByTagName("ballkm").item(0).getTextContent()
                    Long ballkm = null
                    if (!ballkm_s.isEmpty())
                        ballkm = UtCnv.toLong(ballkm_s)

                    String kol_ots_s = rowElement.getElementsByTagName("kol_ots").item(0).getTextContent()
                    Long kol_ots = null
                    if (!kol_ots_s.isEmpty())
                        kol_ots = UtCnv.toLong(kol_ots_s)
                    //
                    StoreRecord rec = mdb.createStoreRecord("Ball")
                    rec.set("rec", ind)
                    rec.set("kod_napr", kod_napr)
                    rec.set("prizn_most", prizn_most)
                    rec.set("date_obn", dte)
                    rec.set("nomer_mdk", nomer_mdk)
                    rec.set("avtor", avtor)
                    rec.set("km", km)
                    rec.set("pk", pk)
                    rec.set("ballkm", ballkm)
                    rec.set("kol_ots", kol_ots)
                    st.add(rec)
/*
                    mdb.execQueryNative("""
                        INSERT INTO Ball (rec,kod_napr,prizn_most,date_obn,nomer_mdk,avtor,km,pk,ballkm,kol_ots)
                        VALUES ($ind,$kod_napr,$prizn_most,'$dte','$nomer_mdk','$avtor',$km,$pk,$ballkm,$kol_ots);
                    """)
*/
                }
            }
        } catch (Exception e) {
            errorImport = true
            e.printStackTrace()
        } finally {
            datetime_create = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        }
        return st
    }

    @DaoMethod
    Store loadAssign(String cods_err) {
        Set<String> cods = cods_err.split(", ")

        Store stObj = apiObjectData().get(ApiObjectData).loadSql("""
            select distinct s.entityid as id, sc.cod, s.cod || ' / ' ||v1.name as name, sc.syscoding
            from SysCodingCod sc
                left join SysCod s on sc.sysCod=s.id
                left join ObjVer v1 on s.entityType=1 and s.entityId=v1.ownerVer and v1.lastVer=1
            where sc.sysCoding=1001
        """, "")

        Store stRelObj = apiNSIData().get(ApiNSIData).loadSql("""
            select distinct s.entityid as id, sc.cod, s.cod || ' / ' ||v1.name as name, sc.syscoding
            from sysCodingCod sc
                left join SysCod s on sc.sysCod=s.id
                left join RelObjVer v1 on s.entityType=2 and s.entityId=v1.ownerVer and v1.lastVer=1
            where sc.sysCoding=1001
        """, "")

        stObj.add(stRelObj)
        Set<Object> setCods = stObj.getUniqueValues("cod")

        if (cods_err != "" && cods_err.contains("kod_")) {
            for (String cod in cods) {
                if (!setCods.contains(cod))
                    stObj.add([cod: cod])
            }
        }
        return stObj
    }

    @DaoMethod
    Store loadObjForSelect(String cod) {
        Store st = apiObjectData().get(ApiObjectData).loadSql("""
            select s.entityid as id, s.cod || ' / ' || v.name as name, 
                s.id as syscod, sc.id as syscodingcod
            from syscod s
                left join syscodingcod sc on s.id=sc.syscod and sc.cod like '${cod}'
                left join objVer v on s.entityid=v.ownerver and v.lastver=1
            where s.entitytype=1 and s.entityid in (
                select id
                from obj 
                where cls=1240
            ) 
        """, "")

        return st
    }

    @DaoMethod
    Store loadRelObjForSelect(String cod) {
        Store st = apiNSIData().get(ApiNSIData).loadSql("""
            select s.entityid as id, s.cod || ' / ' || v.name as name, 
                s.id as syscod, sc.id as syscodingcod            
            from syscod s
            left join syscodingcod sc on s.id=sc.syscod and sc.cod like '${cod}'
                left join RelobjVer v on s.entityid=v.ownerver and v.lastver=1
            where s.entitytype=2 and s.entityid in (
                select id
                from relobj 
                where relcls=1074
            )
        """, "")

        return st
    }

    @DaoMethod
    void saveAssign(Map<String, Object> params) {
        long sysCoding = 1001
        params.put("sysCoding", sysCoding)
        if (UtCnv.toString(params.get("cod")).startsWith("kod_napr")) {
            if (UtCnv.toLong(params.get("syscodingcod")) > 0) {
                params.put("id", UtCnv.toLong(params.get("syscodingcod")))
                apiObjectData().get(ApiObjectData).updateTable("SysCodingCod", params)
            } else {
                params.put("id", null)
                apiObjectData().get(ApiObjectData).insertTable("SysCodingCod", params)
            }
        } else if (UtCnv.toString(params.get("cod")).startsWith("kod_otstup")) {
            if (UtCnv.toLong(params.get("syscodingcod")) > 0) {
                params.put("id", UtCnv.toLong(params.get("syscodingcod")))
                apiNSIData().get(ApiNSIData).updateTable("SysCodingCod", params)
            } else {
                params.put("id", null)
                apiNSIData().get(ApiNSIData).insertTable("SysCodingCod", params)
            }
        } else {
            throw new XError("Неизвестный код")
        }
    }

    @DaoMethod
    String filldata(String domain) {

        return "Ok"
    }

}
