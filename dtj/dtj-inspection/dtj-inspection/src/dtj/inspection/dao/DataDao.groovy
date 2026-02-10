package dtj.inspection.dao

import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.*
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_AttribValType_consts
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.api.mdl.model.consts.FD_PeriodType_consts
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.api.mdl.utils.UtPeriod
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

@CompileStatic
class DataDao extends BaseMdbUtils {

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
    ApinatorApi apiIncidentData() {
        return app.bean(ApinatorService).getApi("incidentdata")
    }

    @DaoMethod
    Store saveBallAndOtstupXml(String domain, Map<String, Object> params) {
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        Map<String, Object> par = new HashMap<>(params)
        par.remove("store")
        //
        Store st = mdb.createStore(domain)
        List<Map<String, Object>> lstStore = (List<Map<String, Object>>) params.get("store")
        for (Map<String, Object> m : lstStore) {
            st.add(m)
        }
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
        // Находим обслуживаемые объекты
        Set<Object> idsSection = stSection.getUniqueValues("id")
        Store stObject = apiObjectData().get(ApiObjectData).loadSql("""
            select o.id 
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj in (0${idsSection.join(",")})
            where cls in (0${mapCls.get("Cls_RailwayStage")}, 0${mapCls.get("Cls_RailwayStation")})
        """, "")
        Set<Object> idsObject = stObject.getUniqueValues("id")
        // Находим параметр балл или отступления
        Set<Object> idsRelobjComponentParams = new HashSet<>()
        StoreIndex indOtstup = null
        if (domain == "Otstup") {
            Set<Object> kodsOtstup = st.getUniqueValues("kod_otstup")
            Store stOtstup = apiNSIData().get(ApiNSIData).loadSql("""
                select s.cod, c.entityid as id from syscod c, syscodingcod s
                where c.id=s.syscod and c.entitytype=2 
                    and s.syscoding=1001 and s.cod like 'kod_otstup_%'
            """, "")
            indOtstup = stOtstup.getIndex("cod")
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
                inner join DataPropVal v2 on d2.id=v2.dataprop ${wheV2}
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
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
        """, map as Map<String, Object>, "Obj.plan")
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
        StoreIndex indWorkPlan = stInspection.getIndex("objWorkPlan")
        // Если нет записей "Журнале осмотров и проверок" то создаем
        if (stInspection.size() != stPlan.size()) {
            for (StoreRecord r in stPlan) {
                StoreRecord rPlan = indWorkPlan.get(r.getLong("id"))
                if (rPlan != null)
                    continue
                Map<String, Object> mapIns = r.getValues()
                mapIns.putAll(par)
                mapIns.put("name", r.getString("id") + "-" + r.getString("PlanDateEnd"))
                mapIns.put("objWorkPlan", r.getLong("id"))
                mapIns.put("FactDateEnd", r.getString("PlanDateEnd"))
                mapIns.put("NumberTrack", st.get(0).getLong("nomer_mdk"))
                mapIns.put("HeadTrack", st.get(0).getString("avtor"))
                mapIns.remove("id")
                mapIns.remove("cls")
                mapIns.remove("beg")
                mapIns.remove("end")
                Store stNewInspection = saveInspection("ins", mapIns)
                stInspection.add(stNewInspection)//Результаты saveInspection надо добавить stInspection
            }
        }
        // Создание параметров
        Map<String, Long> mapRelCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_ParamsComponent", "")
        long pvComponentParams = apiMeta().get(ApiMeta).idPV("relcls", mapRelCls.get("RC_ParamsComponent"), "Prop_ComponentParams")
        if (domain == "Ball") {
            Long relobjComponentParams = UtCnv.toLong(idsRelobjComponentParams[0])
            for (StoreRecord r1 in st) {
                if (r1.getBoolean("import"))
                    continue
                boolean bOk = false
                for (StoreRecord r2 in stInspection) {
                    Long beg = r2.getLong("StartKm") * 1000 + (r2.getLong("StartPicket") - 1) * 100 + r2.getLong("StartLink") * 25
                    Long end = r2.getLong("FinishKm") * 1000 + (r2.getLong("FinishPicket") - 1) * 100 + r2.getLong("FinishLink") * 25
                    if (beg <= (r1.getLong("km") + 1) * 1000 && (r1.getLong("km") + 1) * 1000 <= end) {
                        Map<String, Object> mapIns = new HashMap<>(par)
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
                        mapIns.put("inputType", FD_InputType_consts.sss)
                        saveParameterLog("ins", mapIns)
                        //
                        r1.set("import", 1)
                        bOk = true
                        break
                    }
                }
                //
                if (!bOk)
                    throw new XError("Не найден запись в Журнале осмотров и проверок на ${r1.getLong('km') + 1} км")
            }
        } else if (domain == "Otstup") {
            for (StoreRecord r1 in st) {
                if (r1.getBoolean("import"))
                    continue
                //
                Long metrOts
                if (r1.getLong("metr") > 999)
                    metrOts = (r1.getLong("km") + 1) * 1000 + 1000
                else
                    metrOts = (r1.getLong("km") + 1) * 1000 + r1.getLong("metr")
                //
                boolean bOk = false
                for (StoreRecord r2 in stInspection) {
                    Long beg = r2.getLong("StartKm") * 1000 + (r2.getLong("StartPicket") - 1) * 100 + r2.getLong("StartLink") * 25
                    Long end = r2.getLong("FinishKm") * 1000 + (r2.getLong("FinishPicket") - 1) * 100 + r2.getLong("FinishLink") * 25
                    if (beg <= metrOts && metrOts <= end) {
                        Map<String, Object> mapIns = new HashMap<>(par)
                        mapIns.put("name", r2.getString("id") + "-" + r1.getString("datetime_obn"))
                        mapIns.put("relobjComponentParams", indOtstup.get("kod_otstup_" + r1.getString("kod_otstup")).get("id"))
                        mapIns.put("pvComponentParams", pvComponentParams)
                        mapIns.put("objInspection", r2.getLong("id"))
                        mapIns.put("pvLocationClsSection", r2.getLong("pvLocationClsSection"))
                        mapIns.put("objLocationClsSection", r2.getLong("objLocationClsSection"))
                        mapIns.put("StartKm", r1.getLong("km") + 1)
                        mapIns.put("FinishKm", r1.getLong("km") + 1)
                        mapIns.put("StartPicket", r1.getLong("pk") + 1)
                        mapIns.put("FinishPicket", r1.getLong("pk") + 1)
                        /* Линия
                        mapIns.put("StartLink", Math.ceil(((metrOts - (r1.getLong("km") + 1) * 1000) % 100) / 25 as double))
                        mapIns.put("FinishLink", Math.ceil(((metrOts - (r1.getLong("km") + 1) * 1000 + r1.getLong("dlina_ots")) % 100) / 25 as double))
                        if (UtCnv.toInt(mapIns.get("StartLink")) == 0) {
                            mapIns.put("StartLink", 4)
                            mapIns.put("FinishPicket", r1.getLong("pk") + 2)
                        }
                        if (UtCnv.toInt(mapIns.get("FinishLink")) == 0) {
                            mapIns.put("FinishLink", 1)
                            if (UtCnv.toInt(mapIns.get("FinishPicket")) < 10)
                                mapIns.put("FinishPicket", r1.getLong("pk") + 2)
                            else {
                                mapIns.put("StartKm", r1.getLong("km") + 2)
                                mapIns.put("FinishPicket", 1)
                            }
                        }
                        */
                        mapIns.put("StartLink", Math.ceil(((metrOts - (r1.getLong("km") + 1) * 1000) % 100) / 25 as double))
                        if (UtCnv.toInt(mapIns.get("StartLink")) == 0) {
                            mapIns.put("StartLink", 4)
                        }
                        mapIns.put("FinishLink", mapIns.get("StartLink"))
                        //
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
                        mapIns.put("inputType", FD_InputType_consts.sss)
                        saveParameterLog("ins", mapIns)
                        //
                        r1.set("import", 1)
                        bOk = true
                        break
                    }
                }
                //
                if (!bOk)
                    throw new XError("Не найден запись в Журнале осмотров и проверок на ${r1.getLong('km') + 1} км")
            }
        }
        //
        return st
    }

    //todo Temporary
    @DaoMethod
    Store findLocationOfCoord(Map<String, Object> params) {
        long objWork = UtCnv.toLong(params.get("objWork"))
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Collections", "")
        Store stObj = loadSqlService("""
            select v.obj
            from DataProp d, DataPropval v
            where d.id=v.dataProp and d.objorrelobj=${objWork} and d.prop=${map.get("Prop_Collections")}
        """, "", "nsidata")
        if (stObj.size()==0)
            throw new XError("objWork not found")
        long objCollection = stObj.get(0).getLong("obj")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_LocationMulti", "")
        stObj = loadSqlService("""
            select v.obj
            from DataProp d, DataPropval v
            where d.id=v.dataProp and d.objorrelobj=${objCollection} and d.prop=${map.get("Prop_LocationMulti")}
        """, "", "nsidata")
        if (stObj.size()==0)
            throw new XError("objCollection not found")
        Set<Object> objLocation = stObj.getUniqueValues("obj")

        String whe = "o.id in (${objLocation.join(",")})"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")

        int beg = UtCnv.toInt(params.get('StartKm')) * 1000 + UtCnv.toInt(params.get('StartPicket')) * 100
        int end = UtCnv.toInt(params.get('FinishKm')) * 1000 + UtCnv.toInt(params.get('FinishPicket')) * 100

        String sql = """
            select o.id, o.cls, v.name, null as pv,
                v2.numberVal * 1000 + v4.numberVal * 100 as beg,
                v3.numberVal * 1000 + v5.numberVal *100 as end
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
            where ${whe} and v2.numberVal * 1000 + v4.numberVal*100 <= ${beg} and v3.numberVal * 1000 + v5.numberVal *100 >= ${end}
        """
        Store st = loadSqlServiceWithParams(sql, params, "", "orgstructuredata")
        //mdb.outTable(st)
        if (st.size()==1) {
            long idPV = apiMeta().get(ApiMeta).idPV("cls", st.get(0).getLong("cls"), "Prop_LocationClsSection")
            st.get(0).set("pv", idPV )
            return st
        } else
            throw new XError("Not Found")
    }
    //todo Temporary
    @DaoMethod
    Store getPersonnalInfo(long userId) {

        Store st = loadSqlService("""
            select o.id
            from Obj o
            left join DataProp d on d.isObj=1 and d.prop=:Prop_UserId
            left join DataPropVal v on d.id=v.dataProp and v.strVal='${userId}'
        """, "", "personnaldata")

        if (st.size()==0)
            throw new XError("Not found")
        long own = st.get(0).getLong("id")
        return apiPersonnalData().get(ApiPersonnalData).loadPersonnal(own)

    }

    //todo Temporary
    private Set<Object> getIdsObjWithChildren(long obj) {
        Store st = loadSqlService("""
           WITH RECURSIVE r AS (
               SELECT o.id, v.objParent as parent
               FROM Obj o, ObjVer v
               WHERE o.id=v.ownerver and v.lastver=1 and v.objParent=${obj}
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
           WHERE o.id=v.ownerver and v.lastver=1 and o.id=${obj}
           )
           SELECT * FROM o
           UNION ALL
           SELECT * FROM r
           where 0=0
        """, "", "orgstructuredata")

        return st.getUniqueValues("id")
    }

    @DaoMethod
    Store loadObjLocationSectionForSelect(long obj) {
        Store stTmp = loadSqlService("""
            select cls from Obj where id=${obj}
        """, "", "orgstructuredata")
        if (stTmp.size()==0)
            throw new XError("Не найден объект из [OrgStructure]")
        long clsOrgStruct = stTmp.get(0).getLong("cls")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
        Store st
        if (clsOrgStruct== map.get("Cls_LocationSection")) {
            st = loadSqlService("""
               WITH RECURSIVE r AS (
                   SELECT o.id, v.objParent as parent, o.cls, v.name, v.fullname, null as pv
                   FROM Obj o, ObjVer v
                   WHERE o.id=v.ownerver and v.lastver=1 and v.objParent=${obj}
                   UNION ALL
                   SELECT t.*
                   FROM ( SELECT o.id, v.objParent as parent, o.cls, v.name, v.fullname, null as pv
                          FROM Obj o, ObjVer v
                          WHERE o.id=v.ownerver and v.lastver=1
                        ) t
                      JOIN r
                          ON t.parent = r.id
               ),
               o as (
               SELECT o.id, v.objParent as parent, o.cls, v.name, v.fullname, null as pv
               FROM Obj o, ObjVer v
               WHERE o.id=v.ownerver and v.lastver=1 and o.id=${obj}
               )
               SELECT * FROM o
               UNION ALL
               SELECT * FROM r
               where 0=0
            """, "", "orgstructuredata")
        } else {
            st = loadSqlService("""
               SELECT o.id, v.objParent as parent, o.cls, v.name, v.fullname, null as pv
               FROM Obj o, ObjVer v
               WHERE o.id=v.ownerver and v.lastver=1 and o.cls=${map.get("Cls_LocationSection")}
            """, "", "orgstructuredata")
        }
        //
        StoreIndex indSt = st.getIndex("id")
        long pv = apiMeta().get(ApiMeta).idPV("cls", st.get(0).getLong("cls"), "Prop_LocationClsSection")
        for (StoreRecord r in st) {
            r.set("pv", pv)
            StoreRecord rec = indSt.get(r.getLong("parent"))
            if (rec == null)
                r.set("parent", null)
        }
        return st
    }

    @DaoMethod
    Set<String> loadDateWorkPlanInspection(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pv = UtCnv.toLong(params.get("pv"))
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkPlanInspection", "")

        Store stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v, Obj o
            where d.id=v.dataProp and d.objorrelobj=o.id and 
                o.cls=${map.get("Cls_WorkPlanInspection")} and v.propVal=${pv} and v.obj=${obj}    
        """, "", "plandata")
        Set<Object> idsOwn = stTmp.getUniqueValues("own")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_____DateEnd")
        stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d
                left join DataPropVal v on d.id=v.dataProp 
            where d.objorrelobj in (0${idsOwn.join(",")}) and d.prop=${map.get("Prop_PlanDateEnd")}
            and v.dateTimeVal is not null
            except
            select d.objorrelobj as own
            from DataProp d
                left join DataPropVal v on d.id=v.dataProp 
            where d.objorrelobj in (0${idsOwn.join(",")}) and d.prop=${map.get("Prop_FactDateEnd")}
                and v.dateTimeVal is not null
        """, "", "plandata")
        idsOwn = stTmp.getUniqueValues("own")
        stTmp = loadSqlService("""
            select v.dateTimeVal as plDate
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and d.objorrelobj in (0${idsOwn.join(",")})
                and d.prop=${map.get("Prop_PlanDateEnd")}
        """, "", "plandata")

        return stTmp.getUniqueValues("plDate") as Set<String>
    }

    @DaoMethod
    Store loadComponentsByTypObjectForSelect(long id) {
        // id : idObj from Object
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_ObjectType", "")
        Store stTmp = loadSqlService("""
            select v.obj as own
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and prop=${map.get("Prop_ObjectType")} and d.objorrelobj=${id}
        """, "", "objectdata")
        Set<Object> ids = stTmp.getUniqueValues("own")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_Components", "")
        Store stMeta = loadSqlMeta("""
            select id from relclsmember
            where relcls in (
                select id from RelCls where reltyp=${map.get("RT_Components")}
            )
        """, "")
        Set<Object> idsRCM = stMeta.getUniqueValues("id")
        //
        stTmp = loadSqlService("""
            select obj from relobjmember
            where obj not in (${ids.join(",")}) and relobj in (select relobj from relobjmember where obj in (${ids.join(",")}))
                and relClsMember in (${idsRCM.join(",")})
        """, "", "nsidata")
        Set<Object> idsOwn = stTmp.getUniqueValues("obj")

        stTmp = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsOwn.join(",")})
        """, "", "nsidata")
        //
        return stTmp
    }

    @DaoMethod
    Store loadComponentParametersForSelect(long uch1) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_ParamsComponent", "")

        Store stMemb = loadSqlMeta("""
            select id from relclsmember 
            where relcls in (select id from Relcls where reltyp=${map.get("RT_ParamsComponent")})
            order by id
        """, "")

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store stRO = loadSqlService("""
            select o.id, o.relcls, ov1.name as name, null as pv, v1.numberVal as ParamsLimitMax, v2.numberVal as ParamsLimitMin
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join objver ov1 on ov1.ownerVer=r1.obj and ov1.lastVer=1
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
                left join DataProp d1 on d1.isObj=0 and d1.objorrelobj=o.id and d1.prop=${map.get("Prop_ParamsLimitMax")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.isObj=0 and d2.objorrelobj=o.id and d2.prop=${map.get("Prop_ParamsLimitMin")}
                left join DataPropVal v2 on d2.id=v2.dataProp
            where r2.obj=${uch1}
        """, "", "nsidata")

        Set<Object> idsRC = stRO.getUniqueValues("relcls")
        Store stPV = loadSqlMeta("""
            select id, relcls from PropVal where prop=${map.get("Prop_ComponentParams")}
                and relcls in (0${idsRC.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("relcls")
        for (StoreRecord r in stRO) {
            StoreRecord rec = indPV.get(r.getLong("relcls"))
            if (rec != null)
                r.set("pv", rec.getLong("id"))
        }
        return stRO
    }

    @DaoMethod
    Store loadDefectsByComponentForSelect(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_DefectsComponent", "")
        Store stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and d.prop=${map.get("Prop_DefectsComponent")} and v.obj=${id}
        """, "", "nsidata")
        Set<Object> idsOwn = stTmp.getUniqueValues("own")
        stTmp = loadSqlService("""
            select o.id, o.cls, v.name, null as pv
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsOwn.join(",")})
        """, "", "nsidata")
        //

        if (stTmp.size() > 0) {
            long idPv = apiMeta().get(ApiMeta).idPV("cls", stTmp.get(0).getLong("cls"), "Prop_Defect")
            for (StoreRecord r in stTmp) {
                r.set("pv", idPv)
            }
        }
        //
        return stTmp
    }

    @DaoMethod
    Store loadParameterLogByComponentParameter(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.ParameterLog")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ParameterLog", "")
        String whe = "o.cls = ${map.get('Cls_ParameterLog')}"
        String wheV8 = "and v8.relObj = ${params.get('relobj')}"
        //
        String dte = UtCnv.toString(params.get("date"))
        String d1, d2
        if (params.containsKey("periodType")) {
            long pt = UtCnv.toLong(params.get("periodType"))
            UtPeriod utPeriod = new UtPeriod()
            d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            d2 =  UtCnv.toDate(d2).toJavaLocalDate().plusDays(1) as String

        } else {
            d1 = UtCnv.toDate(dte).toJavaLocalDate().minusMonths(1) as String
            d2 = UtCnv.toDate(dte).toJavaLocalDate().plusDays(1) as String
        }
        String wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls,
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
                v17.numberVal as ParamsLimitMin
            from Obj o
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_ComponentParams
                inner join DataPropVal v8 on d8.id=v8.dataprop ${wheV8}
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_ParamsLimit
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_ParamsLimitMax
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_ParamsLimitMin
                left join DataPropVal v17 on d17.id=v17.dataprop
            where ${whe}
            order by v7.dateTimeVal desc
        """, map)
        //
        if (params.containsKey("periodType") || st.size() == 0)
            return st
        else {
            XDate d22 = UtCnv.toDate(st.get(0).getDate("CreationDateTime").toJavaLocalDate().plusDays(1))
            XDate d11 = UtCnv.toDate(d22.toJavaLocalDate().minusDays(3))

            Store stRez = mdb.createStore("Obj.ParameterLog")
            for (StoreRecord r in st) {
                if (r.getDate("CreationDateTime").toJavaLocalDate().isAfter(d11.toJavaLocalDate()) &&
                        r.getDate("CreationDateTime").toJavaLocalDate().isBefore(d22.toJavaLocalDate())) {
                    stRez.add(r)
                }
            }
            return stRez
        }
    }

    @DaoMethod
    Store loadParameterLog(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.ParameterLog")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ParameterLog", "")

        String whe
        String wheV1 = ""
        String wheV7 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls = ${map.get("Cls_ParameterLog")}"
            //
            Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
            Store stClsLocation = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata")
            if (stClsLocation.size()==0)
                throw new XError("Не найден [objLocation={0}]", params.get("objLocation"))

            long clsLocation = stClsLocation.get(0).getLong("cls")

            if (clsLocation == mapCls.get("Cls_LocationSection")) {
                Set<Object> idsObjLocation = getIdsObjWithChildren(UtCnv.toLong(params.get("objLocation")))
                wheV1 = "and v1.obj in (${idsObjLocation.join(",")})"
            }
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            d2 =  d2.addDays(1)
            wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        }

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_ParamsComponent", "")
        long reltypParamsComponent = map.get("RT_ParamsComponent")

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idLocationClsSection, v1.propVal as pvLocationClsSection, 
                    v1.obj as objLocationClsSection, null as nameLocationClsSection,
                v2.id as idInspection, v2.propVal as pvInspection, v2.obj as objInspection, 
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idCreationDateTime, v7.dateTimeVal as CreationDateTime,
                v8.id as idComponentParams, v8.propVal as pvComponentParams, v8.relobj as relobjComponentParams, null as nameComponentParams,
                        null as objComponent, null as nameComponent,
                v10.id as idOutOfNorm, v10.propVal as pvOutOfNorm, null as fvOutOfNorm, null as nameOutOfNorm,
                v12.id as idStartLink, v12.numberVal as StartLink,
                v13.id as idFinishLink, v13.numberVal as FinishLink,
                v14.id as idDescription, v14.multiStrVal as Description,
                v15.id as idParamsLimit, v15.numberVal as ParamsLimit,
                v16.id as idParamsLimitMax, v16.numberVal as ParamsLimitMax,
                v17.id as idParamsLimitMin, v17.numberVal as ParamsLimitMin,
                v18.id as idUser, v18.propVal as pvUser, v18.obj as objUser, null as fullNameUser,
                v19.id as idCreatedAt, v19.dateTimeVal as CreatedAt,
                v20.id as idUpdatedAt, v20.dateTimeVal as UpdatedAt,
                v21.id as idNumberRetreat, v21.numberVal as NumberRetreat,
                v22.id as idStartMeter, v22.numberVal as StartMeter,
                v23.id as idLengthRetreat, v23.numberVal as LengthRetreat,
                v24.id as idDepthRetreat, v24.numberVal as DepthRetreat,
                v25.id as idDegreeRetreat, v25.numberVal as DegreeRetreat
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Inspection
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_ComponentParams
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_OutOfNorm
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Description
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_ParamsLimit
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_ParamsLimitMax
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_ParamsLimitMin
                left join DataPropVal v17 on d17.id=v17.dataprop
                left join DataProp d18 on d18.objorrelobj=o.id and d18.prop=:Prop_User
                left join DataPropVal v18 on d18.id=v18.dataprop
                left join DataProp d19 on d19.objorrelobj=o.id and d19.prop=:Prop_CreatedAt
                left join DataPropVal v19 on d19.id=v19.dataprop
                left join DataProp d20 on d20.objorrelobj=o.id and d20.prop=:Prop_UpdatedAt
                left join DataPropVal v20 on d20.id=v20.dataprop
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
        //... Пересечение
        Set<Object> idsObjLocation = st.getUniqueValues("objLocationClsSection")
        Store stObjLocation = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObjLocation.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocation = stObjLocation.getIndex("id")
        //
        Set<Object> idsRelObjComponentParams = st.getUniqueValues("relobjComponentParams")
        Map<Long, Map<String, Object>> mapParams = new HashMap<>()

        Store stMemb = loadSqlMeta("""
            select id from relclsmember 
            where relcls in (select id from Relcls where reltyp=${reltypParamsComponent})
            order by id
        """, "")
        Store stRO = loadSqlService("""
            select o.id, r1.obj as obj1, ov1.name as name1, r2.obj as obj2, ov2.name as name2
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join objver ov1 on ov1.ownerVer=r1.obj and ov1.lastVer=1
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
                left join objver ov2 on ov2.ownerVer=r2.obj and ov2.lastVer=1
            where o.id in (0${idsRelObjComponentParams.join(",")})
        """, "", "nsidata")

        for (StoreRecord r in stRO) {
            mapParams.put(r.getLong("id"), r.getValues())
        }
        //
        Set<Object> idsInspection = st.getUniqueValues("objInspection")
        Store stWP = mdb.loadQuery("""
            select o.id as objInspection, v1.obj as objWorkPlan, v2.dateTimeVal as FactDateEnd
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_WorkPlan")}
                left join DataPropval v1 on d1.id=v1.dataProp 
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropval v2 on d2.id=v2.dataProp
            where o.id in (0${idsInspection.join(",")})
        """)
        StoreIndex indWP = stWP.getIndex("objInspection")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recLocation = indLocation.get(r.getLong("objLocationClsSection"))
            if (recLocation != null)
                r.set("nameLocationClsSection", recLocation.getString("name"))

            r.set("nameComponentParams", mapParams.get(r.getLong("relobjComponentParams")).get("name1"))
            r.set("nameComponent", mapParams.get(r.getLong("relobjComponentParams")).get("name2"))
            r.set("objComponent", mapParams.get(r.getLong("relobjComponentParams")).get("obj2"))

            StoreRecord recWP = indWP.get(r.getLong("objInspection"))
            if (recWP != null) {
                r.set("objWorkPlan", recWP.getLong("objWorkPlan"))
                r.set("FactDateEnd", recWP.getString("FactDateEnd"))
            }

            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        Set<Object> idsWP = stWP.getUniqueValues("objWorkPlan")
        Store stObject = loadSqlService("""
            select d.objorrelobj as objWorkPlan, v.obj as objObject
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and d.prop=${map.get("Prop_Object")} and d.objorrelobj in (0${idsWP.join(",")})
        """, "", "plandata")
        StoreIndex indObject = stObject.getIndex("objWorkPlan")
        //

        Set<Object> pvsOutOfNorm = st.getUniqueValues("pvOutOfNorm")
        Store stOutOfNorm = loadSqlMeta("""
            select p.id, p.factorVal, f.name
            from PropVal p
                left join Factor f on p.factorVal=f.id 
            where p.id in (0${pvsOutOfNorm.join(",")})   
        """, "")
        StoreIndex indOutOfNorm = stOutOfNorm.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recObject = indObject.get(r.getLong("objWorkPlan"))
            if (recObject != null)
                r.set("objObject", recObject.getLong("objObject"))

            StoreRecord recOutOfNorm = indOutOfNorm.get(r.getLong("pvOutOfNorm"))
            if (recOutOfNorm != null) {
                r.set("fvOutOfNorm", recOutOfNorm.getLong("factorVal"))
                r.set("nameOutOfNorm", recOutOfNorm.getString("name"))
            }
        }

        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObjectName = loadSqlService("""
            select o.id, v.fullName as nameObject, v1.obj as objSection, ov1.name as nameSection 
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1 
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObjectName = stObjectName.getIndex("id")

        for (StoreRecord r in st) {
            StoreRecord recObject = indObjectName.get(r.getLong("objObject"))
            if (recObject != null) {
                r.set("nameObject", recObject.getString("nameObject"))
                r.set("objSection", recObject.getLong("objSection"))
                r.set("nameSection", recObject.getString("nameSection"))
            }
        }
        return st
    }

    @DaoMethod
    Store loadFault(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.fault")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Fault", "")

        String whe
        String wheV1 = ""
        String wheV7 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls = ${map.get("Cls_Fault")}"
            //
            Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
            long clsLocation = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata").get(0).getLong("cls")
            if (clsLocation == mapCls.get("Cls_LocationSection")) {
                Set<Object> idsObjLocation = getIdsObjWithChildren(UtCnv.toLong(params.get("objLocation")))
                wheV1 = "and v1.obj in (${idsObjLocation.join(",")})"
            }
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            d2 =  d2.addDays(1)
            wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        }

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idLocationClsSection, v1.propVal as pvLocationClsSection, 
                    v1.obj as objLocationClsSection, null as nameLocationClsSection,
                v2.id as idInspection, v2.propVal as pvInspection, v2.obj as objInspection, 
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idCreationDateTime, v7.dateTimeVal as CreationDateTime,
                v8.id as idDefect, v8.propVal as pvDefect, v8.obj as objDefect, null as nameDefect,
                v12.id as idStartLink, v12.numberVal as StartLink,
                v13.id as idFinishLink, v13.numberVal as FinishLink,
                v14.id as idDescription, v14.multiStrVal as Description,
                v15.id as idUser, v15.propVal as pvUser, v15.obj as objUser, null as fullNameUser,
                v16.id as idCreatedAt, v16.dateTimeVal as CreatedAt,
                v17.id as idUpdatedAt, v17.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Inspection
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_Defect
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Description
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_User
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_CreatedAt
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_UpdatedAt
                left join DataPropVal v17 on d17.id=v17.dataprop
            where ${whe}
            order by o.id
        """, map)
        //... Пересечение
        Set<Object> idsLocation = st.getUniqueValues("objLocationClsSection")
        Store stLocation = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsLocation.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocation = stLocation.getIndex("id")
        //
        Set<Object> idsDefect = st.getUniqueValues("objDefect")
        Store stDefect = loadSqlService("""
            select o.id, v.name,
                v1.obj as objDefectsComponent, ov1.name as nameDefectsComponent, 
                v2.propVal as pvDefectsCategory, null as fvDefectsCategory, null as nameDefectsCategory 
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_DefectsComponent")}
                left join DataPropval v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on v1.obj=ov1.ownerVer and ov1.lastVer=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_DefectsCategory")}
                left join DataPropval v2 on d2.id=v2.dataProp
            where o.id in (0${idsDefect.join(",")})
        """, "", "nsidata")
        StoreIndex indDefect = stDefect.getIndex("id")
        //
        Set<Object> idsInspection = st.getUniqueValues("objInspection")
        Store stWP = mdb.loadQuery("""
            select o.id as objInspection, v1.obj as objWorkPlan, v2.dateTimeVal as FactDateEnd
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_WorkPlan")}
                left join DataPropval v1 on d1.id=v1.dataProp 
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropval v2 on d2.id=v2.dataProp
            where o.id in (0${idsInspection.join(",")})
        """)
        StoreIndex indWP = stWP.getIndex("objInspection")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recLocation = indLocation.get(r.getLong("objLocationClsSection"))
            if (recLocation != null)
                r.set("nameLocationClsSection", recLocation.getString("name"))
            StoreRecord recDefect = indDefect.get(r.getLong("objDefect"))
            if (recDefect != null) {
                r.set("nameDefect", recDefect.getString("name"))
                r.set("objDefectsComponent", recDefect.getLong("objDefectsComponent"))
                r.set("nameDefectsComponent", recDefect.getString("nameDefectsComponent"))
                r.set("pvDefectsCategory", recDefect.getString("pvDefectsCategory"))
            }
            StoreRecord recWP = indWP.get(r.getLong("objInspection"))
            if (recWP != null) {
                r.set("objWorkPlan", recWP.getLong("objWorkPlan"))
                r.set("FactDateEnd", recWP.getString("FactDateEnd"))
            }
            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rUser != null)
                r.set("fullNameUser", rUser.getString("fullName"))
        }
        //
        Set<Object> idsWP = stWP.getUniqueValues("objWorkPlan")
        Store stObject = loadSqlService("""
            select d.objorrelobj as objWorkPlan, v.obj as objObject
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and d.prop=${map.get("Prop_Object")} and d.objorrelobj in (0${idsWP.join(",")})
        """, "", "plandata")
        StoreIndex indObject = stObject.getIndex("objWorkPlan")
        //
        Set<Object> pvsDefectsCategory = stDefect.getUniqueValues("pvDefectsCategory")
        Store stFV = loadSqlMeta("""
            select p.id, p.factorVal, f.name
            from PropVal p
                left join Factor f on p.factorVal=f.id 
            where p.id in (0${pvsDefectsCategory.join(",")})   
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recObject = indObject.get(r.getLong("objWorkPlan"))
            if (recObject != null)
                r.set("objObject", recObject.getLong("objObject"))

            StoreRecord recFV = indFV.get(r.getLong("pvDefectsCategory"))
            if (recFV != null) {
                r.set("fvDefectsCategory", recFV.getLong("factorVal"))
                r.set("nameDefectsCategory", recFV.getString("name"))
            }
        }
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObjectName = loadSqlService("""
            select o.id, v.fullName as nameObject, v1.obj as objSection, ov1.name as nameSection 
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1 
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObjectName = stObjectName.getIndex("id")

        for (StoreRecord r in st) {
            StoreRecord recObject = indObjectName.get(r.getLong("objObject"))
            if (recObject != null) {
                r.set("nameObject", recObject.getString("nameObject"))
                r.set("objSection", recObject.getLong("objSection"))
                r.set("nameSection", recObject.getString("nameSection"))
            }
        }
        return st
    }

    @DaoMethod
    Store saveParameterLog(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            //
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ParameterLog", "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@Cls_ParameterLog")

            par.put("cls", map.get("Cls_ParameterLog"))
            par.put("fullName", par.get("name"))
            own = eu.insertEntity(par)
            pms.put("own", own)
            long idFV_Flag
            if (pms.getDouble("ParamsLimit") >= pms.getDouble("ParamsLimitMin") &&
                    pms.getDouble("ParamsLimit") <= pms.getDouble("ParamsLimitMax")) {
                map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_False", "")
                idFV_Flag = map.get("FV_False")
            } else {
                map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_True", "")
                idFV_Flag = map.get("FV_True")
            }

            long pvOutOfNorm = apiMeta().get(ApiMeta).idPV("factorVal", idFV_Flag, "Prop_OutOfNorm")

            //1 Prop_Defect
            if (pms.getLong("relobjComponentParams") > 0)
                fillProperties(true, "Prop_ComponentParams", pms)
            else
                throw new XError("[relobjComponentParams] not specified")
            //1.1 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", pms)
            else
                throw new XError("[objLocationClsSection] not specified")
            //2 Prop_Inspection
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
            long pvInspection = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_Inspection"), "Prop_Inspection")
            pms.put("pvInspection", pvInspection)
            if (pms.getLong("objInspection") > 0) {
                fillProperties(true, "Prop_Inspection", pms)
                //
                map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_FlagParameter", "")
                Store stTmp = mdb.loadQuery("""
                    select v.id, v.propVal
                    from DataProp d, DataPropVal v
                    where d.id=v.dataProp and d.objorrelobj=${pms.getLong("objInspection")} 
                        and d.prop=${map.get("Prop_FlagParameter")}
                """)
                if (stTmp.size() > 0) {
                    map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_True", "")
                    long idFV_True = map.get("FV_True")
                    long pvFlag = apiMeta().get(ApiMeta).idPV("factorVal", idFV_True, "Prop_FlagParameter")
                    if (stTmp.get(0).getLong("propVal") != pvFlag) {
                        long id = stTmp.get(0).getLong("id")
                        mdb.execQuery("""
                            update DataPropVal set propVal=${pvFlag}
                            where id=${id}
                        """)
                    }
                }
            } else
                throw new XError("[objInspection] not specified")
            //3 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] not specified")
            //4 Prop_FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] not specified")
            //5 Prop_StartPicket
            if (pms.getLong("StartPicket") > 0)
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] not specified")
            //6 Prop_FinishPicket
            if (pms.getLong("FinishPicket") > 0)
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] not specified")
            //7 Prop_StartLink
            if (pms.getLong("StartLink") > 0)
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("[StartLink] not specified")
            //8 Prop_FinishLink
            if (pms.getLong("FinishLink") > 0)
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("[FinishLink] not specified")
            //9 Prop_ParamsLimit
            if (pms.getString("ParamsLimit") != "")
                fillProperties(true, "Prop_ParamsLimit", pms)
            else
                throw new XError("[ParamsLimit] not specified")
            //10 Prop_ParamsLimitMax
            if (pms.getString("ParamsLimitMax") != "")
                fillProperties(true, "Prop_ParamsLimitMax", pms)
            else
                throw new XError("[ParamsLimitMax] not specified")
            //11 Prop_ParamsLimitMin
            if (pms.getString("ParamsLimitMin") != "")
                fillProperties(true, "Prop_ParamsLimitMin", pms)
            else
                throw new XError("[ParamsLimitMin] not specified")
            //12 Prop_OutOfNorm
            pms.put("fvOutOfNorm", idFV_Flag)
            pms.put("pvOutOfNorm", pvOutOfNorm)
            fillProperties(true, "Prop_OutOfNorm", pms)
            //13 Prop_CreationDateTime
            if (!pms.getString("CreationDateTime").isEmpty())
                fillProperties(true, "Prop_CreationDateTime", pms)
            else
                throw new XError("[CreationDateTime] not specified")
            //14 Prop_Description
            if (!pms.getString("Description").isEmpty())
                fillProperties(true, "Prop_Description", pms)
            //15 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[objUser] not specified")
            //
            pms.put("CreatedAt", pms.getString("CreationDateTime").substring(0,10))
            pms.put("UpdatedAt", pms.getString("CreationDateTime").substring(0,10))
            //16 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] not specified")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //17 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] not specified")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)
            //18 Prop_NumberRetreat
            if (pms.getLong("NumberRetreat") > 0)
                fillProperties(true, "Prop_NumberRetreat", pms)
            //19 Prop_StartMeter
            if (pms.getLong("StartMeter") > 0)
                fillProperties(true, "Prop_StartMeter", pms)
            //20 Prop_LengthRetreat
            if (pms.getLong("LengthRetreat") > 0)
                fillProperties(true, "Prop_LengthRetreat", pms)
            //21 Prop_DepthRetreat
            if (pms.getLong("DepthRetreat") > 0)
                fillProperties(true, "Prop_DepthRetreat", pms)
            //22 Prop_DegreeRetreat
            if (pms.getLong("DegreeRetreat") > 0)
                fillProperties(true, "Prop_DegreeRetreat", pms)
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            throw new XError("Режим [update] отключен")
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        Store stTemp = loadParameterLog(mapRez)
        //**********************************************
        if (stTemp.get(0).getString("nameOutOfNorm") == "да") {
            Map<String, Object> mapIncident = stTemp.get(0).getValues()
            Store stInspection = loadInspection(Map.of("id", mapIncident.get("objInspection")))
            String nameIncident = stInspection.get(0).getString("fullNameWork")
            //
            long pvParameterLog = apiMeta().get(ApiMeta).idPV("cls", UtCnv.toLong(mapIncident.get("cls")), "Prop_ParameterLog")

            Store stObject = loadSqlService("""
                select cls 
                from Obj
                where id=${mapIncident.get("objObject")}
            """, "", "objectdata")
            long pvObject = apiMeta().get(ApiMeta).idPV("cls", stObject.get(0).getLong("cls"), "Prop_Object")
            //
            String description = ""
            if(!UtCnv.toString(mapIncident.get("Description")).isEmpty())
                description = "\nПримечание: " + mapIncident.get("Description")
            //
            mapIncident.put("name", nameIncident)
            mapIncident.put("codCls", "Cls_IncidentParameter")
            mapIncident.put("Description", "Компонент: " + mapIncident.get("nameComponent") +
                    "\nПараметр: " + mapIncident.get("nameComponentParams") +
                    "\nЗначение: " + mapIncident.get("ParamsLimit") + " (min: " + mapIncident.get("ParamsLimitMin") +
                    ", max: " + mapIncident.get("ParamsLimitMax") + ")"  + description)
            mapIncident.put("objParameterLog", mapIncident.get("id"))
            mapIncident.put("pvParameterLog", pvParameterLog)
            mapIncident.put("pvObject", pvObject)
            mapIncident.put("RegistrationDateTime", mapIncident.get("CreationDateTime"))
            mapIncident.put("InfoApplicant", "" + pms.get("nameLocation") + ", " + pms.get("fullNameUser"))
            mapIncident.remove("id")
            mapIncident.remove("cls")
            if (pms.containsKey("inputType"))
                mapIncident.put("inputType", pms.getLong("inputType"))

            if (pms.getInt("DegreeRetreat") > 1 || !pms.containsKey("DegreeRetreat"))
                apiIncidentData().get(ApiIncidentData).saveIncident("ins", mapIncident)
        }
        //******************************************************
        return stTemp
    }

    @DaoMethod
    Store saveFault(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            //
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Fault", "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@Cls_Fault")

            par.put("cls", map.get("Cls_Fault"))
            par.put("fullName", par.get("name"))
            own = eu.insertEntity(par)
            pms.put("own", own)

            //1 Prop_Defect
            if (pms.getLong("objDefect") > 0)
                fillProperties(true, "Prop_Defect", pms)
            else
                throw new XError("[objDefect] not specified")
            //2 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", pms)
            else
                throw new XError("[objLocationClsSection] not specified")
            //3 Prop_Inspection
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
            long pvInspection = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_Inspection"), "Prop_Inspection")
            pms.put("pvInspection", pvInspection)
            if (pms.getLong("objInspection") > 0) {
                fillProperties(true, "Prop_Inspection", pms)
                //
                map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_FlagDefect", "")
                Store stTmp = mdb.loadQuery("""
                    select v.id, v.propVal
                    from DataProp d, DataPropVal v
                    where d.id=v.dataProp and d.objorrelobj=${pms.getLong("objInspection")} 
                        and d.prop=${map.get("Prop_FlagDefect")}
                """)
                if (stTmp.size() > 0) {
                    map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_True", "")
                    long idFV_True = map.get("FV_True")
                    long pvFlagDefect = apiMeta().get(ApiMeta).idPV("factorVal", idFV_True, "Prop_FlagDefect")
                    if (stTmp.get(0).getLong("propVal") != pvFlagDefect) {
                        mdb.execQuery("""
                            update DataPropVal set propVal=${pvFlagDefect}
                            where id=${stTmp.get(0).getLong("id")}
                        """)
                    }
                }
            } else
                throw new XError("[objInspection] not specified")
            //4 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] not specified")
            //5 Prop_FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] not specified")
            //6 Prop_StartPicket
            if (pms.getLong("StartPicket") > 0)
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] not specified")
            //7 Prop_FinishPicket
            if (pms.getLong("FinishPicket") > 0)
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] not specified")
            //8 Prop_StartLink
            if (pms.getLong("StartLink") > 0)
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("[StartLink] not specified")
            //9 Prop_FinishLink
            if (pms.getLong("FinishLink") > 0)
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("[FinishLink] not specified")
            //10 Prop_CreationDateTime
            if (!pms.getString("CreationDateTime").isEmpty())
                fillProperties(true, "Prop_CreationDateTime", pms)
            else
                throw new XError("[CreationDateTime] not specified")
            //11 Prop_Description
            if (!pms.getString("Description").isEmpty())
                fillProperties(true, "Prop_Description", pms)
            //12 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[objUser] not specified")
            //
            pms.put("CreatedAt", pms.getString("CreationDateTime").substring(0,10))
            pms.put("UpdatedAt", pms.getString("CreationDateTime").substring(0,10))
            //13 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] not specified")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //14 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] not specified")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            throw new XError("Режим [update] отключен")
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        Store stTemp = loadFault(mapRez)
        //**********************************************
        Map<String, Object> mapIncident = stTemp.get(0).getValues()
        Store stInspection = loadInspection(Map.of("id", mapIncident.get("objInspection")))
        String nameIncident = stInspection.get(0).getString("fullNameWork")
        //
        long pvFault = apiMeta().get(ApiMeta).idPV("cls", UtCnv.toLong(mapIncident.get("cls")), "Prop_Fault")

        Store stObject = loadSqlService("""
            select cls 
            from Obj
            where id=${mapIncident.get("objObject")}
        """, "", "objectdata")
        long pvObject = apiMeta().get(ApiMeta).idPV("cls", stObject.get(0).getLong("cls"), "Prop_Object")
        //
        String description = ""
        if(!UtCnv.toString(mapIncident.get("Description")).isEmpty())
            description = "\nПримечание: " + mapIncident.get("Description")
        //
        mapIncident.put("name", nameIncident)
        mapIncident.put("codCls", "Cls_IncidentFault")
        mapIncident.put("Description", "Компонент: " + mapIncident.get("nameDefectsComponent") +
                        "\nНеисправность: " + mapIncident.get("nameDefect") + description)
        mapIncident.put("objFault", mapIncident.get("id"))
        mapIncident.put("pvFault", pvFault)
        mapIncident.put("pvObject", pvObject)
        mapIncident.put("RegistrationDateTime", mapIncident.get("CreationDateTime"))
        mapIncident.put("InfoApplicant", "" + pms.get("nameLocation") + ", " + pms.get("fullNameUser"))
        mapIncident.remove("id")
        mapIncident.remove("cls")

        apiIncidentData().get(ApiIncidentData).saveIncident("ins", mapIncident)
        //******************************************************
        return stTemp
    }

    @DaoMethod
    Store loadObjClsWorkPlanInspectionUnfinishedByDate(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pvObj = UtCnv.toLong(params.get("pv"))
        String dte = UtCnv.toString(params.get("date"))

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkPlanInspection", "")
        long cls_WorkPlanInspection = map.get("Cls_WorkPlanInspection")
        Store stTmp = loadSqlService("""
            select id
            from Obj
            where cls=${map.get("Cls_WorkPlanInspection")}    
        """, "", "plandata")
        Set<Object> idsWorkPlan = stTmp.getUniqueValues("id")
        //
        stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and v.propVal=${pvObj} and v.obj=${obj} and d.objorrelobj in (0${idsWorkPlan.join(",")})
        """, "", "plandata")

        Set<Object> idsOwn = stTmp.getUniqueValues("own")

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_____DateEnd")
        stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d
                left join DataPropVal v on d.id=v.dataProp 
            where d.objorrelobj in (0${idsOwn.join(",")}) and d.prop=${map.get("Prop_PlanDateEnd")}
            and v.dateTimeVal::date='${dte}'
            except
            select d.objorrelobj as own
            from DataProp d
                left join DataPropVal v on d.id=v.dataProp 
            where d.objorrelobj in (0${idsOwn.join(",")}) and d.prop=${map.get("Prop_FactDateEnd")}
                and v.dateTimeVal is not null
        """, "", "plandata")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        long prop_WorkPlan = map.get("Prop_WorkPlan")
        idsOwn = stTmp.getUniqueValues("own")
        Store st = loadSqlService("""
            select o.id, o.cls, null as pv, null as objSection, null as nameSection,
                v1.id as idWork, v1.propVal as pvWork, v1.obj as objWork, null as fullNameWork,
                v2.id as idObject, v2.propVal as pvObject, v2.obj as objObject, 
                    null as nameClsObject, null as fullNameObject,
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idStartLink, v7.numberVal as StartLink,
                v8.id as idFinishLink, v8.numberVal as FinishLink
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataprop             
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.id in (0${idsOwn.join(",")})
        """, "Obj.UnfinishedByDate", "plandata")
        // find pv...
        Store stPV = loadSqlMeta("""
            select id, prop, cls
            from PropVal
            where prop=${prop_WorkPlan} and cls=${cls_WorkPlanInspection}
        """, "")
        long pv
        if (stPV.size() > 0) {
            pv = stPV.get(0).getLong("id")
        } else {
            throw new XError("Не найден pvWorkPlan")
        }
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        Store stCls = loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get("Typ_Object")}
        """, "")
        StoreIndex indCls = stCls.getIndex("id")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        Store stObject = loadSqlService("""
            select o.id, o.cls, v.fullName, null as nameClsObject
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.cls in (${idsCls.join(",")})
        """, "", "objectdata")

        for (StoreRecord r in stObject) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null)
                r.set("nameClsObject", rec.getString("name"))
        }
        StoreIndex indObject = stObject.getIndex("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkInspection", "")
        Store stWork = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get("Cls_WorkInspection")}
        """, "", "nsidata")
        StoreIndex indWork = stWork.getIndex("id")
        //
        for (StoreRecord r in st) {
            r.set("pv", pv)
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null) {
                r.set("fullNameWork", rWork.getString("fullName"))
            }
            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null) {
                r.set("fullNameObject", rObject.getString("fullName"))
                r.set("nameClsObject", rObject.getString("nameClsObject"))
            }
        }
        Set<Object> idsObject = st.getUniqueValues("objObject")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Section", "")
        Store stSection = loadSqlService("""
            select o.id, o.cls, v1.obj as objSection, ov1.name as nameSection
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")

        StoreIndex indSection = stSection.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indSection.get(r.getLong("objObject"))
            if (rec != null) {
                r.set("nameSection", rec.getString("nameSection"))
                r.set("objSection", rec.getString("objSection"))
            }
        }

        return st
    }

    @DaoMethod
    Store loadWorkPlanUnfinished(Long objLocation, String codCls) {
        if (objLocation == 0)
            throw new XError("Не найден [objLocation]")
        if (codCls.isEmpty())
            throw new XError("Не найден [codCls]")
        //
        Store stLocation = loadObjLocationSectionForSelect(objLocation)
        Set<Object> idsLocation = stLocation.getUniqueValues("id")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codCls, "")
        long cls_WorkPlan = map.get(codCls)
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        //
        Store stTmp = loadSqlService("""
            select o.id as own
            from Obj o
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=${map.get("Prop_LocationClsSection")}
                inner join DataPropVal v11 on d11.id=v11.dataprop and v11.obj in (0${idsLocation.join(",")})
            where o.cls=0${cls_WorkPlan}
            except
            select o.id as own
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_FactDateEnd")}
                inner join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_LocationClsSection")}
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.obj in (0${idsLocation.join(",")})
            where o.cls=0${cls_WorkPlan}
        """, "", "plandata")
        Set<Object> idsWP = stTmp.getUniqueValues("own")
        //
        Store st = loadSqlService("""
            select o.id, o.cls, null as pv,
                null as objSection, null as nameSection,
                v1.obj as objWork, null as fullNameWork,
                v2.obj as objObject, null as nameClsObject, null as fullNameObject,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.numberVal as StartLink,
                v8.numberVal as FinishLink,
                v9.dateTimeVal as PlanDateEnd,
                v10.dateTimeVal as FactDateEnd,
                v11.propVal as pvLocationClsSection, v11.obj as objLocationClsSection, null as nameLocationClsSection
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataprop             
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=${map.get("Prop_LocationClsSection")}
                left join DataPropVal v11 on d11.id=v11.dataprop
            where o.id in (0${idsWP.join(",")})
            order by v9.dateTimeVal, v11.obj
        """, "Obj.UnfinishedByDate", "plandata")
        mdb.outTable(st)
        // find pv...
        Store stPV = loadSqlMeta("""
            select id, prop, cls
            from PropVal
            where prop=0${map.get("Prop_WorkPlan")} and cls=${cls_WorkPlan}
        """, "")
        long pv
        if (stPV.size() > 0) {
            pv = stPV.get(0).getLong("id")
        } else {
            throw new XError("Не найден [pvWorkPlan]")
        }
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, o.cls, ov.fullName, null as nameClsObject,
                v1.obj as objSection, ov1.name as nameSection
            from Obj o
                left join ObjVer ov on ov.ownerVer=o.id and ov.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        //
        Set<Object> idsClsObject = stObject.getUniqueValues("cls")
        Store stCls = loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v 
            where c.id=v.ownerVer and v.lastVer=1 and c.id in (0${idsClsObject.join(",")})
        """, "")
        StoreIndex indCls = stCls.getIndex("id")
        //
        for (StoreRecord r in stObject) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null)
                r.set("nameClsObject", rec.getString("name"))
        }
        StoreIndex indObject = stObject.getIndex("id")
        //
        Set<Object> idsWork = st.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        StoreIndex indWork = stWork.getIndex("id")
        //
        Set<Object> idsLocationClsSection = st.getUniqueValues("objLocationClsSection")
        Store stLocationClsSection = loadSqlService("""
            select o.id, o.cls, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsLocationClsSection.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocationClsSection = stLocationClsSection.getIndex("id")
        //
        for (StoreRecord r in st) {
            r.set("pv", pv)
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null) {
                r.set("fullNameWork", rWork.getString("fullName"))
            }
            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null) {
                r.set("fullNameObject", rObject.getString("fullName"))
                r.set("nameClsObject", rObject.getString("nameClsObject"))
                r.set("objSection", rObject.getLong("objSection"))
                r.set("nameSection", rObject.getString("nameSection"))
            }
            StoreRecord rLocation = indLocationClsSection.get(r.getLong("objLocationClsSection"))
            if (rLocation != null) {
                r.set("nameLocationClsSection", rLocation.getString("name"))
            }
        }
        //
        return st
    }

    @DaoMethod
    Store loadInspectionEntriesForWorkPlan(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pv = UtCnv.toLong(params.get("pv"))
        Store stOwn = mdb.loadQuery("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and v.propVal=:pv and v.obj=:o
        """, [pv: pv, o: obj])
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        Store st = mdb.createStore("Obj.InspectionEntriesForWorkPlan")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as FactDateEnd,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink
            from Obj o 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_FactDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
            where o.id in (0${idsOwn.join(",")})
        """, map)
        return st
    }

    @DaoMethod
    Store loadParameterEntriesForInspection(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_Inspection"), "Prop_Inspection")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ParameterLog", "")

        Store stOwn = mdb.loadQuery("""
            select o.id as own
            from Obj o, DataProp d, DataPropVal v
            where o.id=d.objorrelobj and o.cls=${map.get("Cls_ParameterLog")} and d.id=v.dataProp and v.propVal=:pv and v.obj=:o
        """, [pv: pv, o: id])
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        Store st = mdb.createStore("Obj.ParameterEntriesForInspection")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_ParamsComponent", "")
        long reltypParamsComponent = map.get("RT_ParamsComponent")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        mdb.loadQuery(st, """
            select o.id,
                v1.relobj as relobjComponentParams, null as nameComponentParams,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as CreationDateTime,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink,
                v15.numberVal as ParamsLimit,
                v16.numberVal as ParamsLimitMax,
                v17.numberVal as ParamsLimitMin
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ComponentParams
                left join DataPropVal v1 on d1.id=v1.dataprop 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime 
                inner join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_ParamsLimit
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_ParamsLimitMax
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_ParamsLimitMin
                left join DataPropVal v17 on d17.id=v17.dataprop
            where o.id in (0${idsOwn.join(",")})
        """, map)

        //
        Set<Object> idsRelObjComponentParams = st.getUniqueValues("relobjComponentParams")
        Map<Long, Map<String, Object>> mapParams = new HashMap<>()

        Store stMemb = loadSqlMeta("""
            select id from relclsmember 
            where relcls in (select id from Relcls where reltyp=${reltypParamsComponent})
            order by id
        """, "")
        Store stRO = loadSqlService("""
            select o.id, r1.obj as obj1, ov1.name as name1, r2.obj as obj2, ov2.name as name2
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join objver ov1 on ov1.ownerVer=r1.obj and ov1.lastVer=1
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
                left join objver ov2 on ov2.ownerVer=r2.obj and ov2.lastVer=1
            where o.id in (0${idsRelObjComponentParams.join(",")})
        """, "", "nsidata")

        for (StoreRecord r in stRO) {
            mapParams.put(r.getLong("id"), r.getValues())
        }

        for (StoreRecord r in st) {
            r.set("nameComponentParams", mapParams.get(r.getLong("relobjComponentParams")).get("name1"))
            r.set("nameComponent", mapParams.get(r.getLong("relobjComponentParams")).get("name2"))
            r.set("objComponent", mapParams.get(r.getLong("relobjComponentParams")).get("obj2"))
        }
        return st
    }

    @DaoMethod
    Store loadFaultEntriesForInspection(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_Inspection"), "Prop_Inspection")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Fault", "")
        Store stOwn = mdb.loadQuery("""
            select o.id as own
            from Obj o, DataProp d, DataPropVal v
            where o.id=d.objorrelobj and o.cls=${map.get("Cls_Fault")} and d.id=v.dataProp and v.propVal=:pv and v.obj=:o
        """, [pv: pv, o: id])
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        Store st = mdb.createStore("Obj.FaultEntriesForInspection")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        mdb.loadQuery(st, """
            select o.id,
                v1.obj as objDefect, null as nameDefect,
                null as nameDefectsComponent,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as CreationDateTime,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Defect
                left join DataPropVal v1 on d1.id=v1.dataprop 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreationDateTime 
                inner join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop 
            where o.id in (0${idsOwn.join(",")})
        """, map)

        Set<Object> idsDefect = st.getUniqueValues("objDefect")

        Store stTmp = loadSqlService("""
            select o.id, v.name, ov1.name as nameDefectsComponent
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.isObj=1 and d1.objorrelobj=o.id and d1.prop=${map.get("Prop_DefectsComponent")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on ov1.ownerver=v1.obj and ov1.lastVer=1 
            where o.id in (0${idsDefect.join(",")})
        """, "", "nsidata")
        StoreIndex indTmp = stTmp.getIndex("id")

        for (StoreRecord r in st) {
            StoreRecord rec = indTmp.get(r.getLong("objDefect"))
            if (rec != null) {
                r.set("nameDefect", rec.getString("name"))
                r.set("nameDefectsComponent", rec.getString("nameDefectsComponent"))
            }
        }
        return st
    }

    @DaoMethod
    Store loadInspection(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.inspection")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
        String whe
        String wheV1 = ""
        String wheV7 = ""
        if (params.containsKey("id"))
                whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls = ${map.get("Cls_Inspection")}"
            //
            Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
            long clsLocation = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata").get(0).getLong("cls")
            if (clsLocation == mapCls.get("Cls_LocationSection")) {
                Set<Object> idsObjLocation = getIdsObjWithChildren(UtCnv.toLong(params.get("objLocation")))
                wheV1 = "and v1.obj in (${idsObjLocation.join(",")})"
            }
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        }

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, null as nameCls,
                v1.id as idLocationClsSection, v1.propVal as pvLocationClsSection, 
                    v1.obj as objLocationClsSection, null as nameLocationClsSection,
                v2.id as idWorkPlan, v2.propVal as pvWorkPlan, v2.obj as objWorkPlan, 
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idFactDateEnd, v7.dateTimeVal as FactDateEnd,
                v8.id as idUser, v8.propVal as pvUser, v8.obj as objUser, null as fullNameUser,
                v9.id as idCreatedAt, v9.dateTimeVal as CreatedAt,
                v10.id as idUpdatedAt, v10.dateTimeVal as UpdatedAt,
                v11.id as idFlagDefect, v11.propVal as pvFlagDefect, null as fvFlagDefect,
                    null as nameFlagDefect,
                v12.id as idStartLink, v12.numberVal as StartLink,
                v13.id as idFinishLink, v13.numberVal as FinishLink,
                v14.id as idReasonDeviation, v14.multiStrVal as ReasonDeviation,
                v15.id as idFlagParameter, v15.propVal as pvFlagParameter, null as fvFlagParameter,
                    null as nameFlagParameter,
                v16.id as idNumberTrack, v16.strVal as NumberTrack,
                v17.id as idHeadTrack, v17.strVal as HeadTrack
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_WorkPlan
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_FactDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_User
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_CreatedAt
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_UpdatedAt
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_FlagDefect
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_ReasonDeviation
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_FlagParameter
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_NumberTrack
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_HeadTrack
                left join DataPropVal v17 on d17.id=v17.dataprop
            where ${whe}
        """, map)
        //mdb.outTable(st)
        //... Пересечение
        Set<Object> idsObjLocation = st.getUniqueValues("objLocationClsSection")
        Store stObjLocation = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObjLocation.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocation = stObjLocation.getIndex("id")

        Set<Object> pvsFlagDefect = st.getUniqueValues("pvFlagDefect")
        Store stFlagDefect = loadSqlMeta("""
            select pv.id, pv.factorval, f.name
            from PropVal pv
                left join Factor f on pv.factorVal=f.id 
            where pv.id in (0${pvsFlagDefect.join(",")})
        """, "")
        StoreIndex indFlagDefect = stFlagDefect.getIndex("id")

        Set<Object> pvsFlagParameter = st.getUniqueValues("pvFlagParameter")
        Store stFlagParameter = loadSqlMeta("""
            select pv.id, pv.factorval, f.name
            from PropVal pv
                left join Factor f on pv.factorVal=f.id 
            where pv.id in (0${pvsFlagParameter.join(",")})
        """, "")
        StoreIndex indFlagParameter = stFlagParameter.getIndex("id")

        Set<Object> isdUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${isdUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Set<Object> idsWorkPlan = st.getUniqueValues("objWorkPlan")
        Store stWP = loadSqlService("""
            select o.id, v1.obj as objWork, v2.obj as objObject, v3.dateTimeVal as PlanDateEnd, v4.dateTimeVal as ActualDateEnd
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v3 on d3.id=v3.dataProp
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v4 on d4.id=v4.dataProp            
            where o.id in (0${idsWorkPlan.join(",")})
        """, "", "plandata")
        StoreIndex indWorkPlan = stWP.getIndex("id")

        Set<Object> idsWork = stWP.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        StoreIndex indWork = stWork.getIndex("id")

        Set<Object> idsObject = stWP.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, ov1.fullName,
                v.obj as objSection, ov2.name as nameSection
            from Obj o
                left join DataProp d on d.objorrelobj=o.id and prop=${map.get("Prop_Section")}
                left join DataPropVal v on d.id=v.dataProp
                left join ObjVer ov1 on o.id=ov1.ownerVer and ov1.lastVer=1
                left join ObjVer ov2 on v.obj=ov2.ownerVer and ov2.lastVer=1           
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObject = stObject.getIndex("id")
        //...
        for (StoreRecord r in st) {
            StoreRecord rLocation = indLocation.get(r.getLong("objLocationClsSection"))
            if (rLocation != null)
                r.set("nameLocationClsSection", rLocation.getString("name"))

            StoreRecord rFlagDefect = indFlagDefect.get(r.getLong("pvFlagDefect"))
            if (rFlagDefect != null) {
                r.set("fvFlagDefect", rFlagDefect.getLong("factorval"))
                r.set("nameFlagDefect", rFlagDefect.getString("name"))
            }

            StoreRecord rFlagParameter = indFlagParameter.get(r.getLong("pvFlagParameter"))
            if (rFlagParameter != null) {
                r.set("fvFlagParameter", rFlagParameter.getLong("factorval"))
                r.set("nameFlagParameter", rFlagParameter.getString("name"))
            }

            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rUser != null)
                r.set("fullNameUser", rUser.getString("fullName"))

            StoreRecord rWorkPlan = indWorkPlan.get(r.getLong("objWorkPlan"))
            if (rWorkPlan != null) {
                r.set("objWork", rWorkPlan.getLong("objWork"))
                r.set("objObject", rWorkPlan.getLong("objObject"))
                r.set("PlanDateEnd", rWorkPlan.getString("PlanDateEnd"))
                r.set("ActualDateEnd", rWorkPlan.getString("ActualDateEnd"))
            }

            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null)
                r.set("fullNameWork", rWork.getString("fullName"))

            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null) {
                r.set("fullNameObject", rObject.getString("fullName"))
                r.set("objSection", rObject.getLong("objSection"))
                r.set("nameSection", rObject.getString("nameSection"))
            }
        }
        //
        return st
    }

    @DaoMethod
    Store saveInspection(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        if (!params.containsKey("flag")){
            int beg = pms.getInt('StartKm') * 1000 + pms.getInt('StartPicket') * 100 + pms.getInt('StartLink') * 25
            int end = pms.getInt('FinishKm') * 1000 + pms.getInt('FinishPicket') * 100 + pms.getInt('FinishLink') * 25
            if (beg > end)
                throw new XError("Координаты начала не могут быть больше координаты конца")
            //
            long objWorkPlan = pms.getLong("objWorkPlan")
            long pvWorkPlan = pms.getLong("pvWorkPlan")

            Store stTmp = mdb.loadQuery("""
                select d.objorrelobj 
                from dataprop d, datapropval v
                where d.id=v.dataprop and v.obj=${objWorkPlan} and v.propval=${pvWorkPlan}
            """)
            Set<Object> idsOwn = stTmp.getUniqueValues("objorrelobj")

            if (!idsOwn.isEmpty()) {
                Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
                stTmp = mdb.loadQuery("""
                    select o.id,
                        v1.numberVal * 1000 + coalesce(v3.numberVal,0) * 100 + coalesce(v5.numberVal,0) * 25 as beg,
                        v2.numberVal * 1000 + coalesce(v4.numberVal,0) * 100 + coalesce(v6.numberVal,0) * 25 as end
                    from Obj o
                        left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_StartKm
                        left join DataPropVal v1 on d1.id=v1.dataprop
                        left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_FinishKm
                        left join DataPropVal v2 on d2.id=v2.dataprop
                        left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartPicket
                        left join DataPropVal v3 on d3.id=v3.dataprop
                        left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishPicket
                        left join DataPropVal v4 on d4.id=v4.dataprop
                        left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartLink
                        left join DataPropVal v5 on d5.id=v5.dataprop
                        left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishLink
                        left join DataPropVal v6 on d6.id=v6.dataprop
                    where o.id in (${idsOwn.join(",")})
                """, map)
                boolean bOk = true
                for (StoreRecord r in stTmp) {
                    if (!(beg > r.getInt("end") || end < r.getInt("beg")))
                        bOk = false
                }
                if (!bOk)
                    throw new XError("По данным координатам существует запись")
            }
        }
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            //
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@Cls_Inspection")
            par.put("cls", map.get("Cls_Inspection"))
            par.put("fullName", par.get("name"))
            own = eu.insertEntity(par)
            pms.put("own", own)

            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_False", "")
            long idFV_False = map.get("FV_False")
            long pvFlagDefect = apiMeta().get(ApiMeta).idPV("factorVal", idFV_False, "Prop_FlagDefect")
            long pvFlagParameter = apiMeta().get(ApiMeta).idPV("factorVal", idFV_False, "Prop_FlagParameter")

            //1 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", pms)
            else
                throw new XError("[objLocationClsSection] not specified")
            //2 Prop_WorkPlan
            if (pms.getLong("objWorkPlan") > 0)
                fillProperties(true, "Prop_WorkPlan", pms)
            else
                throw new XError("[objWorkPlan] not specified")
            //3 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[objUser] not specified")
            //4 Prop_FlagDefect
            pms.put("fvFlagDefect", idFV_False)
            pms.put("pvFlagDefect", pvFlagDefect)
            fillProperties(true, "Prop_FlagDefect", pms)
            //4.1 Prop_FlagParameter
            pms.put("fvFlagParameter", idFV_False)
            pms.put("pvFlagParameter", pvFlagParameter)
            fillProperties(true, "Prop_FlagParameter", pms)
            //5 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] not specified")
            //6 Prop_FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] not specified")
            //7 Prop_StartPicket
            if (pms.getLong("StartPicket") > 0)
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] not specified")
            //8 Prop_FinishPicket
            if (pms.getLong("FinishPicket") > 0)
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] not specified")
            //9 Prop_StartLink
            if (pms.getLong("StartLink") > 0)
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("[StartLink] not specified")
            //10 Prop_FinishLink
            if (pms.getLong("FinishLink") > 0)
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("[FinishLink] not specified")
            //11 Prop_FactDateEnd
            if (!pms.getString("FactDateEnd").isEmpty())
                fillProperties(true, "Prop_FactDateEnd", pms)
            else
                throw new XError("[FactDateEnd] not specified")
            //12 Prop_CreatedAt
            if (!pms.getString("CreatedAt").isEmpty())
                fillProperties(true, "Prop_CreatedAt", pms)
            else
                throw new XError("[CreatedAt] not specified")
            //13 Prop_UpdatedAt
            if (!pms.getString("UpdatedAt").isEmpty())
                fillProperties(true, "Prop_UpdatedAt", pms)
            else
                throw new XError("[UpdatedAt] not specified")
            //14 Prop_ReasonDeviation
            if (!pms.getString("ReasonDeviation").isEmpty())
                fillProperties(true, "Prop_ReasonDeviation", pms)
            //15 Prop_NumberTrack
            if (!pms.getString("NumberTrack").isEmpty())
                fillProperties(true, "Prop_NumberTrack", pms)
            //16 Prop_HeadTrack
            if (!pms.getString("HeadTrack").isEmpty())
                fillProperties(true, "Prop_HeadTrack", pms)
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.put("fullName", par.get("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_LocationClsSection
            if (pms.containsKey("idLocationClsSection")) {
                if (pms.getLong("objLocationClsSection") > 0)
                    updateProperties("Prop_LocationClsSection", pms)
                else
                    throw new XError("[objLocationClsSection] not specified")
            }
            //2 Prop_WorkPlan
            if (pms.containsKey("idWorkPlan")) {
                if (pms.getLong("objWorkPlan") > 0)
                    updateProperties("Prop_WorkPlan", pms)
                else
                    throw new XError("[objWorkPlan] not specified")
            }
            //3 Prop_User
            if (pms.containsKey("idUser")) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[objUser] not specified")
            }
            //4 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getLong("StartKm") > 0)
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("[StartKm] not specified")
            }
            //5 Prop_StartPicket
            if (pms.containsKey("idStartPicket")) {
                if (pms.getLong("StartPicket") > 0)
                    updateProperties("Prop_StartPicket", pms)
                else
                    throw new XError("[StartPicket] not specified")
            }
            //6 Prop_StartLink
            if (pms.containsKey("idStartLink")) {
                if (pms.getLong("StartLink") > 0)
                    updateProperties("Prop_StartLink", pms)
                else
                    throw new XError("[StartLink] not specified")
            }
            //7 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getLong("FinishKm") > 0)
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("[FinishKm] not specified")
            }
            //8 Prop_FinishPicket
            if (pms.containsKey("idFinishPicket")) {
                if (pms.getLong("FinishPicket") > 0)
                    updateProperties("Prop_FinishPicket", pms)
                else
                    throw new XError("[FinishPicket] not specified")
            }
            //9 Prop_FinishLink
            if (pms.containsKey("idFinishLink")) {
                if (pms.getLong("FinishLink") > 0)
                    updateProperties("Prop_FinishLink", pms)
                else
                    throw new XError("[FinishLink] not specified")
            }
            //10 Prop_FactDateEnd
            if (pms.containsKey("idFactDateEnd")) {
                if (pms.getString("FactDateEnd").isEmpty())
                    throw new XError("[FactDateEnd] not specified")
                else
                    updateProperties("Prop_FactDateEnd", pms)
            }
            //11 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] not specified")
                else
                    updateProperties("Prop_UpdatedAt", pms)
            }
            //12 Prop_ReasonDeviation
            if (pms.containsKey("idReasonDeviation")) {
                updateProperties("Prop_ReasonDeviation", pms)
            } else {
                if (!pms.getString("ReasonDeviation").isEmpty())
                    fillProperties(true, "Prop_ReasonDeviation", pms)
            }
            //
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        return loadInspection(mapRez)
    }

    @DaoMethod
    Store saveSeveralInspections(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.inspection")
        VariantMap pms = new VariantMap(params)
        List<Map<String, Object>> objWorkPlan = pms.get("objWorkPlan") as ArrayList
        Set<String> planDateEnd = new HashSet<>()
        //
        if (objWorkPlan.size() == 0)
            throw new XError("Не указан [objWorkPlan]")
        if (pms.getString("FactDateEnd").isEmpty())
            throw new XError("Не указан [FactDateEnd]")
        if (pms.getLong("objUser") == 0)
            throw new XError("Не указан [objUser]")
        if (pms.getLong("pvUser") == 0)
            throw new XError("Не указан [pvUser]")
        if (pms.getString("CreatedAt").isEmpty())
            throw new XError("Не указан [CreatedAt]")
        if (pms.getString("UpdatedAt").isEmpty())
            throw new XError("Не указан [UpdatedAt]")
        // Проверка обязательных полей
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        objWorkPlan.forEach {Map<String, Object> m -> {
            if (m.size() == 0)
                return
            //
            VariantMap pmsVer = new VariantMap(m)
            if (pmsVer.getLong("id") == 0)
                throw new XError("Не указан [id] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("pv") == 0)
                throw new XError("Не указан [pv] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("objLocationClsSection") == 0)
                throw new XError("Не указан [objLocationClsSection] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("pvLocationClsSection") == 0)
                throw new XError("Не указан [pvLocationClsSection] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("StartKm") == 0)
                throw new XError("Не указан [StartKm] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("FinishKm") == 0)
                throw new XError("Не указан [FinishKm] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("StartPicket") == 0)
                throw new XError("Не указан [StartPicket] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("FinishPicket") == 0)
                throw new XError("Не указан [FinishPicket] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("StartLink") == 0)
                throw new XError("Не указан [StartLink] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getLong("FinishLink") == 0)
                throw new XError("Не указан [FinishLink] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            if (pmsVer.getString("PlanDateEnd").isEmpty())
                throw new XError("Не указан [PlanDateEnd] у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            //
            planDateEnd.add(pmsVer.getString("PlanDateEnd"))
            // Проверка Существует ли запись в Журнале осмотров и проверок
            int beg = pmsVer.getInt('StartKm') * 1000 + pmsVer.getInt('StartPicket') * 100 + pmsVer.getInt('StartLink') * 25
            int end = pmsVer.getInt('FinishKm') * 1000 + pmsVer.getInt('FinishPicket') * 100 + pmsVer.getInt('FinishLink') * 25
            if (beg > end)
                throw new XError("Координаты начала не могут быть больше координаты конца [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))

            long obj = pmsVer.getLong("id")
            long pv = pmsVer.getLong("pv")

            Store stTmp = mdb.loadQuery("""
                select d.objorrelobj
                from dataprop d, datapropval v
                where d.id=v.dataprop and v.obj=${obj} and v.propval=${pv}
            """)
            Set<Object> idsOwn = stTmp.getUniqueValues("objorrelobj")

            if (!idsOwn.isEmpty()) {
                stTmp = mdb.loadQuery("""
                select o.id,
                    v1.numberVal * 1000 + coalesce(v3.numberVal,0) * 100 + coalesce(v5.numberVal,0) * 25 as beg,
                    v2.numberVal * 1000 + coalesce(v4.numberVal,0) * 100 + coalesce(v6.numberVal,0) * 25 as end
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_StartKm
                    left join DataPropVal v1 on d1.id=v1.dataprop
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_FinishKm
                    left join DataPropVal v2 on d2.id=v2.dataprop
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartPicket
                    left join DataPropVal v3 on d3.id=v3.dataprop
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishPicket
                    left join DataPropVal v4 on d4.id=v4.dataprop
                    left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartLink
                    left join DataPropVal v5 on d5.id=v5.dataprop
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishLink
                    left join DataPropVal v6 on d6.id=v6.dataprop
                where o.id in (${idsOwn.join(",")})
            """, map)
                boolean bOk = true
                for (StoreRecord r in stTmp) {
                    if (!(beg > r.getInt("end") || end < r.getInt("beg")))
                        bOk = false
                }
                if (!bOk)
                    throw new XError("Существует запись в Журнале осмотров и проверок у плана работ [{0} - {1}]", m.get("fullNameWork"), m.get("fullNameObject"))
            }
        }}
        //
        if (planDateEnd.size() > 1)
            throw new XError("Выберите плановые работы на одну дату. Выбрано на несколько дат [${planDateEnd.join(", ")}].")
        // Создаем запись в Журнале осмотров и проверок на каждый objWorkPlan
        objWorkPlan.forEach {Map<String, Object> m -> {
            if (m.size() == 0)
                return
            //
            Map<String, Object> mapIns = new HashMap<>(pms)
            mapIns.put("name", UtCnv.toString("" + UtCnv.toLong(m.get("id")) + "-" + pms.getString("FactDateEnd")))
            mapIns.put("objWorkPlan", UtCnv.toLong(m.get("id")))
            mapIns.put("pvWorkPlan", UtCnv.toLong(m.get("pv")))
            mapIns.put("objLocationClsSection", UtCnv.toLong(m.get("objLocationClsSection")))
            mapIns.put("pvLocationClsSection", UtCnv.toLong(m.get("pvLocationClsSection")))
            mapIns.put("StartKm", UtCnv.toLong(m.get("StartKm")))
            mapIns.put("FinishKm", UtCnv.toLong(m.get("FinishKm")))
            mapIns.put("StartPicket", UtCnv.toLong(m.get("StartPicket")))
            mapIns.put("FinishPicket", UtCnv.toLong(m.get("FinishPicket")))
            mapIns.put("StartLink", UtCnv.toLong(m.get("StartLink")))
            mapIns.put("FinishLink", UtCnv.toLong(m.get("FinishLink")))
            mapIns.put("flag", 1)
            //
            Store stTmp = saveInspection("ins", mapIns)
            st.add(stTmp)
        }}
        //
        return st
    }

    /**
     *
     * @param id Id Obj
     * Delete object with properties
     */
    @DaoMethod
    void deleteObjWithProperties(long id) {
        validateForDeleteOwner(id)
        //
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        mdb.execQueryNative("""
            delete from DataPropVal
            where dataProp in (select id from DataProp where isobj=1 and objorrelobj=${id});
            delete from DataProp where id in (
                select id from dataprop
                except
                select dataProp as id from DataPropVal
            );
        """)
        //
        eu.deleteEntity(id)
    }

    @DaoMethod
    Store loadWorkForSelect(long id) {
        //id: from OrgStructure idLocation (Typ_Location)

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_LocationMulti", "")

        Store stNSI = loadSqlService("""
            select d.objOrRelObj as owner
            from DataProp d, DataPropval v
            where d.id=v.dataProp and d.prop=${map.get("Prop_LocationMulti")} and v.obj=${id}
        """, "", "nsidata")
        Set<Object> ids = stNSI.getUniqueValues("owner")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Collections", "")
        stNSI = loadSqlService("""
            select d.objOrRelObj as owner
            from DataProp d, DataPropval v
            where d.id=v.dataProp and d.prop=${map.get("Prop_Collections")} and v.obj in (0${ids.join(",")})
        """, "", "nsidata")
        Set<Object> idsWork = stNSI.getUniqueValues("owner")
        Store st = loadSqlService("""
            select o.id, o.cls, v.fullName, null as pv
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")

        for (StoreRecord r in st) {
            long pv = apiMeta().get(ApiMeta).idPV("cls", r.getLong("cls"), "Prop_Work")
            r.set("pv", pv)
        }
        return st
    }

    @DaoMethod
    Store loadObjectServedForSelect(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_Works", "")
        Store stTyp = loadSqlMeta("""
            select typ from reltypmember
            where reltyp=${map.get("RT_Works")}
            order by ord
        """, "")
        long typ1 = stTyp.get(0).getLong("typ")
        long typ2 = stTyp.get(1).getLong("typ")

        Store stRCM1 = loadSqlMeta("""
            select distinct cls 
            from relclsmember
            where cls in (
                select id from Cls where typ=${typ1}
            )
        """, "")

        Set<Object> idsCls1 = stRCM1.getUniqueValues("cls")
        Store stRCM2 = loadSqlMeta("""
            select distinct cls 
            from relclsmember
            where cls in (
                select id from Cls where typ=${typ2}
            )
        """, "")
        Set<Object> idsCls2 = stRCM2.getUniqueValues("cls")

        Store stTmp = loadSqlService("""
            select obj from relobjmember
            where cls in (${idsCls2.join(",")})
                and relobj in (
                    select relobj from relobjmember
                    where cls in (${idsCls1.join(",")}) and obj=${id}
                )
        """, "", "nsidata")

        Set<Object> idsObj = stTmp.getUniqueValues("obj")

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_ObjectType", "")
        stTmp = loadSqlService("""
            select d.objOrRelObj as owner
            from DataProp d, DataPropval v
            where d.id=v.dataProp and d.prop=${map.get("Prop_ObjectType")} and v.obj in (0${idsObj.join(",")})
        """, "", "objectdata")
        Set<Object> owners = stTmp.getUniqueValues("owner")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        stTmp = loadSqlService("""
            select o.id as objObject, o.cls as linkCls, v.fullName as nameObject, null as pvObject,
                v1.obj as objObjectType, null as nameObjectType, 
                v2.obj as objSection, null as nameSection,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_ObjectType")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Section")}
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v3 on d3.id=v3.dataProp
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v4 on d4.id=v4.dataProp
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v5 on d5.id=v5.dataProp
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v6 on d6.id=v6.dataProp
            where o.id in (0${owners.join(",")})
        """, "Obj.objectServedForSelect", "objectdata")

        Set<Object> idsOT = stTmp.getUniqueValues("objObjectType")
        Set<Object> idsSec = stTmp.getUniqueValues("objSection")
        Set<Object> idsCls = stTmp.getUniqueValues("linkCls")

        Store stObjOT = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1
                and o.id in (0${idsOT.join(",")})
        """, "", "nsidata")
        StoreIndex indObjOT = stObjOT.getIndex("id")
        Store stObjSec = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1
                and o.id in (0${idsSec.join(",")})
        """, "", "nsidata")
        StoreIndex indObjSec = stObjSec.getIndex("id")
        //
        Store stPV = apiMeta().get(ApiMeta).getPvFromCls(idsCls, "Prop_Object")
        StoreIndex indPV = stPV.getIndex("cls")
        for (StoreRecord r in stTmp) {
            StoreRecord rPV = indPV.get(r.getLong("linkCls"))
            if (rPV != null)
                r.set("pvObject", rPV.getLong("propVal"))

            StoreRecord rec = indObjOT.get(r.getLong("objObjectType"))
            if (rec != null)
                r.set("nameObjectType", rec.getString("name"))
            rec = indObjSec.get(r.getLong("objSection"))
            if (rec != null)
                r.set("nameSection", rec.getString("name"))
        }

        return stTmp
    }

    private void validateForDeleteOwner(long owner) {
        //---< check data in other DB
        Store stObj = mdb.loadQuery("""
            select o.cls, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${owner}
        """)
        if (stObj.size() > 0) {
            //
            List<String> lstService = new ArrayList<>()
            long cls = stObj.get(0).getLong("cls")
            String name = stObj.get(0).getString("name")
            Store stPV = loadSqlMeta("""
                select id from PropVal where cls=${cls}
            """, "")
            Set<Object> idsPV = stPV.getUniqueValues("id")
            if (stPV.size() > 0) {
                //inspectiondata
                Store stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "inspectiondata")
                if (stData.size() > 0)
                    lstService.add("inspectiondata")
                //incidentdata
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "incidentdata")
                if (stData.size() > 0)
                    lstService.add("incidentdata")
                //
                if (lstService.size()>0) {
                    throw new XError("${name} используется в ["+ lstService.join(", ") + "]")
                }

            }
        }
    }

    //-------------------------
    private void fillProperties(boolean isObj, String cod, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("own"))
        String keyValue = cod.split("_")[1]
        def objRef = UtCnv.toLong(params.get("obj"+keyValue))
        def propVal = UtCnv.toLong(params.get("pv"+keyValue))
        def relRef = UtCnv.toLong(params.get("relobj"+keyValue))

        Store stProp = apiMeta().get(ApiMeta).getPropInfo(cod)
        //
        long prop = stProp.get(0).getLong("id")
        long propType = stProp.get(0).getLong("propType")
        long attribValType = stProp.get(0).getLong("attribValType")
        Integer digit = null
        double koef = stProp.get(0).getDouble("koef")
        if (koef == 0) koef = 1
        if (!stProp.get(0).isValueNull("digit"))
            digit = stProp.get(0).getInt("digit")

        //
        long idDP
        StoreRecord recDP = mdb.createStoreRecord("DataProp")
        String whe = isObj ? "and isObj=1 " : "and isObj=0 "
        if (stProp.get(0).getLong("statusFactor") > 0) {
            long fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
            whe += "and status = ${fv} "
        } else {
            whe += "and status is null "
        }
        whe += "and provider is null "
        //todo if (stProp.get(0).getLong("providerTyp") > 0)

        if (stProp.get(0).getLong("providerTyp") > 0) {
            whe += "and periodType is not null "
        } else {
            whe += "and periodType is null"
        }
        Store stDP = mdb.loadQuery("""
            select * from DataProp
            where objOrRelObj=${own} and prop=${prop} ${whe}
        """)
        if (stDP.size() > 0) {
            idDP = stDP.get(0).getLong("id")
        } else {
            recDP.set("isObj", isObj)
            recDP.set("objOrRelObj", own)
            recDP.set("prop", prop)
            if (stProp.get(0).getLong("statusFactor") > 0) {
                long fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
                recDP.set("status", fv)
            }
            if (stProp.get(0).getLong("providerTyp") > 0) {
                //todo
                // provider
                //
            }
            if (stProp.get(0).getBoolean("dependperiod")) {
                recDP.set("periodType", FD_PeriodType_consts.year)
            }
            idDP = mdb.insertRec("DataProp", recDP, true)
        }
        //
        StoreRecord recDPV = mdb.createStoreRecord("DataPropVal")
        recDPV.set("dataProp", idDP)
        // Attrib
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_TabNumber") ||
                    cod.equalsIgnoreCase("Prop_NumberTrack") ||
                    cod.equalsIgnoreCase("Prop_HeadTrack")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_ReasonDeviation") ||
                    cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt") ||
                    cod.equalsIgnoreCase("Prop_FactDateEnd")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        if ([FD_AttribValType_consts.dttm].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_CreationDateTime")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }


        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_FlagDefect") ||
                    cod.equalsIgnoreCase("Prop_FlagParameter") ||
                        cod.equalsIgnoreCase("Prop_OutOfNorm")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_ParamsMeasure")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_StartKm") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_StartLink") ||
                    cod.equalsIgnoreCase("Prop_FinishLink") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimit") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMax") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMin") ||
                    cod.equalsIgnoreCase("Prop_NumberRetreat") ||
                    cod.equalsIgnoreCase("Prop_StartMeter") ||
                    cod.equalsIgnoreCase("Prop_LengthRetreat") ||
                    cod.equalsIgnoreCase("Prop_DepthRetreat") ||
                    cod.equalsIgnoreCase("Prop_DegreeRetreat")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    double v = UtCnv.toDouble(params.get(keyValue))
                    v = v / koef
                    if (digit) v = v.round(digit)
                    recDPV.set("numberval", v)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // For Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_LocationClsSection") ||
                    cod.equalsIgnoreCase("Prop_WorkPlan") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                        cod.equalsIgnoreCase("Prop_Defect") ||
                        cod.equalsIgnoreCase("Prop_Inspection")) {
                if (objRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("obj", objRef)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        // For RelTyp
        if ([FD_PropType_consts.reltyp].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ComponentParams")) {
                if (relRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("relobj", relRef)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        //
        if (recDP.getLong("periodType") > 0) {
            if (!params.containsKey("dte"))
                params.put("dte", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(params.get("dte")), recDP.getLong("periodType"), 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(params.get("dte")), recDP.getLong("periodType"), 0)
            recDPV.set("dbeg", d1.toString(XDateTimeFormatter.ISO_DATE))
            recDPV.set("dend", d2.toString(XDateTimeFormatter.ISO_DATE))
        } else {
            recDPV.set("dbeg", "1800-01-01")
            recDPV.set("dend", "3333-12-31")
        }

        long au = getUser()
        recDPV.set("authUser", au)
        if (params.containsKey("inputType"))
            recDPV.set("inputType", params.get("inputType"))
        else
            recDPV.set("inputType", FD_InputType_consts.app)

        long idDPV = mdb.getNextId("DataPropVal")
        recDPV.set("id", idDPV)
        recDPV.set("ord", idDPV)
        recDPV.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        mdb.insertRec("DataPropVal", recDPV, false)

    }

    private void updateProperties(String cod, Map<String, Object> params) {
        VariantMap mapProp = new VariantMap(params)
        String keyValue = cod.split("_")[1]
        long idVal = mapProp.getLong("id" + keyValue)
        long propVal = mapProp.getLong("pv" + keyValue)
        long objRef = mapProp.getLong("obj" + keyValue)
        Store stProp = apiMeta().get(ApiMeta).getPropInfo(cod)
        //
        long propType = stProp.get(0).getLong("propType")
        long attribValType = stProp.get(0).getLong("attribValType")
        Integer digit = null
        double koef = stProp.get(0).getDouble("koef")
        if (koef == 0) koef = 1
        if (!stProp.get(0).isValueNull("digit"))
            digit = stProp.get(0).getInt("digit")

        String sql = ""
        def tmst = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        def strValue = mapProp.getString(keyValue)
        // For Attrib
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_TabNumber")) {   //For Template
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set strVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_ReasonDeviation") ||
                    cod.equalsIgnoreCase("Prop_Descreption")) {
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set multiStrVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt") ||
                    cod.equalsIgnoreCase("Prop_FactDateEnd")) {
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set dateTimeVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        if ([FD_AttribValType_consts.dttm].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreationDateTime")) {
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set dateTimeVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }
        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_FlagDefect") ||
                    cod.equalsIgnoreCase("Prop_FlagParameter") ||
                        cod.equalsIgnoreCase("Prop_OutOfNorm")) {
                if (propVal > 0)
                    sql = "update DataPropval set propVal=${propVal}, timeStamp='${tmst}' where id=${idVal}"
                else {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_ParamsMeasure") ) {
                if (propVal > 0)
                    sql = "update DataPropval set propVal=${propVal}, timeStamp='${tmst}' where id=${idVal}"
                else {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_StartKm") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_StartLink") ||
                    cod.equalsIgnoreCase("Prop_FinishLink") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimit") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMax") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMin") ||
                    cod.equalsIgnoreCase("Prop_NumberRetreat") ||
                    cod.equalsIgnoreCase("Prop_StartMeter") ||
                    cod.equalsIgnoreCase("Prop_LengthRetreat") ||
                    cod.equalsIgnoreCase("Prop_DepthRetreat") ||
                    cod.equalsIgnoreCase("Prop_DegreeRetreat")) {
                if (!(mapProp[keyValue] == null || mapProp[keyValue] == "")) {
                    def v = mapProp.getDouble(keyValue)
                    v = v / koef
                    if (digit) v = v.round(digit)
                    sql = "update DataPropval set numberVal=${v}, timeStamp='${tmst}' where id=${idVal}"
                } else {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // For Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_LocationClsSection") ||
                    cod.equalsIgnoreCase("Prop_WorkPlan") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                        cod.equalsIgnoreCase("Prop_Defect") ||
                        cod.equalsIgnoreCase("Prop_Inspection")) {
                if (objRef > 0)
                    sql = "update DataPropval set propVal=${propVal}, obj=${objRef}, timeStamp='${tmst}' where id=${idVal}"
                else {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        mdb.execQueryNative(sql)
    }

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
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
        else
            throw new XError("Unknown model [${model}]")
    }

    private Store loadSqlServiceWithParams(String sql, Map<String, Object> params, String domain, String model) {
        if (model.equalsIgnoreCase("userdata"))
            return apiUserData().get(ApiUserData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("nsidata"))
            return apiNSIData().get(ApiNSIData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("objectdata"))
            return apiObjectData().get(ApiObjectData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("personnaldata"))
            return apiPersonnalData().get(ApiPersonnalData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("orgstructuredata"))
            return apiOrgStructureData().get(ApiOrgStructureData).loadSqlWithParams(sql, params, domain)
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
