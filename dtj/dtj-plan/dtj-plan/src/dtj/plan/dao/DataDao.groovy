package dtj.plan.dao

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
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiRepairData
import tofi.api.dta.ApiUserData
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
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }

    //todo methad savePeriodPlan не используется
    @DaoMethod
    Store savePeriodPlan(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        if (pms.getString("PlanDateEnd").isEmpty())
            throw new XError("Не указан [PlanDateEnd]")
        if (pms.getString("weekDays").isEmpty())
            throw new XError("Не указан [weekDays]")
        if (pms.getString("periodType").isEmpty())
            throw new XError("Не указан [periodType]")
        if (pms.getLong("Periodicity") == 0)
            throw new XError("Не указан [Periodicity]")
        //

        long pt = pms.getLong("periodType")
        XDate dte = UtCnv.toDate(pms.getString("PlanDateEnd"))
        UtPeriod utPeriod = new UtPeriod()
        XDate d1 = utPeriod.calcDbeg(dte, pt, 0)
        XDate d2 = utPeriod.calcDend(dte, pt, 0)
        //
        d2.toJavaLocalDate().toEpochDay()
        //
        XDate d3 = UtCnv.toDate(pms.getString("PlanDateEnd").split("-")[0] + "-12-31")
        if (d1.toJavaLocalDate().isAfter(d2.toJavaLocalDate()))
            throw new XError("Начальная дата копирования больше конечной даты")
        //
        //
        return null
    }

    @DaoMethod
    Store saveSeveralPlans(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.plan")
        VariantMap pms = new VariantMap(params)
        long linkCls = pms.getLong("linkCls")
        long objWork = pms.getLong("objWork")
        List<Map<String, Object>> objObject = pms.get("objObject") as ArrayList
        //
        if (linkCls == 0)
            throw new XError("Не указан [linkCls]")
        if (objWork == 0)
            throw new XError("Не указан [objWork]")
        if (objObject.size() == 0)
            throw new XError("Не указан [objObject]")
        if (pms.getLong("pvWork") == 0)
            throw new XError("Не указан [pvWork]")
        if (pms.getLong("objLocationClsSection") == 0)
            throw new XError("Не указан [objLocationClsSection]")
        if (pms.getLong("pvLocationClsSection") == 0)
            throw new XError("Не указан [pvLocationClsSection]")
        if (pms.getLong("objUser") == 0)
            throw new XError("Не указан [objUser]")
        if (pms.getLong("pvUser") == 0)
            throw new XError("Не указан [pvUser]")
        if (pms.getString("PlanDateEnd").isEmpty())
            throw new XError("Не указан [PlanDateEnd]")
        if (pms.getString("CreatedAt").isEmpty())
            throw new XError("Не указан [CreatedAt]")
        if (pms.getString("UpdatedAt").isEmpty())
            throw new XError("Не указан [UpdatedAt]")
        //
        pms.remove("linkCls")
        pms.remove("objObject")
        // Поиск класса плана работ по linkCls
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_WorkPlan", "")
        Store stTmp = loadSqlMeta("""
                with fv as (
                    select cls,
                    string_agg (cast(factorval as varchar(2000)), ',' order by factorval) as fvlist
                    from clsfactorval
                    where cls=${linkCls}
                    group by cls
                )
                select * from (
                    select c.cls,
                    string_agg (cast(c.factorval as varchar(1000)), ',' order by factorval) as fvlist
                    from clsfactorval c, factor f  
                    where c.factorval =f.id and c.cls in (
                        select id from Cls where typ=${map.get("Typ_WorkPlan")}
                    )
                    group by c.cls
                ) t where t.fvlist in (select fv.fvlist from fv)
            """, "")
        //
        if (stTmp.size() > 0)
            pms.put("cls", stTmp.get(0).getLong("cls"))
        else
            throw new XError("Не найден класс сответствующий классу {0}", linkCls)
        // Проверка обязательных полей
        objObject.forEach {Map<String, Object> m -> {
            if (m.size() == 0)
                return
            if (UtCnv.toLong(m.get("id")) == 0)
                throw new XError("Не указан [id] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("pv")) == 0)
                throw new XError("Не указан [pv] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("StartKm")) == 0)
                throw new XError("Не указан [StartKm] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("FinishKm")) == 0)
                throw new XError("Не указан [FinishKm] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("StartPicket")) == 0)
                throw new XError("Не указан [StartPicket] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("FinishPicket")) == 0)
                throw new XError("Не указан [FinishPicket] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("StartLink")) == 0)
                throw new XError("Не указан [StartLink] у объекта [{0}]", m.get("name"))
            if (UtCnv.toLong(m.get("FinishLink")) == 0)
                throw new XError("Не указан [FinishLink] у объекта [{0}]", m.get("name"))
        }}
        // Создаем план работ на каждый objObject
        objObject.forEach {Map<String, Object> m -> {
            if (m.size() == 0)
                return
            Map<String, Object> mapIns = new HashMap<>(pms)
            mapIns.put("name", UtCnv.toString("" + objWork + "_" + UtCnv.toLong(m.get("id")) + "_" + pms.getString("PlanDateEnd")))
            mapIns.put("objObject", UtCnv.toLong(m.get("id")))
            mapIns.put("pvObject", UtCnv.toLong(m.get("pv")))
            mapIns.put("StartKm", UtCnv.toLong(m.get("StartKm")))
            mapIns.put("FinishKm", UtCnv.toLong(m.get("FinishKm")))
            mapIns.put("StartPicket", UtCnv.toLong(m.get("StartPicket")))
            mapIns.put("FinishPicket", UtCnv.toLong(m.get("FinishPicket")))
            mapIns.put("StartLink", UtCnv.toLong(m.get("StartLink")))
            mapIns.put("FinishLink", UtCnv.toLong(m.get("FinishLink")))
            stTmp = savePlan("ins", mapIns)
            st.add(stTmp)
        }}
        //
        return st
    }

    @DaoMethod
    Store copyPlan(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        def idsWorkPlan = pms.get("idsWorkPlan")
        List<Long> ids = new ArrayList<>()
        for (id in idsWorkPlan) {
            ids.add(UtCnv.toLong(id))
        }
        //
        if (ids.size() == 0)
            throw new XError("Не указан [idsWorkPlan]")
        if (pms.getString("dbegCopy").isEmpty())
            throw new XError("Не указан [dbegCopy]")
        if (pms.getString("dendCopy").isEmpty())
            throw new XError("Не указан [dendCopy]")
        if (pms.getString("dbegPlan").isEmpty())
            throw new XError("Не указан [dbegPlan]")
        if (pms.getString("dendPlan").isEmpty())
            throw new XError("Не указан [dendPlan]")
        //
        if (pms.getLong("objUser") == 0)
            throw new XError("Не указан [objUser]")
        if (pms.getLong("pvUser") == 0)
            throw new XError("Не указан [pvUser]")
        if (pms.getString("CreatedAt").isEmpty())
            throw new XError("Не указан [CreatedAt]")
        if (pms.getString("UpdatedAt").isEmpty())
            throw new XError("Не указан [UpdatedAt]")
        //
        Store st = mdb.createStore("Obj.plan")
        String whe = "o.id in (0${ids.join(",")})"
        XDate d1 = UtCnv.toDate(pms.getString("dbegCopy"))
        XDate d2 = UtCnv.toDate(pms.getString("dendCopy"))
        //
        if (d1.toJavaLocalDate().isAfter(d2.toJavaLocalDate()))
            throw new XError("Начальная дата копирования больше конечной даты")
        //
        String wheV9 = "and v9.dateTimeVal between '${d1}' and '${d2}'"
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.propVal as pvLocationClsSection, v1.obj as objLocationClsSection,
                v2.propVal as pvObject, v2.obj as objObject,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.numberVal as StartLink,
                v8.numberVal as FinishLink,
                v9.dateTimeVal as PlanDateEnd,
                v10.propVal as pvWork, v10.obj as objWork
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Object
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_StartLink
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_FinishLink
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_PlanDateEnd
                inner join DataPropVal v9 on d9.id=v9.dataprop ${wheV9}
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_Work
                left join DataPropVal v10 on d10.id=v10.dataprop
            where ${whe}
        """, map)
        //
        if (st.size() == 0)
            throw new XError("План работ не найден")
        //
        Map<String, String> mapDate = new HashMap<>()
        if (d1 == d2) {
            mapDate.put(UtCnv.toString(d1), pms.getString("dbegPlan"))
        } else {
            XDate d3 = d1
            XDate d4 = UtCnv.toDate(pms.getString("dbegPlan"))
            while (d2 != d3) {
                mapDate.put(UtCnv.toString(d3), UtCnv.toString(d4))
                d3 = d3.addDays(1)
                d4 = d4.addDays(1)
            }
        }
        //
        for (StoreRecord r in st) {
            r.set("PlanDateEnd", mapDate.get(r.getString("PlanDateEnd")))
            Map<String, Object> mapRes = r.getValues()
            mapRes.putAll(pms)
            mapRes.remove("id")
            mapRes.put("name", "" + r.getString("objWork") + "-" + r.getString("PlanDateEnd"))
            savePlan("ins", mapRes)
        }
        //
        return st
    }

    @DaoMethod
    void completeThePlanWork(Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("id"))
        long cls = UtCnv.toLong(params.get("cls"))
        String FactDateEnd = UtCnv.toString(params.get("date"))
        if (own <= 0)
            throw new XError("Не указан параметр [id]")
        if (FactDateEnd.isEmpty())
            throw new XError("Не указан параметр [date]")
        if (cls <= 0)
            throw new XError("Не указан параметр [cls]")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        Store st = mdb.loadQuery("""
            select o.id, v1.dateTimeVal as FactDateEnd,
                v2.numberVal * 1000 + v4.numberVal * 100 + v6.numberVal * 25 as beg,
                v3.numberVal * 1000 + v5.numberVal * 100 + v7.numberVal * 25 as end
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v7 on d7.id=v7.dataprop
            where o.id=0${own}
        """)
        //mdb.outTable(st)
        if (st.size() > 0 && st.get(0).getString("FactDateEnd") != "0000-01-01")
            throw new XError("Фактическая дата завершения [{0}] уже существует", st.get(0).getString("FactDateEnd"))
        //
        Map<String, Object> mapSql = new HashMap<>()
        mapSql.put("own", own)
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkPlanCorrectional", "")
        if (cls == map.get("Cls_WorkPlanCorrectional")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
            mapSql.put("cls", map.get("Cls_TaskLog"))
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_WorkPlan", "")
            mapSql.put("Prop_WorkPlan", map.get("Prop_WorkPlan"))
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_FactDateEnd", "")
            mapSql.put("Prop_FactDateEnd", map.get("Prop_FactDateEnd"))

            Store stRepair = loadSqlServiceWithParams("""
                select o.id, v2.dateTimeVal as FactDateEnd
                    from Obj o
                    left join DataProp d1 on d1.objOrRelObj=o.id and d1.prop=:Prop_WorkPlan
                    inner join DataPropVal v1 on d1.id=v1.dataProp and v1.obj=:own
                    left join DataProp d2 on d2.objOrRelObj=o.id and d2.prop=:Prop_FactDateEnd
                    left join DataPropVal v2 on d2.id=v2.dataProp
                where o.cls=:cls
            """, mapSql, "", "repairdata")
            for (StoreRecord r in stRepair) {
                if (r.getString("FactDateEnd").startsWith("0000-01-01")) {
                    throw new XError("Задача [{0}] еще не завершена", r.getString("id"))
                }
            }
            if (stRepair.size() == 0)
                throw new XError("Задача еще не добавлена")
            //Проверка статуса Incident
            apiRepairData().get(ApiRepairData).checkStatusOfIncident(own, "FV_StatusAtWork", "FV_StatusEliminated")
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Inspection", "")
            mapSql.put("cls", map.get("Cls_Inspection"))
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
            //
            Store stInspection = loadSqlService("""
                select o.id,
                    v2.numberVal * 1000 + v4.numberVal * 100 + v6.numberVal * 25 as beg,
                    v3.numberVal * 1000 + v5.numberVal * 100 + v7.numberVal * 25 as end
                from Obj o 
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_WorkPlan")}
                    inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj=${own}
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_StartKm")}
                    left join DataPropVal v2 on d2.id=v2.dataprop
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_FinishKm")}
                    left join DataPropVal v3 on d3.id=v3.dataprop
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_StartPicket")}
                    left join DataPropVal v4 on d4.id=v4.dataprop
                    left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_FinishPicket")}
                    left join DataPropVal v5 on d5.id=v5.dataprop
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_StartLink")}
                    left join DataPropVal v6 on d6.id=v6.dataprop
                    left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_FinishLink")}
                    left join DataPropVal v7 on d7.id=v7.dataprop
                where o.cls=0${UtCnv.toLong(mapSql.get("cls"))}
            """, "", "inspectiondata")
            //mdb.outTable(stInspection)
            if (stInspection.size() == 0) {
                throw new XError("Осмотр и проверка не проведена")
            } else if (stInspection.size() == 1) {
                if (st.get(0).getLong("beg") < stInspection.get(0).getLong("beg") ||
                        st.get(0).getLong("end") > stInspection.get(0).getLong("end")) {
                    throw new XError("Осмотр и проверка проведена не полностью")
                }
            } else if (stInspection.size() > 1) {
                Long lnInsp = (stInspection.size() - 1) * 25
                for (StoreRecord r in stInspection) {
                    lnInsp += r.getLong("end") - r.getLong("beg")
                }
                if (lnInsp < (st.get(0).getLong("end") - st.get(0).getLong("beg")))
                    throw new XError("Осмотр и проверка проведена не полностью")
            }
        }
        //
        Map<String, Object> par = new HashMap<>()
        par.put("own", own)
        par.put("FactDateEnd", FactDateEnd)
        fillProperties(true, "Prop_FactDateEnd", par)
    }

    @DaoMethod
    Store findLocationOfCoord(Map<String, Object> params) {
        int beg = UtCnv.toInt(params.get('StartKm')) * 1000 + (UtCnv.toInt(params.get('StartPicket')) - 1) * 100 + UtCnv.toInt(params.get('StartLink')) * 25 + 1
        int end = UtCnv.toInt(params.get('FinishKm')) * 1000 + (UtCnv.toInt(params.get('FinishPicket')) - 1) * 100 + UtCnv.toInt(params.get('FinishLink')) * 25 + 1
        if (beg > end)
            throw new XError("Координаты начала не могут быть больше координаты конца")

        Set<Object> objLocation
        String whe

        long objWork = UtCnv.toLong(params.get("objWork"))
        if (objWork > 0) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Collections", "")
            Store stObj = loadSqlService("""
                select v.obj
                from DataProp d, DataPropval v
                where d.id=v.dataProp and d.objorrelobj=${objWork} and d.prop=${map.get("Prop_Collections")}
            """, "", "nsidata")
            if (stObj.size() == 0)
                throw new XError("objWork not found")
            long objCollection = stObj.get(0).getLong("obj")
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_LocationMulti", "")
            stObj = loadSqlService("""
                select v.obj
                from DataProp d, DataPropval v
                where d.id=v.dataProp and d.objorrelobj=${objCollection} and d.prop=${map.get("Prop_LocationMulti")}
            """, "", "nsidata")
            if (stObj.size() == 0)
                throw new XError("objCollection not found")
            objLocation = stObj.getUniqueValues("obj")
            whe = "o.id in (${objLocation.join(",")})"
        } else {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
            whe = "o.cls=${map.get("Cls_LocationSection")}"
        }

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        String sql = """
            select o.id, o.cls, v.name, null as pv,
                v2.numberVal * 1000 as beg,
                (v3.numberVal + 1) * 1000 as end
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
            where ${whe} and v2.numberVal * 1000 < ${beg} and (v3.numberVal + 1) * 1000 >= ${end}
        """
        Store st = loadSqlServiceWithParams(sql, params, "", "orgstructuredata")
        //mdb.outTable(st)

        if (st.size() == 0) {
            throw new XError("Not Found")
        }

        long idPV = apiMeta().get(ApiMeta).idPV("cls", st.get(0).getLong("cls"), "Prop_LocationClsSection")
        for (StoreRecord r in st) {
            r.set("pv", idPV)
        }
        return st
    }

    @DaoMethod
    Store getPersonnalInfo(long userId) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UserId", "")
        Store st = loadSqlService("""
            select d.objorrelobj as id
            from DataPropVal v 
            inner join DataProp d on d.id=v.dataProp and d.isObj=1 and d.prop=${map.get("Prop_UserId")}
            where v.strVal='${userId}'
        """, "", "personnaldata")
        if (st.size() == 0)
            throw new XError("Not found")
        long own = st.get(0).getLong("id")
        st = apiPersonnalData().get(ApiPersonnalData).loadPersonnal(own)
        long pv = apiMeta().get(ApiMeta).idPV("cls",st.get(0).getLong("cls"),"Prop_User")
        st.get(0).set("pv", pv)
        //
        return st
    }

    private Set<Object> getIdsObjLocation(long obj) {
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
    Store loadPlanCorrectional(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.plan.correctional")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkPlanCorrectional", "")
        String whe
        String wheV1 = ""
        String wheV7 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls=${map.get("Cls_WorkPlanCorrectional")}"
            //
            Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")

            Store stTmp = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata")

            long clsLocation = stTmp.size() > 0 ? stTmp.get(0).getLong("cls") : 0

            if (clsLocation == mapCls.get("Cls_LocationSection")) {
                Set<Object> idsObjLocation = getIdsObjLocation(UtCnv.toLong(params.get("objLocation")))
                wheV1 = "and v1.obj in (${idsObjLocation.join(",")})"
            }
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        }

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, null as nameCls,
                v1.id as idLocationClsSection, v1.propVal as pvLocationClsSection, 
                    v1.obj as objLocationClsSection, null as nameLocationClsSection,
                v2.id as idObject, v2.propVal as pvObject, v2.obj as objObject, 
                    null as nameClsObject, null as fullNameObject,
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idPlanDateEnd, v7.dateTimeVal as PlanDateEnd,
                v8.id as idUser, v8.propVal as pvUser, v8.obj as objUser, null as fullNameUser,
                v9.id as idCreatedAt, v9.dateTimeVal as CreatedAt,
                v10.id as idUpdatedAt, v10.dateTimeVal as UpdatedAt,
                v11.id as idWork, v11.propVal as pvWork, v11.obj as objWork, null as fullNameWork,
                v12.id as idFactDateEnd, v12.dateTimeVal as FactDateEnd,
                v13.id as idStatus, v13.propVal as pvStatus, null as fvStatus, null as nameStatus,
                v14.id as idIncident, v14.propVal as pvIncident, v14.obj as objIncident
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Object
                left join DataPropVal v2 on d2.id=v2.dataprop
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
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_User
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_CreatedAt
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_UpdatedAt
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_Work
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_Status
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Incident
                left join DataPropVal v14 on d14.id=v14.dataprop
            where ${whe}
        """, map)
        //... Пересечение
/*        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Location", "")
        Store stCls = loadSqlMeta("""
            select c.id from Cls c where typ=${map.get("Typ_Location")}
        """, "")*/
        Set<Object> idsLocation = st.getUniqueValues("objLocationClsSection")
        Store stLocation = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsLocation.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocation = stLocation.getIndex("id")
        //
        /* map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Work", "")
         stCls = loadSqlMeta("""
             select c.id, v.name from Cls c, ClsVer v where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get("Typ_Work")}
         """, "")
         StoreIndex indCls = stCls.getIndex("id")*/
        //
        Set<Object> idsWork = st.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (${idsWork.join(",")})
        """, "", "nsidata")

/*        for (StoreRecord r in stWork) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null)
                r.set("nameClsWork", rec.getString("name"))
        }*/
        StoreIndex indWork = stWork.getIndex("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        Store stCls = loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v 
            where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get("Typ_Object")}
        """, "")
        StoreIndex indCls = stCls.getIndex("id")
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, o.cls, v.fullName, null as nameClsObject
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObject.join(",")})
        """, "", "objectdata")

        for (StoreRecord r in stObject) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null)
                r.set("nameClsObject", rec.getString("name"))
        }
        StoreIndex indObject = stObject.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Set<Object> pvsStatus = st.getUniqueValues("pvStatus")
        Store stFV = apiMeta().get(ApiMeta).loadSql("""
            select pv.id as pv, f.id as fv, f.name
            from PropVal pv, Factor f
            where pv.id in (0${pvsStatus.join(",")}) and pv.factorVal=f.id            
        """, "")
        StoreIndex indFV = stFV.getIndex("pv")
        //
        for (StoreRecord r in st) {
            StoreRecord rObj = indLocation.get(r.getLong("objLocationClsSection"))
            if (rObj != null)
                r.set("nameLocationClsSection", rObj.getString("name"))
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null) {
                r.set("fullNameWork", rWork.getString("fullName"))
            }
            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null) {
                r.set("fullNameObject", rObject.getString("fullName"))
                r.set("nameClsObject", rObject.getString("nameClsObject"))
            }
            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rObject != null) {
                r.set("fullNameUser", rUser.getString("fullName"))
            }
            StoreRecord rStatus = indFV.get(r.getLong("pvStatus"))
            if (rStatus != null) {
                r.set("fvStatus", rStatus.getLong("fv"))
                r.set("nameStatus", rStatus.getString("name"))
            }

        }
        //...
        return st
    }

    @DaoMethod
    double loadSizePlanOfMonth(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.plan")
        Store st1 = mdb.createStore("Obj.plan")
        Store st2 = mdb.createStore("Obj.plan")
        Store st3 = mdb.createStore("Obj.plan")
        Map<String, Long> map
        String wheClsOrTyp
        if (params.containsKey("codCls")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", UtCnv.toString(params.get("codCls")), "")
            wheClsOrTyp = "c.id=${map.get(UtCnv.toString(params.get("codCls")))}"
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_WorkPlan", "")
            wheClsOrTyp = "typ=${map.get('Typ_WorkPlan')}"
        }

        Store stCls = loadSqlMeta("""
                select c.id , v.name
                from Cls c, ClsVer v
                where c.id=v.ownerVer and v.lastVer=1 and ${wheClsOrTyp}
            """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        StoreIndex indClsWork = stCls.getIndex("id")

        String whe
        String wheV1 = ""
        String wheV7 = ""
        String wheV12 = ""
        whe = "o.cls in (${idsCls.join(",")})"
        //
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")

        Store stTmp = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata")

        long clsLocation = stTmp.size() > 0 ? stTmp.get(0).getLong("cls") : 0

        if (clsLocation == mapCls.get("Cls_LocationSection")) {
            Set<Object> idsObjLocation = getIdsObjLocation(UtCnv.toLong(params.get("objLocation")))
            wheV1 = "and v1.obj in (${idsObjLocation.join(",")})"
        }
        String dte = UtCnv.toString(params.get("date"))
        String d1 = UtCnv.toDate(dte).toJavaLocalDate().minusMonths(1).toString()
        String d2 = UtCnv.toString(UtCnv.toDate(dte).toJavaLocalDate().minusDays(1))
        wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        wheV12 = "and v12.dateTimeVal between '${d1}' and '${d2}'"
/*        wheV12 = """
             and o.id not in (
                select o.id
                from Obj o
                    left join ObjVer v on o.id=v.ownerver and v.lastver=1
                    left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                    inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                    left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                    inner join DataPropVal v12 on d12.id=v12.dataprop and v12.dateTimeVal is not null
                where ${whe}
                )
            """*/

        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id,
                v7.dateTimeVal as PlanDateEnd,
                v12.dateTimeVal as FactDateEnd
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                inner join DataPropVal v12 on d12.id=v12.dataprop ${wheV12}
            where ${whe}
        """, map)
        //
        String d3 = UtCnv.toString(UtCnv.toDate(d1).toJavaLocalDate().minusDays(1))
        wheV7 = "and v7.dateTimeVal between '1800-01-01' and '${d3}'"
        mdb.loadQuery(st1, """
            select o.id,
                v7.dateTimeVal as PlanDateEnd,
                v12.dateTimeVal as FactDateEnd
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                inner join DataPropVal v12 on d12.id=v12.dataprop ${wheV12}
            where ${whe}
        """, map)
        //
        d3 = UtCnv.toString(UtCnv.toDate(d2).toJavaLocalDate().plusDays(1))
        wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
        wheV12 = "and v12.dateTimeVal between '${d3}' and '3333-12-31'"
        mdb.loadQuery(st2, """
            select o.id,
                v7.dateTimeVal as PlanDateEnd,
                v12.dateTimeVal as FactDateEnd
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                inner join DataPropVal v12 on d12.id=v12.dataprop ${wheV12}
            where ${whe}
        """, map)
        //
        wheV12 = "and v12.dateTimeVal between '${d1}' and '3333-12-31'"
        mdb.loadQuery(st3, """
            select o.id,
                v7.dateTimeVal as PlanDateEnd,
                v12.dateTimeVal as FactDateEnd
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                left join DataPropVal v12 on d12.id=v12.dataprop
            where ${whe} and o.id not in (
                select o.id
                from Obj o 
                    left join ObjVer v on o.id=v.ownerver and v.lastver=1
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                    inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                    left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                    inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                    left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                    inner join DataPropVal v12 on d12.id=v12.dataprop ${wheV12}
                where ${whe}
            )
        """, map)
        //
        st.add(st1)
        st.add(st2)
        st.add(st3)
        //
        int diffDay = UtCnv.toDate(d2).diffDays(UtCnv.toDate(d1))
        long count=0
        for (int k = 0; k < diffDay; k++) {
            XDate dt = UtCnv.toDate(UtCnv.toDate(d1).toJavaLocalDate().plusDays(k))
            for (StoreRecord r in st) {
                if (r.getString("FactDateEnd") == "0000-01-01") {
                    if (dt >= r.getDate("PlanDateEnd"))
                        count++
                } else if (dt >= r.getDate("PlanDateEnd") && dt <= r.getDate("FactDateEnd")) {
                    count++
                }
            }
        }
        //
        count = count == 0 ? 1 : count
        //...
        return st.size()/count
    }

    @DaoMethod
    Store loadPlan(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.plan")
        Map<String, Long> map
        String wheClsOrTyp
        if (params.containsKey("codCls")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", UtCnv.toString(params.get("codCls")), "")
            wheClsOrTyp = "c.id=${map.get(UtCnv.toString(params.get("codCls")))}"
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_WorkPlan", "")
            wheClsOrTyp = "typ=${map.get('Typ_WorkPlan')}"
        }

        Store stCls = loadSqlMeta("""
                select c.id , v.name
                from Cls c, ClsVer v
                where c.id=v.ownerVer and v.lastVer=1 and ${wheClsOrTyp}
            """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        StoreIndex indClsWork = stCls.getIndex("id")

        String whe
        String wheV1 = ""
        String wheV7 = ""
        String wheV12 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls in (${idsCls.join(",")})"
            //
            Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")

            Store stTmp = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata")

            long clsLocation = stTmp.size() > 0 ? stTmp.get(0).getLong("cls") : 0

            if (clsLocation == mapCls.get("Cls_LocationSection")) {
                Set<Object> idsObjLocation = getIdsObjLocation(UtCnv.toLong(params.get("objLocation")))
                wheV1 = "and v1.obj in (${idsObjLocation.join(",")})"
            }
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            if (pt > 0) {
                UtPeriod utPeriod = new UtPeriod()
                XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
                XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
                wheV7 = "and v7.dateTimeVal between '${d1}' and '${d2}'"
            } else {
                dte = UtCnv.toString(UtCnv.toDate(params.get("date")).toJavaLocalDate().minusDays(1))
                wheV7 = "and v7.dateTimeVal between '1800-01-01' and '${dte}'"
                wheV12 = """
                 and o.id not in (
                    select o.id
                    from Obj o 
                        left join ObjVer v on o.id=v.ownerver and v.lastver=1
                        left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                        inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                        left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                        inner join DataPropVal v12 on d12.id=v12.dataprop and v12.dateTimeVal is not null
                    where ${whe}
                    )
                """
            }
        }

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, null as nameCls,
                v1.id as idLocationClsSection, v1.propVal as pvLocationClsSection, 
                    v1.obj as objLocationClsSection, null as nameLocationClsSection,
                v2.id as idObject, v2.propVal as pvObject, v2.obj as objObject, 
                    null as nameClsObject, null as fullNameObject,
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idPlanDateEnd, v7.dateTimeVal as PlanDateEnd,
                v8.id as idUser, v8.propVal as pvUser, v8.obj as objUser, null as fullNameUser,
                v9.id as idCreatedAt, v9.dateTimeVal as CreatedAt,
                v10.id as idUpdatedAt, v10.dateTimeVal as UpdatedAt,
                v11.id as idWork, v11.propVal as pvWork, v11.obj as objWork,
                    null as nameClsWork, null as fullNameWork,
                v12.id as idFactDateEnd, v12.dateTimeVal as FactDateEnd,
                v13.id as idIncident, v13.propVal as pvIncident, v13.obj as objIncident,
                v14.id as idStartLink, v14.numberVal as StartLink,
                v15.id as idFinishLink, v15.numberVal as FinishLink
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_LocationClsSection
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Object
                left join DataPropVal v2 on d2.id=v2.dataprop
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
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_User
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_CreatedAt
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_UpdatedAt
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_Work
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FactDateEnd
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_Incident
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_StartLink
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_FinishLink
                left join DataPropVal v15 on d15.id=v15.dataprop
            where ${whe} ${wheV12}
        """, map)
        //
        if (st.size() == 0) return st
        //... Пересечение
        Set<Object> idsLocation = st.getUniqueValues("objLocationClsSection")
        Store stLocation = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsLocation.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocation = stLocation.getIndex("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Work", "")
        stCls = loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get("Typ_Work")}
        """, "")
        StoreIndex indCls = stCls.getIndex("id")
        //
        Set<Object> idsWork = st.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, o.cls, v.fullName, null as nameClsWork
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        //
        for (StoreRecord r in stWork) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null)
                r.set("nameClsWork", rec.getString("name"))
        }
        StoreIndex indWork = stWork.getIndex("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        stCls = loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get("Typ_Object")}
        """, "")
        indCls = stCls.getIndex("id")
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, o.cls, v.fullName, null as nameClsObject
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObject.join(",")})
        """, "", "objectdata")

        for (StoreRecord r in stObject) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null)
                r.set("nameClsObject", rec.getString("name"))
        }
        StoreIndex indObject = stObject.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rCls = indClsWork.get(r.getLong("cls"))
            if (rCls != null)
                r.set("nameCls", rCls.getString("name"))
            StoreRecord rObj = indLocation.get(r.getLong("objLocationClsSection"))
            if (rObj != null)
                r.set("nameLocationClsSection", rObj.getString("name"))
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null) {
                r.set("nameClsWork", rWork.getString("nameClsWork"))
                r.set("fullNameWork", rWork.getString("fullName"))
            }
            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null) {
                r.set("fullNameObject", rObject.getString("fullName"))
                r.set("nameClsObject", rObject.getString("nameClsObject"))
            }
            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rObject != null) {
                r.set("fullNameUser", rUser.getString("fullName"))
            }
        }
        //...
        return st
    }

    @DaoMethod
    Store savePlan(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        par.putIfAbsent("fullName", par.get("name"))
        if (mode.equalsIgnoreCase("ins")) {
            if (pms.getLong("objIncident") > 0) {
                long pv = apiMeta().get(ApiMeta).idPV("cls", pms.getLong("clsIncident"), "Prop_Incident")
                pms.put("pvIncident", pv)
            }
            //
            long cls = pms.getLong("cls")
            // find cls(linkCls)
            if (cls == 0) {
                Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_WorkPlan", "")
                long linkCls = pms.getLong("linkCls")
                if (map.isEmpty())
                    throw new XError("NotFoundCod@Typ_Work")
                map.put("linkCls", linkCls)
                Store stTmp = loadSqlMeta("""
                    with fv as (
                        select cls,
                        string_agg (cast(factorval as varchar(2000)), ',' order by factorval) as fvlist
                        from clsfactorval
                        where cls=${linkCls}
                        group by cls
                    )
                    select * from (
                        select c.cls,
                        string_agg (cast(c.factorval as varchar(1000)), ',' order by factorval) as fvlist
                        from clsfactorval c, factor f  
                        where c.factorval =f.id and c.cls in (
                            select id from Cls where typ=${map.get("Typ_WorkPlan")}
                        )
                        group by c.cls
                    ) t where t.fvlist in (select fv.fvlist from fv)
                """, "")
                //
                if (stTmp.size() > 0)
                    cls = stTmp.get(0).getLong("cls")
                else {
                    throw new XError("Не найден класс сответствующий классу {0}", linkCls)
                }
                //
                pms.put("cls", cls)
            }
            //
            own = apiPlanData().get(ApiPlanData).savePlan(mode, pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_WorkPlan", "")
            Store stInspection = loadSqlService("""
                select v.id
                from DataProp d, DataPropVal v
                where d.id=v.dataprop and d.prop=${map.get("Prop_WorkPlan")} and v.obj=${own}
            """, "", "inspectiondata")

            if (stInspection.size() > 0)
                throw new XError("Существует запись в 'Журнале осмотров и проверок' по данной плановой работе")

            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_ObjectType
            if (pms.containsKey("idLocationClsSection")) {
                if (pms.getLong("objLocationClsSection") > 0)
                    updateProperties("Prop_LocationClsSection", pms)
                else
                    throw new XError("[objLocationClsSection] not specified")
            }
            //2 Prop_Work
            if (pms.containsKey("idWork")) {
                if (pms.getLong("objWork") > 0)
                    updateProperties("Prop_Work", pms)
                else
                    throw new XError("[objWork] not specified")
            }
            //3 Prop_Object
            if (pms.containsKey("idObject")) {
                if (pms.getLong("objObject") > 0)
                    updateProperties("Prop_Object", pms)
                else
                    throw new XError("[objObject] not specified")
            }
            //4 Prop_User
            if (pms.containsKey("idUser")) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[objUser] not specified")
            }
            //5 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getLong("StartKm") > 0)
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("[StartKm] not specified")
            }
            //6 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getLong("FinishKm") > 0)
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("[FinishKm] not specified")
            }
            //7 Prop_StartPicket
            if (pms.containsKey("idStartPicket")) {
                if (pms.getLong("StartPicket") > 0)
                    updateProperties("Prop_StartPicket", pms)
                else
                    throw new XError("[StartPicket] not specified")
            }
            //8 Prop_FinishPicket
            if (pms.containsKey("idFinishPicket")) {
                if (pms.getLong("FinishPicket") > 0)
                    updateProperties("Prop_FinishPicket", pms)
                else
                    throw new XError("[FinishPicket] not specified")
            }
            //9 Prop_StartLink
            if (pms.containsKey("idStartLink")) {
                if (pms.getLong("StartLink") > 0)
                    updateProperties("Prop_StartLink", pms)
                else
                    throw new XError("[StartLink] not specified")
            }
            //10 Prop_FinishLink
            if (pms.containsKey("idFinishLink")) {
                if (pms.getLong("FinishLink") > 0)
                    updateProperties("Prop_FinishLink", pms)
                else
                    throw new XError("[FinishLink] not specified")
            }
            //11 Prop_PlanDateEnd
            if (pms.containsKey("idPlanDateEnd")) {
                if (!pms.getString("PlanDateEnd").isEmpty())
                    updateProperties("Prop_PlanDateEnd", pms)
                else
                    throw new XError("[PlanDateEnd] not specified")
            }
            //12 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                if (!pms.getString("UpdatedAt").isEmpty())
                    updateProperties("Prop_UpdatedAt", pms)
                else
                    throw new XError("[UpdatedAt] not specified")
            }
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        return loadPlan(mapRez)
    }

    @DaoMethod
    long assignPlan(Map<String, Object> params) {
        //WorkPlan
        Map<String, Object> par = new HashMap<>(params)
        par.put("objIncident", UtCnv.toLong(par.get("id")))
        par.put("clsIncident", UtCnv.toLong(par.get("cls")))
        par.remove("id")
        par.remove("cls")
        savePlan("ins", par)
        //
        return apiIncidentData().get(ApiIncidentData).updateIncident("ins", params)
    }

    @DaoMethod
    Map<String, Object> getPeriodInfo(String date, long periodType) {
        if (date.isEmpty())
            throw new XError("Не указан [date]")
        if (periodType == 0)
            throw new XError("Не указан [periodType]")
        //
        UtPeriod utPeriod = new UtPeriod()
        XDate dt = UtCnv.toDate(date)
        String d1 = utPeriod.calcDbeg(dt, periodType, 0).toString(XDateTimeFormatter.ISO_DATE)
        String d2 = utPeriod.calcDend(dt, periodType, 0).toString(XDateTimeFormatter.ISO_DATE)
        //
        Map<String, Object> map = new HashMap<>()
        map.put("dbeg", d1)
        map.put("dend", d2)
        //
        return map
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
        Set<Object> owners

        if (id > 0) {
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
            owners = stTmp.getUniqueValues("owner")
        } else {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
            Store stTmp = loadSqlMeta("""
                select id from Cls where typ=${map.get("Typ_Object")}
            """, "")
            Set<Object> idsCls = stTmp.getUniqueValues("id")
            stTmp = loadSqlService("""
                select id
                from Obj
                where cls in (0${idsCls.join(",")})
            """, "", "objectdata")
            owners = stTmp.getUniqueValues("id")
        }
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store stTmp = loadSqlService("""
            select o.id as objObject, o.cls as linkCls, v.fullName as nameObject, null as pvObject,
                v1.obj as objObjectType, null as nameObjectType, 
                v2.obj as objSection, ov2.name as nameSection,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.numberVal as StartLink,
                v8.numberVal as FinishLink
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_ObjectType")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Section")}
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join ObjVer ov2 on ov2.ownerVer=v2.obj and ov2.lastVer=1
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v3 on d3.id=v3.dataProp
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v4 on d4.id=v4.dataProp
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v5 on d5.id=v5.dataProp
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v6 on d6.id=v6.dataProp
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v7 on d7.id=v7.dataProp
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v8 on d8.id=v8.dataProp
            where o.id in (0${owners.join(",")})
        """, "Obj.objectServedForSelect", "objectdata")

        Set<Object> idsOT = stTmp.getUniqueValues("objObjectType")
        Set<Object> idsCls = stTmp.getUniqueValues("linkCls")

        Store stObjOT = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1
                and o.id in (0${idsOT.join(",")})
        """, "", "nsidata")
        StoreIndex indObjOT = stObjOT.getIndex("id")
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
        }

        return stTmp
    }

    /*
    Рассмотреть возможность объедининения с методом loadObjectServedForSelect
     */

    @DaoMethod
    Store loadWorkOnObjectServedForSelect(long id) {
        Set<Object> owners

        if (id > 0) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_ObjectType", "")
            Store stTmp = loadSqlService("""
                select v.obj
                from DataProp d, DataPropval v
                where d.id=v.dataProp and d.prop=${map.get("Prop_ObjectType")} and d.objOrRelObj=${id}
            """, "", "objectdata")

            long idObjectType = stTmp.get(0).getLong("obj")

            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_Works", "")
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

            stTmp = loadSqlService("""
                select obj as owner
                from relobjmember
                where cls in (${idsCls1.join(",")})
                    and relobj in (
                        select relobj from relobjmember
                        where cls in (${idsCls2.join(",")}) and obj=${idObjectType}
                    )
            """, "", "nsidata")
            owners = stTmp.getUniqueValues("owner")
        } else {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Work", "")
            Store stTmp = loadSqlMeta("""
                select id from Cls where typ=${map.get("Typ_Work")}
            """, "")
            Set<Object> idsCls = stTmp.getUniqueValues("id")
            stTmp = loadSqlService("""
                select id
                from Obj
                where cls in (0${idsCls.join(",")})
            """, "", "nsidata")
            owners = stTmp.getUniqueValues("id")
        }

        Store stTmp = loadSqlService("""
            select o.id, o.cls, v.name, v.fullName, null as pv
            from Obj o, ObjVer v
            where o.id=v.ownerver and lastver=1 and o.id in (0${owners.join(",")})
        """, "", "nsidata")

        Set<Object> idsCls = stTmp.getUniqueValues("cls")
        //
        Store stPV = apiMeta().get(ApiMeta).getPvFromCls(idsCls, "Prop_Work")
        StoreIndex indPV = stPV.getIndex("cls")
        for (StoreRecord r in stTmp) {
            StoreRecord rPV = indPV.get(r.getLong("cls"))
            if (rPV != null)
                r.set("pv", rPV.getLong("propVal"))
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
                Store stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "plandata")
                if (stData.size() > 0)
                    lstService.add("plandata")
                //
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "inspectiondata")
                if (stData.size() > 0)
                    lstService.add("inspectiondata")
                //
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "repairdata")
                if (stData.size() > 0)
                    lstService.add("repairdata")
                //

                if (lstService.size() > 0) {
                    throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                }
            }
        }
    }

    //-------------------------
    private void fillProperties(boolean isObj, String cod, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("own"))
        String keyValue = cod.split("_")[1]
        def objRef = UtCnv.toLong(params.get("obj" + keyValue))
        def propVal = UtCnv.toLong(params.get("pv" + keyValue))

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
            if (cod.equalsIgnoreCase("Prop_TabNumber") ||
                    cod.equalsIgnoreCase("Prop_UserSecondName") ||
                    cod.equalsIgnoreCase("Prop_UserFirstName") ||
                    cod.equalsIgnoreCase("Prop_UserMiddleName") ||
                    cod.equalsIgnoreCase("Prop_UserEmail") ||
                    cod.equalsIgnoreCase("Prop_UserPhone") ||
                    cod.equalsIgnoreCase("Prop_UserId")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
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
                    cod.equalsIgnoreCase("Prop_PlanDateEnd") ||
                    cod.equalsIgnoreCase("Prop_FactDateEnd")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_UserSex")) {    //template
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ParamsMeasure")) {
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
                    cod.equalsIgnoreCase("Prop_FinishLink")) {
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
        //Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_LocationClsSection") ||
                    cod.equalsIgnoreCase("Prop_Work") ||
                    cod.equalsIgnoreCase("Prop_Object") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_Incident")) {
                if (objRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("obj", objRef)
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
            if (cod.equalsIgnoreCase("Prop_Description")) {
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
                    cod.equalsIgnoreCase("Prop_PlanDateEnd")) {
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
            if (cod.equalsIgnoreCase("Prop_UserSex")) {    //template
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
            if (cod.equalsIgnoreCase("Prop_ParamsMeasure")) {
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
                    cod.equalsIgnoreCase("Prop_FinishLink")) {
                if (mapProp[keyValue] != "") {
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
                    cod.equalsIgnoreCase("Prop_Work") ||
                    cod.equalsIgnoreCase("Prop_Object") ||
                    cod.equalsIgnoreCase("Prop_User")) {
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
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSql(sql, domain)
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
        else if (model.equalsIgnoreCase("inspectiondata"))
            return apiInspectionData().get(ApiInspectionData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSqlWithParams(sql, params, domain)

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
