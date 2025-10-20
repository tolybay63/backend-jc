package dtj.repair.dao

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
import tofi.api.dta.ApiClientData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiResourceData
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

import java.sql.ClientInfoStatus


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
    ApinatorApi apiClientData() {
        return app.bean(ApinatorService).getApi("clientdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }

    @DaoMethod
    Store loadObjClsWorkPlanCorrectionalUnfinishedByDate(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pvObj = UtCnv.toLong(params.get("pv"))
        String dte = UtCnv.toString(params.get("date"))

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkPlanCorrectional", "")
        long cls_WorkPlanInspection = map.get("Cls_WorkPlanCorrectional")
        Store stTmp = loadSqlService("""
            select id
            from Obj
            where cls=${map.get("Cls_WorkPlanCorrectional")}    
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
                v6.id as idFinishPicket, v6.numberVal as FinishPicket
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
            where o.id in (0${idsOwn.join(",")})
        """, "Obj.UnfinishedByDate", "plandata")
        // find pv...
        Store stPV = loadSqlMeta("""
            select id, prop, cls
            from PropVal
            where prop=${prop_WorkPlan} and cls=${cls_WorkPlanInspection}
        """, "")
        long pv = 0
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
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, o.cls, v.fullName, null as nameClsObject
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (${idsObject.join(",")})
        """, "", "objectdata")

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
    Set<String> loadDateWorkPlanCorrectional(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pv = UtCnv.toLong(params.get("pv"))
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkPlanCorrectional", "")

        Store stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v, Obj o
            where d.id=v.dataProp and d.objorrelobj=o.id and 
                o.cls=${map.get("Cls_WorkPlanCorrectional")} and v.propVal=${pv} and v.obj=${obj}    
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
    Store loadTaskLog(long id) {

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        Store st = mdb.createStore("Obj.task.log")


        String whe = "o.id=${id}"
        if (id==0)
            whe = "o.cls=${map.get("Cls_TaskLog")}"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idWorkPlan, v1.obj as objWorkPlan, v1.propVal as pvWorkPlan,
                v2.id as idTask, v2.obj as objTask, v2.propVal as pvTask, null as nameTask,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as nameUser,
                v4.id as idValuePlan, v4.numberVal as ValuePlan,
                v5.id as idValueFact, v5.numberVal as ValueFact,
                v6.id as idPlanDateStart, v6.dateTimeVal as PlanDateStart,
                v7.id as idPlanDateEnd, v7.dateTimeVal as PlanDateEnd,
                v8.id as idFactDateStart, v8.dateTimeVal as FactDateStart,
                v9.id as idFactDateEnd, v9.dateTimeVal as FactDateEnd,
                v10.id as idCreatedAt, v10.dateTimeVal as CreatedAt,
                v11.id as idUpdatedAt, v11.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_WorkPlan
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Task
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_ValuePlan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_ValueFact
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_PlanDateStart
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_PlanDateEnd
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_FactDateStart
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_FactDateEnd
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_CreatedAt
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_UpdatedAt
                left join DataPropVal v11 on d11.id=v11.dataprop
            where ${whe}
        """, map)
        //Пересечение
        Set<Object> idsTask = st.getUniqueValues("objTask")
        Store stTask = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTask.join(",")})
        """, "", "nsidata")
        StoreIndex indTask = stTask.getIndex("id")

        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord recTask = indTask.get(r.getLong("objTask"))
            if (recTask != null)
                r.set("nameTask", recTask.getString("fullName"))

            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("nameUser", recUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveTaskLogPlan(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
            par.put("cls", map.get("Cls_TaskLog"))
            par.put("fullName", par.get("name"))
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_WorkPlan
            if (pms.getLong("objWorkPlan") == 0)
                throw new XError("[WorkPlan] не указан")
            else
                fillProperties(true, "Prop_WorkPlan", pms)
            //2 Prop_Task
            if (pms.getLong("objTask") == 0)
                throw new XError("[Task] не указан")
            else
                fillProperties(true, "Prop_Task", pms)
            //3 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //4 Prop_ValuePlan
            if (pms.getDouble("ValuePlan") == 0)
                throw new XError("[ValuePlan] не указан")
            else
                fillProperties(true, "Prop_ValuePlan", pms)
            //5 Prop_PlanDateStart
            if (pms.getString("PlanDateStart").isEmpty())
                throw new XError("[PlanDateStart] не указан")
            else
                fillProperties(true, "Prop_PlanDateStart", pms)
            //6 Prop_PlanDateEnd
            if (pms.getString("PlanDateEnd").isEmpty())
                throw new XError("[PlanDateEnd] не указан")
            else
                fillProperties(true, "Prop_PlanDateEnd", pms)
            //7 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //8 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.put("fullName", par.get("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_Task
            if (pms.containsKey("idTask"))
                if (pms.getLong("objTask") == 0)
                    throw new XError("[Task] не указан")
                else
                    updateProperties("Prop_Task", pms)
            //2 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)
            //3 Prop_ValuePlan
            if (pms.containsKey("idValuePlan"))
                if (pms.getDouble("ValuePlan") == 0)
                    throw new XError("[ValuePlan] не указан")
                else
                    updateProperties("Prop_ValuePlan", pms)
            //4 Prop_PlanDateStart
            if (pms.containsKey("idPlanDateStart"))
                if (pms.getString("PlanDateStart").isEmpty())
                    throw new XError("[PlanDateStart] не указан")
                else
                    updateProperties("Prop_PlanDateStart", pms)
            //5 Prop_PlanDateEnd
            if (pms.containsKey("idPlanDateEnd"))
                if (pms.getString("PlanDateEnd").isEmpty())
                    throw new XError("[PlanDateEnd] не указан")
                else
                    updateProperties("Prop_PlanDateEnd", pms)
            //7 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadTaskLog(own)
    }

    @DaoMethod
    Store saveTaskLogFact(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own = pms.getLong("id")
        pms.put("own", own)
        //1 Prop_User
        if (pms.containsKey("idUser")) {
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                updateProperties("Prop_User", pms)
        }
        //2 Prop_ValueFact
        if (pms.containsKey("idValueFact")) {
            if (pms.getDouble("ValuePlan") == 0)
                throw new XError("[ValueFact] не указан")
            else
                updateProperties("Prop_ValueFact", pms)
        } else {
            if (pms.getDouble("ValueFact") > 0)
                fillProperties(true, "Prop_ValueFact", pms)
        }
        //3 Prop_FactDateStart
        if (pms.containsKey("idFactDateStart")) {
            if (pms.getString("FactDateStart").isEmpty())
                throw new XError("[FactDateStart] не указан")
            else
                updateProperties("Prop_FactDateStart", pms)
        } else {
            if (!pms.getString("FactDateStart").isEmpty())
                fillProperties(true, "Prop_FactDateStart", pms)
        }
        //3 Prop_FactDateEnd
        if (pms.containsKey("idFactDateEnd")) {
            if (pms.getString("FactDateEnd").isEmpty())
                throw new XError("[FactDateEnd] не указан")
            else
                updateProperties("Prop_FactDateEnd", pms)
        } else {
            if (!pms.getString("FactDateEnd").isEmpty())
                fillProperties(true, "Prop_FactDateEnd", pms)
        }
        //5 Prop_ReasonDeviation
        if (pms.containsKey("idUpdatedAt"))
            updateProperties("Prop_FactDateEnd", pms)
        else {
            if (!pms.getString("ReasonDeviation").isEmpty())
                fillProperties(true, "Prop_FactDateEnd", pms)
        }
        //6 Prop_UpdatedAt
        if (pms.containsKey("idUpdatedAt")) {
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                updateProperties("Prop_UpdatedAt", pms)
        }
        //
        return loadTaskLog(own)
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
                """, "", "nsidata")
                if (stData.size() > 0)
                    lstService.add("nsidata")
                //
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "objectdata")
                if (stData.size() > 0)
                    lstService.add("objectdata")
                //
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "orgstructuredata")
                if (stData.size() > 0)
                    lstService.add("orgstructuredata")
                //
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "personnaldata")
                if (stData.size() > 0)
                    lstService.add("personnaldata")
                //
                stData = loadSqlService("""
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
                """, "", "clientdata")
                if (stData.size() > 0)
                    lstService.add("clientndata")
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
        // Attrib str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_BIN")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib multistr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt") ||
                    cod.equalsIgnoreCase("Prop_PlanDateStart") ||
                    cod.equalsIgnoreCase("Prop_PlanDateEnd") ||
                    cod.equalsIgnoreCase("Prop_FactDateStart") ||
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
            if (cod.equalsIgnoreCase("Prop_Measure")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ValuePlan") ||
                    cod.equalsIgnoreCase("Prop_ValueFact")) {
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
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_WorkPlan") ||
                    cod.equalsIgnoreCase("Prop_Task") ||
                    cod.equalsIgnoreCase("Prop_User")) {
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
        // Attrib str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_BIN")) {   //For Template
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
        // Attrib multistr
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
        // Attrib date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt") ||
                    cod.equalsIgnoreCase("Prop_PlanDateStart") ||
                    cod.equalsIgnoreCase("Prop_PlanDateEnd") ||
                    cod.equalsIgnoreCase("Prop_FactDateStart") ||
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
            if (cod.equalsIgnoreCase("Prop_Measure")) {
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
        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ValuePlan") ||
                    cod.equalsIgnoreCase("Prop_ValueFact")) {
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
            if (cod.equalsIgnoreCase("Prop_WorkPlan") ||
                    cod.equalsIgnoreCase("Prop_Task") ||
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
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSql(sql, domain)
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
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSqlWithParams(sql, params, domain)
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
