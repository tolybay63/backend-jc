package dtj.incident.dao

import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.*
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.dta.model.utils.UtPeriod
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_AttribValType_consts
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.api.mdl.model.consts.FD_PeriodType_consts
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

@CompileStatic
class DataDao extends BaseMdbUtils {

    ApinatorApi apiAdm() {
        return app.bean(ApinatorService).getApi("adm")
    }

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }

    ApinatorApi apiNSIData() {
        return app.bean(ApinatorService).getApi("nsidata")
    }

    ApinatorApi apiObjectData() {
        return app.bean(ApinatorService).getApi("objectdata")
    }

    ApinatorApi apiPlanData() {
        return app.bean(ApinatorService).getApi("plandata")
    }

    ApinatorApi apiPersonnalData() {
        return app.bean(ApinatorService).getApi("personnaldata")
    }

    ApinatorApi apiOrgStructureData() {
        return app.bean(ApinatorService).getApi("orgstructuredata")
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

    /* =================================================================== */

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
    Store loadEvent(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Event", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Event")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_Event"), "Prop_Event")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Event")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Criticality", "")
        Store st = mdb.createStore("Obj.Event")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, null as pv,
                v1.id as idCriticality, v1.propVal as pvCriticality,  null as fvCriticality, null as nameCriticality
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Criticality
                left join DataPropVal v1 on d1.id=v1.dataprop
            where ${whe}
        """, map)

        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", true)

        for (StoreRecord record in st) {
            record.set("pv", pv)
            record.set("fvCriticality", mapPV.get(record.getLong("pvCriticality")))
        }
        Set<Object> fvs = st.getUniqueValues("fvCriticality")

        Store stFV = loadSqlMeta("""
            select id, name from Factor where id in (0${fvs.join(",")})
        """, "")

        StoreIndex indFV = stFV.getIndex("id")
        for (StoreRecord record in st) {
            StoreRecord rec = indFV.get(record.getLong("fvCriticality"))
            if (rec != null)
                record.set("nameCriticality", rec.getString("name"))
        }
        //mdb.outTable(st)
        return st
    }

    @DaoMethod
    Store saveEvent(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Event", "")
        Map<String, Object> par = new HashMap<>(pms)
        par.put("cls", map.get("Cls_Event"))
        par.putIfAbsent("fullName", pms.get("name"))
        if (mode.equalsIgnoreCase("ins")) {
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_Criticality
            if (pms.getLong("fvCriticality") > 0)
                fillProperties(true, "Prop_Criticality", pms)
            else
                throw new XError("Не указан [Критичность]")
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_Criticality
            if (pms.getLong("idCriticality") > 0) {
                if (pms.getLong("fvCriticality") > 0)
                    updateProperties("Prop_Criticality", pms)
                else
                    throw new XError("Не указан [Критичность]")
            }
        } else
            throw new XError("Не известный режим записи (mode = ins | upd) в БД")
        //
        return loadEvent(own)
    }

    @DaoMethod
    Store loadIncident(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.Incident")
        Store stCls = apiMeta().get(ApiMeta).loadCls("Typ_Incident")
        String whe
        String wheV17 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls in (0${stCls.getUniqueValues("id").join(",")})"
            //
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            tofi.api.mdl.utils.UtPeriod utPeriod = new tofi.api.mdl.utils.UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            d2 = d2.addDays(1)
            wheV17 = "and v17.dateTimeVal between '${d1}' and '${d2}'"
        }

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        String sql = """
            select o.id, o.cls, v.name, null as nameCls,
                v1.id as idEvent, v1.propVal as pvEvent, v1.obj as objEvent, ov1.name as nameEvent,
                v2.id as idObject, v2.propVal as pvObject, v2.obj as objObject, null as nameObject,
                v3.id as idUser, v3.propVal as pvUser, v3.obj as objUser, null as nameUser,
                v4.id as idParameterLog, v4.propVal as pvParameterLog, v4.obj as objParameterLog,
                v5.id as idFault, v5.propVal as pvFault, v5.obj as objFault,
                v6.id as idStatus, v6.propVal as pvStatus, null as fvStatus, null as nameStatus,
                v7.id as idCriticality, v7.propVal as pvCriticality, null as fvCriticality, null as nameCriticality,
                v8.id as idStartKm, v8.numberVal as StartKm,
                v9.id as idFinishKm, v9.numberVal as FinishKm,
                v10.id as idStartPicket, v10.numberVal as StartPicket,
                v11.id as idFinishPicket, v11.numberVal as FinishPicket,
                v12.id as idStartLink, v12.numberVal as StartLink,
                v13.id as idFinishLink, v13.numberVal as FinishLink,
                v14.id as idDescription, v14.multiStrVal as Description,
                v15.id as idCreatedAt, v15.dateTimeVal as CreatedAt,
                v16.id as idUpdatedAt, v16.dateTimeVal as UpdatedAt,
                v17.id as idRegistrationDateTime, v17.dateTimeVal as RegistrationDateTime,
                v18.id as idInfoApplicant, v18.strVal as InfoApplicant,
                v19.id as idLocationClsSection, v19.propVal as pvLocationClsSection, v19.obj as objLocationClsSection,
                v20.id as idWorkPlan, v20.propVal as pvWorkPlan, v20.obj as objWorkPlan,
                v21.id as idAssignDateTime, v21.dateTimeVal as AssignDateTime
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Event")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_User")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_ParameterLog")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_Fault")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_Status")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_Criticality")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=${map.get("Prop_Description")}
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=${map.get("Prop_CreatedAt")}
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=${map.get("Prop_UpdatedAt")}
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=${map.get("Prop_RegistrationDateTime")}
                inner join DataPropVal v17 on d17.id=v17.dataprop ${wheV17}
                left join DataProp d18 on d18.objorrelobj=o.id and d18.prop=${map.get("Prop_InfoApplicant")}
                left join DataPropVal v18 on d18.id=v18.dataprop
                left join DataProp d19 on d19.objorrelobj=o.id and d19.prop=${map.get("Prop_LocationClsSection")}
                left join DataPropVal v19 on d19.id=v19.dataprop
                left join DataProp d20 on d20.objorrelobj=o.id and d20.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v20 on d20.id=v20.dataprop
                left join DataProp d21 on d21.objorrelobj=o.id and d21.prop=${map.get("Prop_AssignDateTime")}
                left join DataPropVal v21 on d21.id=v21.dataprop
            where ${whe}
        """
        mdb.loadQuery(st, sql, map)
        //... Пересечение
        Set<Object> idsCls = st.getUniqueValues("cls")
        stCls = loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.id in (0${idsCls.join(",")})
        """, "")
        StoreIndex indCls = stCls.getIndex("id")
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, v.fullName from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObject = stObject.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, v.fullName from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Set<Object> pvsStatus = st.getUniqueValues("pvStatus")
        Store stPV = loadSqlMeta("""
            select p.id, p.factorVal, f.name
            from Propval p, Factor f
            where p.id in (0${pvsStatus.join(",")}) and p.factorVal=f.id            
        """, "")
        StoreIndex indStatus = stPV.getIndex("id")
        //
        Set<Object> pvsCriticality = st.getUniqueValues("pvCriticality")
        stPV = loadSqlMeta("""
            select p.id, p.factorVal, f.name
            from Propval p, Factor f
            where p.id in (0${pvsCriticality.join(",")}) and p.factorVal=f.id            
        """, "")
        StoreIndex indCriticality = stPV.getIndex("id")
        //
        Set<Object> idsWorkPlan = st.getUniqueValues("objWorkPlan")
        Store stWorkPlan = loadSqlService("""
            select o.id, 
                v1.id as idWork, v1.obj as objWork, v1.propVal as pvWork,
                v2.id as idPlanDateEnd, v2.dateTimeVal as PlanDateEnd,
                v3.id as idFactDateEnd, v3.dateTimeVal as FactDateEnd
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v3 on d3.id=v3.dataProp
            where o.id in (0${idsWorkPlan.join(",")})                
        """, "", "plandata")
        StoreIndex indWorkPlan = stWorkPlan.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recCls = indCls.get(r.getLong("cls"))
            if (recCls != null)
                r.set("nameCls", recCls.getString("name"))
            //
            StoreRecord recObject = indObject.get(r.getLong("objObject"))
            if (recObject != null)
                r.set("nameObject", recObject.getString("fullName"))
            //
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("nameUser", recUser.getString("fullName"))
            //
            StoreRecord recStatus = indStatus.get(r.getLong("pvStatus"))
            if (recStatus != null) {
                r.set("fvStatus", recStatus.getLong("factorVal"))
                r.set("nameStatus", recStatus.getString("name"))
            }
            //
            StoreRecord recCriticality = indCriticality.get(r.getLong("pvCriticality"))
            if (recCriticality != null) {
                r.set("fvCriticality", recCriticality.getLong("factorVal"))
                r.set("nameCriticality", recCriticality.getString("name"))
            }
            //
            StoreRecord recWorkPlan = indWorkPlan.get(r.getLong("objWorkPlan"))
            if (recWorkPlan != null) {
                r.set("idWork", recWorkPlan.getLong("idWork"))
                r.set("objWork", recWorkPlan.getLong("objWork"))
                r.set("pvWork", recWorkPlan.getLong("pvWork"))
                r.set("idPlanDateEnd", recWorkPlan.getLong("idPlanDateEnd"))
                r.set("PlanDateEnd", recWorkPlan.getString("PlanDateEnd"))
                r.set("idFactDateEnd", recWorkPlan.getLong("idFactDateEnd"))
                r.set("FactDateEnd", recWorkPlan.getString("FactDateEnd"))
            }
        }
        //
        Set<Object> idsWork = st.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, v.fullName from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        //
        StoreIndex indWork = stWork.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord recWork = indWork.get(r.getLong("objWork"))
            if (recWork != null) {
                r.set("fullNameWork", recWork.getString("fullName"))
            }

        }

        return st
    }

    @DaoMethod
    Store saveIncident(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)

        long own
        if (mode.equalsIgnoreCase("ins")) {
            own = apiIncidentData().get(ApiIncidentData).saveIncident("ins", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            throw new XError("Режим [update] отключен")
        } else {

            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }
        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        return loadIncident(mapRez)
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
                stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "incidentdata")
                if (stData.size() > 0)
                    lstService.add("incidentdata")
                //

                if (lstService.size() > 0) {
                    throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                }

            }
        }
    }

    //********************************************************************
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
            if (cod.equalsIgnoreCase("Prop_NumberSource")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // Attrib str multiStr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib str dt
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        if ([FD_AttribValType_consts.dttm].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_RegistrationDateTime")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Criticality") ||
                    cod.equalsIgnoreCase("Prop_Status")) {
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
                if (params.get(keyValue) != null) {
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
            if (cod.equalsIgnoreCase("Prop_Event") ||
                    cod.equalsIgnoreCase("Prop_Object") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_ParameterLog") ||
                    cod.equalsIgnoreCase("Prop_Fault")) {
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
            if (cod.equalsIgnoreCase("Prop_NumberSource")) {
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
                if (params.get(keyValue) != null) {
                    sql = "update DataPropval set multiStrVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_DocumentApprovalDate")) {
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
            if (cod.equalsIgnoreCase("Prop_Criticality")) {
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


        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_StartKm")) {
                if (mapProp.keySet().contains(keyValue) && mapProp[keyValue] != 0) {
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
            if (cod.equalsIgnoreCase("Prop_DefectsComponent")) {
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
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
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
