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
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
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

        long pv = apiMeta().get(ApiMeta).idPV("cls", st.get(0).getLong("cls"), "Prop_LocationClsSection")
        for (StoreRecord r in st) {
            r.set("pv", pv)
        }
        return st
    }

    @DaoMethod
    Set<String> loadDateWorkPlanInspection(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pv = UtCnv.toLong(params.get("pv"))
        Store stTmp = loadSqlService("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and v.propVal=${pv} and v.obj=${obj}    
        """, "", "plandata")
        Set<Object> idsOwn = stTmp.getUniqueValues("own")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_____DateEnd")
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
        long idPv = apiMeta().get(ApiMeta).idPV("cls", stTmp.get(0).getLong("cls"), "Prop_Defect")
        for (StoreRecord r in stTmp) {
            r.set("pv", idPv)
        }
        //
        return stTmp
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

            //1.1 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", pms)
            else
                throw new XError("[objLocationClsSection] not specified")

            //2 Prop_Inspection
            if (pms.getLong("objInspection") > 0)
                fillProperties(true, "Prop_Inspection", pms)
            else
                throw new XError("[objInspection] not specified")
            //3 Prop_StartKm
            if (pms.getString("StartKm") != "")
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] not specified")

            //4 Prop_FinishKm
            if (pms.getString("FinishKm") != "")
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] not specified")

            //5 Prop_StartPicket
            if (pms.getString("StartPicket") != "")
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] not specified")

            //6 Prop_FinishPicket
            if (pms.getString("FinishPicket") != "")
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] not specified")

            //7 Prop_StartLink
            if (pms.getString("StartLink") != "")
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("[StartLink] not specified")

            //8 Prop_FinishLink
            if (pms.getString("FinishLink") != "")
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("[FinishLink] not specified")


            //9 Prop_CreationDateTime
            if (pms.getString("CreationDateTime") != "")
                fillProperties(true, "Prop_CreationDateTime", pms)
            else
                throw new XError("[CreationDateTime] not specified")

            //9.1 Prop_FactDateEnd
            if (pms.getString("FactDateEnd") != "")
                fillProperties(true, "Prop_FactDateEnd", pms)
            else
                throw new XError("[FactDateEnd] not specified")

            //10 Prop_Description
            if (pms.getString("Description") != "")
                fillProperties(true, "Prop_Description", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.put("fullName", par.get("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_Defect
            updateProperties("Prop_Defect", pms)

            //1.1 Prop_LocationClsSection
            updateProperties("Prop_LocationClsSection", pms)

            //2 Prop_Inspection
            updateProperties("Prop_Inspection", pms)
            //3 Prop_StartKm
            updateProperties("Prop_StartKm", pms)
            //4 Prop_FinishKm
            updateProperties("Prop_FinishKm", pms)
            //5Prop_StartPicket
            updateProperties("Prop_StartPicket", pms)
            //6 Prop_FinishPicket
            updateProperties("Prop_FinishPicket", pms)
            //7 Prop_StartLink
            updateProperties("Prop_StartLink", pms)
            //8 Prop_FinishLink
            updateProperties("Prop_FinishLink", pms)
            //9 Prop_CreationDateTime
            updateProperties("Prop_CreationDateTime", pms)
            //9.1 Prop_FactDateEnd
            updateProperties("Prop_FactDateEnd", pms)
            //10 Prop_CreatedAt
            if (pms.containsKey("idDescription"))
                updateProperties("Prop_Description", pms)
            else {
                if (pms.getString("Description") != "")
                    fillProperties(true, "Prop_Description", pms)
            }
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        return null
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
            where o.id in (${idsOwn.join(",")})
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
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
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
                v2.id as idWorkPlan, v2.propVal as pvWorkPlan, v2.obj as objWorkPlan, 
                v3.id as idStartKm, v3.numberVal as StartKm,
                v4.id as idFinishKm, v4.numberVal as FinishKm,
                v5.id as idStartPicket, v5.numberVal as StartPicket,
                v6.id as idFinishPicket, v6.numberVal as FinishPicket,
                v7.id as idFactDateEnd, v7.dateTimeVal as FactDateEnd,
                v8.id as idUser, v8.propVal as pvUser, v8.obj as objUser, null as fullNameUser,
                v9.id as idCreatedAt, v9.dateTimeVal as CreatedAt,
                v10.id as idUpdatedAt, v10.dateTimeVal as UpdatedAt,
                v11.id as idTrueDefect, v11.propVal as pvTrueDefect, null as fvTrueDefect,
                    null as nameTrueDefect,
                v12.id as idStartLink, v12.numberVal as StartLink,
                v13.id as idFinishLink, v13.numberVal as FinishLink,
                v14.id as idReasonDeviation, v14.multiStrVal as ReasonDeviation,
                v15.id as idTrueParameter, v15.propVal as pvTrueParameter, null as fvTrueParameter,
                    null as nameTrueParameter
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
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_TrueDefect
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_ReasonDeviation
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_TrueParameter
                left join DataPropVal v15 on d15.id=v15.dataprop
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
        //
        Set<Object> idsWorkPlan = st.getUniqueValues("objWorkPlan")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store stWPprops = loadSqlService("""
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

        StoreIndex indWPprops = stWPprops.getIndex("id")
        //
        Set<Object> pvsTrueDefect = st.getUniqueValues("pvTrueDefect")
        Store stTrueDefect = loadSqlMeta("""
            select pv.id, pv.factorval, f.name
            from PropVal pv
                left join Factor f on pv.factorVal=f.id 
            where pv.id in (0${pvsTrueDefect.join(",")})
        """, "")
        StoreIndex indTrueDefect = stTrueDefect.getIndex("id")

        Set<Object> pvsTrueParameter = st.getUniqueValues("pvTrueParameter")
        Store stTrueParameter = loadSqlMeta("""
            select pv.id, pv.factorval, f.name
            from PropVal pv
                left join Factor f on pv.factorVal=f.id 
            where pv.id in (0${pvsTrueParameter.join(",")})
        """, "")
        StoreIndex indTrueParameter = stTrueParameter.getIndex("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Personnel", "")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get("Cls_Personnel")}
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rLocation = indLocation.get(r.getLong("objLocationClsSection"))
            if (rLocation != null)
                r.set("nameLocationClsSection", rLocation.getString("name"))

            StoreRecord rWPprops = indWPprops.get(r.getLong("objWorkPlan"))
            if (rWPprops != null) {
                r.set("objWork", rWPprops.getLong("objWork"))
                r.set("objObject", rWPprops.getLong("objObject"))
                r.set("PlanDateEnd", rWPprops.getString("PlanDateEnd"))
                r.set("ActualDateEnd", rWPprops.getString("ActualDateEnd"))
            }

            StoreRecord rTrueDefect = indTrueDefect.get(r.getLong("pvTrueDefect"))
            if (rTrueDefect != null) {
                r.set("fvTrueDefect", rTrueDefect.getLong("factorval"))
                r.set("nameTrueDefect", rTrueDefect.getString("name"))
            }
            StoreRecord rTrueParameter = indTrueParameter.get(r.getLong("pvTrueParameter"))
            if (rTrueParameter != null) {
                r.set("fvTrueParameter", rTrueParameter.getLong("factorval"))
                r.set("nameTrueParameter", rTrueParameter.getString("name"))
            }

            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rUser != null)
                r.set("fullNameUser", rUser.getString("fullName"))
        }
        //
        Set<Object> idsWork = st.getUniqueValues("objWork")
        Set<Object> idsObject = st.getUniqueValues("objObject")
        Store stWork = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        StoreIndex indWork = stWork.getIndex("id")
        //
        Store stObject = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObject = stObject.getIndex("id")

        //...
        for (StoreRecord r in st) {
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null)
                r.set("fullNameWork", rWork.getString("fullName"))
            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null)
                r.set("fullNameObject", rObject.getString("fullName"))

        }
        //
        //idsObject = st.getUniqueValues("objObject")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Section", "")
        stObject = loadSqlService("""
            select o.id, v.obj, ov.name
            from Obj o
                left join DataProp d on d.objorrelobj=o.id and prop=${map.get("Prop_Section")}
                left join DataPropVal v on d.id=v.dataProp
                left join ObjVer ov on v.obj=ov.ownerVer and ov.lastVer=1            
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        indObject = stObject.getIndex("id")
        for (StoreRecord r : st) {
            StoreRecord rObj = indObject.get(r.getLong("objObject"))
            if (rObj != null) {
                r.set("objSection", rObj.getLong("obj"))
                r.set("nameSection", rObj.getString("name"))
            }
        }
        //
        return st
    }

    @DaoMethod
    Store saveInspection(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //StartLink
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
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

            stTmp = mdb.loadQuery("""
                select o.id,
                    v3.numberVal * 1000 + v5.numberVal * 100 + v12.numberVal * 25 as beg,
                    v4.numberVal * 1000 + v6.numberVal * 100 + v13.numberVal * 25 as end
                from Obj o 
                    left join ObjVer v on o.id=v.ownerver and v.lastver=1
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StartKm
                    left join DataPropVal v3 on d3.id=v3.dataprop
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishKm
                    left join DataPropVal v4 on d4.id=v4.dataprop
                    left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_StartPicket
                    left join DataPropVal v5 on d5.id=v5.dataprop
                    left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_FinishPicket
                    left join DataPropVal v6 on d6.id=v6.dataprop
                    left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_StartLink
                    left join DataPropVal v12 on d12.id=v12.dataprop
                    left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_FinishLink
                    left join DataPropVal v13 on d13.id=v13.dataprop
                where o.id in (${idsOwn.join(",")})
            """, map)
            boolean bOk = true
            for (StoreRecord r in stTmp) {
                if (!(beg > r.getInt("end") || end < r.getInt("beg")))
                    bOk = false
            }
            if (!bOk)
                throw new XError("По данным координатам существует запись")
        }        //
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
            long pvTrueDefect = apiMeta().get(ApiMeta).idPV("factorVal", idFV_False, "Prop_TrueDefect")
            long pvTrueParameter = apiMeta().get(ApiMeta).idPV("factorVal", idFV_False, "Prop_TrueParameter")

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
            //4 Prop_TrueDefect
            pms.put("fvTrueDefect", idFV_False)
            pms.put("pvTrueDefect", pvTrueDefect)
            fillProperties(true, "Prop_TrueDefect", pms)
            //4.1 Prop_TrueParameter
            pms.put("fvTrueParameter", idFV_False)
            pms.put("pvTrueParameter", pvTrueParameter)
            fillProperties(true, "Prop_TrueParameter", pms)
            //

            //5 Prop_StartKm
            if (pms.getString("StartKm") != "")
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] not specified")

            //6 Prop_FinishKm
            if (pms.getString("FinishKm") != "")
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] not specified")

            //7 Prop_StartPicket
            if (pms.getString("StartPicket") != "")
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] not specified")

            //8 Prop_FinishPicket
            if (pms.getString("FinishPicket") != "")
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] not specified")

            //9 Prop_StartLink
            if (pms.getString("StartLink") != "")
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("[StartLink] not specified")

            //10 Prop_FinishLink
            if (pms.getString("FinishLink") != "")
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("[FinishLink] not specified")


            //11 Prop_FactDateEnd
            if (pms.getString("FactDateEnd") != "")
                fillProperties(true, "Prop_FactDateEnd", pms)
            else
                throw new XError("[FactDateEnd] not specified")

            //12 Prop_CreatedAt
            if (pms.getString("CreatedAt") != "")
                fillProperties(true, "Prop_CreatedAt", pms)
            //13 Prop_UpdatedAt
            if (pms.getString("UpdatedAt") != "")
                fillProperties(true, "Prop_UpdatedAt", pms)

            //14 Prop_ReasonDeviation
            if (pms.getString("ReasonDeviation") != "")
                fillProperties(true, "Prop_ReasonDeviation", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.put("fullName", par.get("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_LocationClsSection
            updateProperties("Prop_LocationClsSection", pms)
            //2 Prop_WorkPlan
            updateProperties("Prop_WorkPlan", pms)
            //3 Prop_User
            updateProperties("Prop_User", pms)

            //5 Prop_StartKm
            updateProperties("Prop_StartKm", pms)
            //6 Prop_FinishKm
            updateProperties("Prop_FinishKm", pms)
            //7 Prop_StartPicket
            updateProperties("Prop_StartPicket", pms)
            //8 Prop_FinishPicket
            updateProperties("Prop_FinishPicket", pms)
            //9 Prop_StartLink
            updateProperties("Prop_StartLink", pms)
            //10 Prop_FinishLink
            updateProperties("Prop_FinishLink", pms)
            //11 Prop_FactDateEnd
            updateProperties("Prop_FactDateEnd", pms)
            //12 Prop_CreatedAt
            if (pms.containsKey("idCreatedAt"))
                updateProperties("Prop_CreatedAt", pms)
            else {
                if (pms.getString("CreatedAt") != "")
                    fillProperties(true, "Prop_CreatedAt", pms)
            }
            //13 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                updateProperties("Prop_UpdatedAt", pms)
            else {
                if (pms.getString("UpdatedAt") != "")
                    fillProperties(true, "Prop_UpdatedAt", pms)
            }
            //14 Prop_ReasonDeviation
            if (pms.containsKey("idReasonDeviation"))
                updateProperties("Prop_ReasonDeviation", pms)
            else {
                if (pms.getString("ReasonDeviation") != "")
                    fillProperties(true, "Prop_ReasonDeviation", pms)
            }
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        return loadInspection(mapRez)
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
                Store stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "inspectiondata")
                if (stData.size() > 0)
                    lstService.add("inspectiondata")
                //
                stData = loadSqlService("""
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
            if ( cod.equalsIgnoreCase("Prop_TabNumber")) {
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
            if ( cod.equalsIgnoreCase("Prop_TrueDefect") ||
                    cod.equalsIgnoreCase("Prop_TrueParameter")) {
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
            if ( cod.equalsIgnoreCase("Prop_ReasonDeviation") ||
                    cod.equalsIgnoreCase("Prop_Discreption")) {
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
            if ( cod.equalsIgnoreCase("Prop_DeviationDefect")) {
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
