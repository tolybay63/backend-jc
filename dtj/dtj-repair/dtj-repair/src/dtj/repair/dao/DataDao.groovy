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
    ApinatorApi apiClientData() {
        return app.bean(ApinatorService).getApi("clientdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }
    ApinatorApi apiIncidentData() {return app.bean(ApinatorService).getApi("incidentdata")}

    @DaoMethod
    List<Map<String, Object>> loadResourceMaterialFact(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceMaterialFact")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceMaterial", "")
        map.put("cls", map2.get("Cls_ResourceMaterial"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "", "FV_%")
        map.put("FV_Plan", map2.get("FV_Plan"))
        map.put("FV_Fact", map2.get("FV_Fact"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idMaterial, v1.obj as objMaterial, v1.propVal as pvMaterial, null as nameMaterial,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value, v5.numberVal as ValuePlan,
                v6.id as idMeasure, v6.propVal as pvMeasure, null as meaMeasure,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Material
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Fact
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Value and d5.status=:FV_Plan
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_Measure
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Set<Object> idsMaterial = st.getUniqueValues("objMaterial")
        Store stMaterial = loadSqlService("""
            select o.id, o.cls, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsMaterial.join(",")})
        """, "", "resourcedata")
        StoreIndex indMaterial = stMaterial.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Map<Long, Long> mapMea = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_Measure", true)
        Store stMea = loadSqlMeta("""
            select id, name from Measure where 0=0
        """, "")
        StoreIndex indMea = stMea.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recMaterial = indMaterial.get(r.getLong("objMaterial"))
            if (recMaterial != null)
                r.set("nameMaterial", recMaterial.getString("name"))

            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))

            if (r.getLong("pvMeasure") > 0) {
                r.set("meaMeasure", mapMea.get(r.getLong("pvMeasure")))
            }
            StoreRecord rec = indMea.get(r.getLong("meaMeasure"))
            if (rec != null)
                r.set("nameMeasure", rec.getString("name"))
        }
        //
        List<Map<String, Object>> lstRes = new ArrayList<>()
        if (st.size() > 0) {
            for (StoreRecord r in st) {
                Map<String, Object> mapRez = new HashMap<>()
                mapRez.putAll(r.getValues())
                lstRes.add(mapRez)
            }
        }
        return lstRes
    }

    @DaoMethod
    List<Map<String, Object>> loadResourceTpServiceFact(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceTpServiceFact")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceTpService", "")
        map.put("cls", map2.get("Cls_ResourceTpService"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "", "FV_%")
        map.put("FV_Plan", map2.get("FV_Plan"))
        map.put("FV_Fact", map2.get("FV_Fact"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTpService, v1.obj as objTpService, v1.propVal as pvTpService, 
                    null as nameTpService, null as fullNameTpService,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value, v5.numberVal as ValuePlan,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TpService
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Fact
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Value and d5.status=:FV_Plan
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Set<Object> idsTpService = st.getUniqueValues("objTpService")
        Store stTpService = loadSqlService("""
            select o.id, o.cls, v.name, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTpService.join(",")})
        """, "", "resourcedata")
        StoreIndex indTpService = stTpService.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recTpService = indTpService.get(r.getLong("objTpService"))
            if (recTpService != null) {
                r.set("nameTpService", recTpService.getString("name"))
                r.set("fullNameTpService", recTpService.getString("fullName"))
            }

            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        List<Map<String, Object>> lstRes = new ArrayList<>()
        if (st.size() > 0) {
            for (StoreRecord r in st) {
                Map<String, Object> mapRez = new HashMap<>()
                mapRez.putAll(r.getValues())
                lstRes.add(mapRez)
            }
        }
        return lstRes
    }

    @DaoMethod
    List<Map<String, Object>> loadResourceEquipmentFact(long objTaskLog) {
        List<Map<String, Object>> lstRes = new ArrayList<>()
        Store stPlan = loadResourceEquipment(objTaskLog)
        //
        if (stPlan.size() > 0) {
            Set<Object> idsPlan = stPlan.getUniqueValues("id")
            String whe = "o.id in (${idsPlan.join(",")})"
            Store st = mdb.createStore("Obj.Complex.Equipment")
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_Equipment%")
            mdb.loadQuery(st, """
                select o.id, o.cls,
                    v1.id as idEquipmentComplex, v1.strVal as EquipmentComplex,
                    v2.id as idEquipment, v2.obj as objEquipment, v2.propVal as pvEquipment, null as nameEquipment,
                    v3.id as idEquipmentValue, v3.numberVal as EquipmentValue
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_EquipmentComplex
                    inner join DataPropVal v1 on d1.id=v1.dataProp
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Equipment
                    inner join DataPropVal v2 on d2.id=v2.dataProp and v2.parent=v1.id
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_EquipmentValue
                    inner join DataPropVal v3 on d3.id=v3.dataProp and v3.parent=v1.id
                where ${whe}
            """, map)
            //
            if (st.size() > 0) {
                //Пересечение
                Set<Object> idsEquipment = st.getUniqueValues("objEquipment")
                Store stEquipment = loadSqlService("""
                    select o.id, o.cls, v.name
                    from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsEquipment.join(",")})
                """, "", "resourcedata")
                StoreIndex indEquipment = stEquipment.getIndex("id")
                //
                for (StoreRecord r in st) {
                    StoreRecord recEquipment = indEquipment.get(r.getLong("objEquipment"))
                    if (recEquipment != null)
                        r.set("nameEquipment", recEquipment.getString("name"))
                }
                //
                for (StoreRecord r in stPlan) {
                    Map<String, Object> mapR = new HashMap<>()
                    List<Map<String, Object>> lst = new ArrayList<>()
                    mapR.putAll(r.getValues())
                    //
                    for (StoreRecord p in st) {
                        if (r.getLong("id") == p.getLong("id")) {
                            Map<String, Object> mapP = new HashMap<>()
                            p.set("id", null)
                            p.set("cls", null)
                            mapP.putAll(p.getValues())
                            lst.add(mapP)
                        }
                    }
                    mapR.put("complex", lst)
                    //
                    lstRes.add(mapR)
                }
            } else {
                for (StoreRecord r in stPlan) {
                    Map<String, Object> mapR = new HashMap<>()
                    mapR.putAll(r.getValues())
                    //
                    lstRes.add(mapR)
                }
            }
        }
        //
        return lstRes
    }

    @DaoMethod
    List<Map<String, Object>> loadResourceToolFact(long objTaskLog) {
        List<Map<String, Object>> lstRes = new ArrayList<>()
        Store stPlan = loadResourceTool(objTaskLog)
        //
        if (stPlan.size() > 0) {
            Set<Object> idsPlan = stPlan.getUniqueValues("id")
            String whe = "o.id in (${idsPlan.join(",")})"
            Store st = mdb.createStore("Obj.Complex.Tool")
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_Tool%")
            mdb.loadQuery(st, """
                select o.id, o.cls,
                    v1.id as idToolComplex, v1.strVal as ToolComplex,
                    v2.id as idTool, v2.obj as objTool, v2.propVal as pvTool, null as nameTool,
                    v3.id as idToolValue, v3.numberVal as ToolValue
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ToolComplex
                    inner join DataPropVal v1 on d1.id=v1.dataProp
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Tool
                    inner join DataPropVal v2 on d2.id=v2.dataProp and v2.parent=v1.id
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_ToolValue
                    inner join DataPropVal v3 on d3.id=v3.dataProp and v3.parent=v1.id
                where ${whe}
            """, map)
            //
            if (st.size() > 0) {
                //Пересечение
                map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Number", "")
                Set<Object> idsTool = st.getUniqueValues("objTool")
                Store stTool = loadSqlService("""
                    select o.id, o.cls, '[№' || coalesce(v1.strVal, '') ||'] ' || v.name as name
                    from Obj o
                        left join ObjVer v on o.id=v.ownerver and v.lastver=1
                        left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Number")}
                        left join DataPropVal v1 on d1.id=v1.dataprop
                    where o.id in (0${idsTool.join(",")})
                """, "", "resourcedata")
                StoreIndex indTool = stTool.getIndex("id")
                //
                for (StoreRecord r in st) {
                    StoreRecord recTool = indTool.get(r.getLong("objTool"))
                    if (recTool != null)
                        r.set("nameTool", recTool.getString("name"))
                }
                //
                for (StoreRecord r in stPlan) {
                    Map<String, Object> mapR = new HashMap<>()
                    List<Map<String, Object>> lst = new ArrayList<>()
                    mapR.putAll(r.getValues())
                    //
                    for (StoreRecord p in st) {
                        if (r.getLong("id") == p.getLong("id")) {
                            Map<String, Object> mapP = new HashMap<>()
                            p.set("id", null)
                            p.set("cls", null)
                            mapP.putAll(p.getValues())
                            lst.add(mapP)
                        }
                    }
                    mapR.put("complex", lst)
                    //
                    lstRes.add(mapR)
                }
            } else {
                for (StoreRecord r in stPlan) {
                    Map<String, Object> mapR = new HashMap<>()
                    mapR.putAll(r.getValues())
                    //
                    lstRes.add(mapR)
                }
            }
        }
        //
        return lstRes
    }

    @DaoMethod
    List<Map<String, Object>> loadResourcePersonnelFact(long objTaskLog) {
        List<Map<String, Object>> lstRes = new ArrayList<>()
        Store stPlan = loadResourcePersonnel(objTaskLog)
        //
        if (stPlan.size() > 0) {
            Set<Object> idsPlan = stPlan.getUniqueValues("id")
            String whe = "o.id in (${idsPlan.join(",")})"
            Store st = mdb.createStore("Obj.Complex.Personnel")
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_Performer%")
            mdb.loadQuery(st, """
                select o.id, o.cls,
                    v1.id as idPerformerComplex, v1.strVal as PerformerComplex,
                    v2.id as idPerformer, v2.obj as objPerformer, v2.propVal as pvPerformer, null as fullNamePerformer,
                    v3.id as idPerformerValue, v3.numberVal as PerformerValue
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_PerformerComplex
                    inner join DataPropVal v1 on d1.id=v1.dataProp
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Performer
                    inner join DataPropVal v2 on d2.id=v2.dataProp and v2.parent=v1.id
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_PerformerValue
                    inner join DataPropVal v3 on d3.id=v3.dataProp and v3.parent=v1.id
                where ${whe}
            """, map)
            //
            if (st.size() > 0) {
                //Пересечение
                Set<Object> idsPerformer = st.getUniqueValues("objPerformer")
                Store stPerformer = loadSqlService("""
                    select o.id, o.cls, v.fullName
                    from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsPerformer.join(",")})
                """, "", "personnaldata")
                StoreIndex indPerformer = stPerformer.getIndex("id")
                //
                for (StoreRecord r in st) {
                    StoreRecord recPerformer = indPerformer.get(r.getLong("objPerformer"))
                    if (recPerformer != null)
                        r.set("fullNamePerformer", recPerformer.getString("fullName"))
                }
                for (StoreRecord r in stPlan) {
                    Map<String, Object> mapR = new HashMap<>()
                    List<Map<String, Object>> lst = new ArrayList<>()
                    mapR.putAll(r.getValues())
                    //
                    for (StoreRecord p in st) {
                        if (r.getLong("id") == p.getLong("id")) {
                            Map<String, Object> mapP = new HashMap<>()
                            p.set("id", null)
                            p.set("cls", null)
                            mapP.putAll(p.getValues())
                            lst.add(mapP)
                        }
                    }
                    mapR.put("complex", lst)
                    //
                    lstRes.add(mapR)
                }
            } else {
                for (StoreRecord r in stPlan) {
                    Map<String, Object> mapR = new HashMap<>()
                    mapR.putAll(r.getValues())
                    //
                    lstRes.add(mapR)
                }
            }
        }
        //
        return lstRes
    }

    @DaoMethod
    void deleteComplexData(long id) {
        mdb.execQuery("""
            delete from DataPropVal where parent=${id};
            delete from DataPropVal where id=${id};
            delete from DataProp 
            where id in (
                select id from DataProp
                except
                select dataProp as id from DataPropVal
            );
        """)
    }

    //todo
    @DaoMethod
    Store loadComplex(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")

        String whe
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls = ${map.get("Cls_TaskLog")}"
        }

        Store st = mdb.createStore("Obj.Complex")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls,
                v1.id as idPerformerComplex, v1.strVal as PerformerComplex,
                v2.id as idPerformer, v2.obj as objPerformer, v2.propVal as pvPerformer,
                v3.id as idPerformerValue, v3.numberVal as PerformerValue
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_PerformerComplex
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Performer
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_PerformerValue
                left join DataPropVal v3 on d3.id=v3.dataProp
            where ${whe}
        """, map)

        return st
    }

    @DaoMethod
    void saveResourceFact(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
        pms.put("fvStatus", map.get("FV_Fact"))
        //
        long own = pms.getLong("id")
        pms.put("own", own)
        //1 Prop_User
        if (pms.containsKey("idUser")) {
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                updateProperties("Prop_User", pms)
        }
        //2 Prop_Value
        if (pms.containsKey("idValue")) {
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                updateProperties("Prop_Value", pms)
        } else {
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
        }
        //3 Prop_UpdatedAt
        if (pms.containsKey("idUpdatedAt")) {
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                updateProperties("Prop_UpdatedAt", pms)
        }
    }

    @DaoMethod
    void saveComplexEquipment(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own = pms.getLong("id")
        pms.put("own", own)
        if (mode.equalsIgnoreCase("ins")) {
            pms.remove("idComplex")
            pms.put("EquipmentComplex", "EquipmentComplex-" + own + "-" + pms.getString("objEquipment"))

            mdb.startTran()
            try {
                fillProperties(true, "Prop_EquipmentComplex", pms)
                //
                if (pms.getLong("objEquipment") > 0)
                    fillProperties(true, "Prop_Equipment", pms)
                else
                    throw new XError("[Equipment] не указан")

                if (pms.getDouble("EquipmentValue") > 0)
                    fillProperties(true, "Prop_EquipmentValue", pms)
                else
                    throw new XError("[EquipmentValue] не указан")
                mdb.commit()
            } catch (Exception e) {
                mdb.rollback(e)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            //1 Prop_Equipment
            if (pms.containsKey("idEquipment"))
                if (pms.getLong("objEquipment") == 0)
                    throw new XError("[Equipment] не указан")
                else
                    updateProperties("Prop_Equipment", pms)

            //2 Prop_EquipmentValue
            if (pms.containsKey("idEquipmentValue"))
                if (pms.getDouble("EquipmentValue") == 0)
                    throw new XError("[EquipmentValue] не указан")
                else
                    updateProperties("Prop_EquipmentValue", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        //return loadComplex(mapRez)
    }

    @DaoMethod
    void saveComplexTool(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own = pms.getLong("id")
        pms.put("own", own)
        if (mode.equalsIgnoreCase("ins")) {
            pms.remove("idComplex")
            pms.put("ToolComplex", "ToolComplex-" + own + "-" + pms.getString("objTool"))

            mdb.startTran()
            try {
                fillProperties(true, "Prop_ToolComplex", pms)
                //
                if (pms.getLong("objTool") > 0)
                    fillProperties(true, "Prop_Tool", pms)
                else
                    throw new XError("[Tool] не указан")

                if (pms.getDouble("ToolValue") > 0)
                    fillProperties(true, "Prop_ToolValue", pms)
                else
                    throw new XError("[ToolValue] не указан")
                mdb.commit()
            } catch (Exception e) {
                mdb.rollback(e)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            //1 Prop_Tool
            if (pms.containsKey("idTool"))
                if (pms.getLong("objTool") == 0)
                    throw new XError("[Tool] не указан")
                else
                    updateProperties("Prop_Tool", pms)

            //2 Prop_ToolValue
            if (pms.containsKey("idToolValue"))
                if (pms.getDouble("ToolValue") == 0)
                    throw new XError("[ToolValue] не указан")
                else
                    updateProperties("Prop_ToolValue", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        //return loadComplex(mapRez)
    }

    @DaoMethod
    void saveComplexPersonnel(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //Prop_PerformerComplex Prop_Performer  Prop_PerformerValue
        long own = pms.getLong("id")
        pms.put("own", own)
        if (mode.equalsIgnoreCase("ins")) {
            pms.remove("idComplex")
            pms.put("PerformerComplex", "PerformerComplex-" + own + "-" + pms.getString("objPerformer"))

            mdb.startTran()
            try {
                fillProperties(true, "Prop_PerformerComplex", pms)
                //
                if (pms.getLong("objPerformer") > 0)
                    fillProperties(true, "Prop_Performer", pms)
                else
                    throw new XError("[Performer] не указан")

                if (pms.getDouble("PerformerValue") > 0)
                    fillProperties(true, "Prop_PerformerValue", pms)
                else
                    throw new XError("[PerformerValue] не указан")
                mdb.commit()
            } catch (Exception e) {
                mdb.rollback(e)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            //1 Prop_Performer
            if (pms.containsKey("idPerformer"))
                if (pms.getLong("objPerformer") == 0)
                    throw new XError("[Performer] не указан")
                else
                    updateProperties("Prop_Performer", pms)

            //2 Prop_PerformerValue
            if (pms.containsKey("idPerformerValue"))
                if (pms.getDouble("PerformerValue") == 0)
                    throw new XError("[PerformerValue] не указан")
                else
                    updateProperties("Prop_PerformerValue", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        //return loadComplex(mapRez)
    }

    @DaoMethod
    Store loadResourceTpService(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceTpService")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceTpService", "")
        map.put("cls", map2.get("Cls_ResourceTpService"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTpService, v1.obj as objTpService, v1.propVal as pvTpService, null as nameTpService,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TpService
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Set<Object> idsTpService = st.getUniqueValues("objTpService")
        Store stTpService = loadSqlService("""
            select o.id, o.cls, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTpService.join(",")})
        """, "", "resourcedata")
        StoreIndex indTpService = stTpService.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recTpService = indTpService.get(r.getLong("objTpService"))
            if (recTpService != null)
                r.set("nameTpService", recTpService.getString("name"))

            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveResourceTpService(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskLog")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("cls", pms.getLong("linkCls"), "Prop_TaskLog")
            pms.put("pvTaskLog", pv)
        }
        //
        Map<String, Long> map
        if (pms.containsKey("status")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
            pms.put("fvStatus", map.get("FV_Fact"))
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
            pms.put("fvStatus", map.get("FV_Plan"))
        }
        //
        String whe = ""
        if (mode == "upd")
            whe = "and d.objorrelobj <>" + pms.getLong("id")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskLog", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskLog")} and v.propVal=${pv} 
                    and v.obj=${pms.getLong("objTaskLog")} ${whe}
            """)
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TpService", "")
        map.put("objTpService", pms.getLong("objTpService"))
        stOwn = mdb.loadQuery("""
                select o.id
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=:Prop_TpService
                    inner join DataPropVal v on d.id=v.dataProp and v.obj=:objTpService               
                where o.id in (0${idsOwn.join(",")})
            """, map)
        if (stOwn.size() > 0)
            throw new XError("[Услуга сторонней организации] уже существует")
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceTpService", "")
            par.put("cls", map.get("Cls_ResourceTpService"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)

            //1 Prop_TpService
            if (pms.getLong("objTpService") == 0)
                throw new XError("[TpService] не указан")
            else
                fillProperties(true, "Prop_TpService", pms)

            //2 Prop_TaskLog
            if (pms.getLong("objTaskLog") == 0)
                throw new XError("[TaskLog] не указан")
            else
                fillProperties(true, "Prop_TaskLog", pms)
            //3 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //4 Prop_Value
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
            //5 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //6 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_TpService
            if (pms.containsKey("idTpService"))
                if (pms.getLong("objTpService") == 0)
                    throw new XError("[TpService] не указан")
                else
                    updateProperties("Prop_TpService", pms)

            //2 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)

            //3 Prop_Value
            if (pms.containsKey("idValue"))
                if (pms.getDouble("Value") == 0)
                    throw new XError("[Value] не указан")
                else
                    updateProperties("Prop_Value", pms)

            //4 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadResourceTpService(pms.getLong("objTaskLog"))
    }

    @DaoMethod
    Store loadResourcePersonnel(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourcePersonnel")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourcePersonnel", "")
        map.put("cls", map2.get("Cls_ResourcePersonnel"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idPosition, v1.propVal as pvPosition, null as fvPosition, null as namePosition,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v5.id as idQuantity, v5.numberVal as Quantity,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Position
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Quantity
                left join DataPropVal v5 on d5.id=v5.dataprop                
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Map<Long, Long> mapFV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Position", true)
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
            r.set("fvPosition", mapFV.get(r.getLong("pvPosition")))
        }
        Set<Object> fvs = st.getUniqueValues("fvPosition")
        Store stFV = loadSqlMeta("""
            select id, name from Factor where id in (0${fvs.join(",")})
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("fvPosition"))
            if (rec != null)
                r.set("namePosition", rec.getString("name"))
        }
        return st
    }

    @DaoMethod
    Store saveResourcePersonnel(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskLog")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("cls", pms.getLong("linkCls"), "Prop_TaskLog")
            pms.put("pvTaskLog", pv)
        }
        //
        Map<String, Long> map
        if (pms.containsKey("status")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
            pms.put("fvStatus", map.get("FV_Fact"))
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
            pms.put("fvStatus", map.get("FV_Plan"))
        }
        //
        String whe = ""
        if (mode == "upd")
            whe = "and d.objorrelobj <>" + pms.getLong("id")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskLog", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskLog")} and v.propVal=${pv} 
                    and v.obj=${pms.getLong("objTaskLog")} ${whe}
            """)
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Position", "")
        map.put("pvPosition", pms.getLong("pvPosition"))
        stOwn = mdb.loadQuery("""
                select o.id
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=:Prop_Position
                    inner join DataPropVal v on d.id=v.dataProp and v.propVal=:pvPosition                
                where o.id in (0${idsOwn.join(",")})
            """, map)
        if (stOwn.size() > 0)
            throw new XError("[Position] уже существует")
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourcePersonnel", "")
            par.put("cls", map.get("Cls_ResourcePersonnel"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)

            //1 Prop_Position
            if (pms.getLong("fvPosition") == 0)
                throw new XError("[Position] не указан")
            else
                fillProperties(true, "Prop_Position", pms)

            //2 Prop_TaskLog
            if (pms.getLong("objTaskLog") == 0)
                throw new XError("[TaskLog] не указан")
            else
                fillProperties(true, "Prop_TaskLog", pms)
            //3 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //4 Prop_Value
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
            //5 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //6 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)

            //7 Prop_Quantity
            if (pms.getDouble("Quantity") == 0)
                throw new XError("[Quantity] не указан")
            else
                fillProperties(true, "Prop_Quantity", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_Position
            if (pms.containsKey("idPosition"))
                if (pms.getLong("fvPosition") == 0)
                    throw new XError("[Position] не указан")
                else
                    updateProperties("Prop_Position", pms)

            //2 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)

            //3 Prop_Value
            if (pms.containsKey("idValue"))
                if (pms.getDouble("Value") == 0)
                    throw new XError("[Value] не указан")
                else
                    updateProperties("Prop_Value", pms)

            //4 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
            //7 Prop_Quantity
            if (pms.containsKey("idQuantity"))
                if (pms.getDouble("Quantity") == 0)
                    throw new XError("[Quantity] не указан")
                else
                    updateProperties("Prop_Quantity", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadResourcePersonnel(pms.getLong("objTaskLog"))
    }

    @DaoMethod
    Store loadResourceEquipment(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceEquipment")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceEquipment", "")
        map.put("cls", map2.get("Cls_ResourceEquipment"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTypEquipment, v1.obj as objTypEquipment, v1.propVal as pvTypEquipment, null as nameTypEquipment,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v5.id as idQuantity, v5.numberVal as Quantity,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TypEquipment
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Quantity
                left join DataPropVal v5 on d5.id=v5.dataprop                
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Map<Long, Long> mapFV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_TypEquipment", true)
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
            r.set("fvTypEquipment", mapFV.get(r.getLong("pvTypEquipment")))
        }
        //
        Set<Object> fvs = st.getUniqueValues("fvTypEquipment")
        Store stFV = loadSqlMeta("""
            select id, name from Factor where id in (0${fvs.join(",")})
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("fvTypEquipment"))
            if (rec != null)
                r.set("nameTypEquipment", rec.getString("name"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveResourceEquipment(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskLog")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("cls", pms.getLong("linkCls"), "Prop_TaskLog")
            pms.put("pvTaskLog", pv)
        }
        //
        Map<String, Long> map
        if (pms.containsKey("status")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
            pms.put("fvStatus", map.get("FV_Fact"))
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
            pms.put("fvStatus", map.get("FV_Plan"))
        }
        //
        String whe = ""
        if (mode == "upd")
            whe = "and d.objorrelobj <>" + pms.getLong("id")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskLog", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskLog")} and v.propVal=${pv} 
                    and v.obj=${pms.getLong("objTaskLog")} ${whe}
            """)
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TypEquipment", "")
        map.put("pvTypEquipment", pms.getLong("pvTypEquipment"))
        stOwn = mdb.loadQuery("""
                select o.id
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=:Prop_TypEquipment
                    inner join DataPropVal v on d.id=v.dataProp and v.propVal=:pvTypEquipment                
                where o.id in (0${idsOwn.join(",")})
            """, map)
        if (stOwn.size() > 0)
            throw new XError("[Тип техники] уже существует")
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceEquipment", "")
            par.put("cls", map.get("Cls_ResourceEquipment"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)

            //1 Prop_TypEquipment
            if (pms.getLong("fvTypEquipment") == 0)
                throw new XError("[TypEquipment] не указан")
            else
                fillProperties(true, "Prop_TypEquipment", pms)

            //2 Prop_TaskLog
            if (pms.getLong("objTaskLog") == 0)
                throw new XError("[TaskLog] не указан")
            else
                fillProperties(true, "Prop_TaskLog", pms)
            //3 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //4 Prop_Value
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
            //5 Prop_Quantity
            if (pms.getDouble("Quantity") == 0)
                throw new XError("[Quantity] не указан")
            else
                fillProperties(true, "Prop_Quantity", pms)
            //6 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //7 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_TypEquipment
            if (pms.containsKey("idEquipment"))
                if (pms.getLong("fvTypEquipment") == 0)
                    throw new XError("[TypEquipment] не указан")
                else
                    updateProperties("Prop_TypEquipment", pms)

            //2 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)

            //3 Prop_Value
            if (pms.containsKey("idValue"))
                if (pms.getDouble("Value") == 0)
                    throw new XError("[Value] не указан")
                else
                    updateProperties("Prop_Value", pms)
            //4 Prop_Quantity
            if (pms.containsKey("idQuantity"))
                if (pms.getDouble("Quantity") == 0)
                    throw new XError("[Quantity] не указан")
                else
                    updateProperties("Prop_Quantity", pms)
            //5 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadResourceEquipment(pms.getLong("objTaskLog"))
    }

    @DaoMethod
    Store loadResourceTool(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceTool")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceTool", "")
        map.put("cls", map2.get("Cls_ResourceTool"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTypTool, v1.obj as objTypTool, v1.propVal as pvTypTool, null as nameTypTool,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TypTool
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Map<Long, Long> mapFV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_TypTool", true)
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
            r.set("fvTypTool", mapFV.get(r.getLong("pvTypTool")))
        }
        //
        Set<Object> fvs = st.getUniqueValues("fvTypTool")
        Store stFV = loadSqlMeta("""
            select id, name from Factor where id in (0${fvs.join(",")})
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("fvTypTool"))
            if (rec != null)
                r.set("nameTypTool", rec.getString("name"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveResourceTool(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskLog")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("cls", pms.getLong("linkCls"), "Prop_TaskLog")
            pms.put("pvTaskLog", pv)
        }
        //
        Map<String, Long> map
        if (pms.containsKey("status")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
            pms.put("fvStatus", map.get("FV_Fact"))
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
            pms.put("fvStatus", map.get("FV_Plan"))
        }
        //
        String whe = ""
        if (mode == "upd")
            whe = "and d.objorrelobj <>" + pms.getLong("id")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskLog", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskLog")} and v.propVal=${pv} 
                    and v.obj=${pms.getLong("objTaskLog")} ${whe}
            """)
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TypTool", "")
        map.put("pvTypTool", pms.getLong("pvTypTool"))
        stOwn = mdb.loadQuery("""
                select o.id
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=:Prop_TypTool
                    inner join DataPropVal v on d.id=v.dataProp and v.propVal=:pvTypTool                
                where o.id in (0${idsOwn.join(",")})
            """, map)
        if (stOwn.size() > 0)
            throw new XError("[Инструмент] уже существует")
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceTool", "")
            par.put("cls", map.get("Cls_ResourceTool"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_TypTool
            if (pms.getLong("fvTypTool") == 0)
                throw new XError("[TypTool] не указан")
            else
                fillProperties(true, "Prop_TypTool", pms)

            //2 Prop_TaskLog
            if (pms.getLong("objTaskLog") == 0)
                throw new XError("[TaskLog] не указан")
            else
                fillProperties(true, "Prop_TaskLog", pms)
            //3 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //4 Prop_Value
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
            //6 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //7 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_TypTool
            if (pms.containsKey("idTool"))
                if (pms.getLong("fvTypTool") == 0)
                    throw new XError("[TypTool] не указан")
                else
                    updateProperties("Prop_TypTool", pms)

            //2 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)

            //3 Prop_Value
            if (pms.containsKey("idValue"))
                if (pms.getDouble("Value") == 0)
                    throw new XError("[Value] не указан")
                else
                    updateProperties("Prop_Value", pms)
            //5 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadResourceTool(pms.getLong("objTaskLog"))
    }

    @DaoMethod
    Store loadResourceMaterial(long objTaskLog) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceMaterial")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        map.put("objTaskLog", objTaskLog)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceMaterial", "")
        map.put("cls", map2.get("Cls_ResourceMaterial"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idMaterial, v1.obj as objMaterial, v1.propVal as pvMaterial, null as nameMaterial,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v6.id as idMeasure, v6.propVal as pvMeasure, null as meaMeasure,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Material
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj=:objTaskLog 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_Measure
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where o.cls=:cls
        """, map)
        //Пересечение
        Set<Object> idsMaterial = st.getUniqueValues("objMaterial")
        Store stMaterial = loadSqlService("""
            select o.id, o.cls, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsMaterial.join(",")})
        """, "", "resourcedata")
        StoreIndex indMaterial = stMaterial.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Map<Long, Long> mapMea = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_Measure", true)
        Store stMea = loadSqlMeta("""
            select id, name from Measure where 0=0
        """, "")
        StoreIndex indMea = stMea.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recMaterial = indMaterial.get(r.getLong("objMaterial"))
            if (recMaterial != null)
                r.set("nameMaterial", recMaterial.getString("name"))

            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))

            if (r.getLong("pvMeasure") > 0) {
                r.set("meaMeasure", mapMea.get(r.getLong("pvMeasure")))
            }
            StoreRecord rec = indMea.get(r.getLong("meaMeasure"))
            if (rec != null)
                r.set("nameMeasure", rec.getString("name"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveResourceMaterial(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskLog")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("cls", pms.getLong("linkCls"), "Prop_TaskLog")
            pms.put("pvTaskLog", pv)
        }
        //
        Map<String, Long> map
        if (pms.containsKey("status")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
            pms.put("fvStatus", map.get("FV_Fact"))
        } else {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
            pms.put("fvStatus", map.get("FV_Plan"))
        }
        //
        String whe = ""
        if (mode == "upd")
            whe = "and d.objorrelobj <>" + pms.getLong("id")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskLog", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskLog")} and v.propVal=${pv} 
                    and v.obj=${pms.getLong("objTaskLog")} ${whe}
            """)
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Material", "")
        map.put("objMaterial", pms.getLong("objMaterial"))
        stOwn = mdb.loadQuery("""
                select o.id
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=:Prop_Material
                    inner join DataPropVal v on d.id=v.dataProp and v.obj=:objMaterial                
                where o.id in (0${idsOwn.join(",")})
            """, map)
        if (stOwn.size() > 0)
            throw new XError("[Материал] уже существует")
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ResourceMaterial", "")
            par.put("cls", map.get("Cls_ResourceMaterial"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)

            //0 Prop_Material
            if (pms.getLong("objMaterial") == 0)
                throw new XError("[Material] не указан")
            else
                fillProperties(true, "Prop_Material", pms)

            //1 Prop_Measure
            if (pms.getLong("meaMeasure") > 0)
                fillProperties(true, "Prop_Measure", pms)
            else
                throw new XError("[Единица измерения] не указан")
            //
            //2 Prop_TaskLog
            if (pms.getLong("objTaskLog") == 0)
                throw new XError("[TaskLog] не указан")
            else
                fillProperties(true, "Prop_TaskLog", pms)
            //3 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //4 Prop_Value
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
            //5 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //6 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //0 Prop_Material
            if (pms.containsKey("idMaterial"))
                if (pms.getLong("objMaterial") == 0)
                    throw new XError("[Material] не указан")
                else
                    updateProperties("Prop_Material", pms)

            //1 Prop_Measure
            if (pms.containsKey("idMeasure")) {
                if (pms.getLong("meaMeasure") > 0)
                    updateProperties("Prop_Measure", pms)
                else
                    throw new XError("[Единица измерения] не указан")
            } else {
                if (pms.getLong("meaMeasure") > 0)
                    fillProperties(true, "Prop_Measure", pms)
                else
                    throw new XError("[Единица измерения] не указан")
            }

            //2 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)

            //3 Prop_Value
            if (pms.containsKey("idValue"))
                if (pms.getDouble("Value") == 0)
                    throw new XError("[Value] не указан")
                else
                    updateProperties("Prop_Value", pms)

            //4 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadResourceMaterial(pms.getLong("objTaskLog"))
    }

    @DaoMethod
    Store loadTaskLogEntriesForWorkPlan(Map<String, Object> params) {
        long obj = UtCnv.toLong(params.get("id"))
        long pv = UtCnv.toLong(params.get("pv"))
        Store stOwn = mdb.loadQuery("""
            select d.objorrelobj as own
            from DataProp d, DataPropVal v
            where d.id=v.dataProp and v.propVal=:pv and v.obj=:o
        """, [pv: pv, o: obj])
        Set<Object> idsOwn = stOwn.getUniqueValues("own")
        Store st = mdb.createStore("Obj.TaskLogEntriesForWorkPlan")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))
        mdb.loadQuery(st, """
            select o.id,
                v1.dateTimeVal as PlanDateStart,
                v2.dateTimeVal as PlanDateEnd,
                v3.numberVal as Value,
                v4.obj as objTask, null as fullNameTask
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_PlanDateStart
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_PlanDateEnd
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_Value and d3.status=:FV_Plan
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Task
                left join DataPropVal v4 on d4.id=v4.dataprop
            where o.id in (0${idsOwn.join(",")})
        """, map)
        //... Пересечение
        Set<Object> idsTask = st.getUniqueValues("objTask")
        Store stTask = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTask.join(",")})
        """, "", "nsidata")
        StoreIndex indTask = stTask.getIndex("id")

        for (StoreRecord r in st) {
            StoreRecord recTask = indTask.get(r.getLong("objTask"))
            if (recTask != null)
                r.set("fullNameTask", recTask.getString("fullName"))
        }
        //
        return st
    }

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
    Store loadResourceAll(Set<Object> ids) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get("Cls_TaskLog"), "Prop_TaskLog")
        //
        Store st = mdb.createStore("Obj.ResourceAll")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskLog", pv)
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_Resource%")
        Set<Long> idsCls = new HashSet<>()
        idsCls.add(mapCls.get("Cls_ResourcePersonnel"))
        idsCls.add(mapCls.get("Cls_ResourceTool"))
        idsCls.add(mapCls.get("Cls_ResourceEquipment"))
        idsCls.add(mapCls.get("Cls_ResourceTpService"))
        idsCls.add(mapCls.get("Cls_ResourceMaterial"))
        //
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idPosition, v1.propVal as pvPosition, null as fvPosition, null as namePosition,
                v2.id as idTaskLog, v2.obj as objTaskLog, v2.propVal as pvTaskLog,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v5.id as idQuantity, v5.numberVal as Quantity,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt,
                v9.id as idTypTool, v9.propVal as pvTypTool, null as fvTypTool, null as nameTypTool,
                v10.id as idTypEquipment, v10.propVal as pvTypEquipment, null as fvTypEquipment, null as nameTypEquipment,
                v11.id as idTpService, v11.obj as objTpService, v11.propVal as pvTpService, null as nameTpService,
                v12.id as idMaterial, v12.obj as objMaterial, v12.propVal as pvMaterial, null as nameMaterial,
                v13.id as idMeasure, v13.propVal as pvMeasure, null as meaMeasure, null as nameMeasure
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Position
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskLog
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskLog and v2.obj in (0${ids.join(",")}) 
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Quantity
                left join DataPropVal v5 on d5.id=v5.dataprop                
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_TypTool
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_TypEquipment
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_TpService
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_Material
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_Measure
                left join DataPropVal v13 on d13.id=v13.dataprop                
            where o.cls in (0${idsCls.join(",")})
        """, map)
        //Пересечение
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Set<Object> idsMaterial = st.getUniqueValues("objMaterial")
        Set<Object> idsTpService = st.getUniqueValues("objTpService")
        idsMaterial.addAll(idsTpService)
        Store stMaterialAndTpService = loadSqlService("""
            select o.id, o.cls, v.name, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsMaterial.join(",")})
        """, "", "resourcedata")
        StoreIndex indMaterialAndTpService = stMaterialAndTpService.getIndex("id")
        //
        Map<Long, Long> mapMea = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_Measure", true)
        Store stMea = loadSqlMeta("""
            select id, name from Measure where 0=0
        """, "")
        StoreIndex indMea = stMea.getIndex("id")
        //
        Map<Long, Long> mapFV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Position", true)
        Map<Long, Long> mapFV2 = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_TypTool", true)
        Map<Long, Long> mapFV3 = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_TypEquipment", true)
        mapFV.putAll(mapFV2)
        mapFV.putAll(mapFV3)
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
            //
            r.set("fvPosition", mapFV.get(r.getLong("pvPosition")))
            r.set("fvTypTool", mapFV.get(r.getLong("pvTypTool")))
            r.set("fvTypEquipment", mapFV.get(r.getLong("pvTypEquipment")))
            //
            StoreRecord recMaterial = indMaterialAndTpService.get(r.getLong("objMaterial"))
            if (recMaterial != null)
                r.set("nameMaterial", recMaterial.getString("name"))
            StoreRecord recTpService = indMaterialAndTpService.get(r.getLong("objTpService"))
            if (recTpService != null)
                r.set("nameTpService", recTpService.getString("fullName"))
            //
            if (r.getLong("pvMeasure") > 0) {
                r.set("meaMeasure", mapMea.get(r.getLong("pvMeasure")))
            }
            StoreRecord rec = indMea.get(r.getLong("meaMeasure"))
            if (rec != null)
                r.set("nameMeasure", rec.getString("name"))
        }
        Set<Object> fvs = st.getUniqueValues("fvPosition")
        Set<Object> fvs2 = st.getUniqueValues("fvTypTool")
        Set<Object> fvs3 = st.getUniqueValues("fvTypEquipment")
        fvs.addAll(fvs2)
        fvs.addAll(fvs3)
        Store stFV = loadSqlMeta("""
            select id, name from Factor where id in (0${fvs.join(",")})
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("fvPosition"))
            if (rec != null)
                r.set("namePosition", rec.getString("name"))
            //
            rec = indFV.get(r.getLong("fvTypTool"))
            if (rec != null)
                r.set("nameTypTool", rec.getString("name"))
            //
            rec = indFV.get(r.getLong("fvTypEquipment"))
            if (rec != null)
                r.set("nameTypEquipment", rec.getString("name"))
        }
        return st
    }

    @DaoMethod
    Map<String, Object> loadTaskLog(Map<String, Object> params) {
        Store st = mdb.createStore("Obj.task.log")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")

        String whe
        String wheV6 = ""
        String wheV12 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls = ${map.get("Cls_TaskLog")}"
            //
            Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
            long clsLocation = loadSqlService("""
                select cls from Obj where id=${UtCnv.toLong(params.get("objLocation"))}
            """, "", "orgstructuredata").get(0).getLong("cls")
            if (clsLocation == mapCls.get("Cls_LocationSection")) {
                Set<Object> idsObjLocation = getIdsObjWithChildren(UtCnv.toLong(params.get("objLocation")))
                wheV12 = "and v12.obj in (${idsObjLocation.join(",")})"
            }
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            wheV6 = "and v6.dateTimeVal between '${d1}' and '${d2}'"
        }

        Map<String, Long> mapFV = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "", "FV_%")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("FV_Plan", mapFV.get("FV_Plan"))
        map.put("FV_Fact", mapFV.get("FV_Fact"))

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
                v11.id as idUpdatedAt, v11.dateTimeVal as UpdatedAt,
                v12.id as idLocationClsSection, v12.propVal as pvLocationClsSection, 
                    v12.obj as objLocationClsSection, null as nameLocationClsSection,
                v13.id as idReasonDeviation, v13.multiStrVal as ReasonDeviation
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_WorkPlan
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Task
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Value and d5.status=:FV_Fact
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_PlanDateStart
                inner join DataPropVal v6 on d6.id=v6.dataprop  ${wheV6}
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
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_LocationClsSection
                inner join DataPropVal v12 on d12.id=v12.dataprop ${wheV12}
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_ReasonDeviation
                left join DataPropVal v13 on d11.id=v13.dataprop
            where ${whe}
        """, map)

        //... Пересечение
        Set<Object> idsTask = st.getUniqueValues("objTask")
        Store stTask = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTask.join(",")})
        """, "", "nsidata")
        StoreIndex indTask = stTask.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
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
            select o.id, v1.obj as objWork, v2.obj as objObject,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as PlanDate,
                v8.dateTimeVal as ActualDateEnd,
                v5.numberVal as StartLink,
                v6.numberVal as FinishLink
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v7 on d7.id=v7.dataProp
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v8 on d8.id=v8.dataProp
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v10 on d10.id=v10.dataprop
            where o.id in (0${idsWorkPlan.join(",")})
        """, "", "plandata")
        StoreIndex indWPprops = stWPprops.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recTask = indTask.get(r.getLong("objTask"))
            if (recTask != null)
                r.set("fullNameTask", recTask.getString("fullName"))

            StoreRecord rLocation = indLocation.get(r.getLong("objLocationClsSection"))
            if (rLocation != null)
                r.set("nameLocationClsSection", rLocation.getString("name"))

            StoreRecord rWPprops = indWPprops.get(r.getLong("objWorkPlan"))
            if (rWPprops != null) {
                r.set("objWork", rWPprops.getLong("objWork"))
                r.set("objObject", rWPprops.getLong("objObject"))
                r.set("StartKm", rWPprops.getDouble("StartKm"))
                r.set("FinishKm", rWPprops.getDouble("FinishKm"))
                r.set("StartPicket", rWPprops.getDouble("StartPicket"))
                r.set("FinishPicket", rWPprops.getDouble("FinishPicket"))
                r.set("StartLink", rWPprops.getDouble("StartLink"))
                r.set("FinishLink", rWPprops.getDouble("FinishLink"))
                r.set("PlanDate", rWPprops.getString("PlanDate"))
                r.set("ActualDateEnd", rWPprops.getString("ActualDateEnd"))
            }

            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rUser != null)
                r.set("fullNameUser", rUser.getString("fullName"))
        }
        //
        Set<Object> idsWork = st.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        StoreIndex indWork = stWork.getIndex("id")
        //
        Set<Object> idsObject = st.getUniqueValues("objObject")
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
        Map<String, Object> mapRes = new HashMap<>()
        //
        if (!params.containsKey("notResource")) {
            Set<Object> idsSt = st.getUniqueValues("id")
            Store stAll = loadResourceAll(idsSt)
            mapRes.put("resource", stAll)
        }

        mapRes.put("store", st)
        return mapRes
    }

    @DaoMethod
    Map<String, Object> saveTaskLogPlan(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        pms.put("fvStatus", map.get("FV_Plan"))
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            //Проверка статуса Incident
            apiRepairData().get(ApiRepairData).checkStatusOfIncident(pms.getLong("objWorkPlan"), "FV_StatusWorkAssigned", "FV_StatusInPlanning")
            //
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")
            par.put("cls", map.get("Cls_TaskLog"))
            par.putIfAbsent("fullName", par.get("name"))
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
            //4 Prop_Value
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                fillProperties(true, "Prop_Value", pms)
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
            //9 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") == 0)
                throw new XError("[LocationClsSection] не указан")
            else
                fillProperties(true, "Prop_LocationClsSection", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", par.get("name"))
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
            //3 Prop_Value
            if (pms.containsKey("idValue"))
                if (pms.getDouble("Value") == 0)
                    throw new XError("[Value] не указан")
                else
                    updateProperties("Prop_Value", pms)
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
            //9 Prop_LocationClsSection
            if (pms.containsKey("idLocationClsSection"))
                if (pms.getLong("objLocationClsSection") == 0)
                    throw new XError("[LocationClsSection] не указан")
                else
                    updateProperties("Prop_LocationClsSection", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("id", own)
        return loadTaskLog(mapRez)
    }

    @DaoMethod
    Map<String, Object> loadObjTaskLog(long id) {
        if (id <= 0)
            throw new XError("[id] не указан")
        //
        Map<String, Object> params = new HashMap<>()
        params.put("id", id)
        Map<String, Object> taskLog = loadTaskLog(params)
        //
        Store st = taskLog.get("store") as Store
        Map<String, Object> mapRes = new HashMap<>()
        for (StoreRecord r in st) {
            mapRes.putAll(r.getValues())
            //
            List<Map<String, Object>> lstPersonnel = loadResourcePersonnelFact(r.getLong("id"))
            List<Map<String, Object>> lstMaterial = loadResourceMaterialFact(r.getLong("id"))
            List<Map<String, Object>> lstEquipment = loadResourceEquipmentFact(r.getLong("id"))
            List<Map<String, Object>> lstTool = loadResourceToolFact(r.getLong("id"))
            List<Map<String, Object>> lstTpService = loadResourceTpServiceFact(r.getLong("id"))
            //
            if (!lstPersonnel.isEmpty())
                mapRes.put("personnel", lstPersonnel)
            if (!lstMaterial.isEmpty())
                mapRes.put("material", lstMaterial)
            if (!lstEquipment.isEmpty())
                mapRes.put("equipment", lstEquipment)
            if (!lstTool.isEmpty())
                mapRes.put("tool", lstTool)
            if (!lstTpService.isEmpty())
                mapRes.put("tpService", lstTpService)
        }
        return mapRes
    }

    @DaoMethod
    Map<String, Object> saveTaskLogFact(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own = pms.getLong("id")
        long objWorkPlan = pms.getLong("objWorkPlan")
        if (own == 0)
            throw new XError("Не указан [id]")
        if (objWorkPlan == 0)
            throw new XError("Не указан [objWorkPlan]")
        //
        pms.put("own", own)
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
        pms.put("fvStatus", map.get("FV_Fact"))
        // Замена статуса Incident
        if (pms.containsKey("FactDateStart"))
            apiRepairData().get(ApiRepairData).checkStatusOfIncident(objWorkPlan, "FV_StatusInPlanning", "FV_StatusAtWork")
        //1 Prop_User
        if (pms.containsKey("idUser")) {
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                updateProperties("Prop_User", pms)
        }
        //2 Prop_Value
        if (pms.containsKey("idValue")) {
            if (pms.getDouble("Value") == 0)
                throw new XError("[Value] не указан")
            else
                updateProperties("Prop_Value", pms)
        } else {
            if (pms.getDouble("Value") > 0)
                fillProperties(true, "Prop_Value", pms)
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
        if (pms.containsKey("idReasonDeviation"))
            updateProperties("Prop_ReasonDeviation", pms)
        else {
            if (!pms.getString("ReasonDeviation").isEmpty())
                fillProperties(true, "Prop_ReasonDeviation", pms)
        }
        //6 Prop_UpdatedAt
        if (pms.containsKey("idUpdatedAt")) {
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                updateProperties("Prop_UpdatedAt", pms)
        }
        // Новый номер и дата установки объекта
        if (pms.containsKey("Number") && pms.getString("fullNameWork").contains("приборов СЦБ и другой аппаратуры")) {
            Map<String, Object> mapObj = new HashMap<>()
            //
            if (pms.getLong("objObject") == 0)
                throw new XError("Не указан [objObject]")
            if (pms.getLong("objUser") == 0)
                throw new XError("Не указан [objUser]")
            if (pms.getLong("pvUser") == 0)
                throw new XError("Не указан [pvUser]")
            if (pms.getString("Number").isEmpty())
                throw new XError("Не указан [Number]")
            if (pms.getString("FactDateEnd").isEmpty())
                throw new XError("Не указан [FactDateEnd]")
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("Не указан [UpdatedAt]")

            mapObj.put("own", pms.getLong("objObject"))
            mapObj.put("Number", pms.getString("Number"))
            mapObj.put("InstallationDate", pms.getString("FactDateEnd"))
            mapObj.put("UpdatedAt", pms.getString("UpdatedAt"))
            mapObj.put("objUser", pms.getLong("objUser"))
            mapObj.put("pvUser", pms.getLong("pvUser"))
            //
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
            Store stTmp = loadSqlService("""
                select o.id, o.cls, v.name, v.fullName,
                    v1.id as idNumber, v1.strVal as Number,
                    v2.id as idInstallationDate, v2.dateTimeVal as InstallationDate,
                    v3.id as idUpdatedAt, v3.dateTimeVal as UpdatedAt,
                    v4.id as idUser, v4.propVal as pvUser, v4.obj as objUser,
                    v5.numberVal as PeriodicityReplacement
                from Obj o
                    left join ObjVer v on v.ownerver=o.id and v.lastver=1
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Number")}
                    left join DataPropVal v1 on d1.id=v1.dataprop
                    left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_InstallationDate")}
                    left join DataPropVal v2 on d2.id=v2.dataprop
                    left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_UpdatedAt")}
                    left join DataPropVal v3 on d3.id=v3.dataprop
                    left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_User")}
                    left join DataPropVal v4 on d4.id=v4.dataprop
                    left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_PeriodicityReplacement")}
                    left join DataPropVal v5 on d5.id=v5.dataprop
                where o.id=${pms.getLong("objObject")}
            """, "", "objectdata")
            //
            int periodRep = 0
            for (StoreRecord r in stTmp) {
                mapObj.put("idNumber", r.getLong("idNumber"))
                mapObj.put("idInstallationDate", r.getLong("idInstallationDate"))
                mapObj.put("idUpdatedAt", r.getLong("idUpdatedAt"))
                mapObj.put("idUser", r.getLong("idUser"))
                if (r.getInt("PeriodicityReplacement") > 0) {
                    periodRep = r.getInt("PeriodicityReplacement")
                }
            }
            //
            apiObjectData().get(ApiObjectData).updateObject(mapObj)
            // Создаем новый план работ по Периодичности замены прибора
            if (periodRep > 0) {
                stTmp = loadSqlService("""
                    select o.cls,
                        v1.propVal as pvLocationClsSection, v1.obj as objLocationClsSection,
                        v2.propVal as pvObject, v2.obj as objObject,
                        v3.numberVal as StartKm,
                        v4.numberVal as FinishKm,
                        v5.numberVal as StartPicket,
                        v6.numberVal as FinishPicket,
                        v7.numberVal as StartLink,
                        v8.numberVal as FinishLink,
                        v9.propVal as pvWork, v9.obj as objWork
                    from Obj o
                        left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_LocationClsSection")}
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
                        left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_Work")}
                        left join DataPropVal v9 on d9.id=v9.dataprop
                    where o.id=${objWorkPlan}
                """, "Obj.plan", "plandata")
                //
                String dte = UtCnv.toDate(pms.getString("FactDateEnd")).toJavaLocalDate().plusYears(periodRep).toString()
                Map<String, Object> mapPlan = stTmp.get(0).getValues()
                mapPlan.put("name", "" + mapPlan.get("objWork") + "_" + pms.getLong("objObject") + "_" + pms.getString("FactDateEnd"))
                mapPlan.put("PlanDateEnd", dte)
                mapPlan.put("CreatedAt", pms.getString("UpdatedAt"))
                mapPlan.put("UpdatedAt", pms.getString("UpdatedAt"))
                mapPlan.put("objUser", pms.getLong("objUser"))
                mapPlan.put("pvUser", pms.getLong("pvUser"))
                //
                apiPlanData().get(ApiPlanData).savePlan("ins", mapPlan)
            }
        }
        //
        return loadObjTaskLog(own)
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
            long fv = UtCnv.toLong(params.get("fvStatus"))
            if (fv == 0)
                fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
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
                long fv = UtCnv.toLong(params.get("fvStatus"))
                if (fv == 0)
                    fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
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
        //Complex
        if ([FD_PropType_consts.complex].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_PerformerComplex") ||
                    cod.equalsIgnoreCase("Prop_ToolComplex") ||
                    cod.equalsIgnoreCase("Prop_EquipmentComplex")) {
                recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

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
            if (cod.equalsIgnoreCase("Prop_Description") ||
                    cod.equalsIgnoreCase("Prop_ReasonDeviation")) {
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
            if (cod.equalsIgnoreCase("Prop_Position") ||
                    cod.equalsIgnoreCase("Prop_TypEquipment") ||
                    cod.equalsIgnoreCase("Prop_TypTool")) {
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
            if (cod.equalsIgnoreCase("Prop_Value") ||
                    cod.equalsIgnoreCase("Prop_Quantity") ||
                    cod.equalsIgnoreCase("Prop_PerformerValue") ||
                    cod.equalsIgnoreCase("Prop_ToolValue") ||
                    cod.equalsIgnoreCase("Prop_EquipmentValue")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    double v = UtCnv.toDouble(params.get(keyValue))
                    v = v / koef
                    if (digit) v = v.round(digit)
                    recDPV.set("numberval", v)
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_WorkPlan") ||
                    cod.equalsIgnoreCase("Prop_Task") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_LocationClsSection") ||
                    cod.equalsIgnoreCase("Prop_Material") ||
                    cod.equalsIgnoreCase("Prop_Tool") ||
                    cod.equalsIgnoreCase("Prop_Equipment") ||
                    cod.equalsIgnoreCase("Prop_Personnel") ||
                    cod.equalsIgnoreCase("Prop_TpService") ||
                    cod.equalsIgnoreCase("Prop_TaskLog") ||
                    cod.equalsIgnoreCase("Prop_Performer")) {
                if (objRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("obj", objRef)
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
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
        if ([FD_PropType_consts.complex].contains(propType)) {
            params.put("idComplex", idDPV)
        }
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
            if (cod.equalsIgnoreCase("Prop_Description") ||
                    cod.equalsIgnoreCase("Prop_ReasonDeviation")) {
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
            if (cod.equalsIgnoreCase("Prop_Position") ||
                    cod.equalsIgnoreCase("Prop_TypEquipment") ||
                    cod.equalsIgnoreCase("Prop_TypTool")) {
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
            if (cod.equalsIgnoreCase("Prop_Value") ||
                    cod.equalsIgnoreCase("Prop_Quantity") ||
                    cod.equalsIgnoreCase("Prop_PerformerValue") ||
                    cod.equalsIgnoreCase("Prop_ToolValue") ||
                    cod.equalsIgnoreCase("Prop_EquipmentValue")) {
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
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_LocationClsSection") ||
                    cod.equalsIgnoreCase("Prop_Material") ||
                    cod.equalsIgnoreCase("Prop_Tool") ||
                    cod.equalsIgnoreCase("Prop_Personnel") ||
                    cod.equalsIgnoreCase("Prop_TpService") ||
                    cod.equalsIgnoreCase("Prop_Equipment") ||
                    cod.equalsIgnoreCase("Prop_Performer")) {
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
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSql(sql, domain)
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
        else if (model.equalsIgnoreCase("inspectiondata"))
            return apiInspectionData().get(ApiInspectionData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSqlWithParams(sql, params, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
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
