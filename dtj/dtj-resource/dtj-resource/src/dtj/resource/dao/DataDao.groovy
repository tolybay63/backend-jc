package dtj.resource.dao

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
import tofi.api.dta.ApiRepairData
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

    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }

    @DaoMethod
    Store loadResourceByTyp(long propVal, String flag, String codProp) {
        long pv
        String whe, wheD1
        Map<String, Long> map
        //
        if (flag == "tool") {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Tool", "")
            whe = "o.cls=${map.get("Cls_Tool")}"
            wheD1 = "d1.prop=:Prop_TypTool"
            pv = apiMeta().get(ApiMeta).idPV("Cls", UtCnv.toLong(map.get("Cls_Tool")), codProp)
        } else if (flag == "equipment") {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Equipment", "")
            whe = "o.cls=${map.get("Cls_Equipment")}"
            wheD1 = "d1.prop=:Prop_TypEquipment"
            pv = apiMeta().get(ApiMeta).idPV("Cls", UtCnv.toLong(map.get("Cls_Equipment")), codProp)
        } else
            throw new XError("Неверный флаг")
        //
        Store st = mdb.createStore("Obj.resource")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, '[№' || coalesce(v3.strVal, '') ||'] ' || v.name as name, null as pv,
                v2.obj as objLocationClsSection, null as nameLocationClsSection
            from Obj o
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and ${wheD1}
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.propVal=${propVal}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_LocationClsSection
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_Number
                left join DataPropVal v3 on d3.id=v3.dataprop
            where ${whe}
        """, map)
        //
        Set<Object> ids = st.getUniqueValues("objLocationClsSection")
        Store stObj = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${ids.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indObj = stObj.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indObj.get(r.getLong("objLocationClsSection"))
            if (rec != null)
                r.set("nameLocationClsSection", rec.getString("name"))
            //
            r.set("pv", pv)
        }
        //
        return st
    }
    
    @DaoMethod
    Store loadTpService(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TpService", "")
        Store st = mdb.createStore("Obj.TpService")
        String whe = "o.id=${id}"
        if (id == 0)
            whe = "o.cls=${map.get("Cls_TpService")}"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.fullName,
                v1.id as idMeasure, v1.propVal as pvMeasure, null as meaMeasure, null as nameMeasure,
                v2.id as idDescription, v2.multiStrVal as Description
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Measure
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Description
                left join DataPropVal v2 on d2.id=v2.dataprop
            where ${whe}
        """, map)
        //

        Map<Long, Long> mapMea = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_Measure", true)

        Store stMea = loadSqlMeta("""
            select id, name from Measure where 0=0
        """, "")
        StoreIndex indMea = stMea.getIndex("id")
        for (StoreRecord r in st) {
            if (r.getLong("pvMeasure") > 0) {
                r.set("meaMeasure", mapMea.get(r.getLong("pvMeasure")))
            }
            StoreRecord rec = indMea.get(r.getLong("meaMeasure"))
            if (rec != null)
                r.set("nameMeasure", rec.getString("name"))
        }
        return st
    }

    @DaoMethod
    Store saveTpService(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TpService", "")
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get("Cls_TpService")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            par.put("cls", map.get("Cls_TpService"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_Measure
            if (pms.getLong("meaMeasure") > 0)
                fillProperties(true, "Prop_Measure", pms)
            else
                throw new XError("[Единица измерения] не указан")
            //
            //2 Prop_Description
            if (pms.containsKey("Description")) {
                if (!pms.getString("Description").isEmpty())
                    fillProperties(true, "Prop_Description", pms)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and o.id<>${pms.getLong("id")} and 
                    v.lastVer=1 and o.cls=${pms.getLong("cls")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

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

            //2 Prop_Description
            if (pms.getLong("idDescription") > 0) {
                updateProperties("Prop_Description", pms)
            } else {
                if (pms.containsKey("Description")) {
                    if (!pms.getString("Description").isEmpty())
                        fillProperties(true, "Prop_Description", pms)
                }
            }
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadTpService(own)
    }

    @DaoMethod
    Store loadEquipment(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Equipment", "")
        Store st = mdb.createStore("Obj.equipment")
        String whe = "o.id=${id}"
        if (id == 0)
            whe = "o.cls=${map.get("Cls_Equipment")}"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTypEquipment, v1.propVal as pvTypEquipment, null as fvTypEquipment,
                v2.id as idLocationClsSection, v2.propVal as pvLocationClsSection, 
                    v2.obj as objLocationClsSection, null as nameLocationClsSection,
                v3.id as idUser, v3.propVal as pvUser, v3.obj as objUser, null as fullNameUser,
                v4.id as idCreatedAt, v4.dateTimeVal as CreatedAt,
                v5.id as idUpdatedAt, v5.dateTimeVal as UpdatedAt,
                v6.id as idDescription, v6.multiStrVal as Description,
                v7.id as idNumber, v7.strVal as Number
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TypEquipment
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_LocationClsSection
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_CreatedAt
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_UpdatedAt
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_Description
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_Number
                left join DataPropVal v7 on d7.id=v7.dataprop
            where ${whe}
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
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Set<Object> pvs = st.getUniqueValues("pvTypEquipment")
        Store stFV = loadSqlMeta("""
            select pv.id, pv.factorVal, f.name
            from PropVal pv, Factor f
            where pv.factorVal=f.id and pv.id in (0${pvs.join(",")})
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("pvTypEquipment"))
            if (rec != null) {
                r.set("fvTypEquipment", rec.getLong("factorVal"))
                r.set("nameTypEquipment", rec.getString("name"))
            }
            //
            rec = indLocation.get(r.getLong("objLocationClsSection"))
            if (rec != null)
                r.set("nameLocationClsSection", rec.getString("name"))
            //
            rec = indUser.get(r.getLong("objUser"))
            if (rec != null)
                r.set("fullNameUser", rec.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveEquipment(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Equipment", "")
            Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Number", "")
            pms.put("Number", pms.getString("Number").trim())
            String num = pms.getString("Number").toLowerCase()
            Store st = mdb.loadQuery("""
                select v.strVal 
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=${map2.get("Prop_Number")}
                    inner join DataPropval v on d.id=v.dataProp and lower(v.strVal)='${num}'
                where o.cls=${map.get("Cls_Equipment")} 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", pms.getString("Number"))

            par.put("cls", map.get("Cls_Equipment"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)

            //1 Prop_Number
            if (!pms.getString("Number").isEmpty())
                fillProperties(true, "Prop_Number", pms)
            else
                throw new XError("[Number] not specified")
            //2 Prop_TypEquipment
            if (pms.getLong("fvTypEquipment") > 0)
                fillProperties(true, "Prop_TypEquipment", pms)
            else
                throw new XError("[TypEquipment] not specified")
            //3 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", pms)
            else
                throw new XError("[objLocationClsSection] not specified")
            //4 Prop_CreatedAt
            if (pms.getString("CreatedAt") != "")
                fillProperties(true, "Prop_CreatedAt", pms)
            else
                throw new XError("[CreatedAt] not specified")
            //5 Prop_UpdatedAt
            if (pms.getString("UpdatedAt") != "")
                fillProperties(true, "Prop_UpdatedAt", pms)
            else
                throw new XError("[UpdatedAt] not specified")
            //6 Prop_User
            if (pms.getLong("objUser") > 0) {
                fillProperties(true, "Prop_User", pms)
            } else
                throw new XError("[objUser] not specified")
            //7 Prop_Description
            if (!pms.getString("Description").isEmpty())
                fillProperties(true, "Prop_Description", pms)

        } else if (mode.equalsIgnoreCase("upd")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Number", "")
            Store st = mdb.loadQuery("""
                select v.strVal 
                from Obj o
                    left join DataProp d on d.objorrelobj=o.id and d.prop=${map.get("Prop_Number")}
                    inner join DataPropval v on d.id=v.dataProp and v.strVal='${pms.getString("Number")}'
                where o.cls=${pms.getLong("cls")} and o.id<>${pms.getLong("id")} 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", pms.getString("Number"))

            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_Number
            if (pms.getLong("idNumber") > 0) {
                if (!pms.getString("Number").isEmpty())
                    updateProperties("Prop_Number", pms)
                else
                    throw new XError("[Number] not specified")
            }
            //2 Prop_TypEquipment
            if (pms.getLong("idTypEquipment") > 0) {
                if (pms.getLong("fvTypEquipment") > 0)
                    updateProperties("Prop_TypEquipment", pms)
                else
                    throw new XError("[TypEquipment] not specified")
            }
            //3 Prop_LocationClsSection
            if (pms.getLong("idLocationClsSection") > 0) {
                if (pms.getLong("objLocationClsSection") > 0)
                    updateProperties("Prop_LocationClsSection", pms)
                else
                    throw new XError("[objLocationClsSection] not specified")
            }
            //5 Prop_UpdatedAt
            if (pms.getLong("idUpdatedAt") > 0) {
                if (pms.getString("UpdatedAt") != "")
                    updateProperties("Prop_UpdatedAt", pms)
                else
                    throw new XError("[UpdatedAt] not specified")
            }
            //6 Prop_User
            if (pms.getLong("idUser") > 0) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[objUser] not specified")
            }
            //7 Prop_Description
            if (pms.getLong("idDescription") > 0) {
                updateProperties("Prop_Description", pms)
            } else {
                if (!pms.getString("Description").isEmpty())
                    fillProperties(true, "Prop_Description", pms)
            }
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadEquipment(own)
    }

    @DaoMethod
    Store loadTool(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Tool", "")
        Store st = mdb.createStore("Obj.tool")
        String whe = "o.id=${id}"
        if (id == 0)
            whe = "o.cls=${map.get("Cls_Tool")}"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTypTool, v1.propVal as pvTypTool, null as fvTypTool,
                v2.id as idLocationClsSection, v2.propVal as pvLocationClsSection, 
                    v2.obj as objLocationClsSection, null as nameLocationClsSection,
                v3.id as idUser, v3.propVal as pvUser, v3.obj as objUser, null as fullNameUser,
                v4.id as idCreatedAt, v4.dateTimeVal as CreatedAt,
                v5.id as idUpdatedAt, v5.dateTimeVal as UpdatedAt,
                v6.id as idDescription, v6.multiStrVal as Description,
                v7.id as idNumber, v7.strVal as Number
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TypTool
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_LocationClsSection
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_User
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_CreatedAt
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_UpdatedAt
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_Description
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_Number
                left join DataPropVal v7 on d7.id=v7.dataprop
            where ${whe}
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
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        Set<Object> pvs = st.getUniqueValues("pvTypTool")
        Store stFV = loadSqlMeta("""
            select pv.id, pv.factorVal, f.name
            from PropVal pv, Factor f
            where pv.factorVal=f.id and pv.id in (0${pvs.join(",")})
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("pvTypTool"))
            if (rec != null) {
                r.set("fvTypTool", rec.getLong("factorVal"))
                r.set("nameTypTool", rec.getString("name"))
            }
            //
            rec = indLocation.get(r.getLong("objLocationClsSection"))
            if (rec != null)
                r.set("nameLocationClsSection", rec.getString("name"))
            //
            rec = indUser.get(r.getLong("objUser"))
            if (rec != null)
                r.set("fullNameUser", rec.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveTool(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Tool", "")
            pms.put("name", pms.getString("name").trim())
            String nm = pms.getString("name").toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get("Cls_Tool")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            par.put("cls", map.get("Cls_Tool"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)

            //1 Prop_Number
            if (!pms.getString("Number").isEmpty())
                fillProperties(true, "Prop_Number", pms)
            else
                throw new XError("[Number] not specified")
            //2 Prop_TypTool
            if (pms.getLong("fvTypTool") > 0)
                fillProperties(true, "Prop_TypTool", pms)
            else
                throw new XError("[TypTool] not specified")
            //3 Prop_LocationClsSection
            if (pms.getLong("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", pms)
            else
                throw new XError("[objLocationClsSection] not specified")
            //4 Prop_CreatedAt
            if (pms.getString("CreatedAt") != "")
                fillProperties(true, "Prop_CreatedAt", pms)
            else
                throw new XError("[CreatedAt] not specified")
            //5 Prop_UpdatedAt
            if (pms.getString("UpdatedAt") != "")
                fillProperties(true, "Prop_UpdatedAt", pms)
            else
                throw new XError("[UpdatedAt] not specified")
            //6 Prop_User
            if (pms.getLong("objUser") > 0) {
                fillProperties(true, "Prop_User", pms)
            } else
                throw new XError("[objUser] not specified")
            //7 Prop_Description
            if (!pms.getString("Description").isEmpty())
                fillProperties(true, "Prop_Description", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and o.id<>${pms.getLong("id")} and 
                    v.lastVer=1 and o.cls=${pms.getLong("cls")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_Number
            if (pms.getLong("idNumber") > 0) {
                if (!pms.getString("Number").isEmpty())
                    updateProperties("Prop_Number", pms)
                else
                    throw new XError("[Number] not specified")
            }
            //2 Prop_TypTool
            if (pms.getLong("idTypTool") > 0) {
                if (pms.getLong("fvTypTool") > 0)
                    updateProperties("Prop_TypTool", pms)
                else
                    throw new XError("[TypTool] not specified")
            }
            //3 Prop_LocationClsSection
            if (pms.getLong("idLocationClsSection") > 0) {
                if (pms.getLong("objLocationClsSection") > 0)
                    updateProperties("Prop_LocationClsSection", pms)
                else
                    throw new XError("[objLocationClsSection] not specified")
            }
            //5 Prop_UpdatedAt
            if (pms.getLong("idUpdatedAt") > 0) {
                if (pms.getString("UpdatedAt") != "")
                    updateProperties("Prop_UpdatedAt", pms)
                else
                    throw new XError("[UpdatedAt] not specified")
            }
            //6 Prop_User
            if (pms.getLong("idUser") > 0) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[objUser] not specified")
            }
            //7 Prop_Description
            if (pms.getLong("idDescription") > 0) {
                updateProperties("Prop_Description", pms)
            } else {
                if (!pms.getString("Description").isEmpty())
                    fillProperties(true, "Prop_Description", pms)
            }
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadTool(own)
    }

    @DaoMethod
    Store loadMaterial(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Material", "")
        Store st = mdb.createStore("Obj.material")
        String whe = "o.id=${id}"
        if (id == 0)
            whe = "o.cls=${map.get("Cls_Material")}"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.fullName,
                v1.id as idMeasure, v1.propVal as pvMeasure, null as meaMeasure, null as nameMeasure,
                v2.id as idDescription, v2.multiStrVal as Description
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Measure
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Description
                left join DataPropVal v2 on d2.id=v2.dataprop
            where ${whe}
        """, map)
        //

        Map<Long, Long> mapMea = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_Measure", true)

        Store stMea = loadSqlMeta("""
            select id, name from Measure where 0=0
        """, "")
        StoreIndex indMea = stMea.getIndex("id")
        for (StoreRecord r in st) {
            if (r.getLong("pvMeasure") > 0) {
                r.set("meaMeasure", mapMea.get(r.getLong("pvMeasure")))
            }
            StoreRecord rec = indMea.get(r.getLong("meaMeasure"))
            if (rec != null)
                r.set("nameMeasure", rec.getString("name"))
        }
        return st
    }

    @DaoMethod
    Store saveMaterial(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Material", "")
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get("Cls_Material")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            par.put("cls", map.get("Cls_Material"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_Measure
            if (pms.getLong("meaMeasure") > 0)
                fillProperties(true, "Prop_Measure", pms)
            else
                throw new XError("[Единица измерения] не указан")
            //
            //2 Prop_Description
            if (pms.containsKey("Description")) {
                if (!pms.getString("Description").isEmpty())
                    fillProperties(true, "Prop_Description", pms)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and o.id<>${pms.getLong("id")} and 
                    v.lastVer=1 and o.cls=${pms.getLong("cls")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

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
            //2 Prop_Description
            if (pms.getLong("idDescription") > 0) {
                updateProperties("Prop_Description", pms)
            } else {
                if (pms.containsKey("Description")) {
                    if (!pms.getString("Description").isEmpty())
                        fillProperties(true, "Prop_Description", pms)
                }
            }
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadMaterial(own)
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
                """, "", "resourcedata")
                if (stData.size() > 0)
                    lstService.add("resourcedata")
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
            if (cod.equalsIgnoreCase("Prop_Number")) {
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
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_TypTool") ||
                    cod.equalsIgnoreCase("Prop_TypEquipment")) {
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
            if (cod.equalsIgnoreCase("Prop_StartKm")) {
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
            if (cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_LocationClsSection")) {
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
            if (cod.equalsIgnoreCase("Prop_Number")) {   //For Template
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
            if (cod.equalsIgnoreCase("Prop_CreatedAt")||
                cod.equalsIgnoreCase("Prop_UpdatedAt")) {
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
            if (cod.equalsIgnoreCase("Prop_TypTool") ||
                    cod.equalsIgnoreCase("Prop_TypEquipment")) {
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

        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_StartKm")) {
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
            if (cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_LocationClsSection")) {
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
