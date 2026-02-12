package dtj.nsi.dao

import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.UtFile
import jandcode.commons.conf.Conf
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.std.DataDirService
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
import tofi.api.mdl.utils.CartesianProduct
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageItem
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService
import tofi.api.mdl.utils.tree.DataTreeNode
import tofi.api.mdl.utils.tree.ITreeNodeVisitor
import tofi.api.mdl.utils.tree.UtData
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

import java.nio.file.Files
import java.nio.file.Paths

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
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }

    //-------------------------
    @DaoMethod
    List<Map<String, Object>> loadRelObjResourceNormative(Map<String, Object> params) {
        List<Map<String, Object>> lst = new ArrayList<>()
        VariantMap pms = new VariantMap(params)
        if (pms.getLong("objWork") == 0)
            throw new XError("Не указан [objWork]")
        if (pms.getLong("objTask") == 0)
            throw new XError("Не указан [objTask]")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long relCls = map.get("RC_TaskWork")

        Store st = mdb.loadQuery("""
            select o.id
            from RelObj o
                inner join RelObjMember r1 on o.id=r1.relobj and r1.obj=${pms.getLong("objWork")}
                inner join RelObjMember r2 on o.id=r2.relobj and r2.obj=${pms.getLong("objTask")}
            where o.relcls=${relCls}
        """)
        if (st.size() == 0)
            return lst

        return loadResourceNormative(st.get(0).getLong("id"), "ru")
    }

    @DaoMethod
    List<Map<String, Object>> loadResourceNormative(long relobjTaskWork, String lang) {
        //
        List<Map<String, Object>> lst = new ArrayList<>()
        Store st = mdb.createStore("RelObj.Normative")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long relCls = map.get("RC_TaskWork")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_WorkCorrectional", "")
        long uchCls1 = map.get("Cls_WorkCorrectional")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Task", "")
        long uchCls2 = map.get("Cls_Task")

        String whe = "o.id=${relobjTaskWork}"
        if (relobjTaskWork == 0) {
            whe = "o.relcls=${relCls}"
        }

        mdb.loadQuery(st, """
            select o.id, o.relCls, v.name, r1.obj as idROM1, r1.cls as clsROM1, v1.name as nameROM1, 
                r2.obj as idROM2, r2.cls as clsROM2, v2.name as nameROM2
            from RelObj o
                left join RelObjVer v on o.id=v.ownerver and v.lastver=1 
                left join RelObjMember r1 on o.id=r1.relobj and r1.cls=${uchCls1}
                left join RelObjMember r2 on o.id=r2.relobj and r2.cls=${uchCls2}
                left join ObjVer v1 on r1.obj=v1.ownerver and v1.lastver=1
                left join ObjVer v2 on r2.obj=v2.ownerver and v2.lastver=1
            where ${whe}
        """)
        //
        if (st.size() == 0)
            return lst
        //
        Set<Object> idsRelObj = st.getUniqueValues("id")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_Normative")
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))
        // Tool
        Store stTmp = mdb.createStore("Obj.Normative")
        mdb.loadQuery(stTmp, """
            select o.id, o.cls, v.name,
                v1.id as idUser, v1.obj as objUser, v1.propVal as pvUser, null as fullNameUser,
                v2.id as idTaskWork, v2.relobj as relobjTaskWork, v2.propVal as pvTaskWork,
                v3.id as idMeasure, v3.propVal as pvMeasure, null as meaMeasure, null as nameMeasure,
                v4.id as idValue, v4.numberVal as Value,
                v5.id as idQuantity, v5.numberVal as Quantity,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt,
                v9.id as idMaterial, v9.obj as objMaterial, v9.propVal as pvMaterial, null as nameMaterial,
                v10.id as idTypTool, v10.propVal as pvTypTool, null as fvTypTool, null as nameTypTool,
                v11.id as idTypEquipment, v11.propVal as pvTypEquipment, null as fvTypEquipment, null as nameTypEquipment,
                v12.id as idTpService, v12.obj as objTpService, v12.propVal as pvTpService, null as nameTpService,
                v13.id as idPosition, v13.propVal as pvPosition, null as fvPosition, null as namePosition
            from Obj o
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_User
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskWork
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.relobj in (0${idsRelObj.join(",")})
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_Measure
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Value and d4.status=:FV_Plan
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Quantity
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_Material
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_TypTool
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_TypEquipment
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_TpService
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_Position
                left join DataPropVal v13 on d13.id=v13.dataprop
            where o.cls in (0${mapCls.values().join(",")})
        """, map)
        //Пересечение
        if (stTmp.size() > 0) {
            Set<Object> idsUser = stTmp.getUniqueValues("objUser")
            Store stUser = loadSqlService("""
                select o.id, o.cls, v.fullName
                from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
            """, "", "personnaldata")
            StoreIndex indUser = stUser.getIndex("id")
            //
            Set<Object> idsMaterial = stTmp.getUniqueValues("objMaterial")
            Store stMaterial = loadSqlService("""
                select o.id, o.cls, v.name
                from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsMaterial.join(",")})
            """, "", "resourcedata")
            StoreIndex indMaterial = stMaterial.getIndex("id")
            //
            Set<Object> idsTpService = stTmp.getUniqueValues("objTpService")
            Store stTpService = loadSqlService("""
                select o.id, o.cls, v.name
                from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTpService.join(",")})
            """, "", "resourcedata")
            StoreIndex indTpService = stTpService.getIndex("id")
            //
            Map<Long, Long> mapMea = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_Measure", true)
            Store stMea = loadSqlMeta("""
                select id, name from Measure where 0=0
            """, "")
            StoreIndex indMea = stMea.getIndex("id")
            //
            Set<Object> idsPV = stTmp.getUniqueValues("pvPosition")
            idsPV.addAll(stTmp.getUniqueValues("pvTypTool"))
            idsPV.addAll(stTmp.getUniqueValues("pvTypEquipment"))
            Store stPV = apiMeta().get(ApiMeta).loadSql("""
                select pv.id, fv.id as fv, fv.name
                from PropVal pv, Factor fv
                where fv.id=pv.factorval and pv.id in (0${idsPV.join(",")})
            """, "")
            StoreIndex indPV = stPV.getIndex("id")
            //
            for (StoreRecord r in stTmp) {
                StoreRecord recUser = indUser.get(r.getLong("objUser"))
                if (recUser != null)
                    r.set("fullNameUser", recUser.getString("fullName"))

                StoreRecord recMaterial = indMaterial.get(r.getLong("objMaterial"))
                if (recMaterial != null)
                    r.set("nameMaterial", recMaterial.getString("name"))

                StoreRecord recTpService = indTpService.get(r.getLong("objTpService"))
                if (recTpService != null)
                    r.set("nameTpService", recTpService.getString("name"))

                if (r.getLong("pvMeasure") > 0) {
                    r.set("meaMeasure", mapMea.get(r.getLong("pvMeasure")))
                }
                StoreRecord rec = indMea.get(r.getLong("meaMeasure"))
                if (rec != null)
                    r.set("nameMeasure", rec.getString("name"))

                if (r.getLong("pvPosition") > 0) {
                    r.set("fvPosition", indPV.get(r.getLong("pvPosition")).getLong("fv"))
                    r.set("namePosition", indPV.get(r.getLong("pvPosition")).getString("name"))
                }
                if (r.getLong("pvTypTool") > 0) {
                    r.set("fvTypTool", indPV.get(r.getLong("pvTypTool")).getLong("fv"))
                    r.set("nameTypTool", indPV.get(r.getLong("pvTypTool")).getString("name"))
                }
                if (r.getLong("pvTypEquipment") > 0) {
                    r.set("fvTypEquipment", indPV.get(r.getLong("pvTypEquipment")).getLong("fv"))
                    r.set("nameTypEquipment", indPV.get(r.getLong("pvTypEquipment")).getString("name"))
                }
            }
        }
        //
        for (StoreRecord r1 in st) {
            Map<String, Object> mapRes = r1.getValues()
            List<Map<String, Object>> material = new ArrayList<>()
            List<Map<String, Object>> tool = new ArrayList<>()
            List<Map<String, Object>> equipment = new ArrayList<>()
            List<Map<String, Object>> service = new ArrayList<>()
            List<Map<String, Object>> personnel = new ArrayList<>()
            //
            for (StoreRecord r2 in stTmp) {
                if (r1.getLong("id") == r2.getLong("relobjTaskWork")) {
                    if (r2.getLong("cls") == mapCls.get("Cls_NormativeMaterial")) {
                        material.add(r2.getValues())
                    }
                    if (r2.getLong("cls") == mapCls.get("Cls_NormativeTool")) {
                        tool.add(r2.getValues())
                    }
                    if (r2.getLong("cls") == mapCls.get("Cls_NormativeEquipment")) {
                        equipment.add(r2.getValues())
                    }
                    if (r2.getLong("cls") == mapCls.get("Cls_NormativeTpService")) {
                        service.add(r2.getValues())
                    }
                    if (r2.getLong("cls") == mapCls.get("Cls_NormativePersonnel")) {
                        personnel.add(r2.getValues())
                    }
                }
            }
            //
            if (material.size() > 0)
                mapRes.put("material", material)
            if (tool.size() > 0)
                mapRes.put("tool", tool)
            if (equipment.size() > 0)
                mapRes.put("equipment", equipment)
            if (service.size() > 0)
                mapRes.put("service", service)
            if (personnel.size() > 0)
                mapRes.put("personnel", personnel)
            lst.add(mapRes)
        }
        //
        return lst
    }

    @DaoMethod
    Store loadNormativeMaterial(long relobjTaskWork, String lang) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long pv = apiMeta().get(ApiMeta).idPV("relcls", map.get("RC_TaskWork"), "Prop_TaskWork")
        //
        Store st = mdb.createStore("Obj.NormativeMaterial")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        map.put("pvTaskWork", pv)
        map.put("relobjTaskWork", relobjTaskWork)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeMaterial", "")
        map.put("cls", map2.get("Cls_NormativeMaterial"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idMaterial, v1.obj as objMaterial, v1.propVal as pvMaterial, null as nameMaterial,
                v2.id as idTaskWork, v2.relobj as relobjTaskWork, v2.propVal as pvTaskWork,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v6.id as idMeasure, v6.propVal as pvMeasure, null as meaMeasure,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Material
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskWork
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskWork and v2.relobj=:relobjTaskWork 
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
    Store saveNormativeMaterial(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskWork")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("relcls", pms.getLong("linkCls"), "Prop_TaskWork")
            pms.put("pvTaskWork", pv)
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
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskWork", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskWork")} and v.propVal=${pv} 
                    and v.relobj=${pms.getLong("relobjTaskWork")} ${whe}
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
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeMaterial", "")
            par.put("cls", map.get("Cls_NormativeMaterial"))
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
            //2 Prop_TaskWork
            if (pms.getLong("relobjTaskWork") == 0)
                throw new XError("[TaskWork] не указан")
            else
                fillProperties(true, "Prop_TaskWork", pms)
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
        return loadNormativeMaterial(pms.getLong("relobjTaskWork"), "ru")
    }

    @DaoMethod
    Store loadNormativeTool(long relobjTaskWork, String lang) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long pv = apiMeta().get(ApiMeta).idPV("relcls", map.get("RC_TaskWork"), "Prop_TaskWork")
        //
        Store st = mdb.createStore("Obj.NormativeTool")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("pvTaskWork", pv)
        map.put("relobjTaskWork", relobjTaskWork)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeTool", "")
        map.put("cls", map2.get("Cls_NormativeTool"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTypTool, v1.propVal as pvTypTool, null as fvTypTool, null as nameTypTool,
                v2.id as idTaskWork, v2.relobj as relobjTaskWork, v2.propVal as pvTaskWork,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TypTool
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskWork
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskWork and v2.relobj=:relobjTaskWork 
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
    Store saveNormativeTool(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskWork")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("relcls", pms.getLong("linkCls"), "Prop_TaskWork")
            pms.put("pvTaskWork", pv)
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
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskWork", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskWork")} and v.propVal=${pv} 
                    and v.relobj=${pms.getLong("relobjTaskWork")} ${whe}
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
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeTool", "")
            par.put("cls", map.get("Cls_NormativeTool"))
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

            //2 Prop_TaskWork
            if (pms.getLong("relobjTaskWork") == 0)
                throw new XError("[TaskWork] не указан")
            else
                fillProperties(true, "Prop_TaskWork", pms)
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
        return loadNormativeTool(pms.getLong("relobjTaskWork"), "ru")
    }

    @DaoMethod
    Store loadNormativeEquipment(long relobjTaskWork, String lang) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long pv = apiMeta().get(ApiMeta).idPV("relcls", map.get("RC_TaskWork"), "Prop_TaskWork")
        //
        Store st = mdb.createStore("Obj.NormativeEquipment")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        map.put("pvTaskWork", pv)
        map.put("relobjTaskWork", relobjTaskWork)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeEquipment", "")
        map.put("cls", map2.get("Cls_NormativeEquipment"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTypEquipment, v1.propVal as pvTypEquipment, null as fvTypEquipment, null as nameTypEquipment,
                v2.id as idTaskWork, v2.relobj as relobjTaskWork, v2.propVal as pvTaskWork,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v5.id as idQuantity, v5.numberVal as Quantity,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TypEquipment
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskWork
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskWork and v2.relobj=:relobjTaskWork
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
    Store saveNormativeEquipment(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskWork")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("relcls", pms.getLong("linkCls"), "Prop_TaskWork")
            pms.put("pvTaskWork", pv)
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
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskWork", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskWork")} and v.propVal=${pv} 
                    and v.relobj=${pms.getLong("relobjTaskWork")} ${whe}
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
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeEquipment", "")
            par.put("cls", map.get("Cls_NormativeEquipment"))
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

            //2 Prop_TaskWork
            if (pms.getLong("relobjTaskWork") == 0)
                throw new XError("[TaskWork] не указан")
            else
                fillProperties(true, "Prop_TaskWork", pms)
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
        return loadNormativeEquipment(pms.getLong("relobjTaskWork"), "ru")
    }

    @DaoMethod
    Store loadNormativeTpService(long relobjTaskWork, String lang) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long pv = apiMeta().get(ApiMeta).idPV("relcls", map.get("RC_TaskWork"), "Prop_TaskWork")
        //
        Store st = mdb.createStore("Obj.NormativeTpService")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        map.put("pvTaskWork", pv)
        map.put("relobjTaskWork", relobjTaskWork)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeTpService", "")
        map.put("cls", map2.get("Cls_NormativeTpService"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idTpService, v1.obj as objTpService, v1.propVal as pvTpService, null as nameTpService,
                v2.id as idTaskWork, v2.relobj as relobjTaskWork, v2.propVal as pvTaskWork,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TpService
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskWork
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskWork and v2.relobj=:relobjTaskWork 
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
    Store saveNormativeTpService(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        // Проверка комбинации Отношения <-> Услуга сторонней организации
        long pv = pms.getLong("pvTaskWork")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("relcls", pms.getLong("linkCls"), "Prop_TaskWork")
            pms.put("pvTaskWork", pv)
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
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskWork", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskWork")} and v.propVal=${pv} 
                    and v.relobj=${pms.getLong("relobjTaskWork")} ${whe}
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
        // Создание и изменение объекта
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativeTpService", "")
            par.put("cls", map.get("Cls_NormativeTpService"))
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

            //2 Prop_TaskWork
            if (pms.getLong("relobjTaskWork") == 0)
                throw new XError("[TaskWork] не указан")
            else
                fillProperties(true, "Prop_TaskWork", pms)
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
        return loadNormativeTpService(pms.getLong("relobjTaskWork"), "kz")
    }

    @DaoMethod
    Store loadNormativePersonnel(long relobjTaskWork, String lang) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        long pv = apiMeta().get(ApiMeta).idPV("relcls", map.get("RC_TaskWork"), "Prop_TaskWork")
        //
        Store st = mdb.createStore("Obj.NormativePersonnel")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        map.put("pvTaskWork", pv)
        map.put("relobjTaskWork", relobjTaskWork)
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativePersonnel", "")
        map.put("cls", map2.get("Cls_NormativePersonnel"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Plan", "")
        map.put("FV_Plan", map2.get("FV_Plan"))

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idPosition, v1.propVal as pvPosition, null as fvPosition, null as namePosition,
                v2.id as idTaskWork, v2.relobj as relobjTaskWork, v2.propVal as pvTaskWork,
                v3.id as idUser, v3.obj as objUser, v3.propVal as pvUser, null as fullNameUser,
                v4.id as idValue, v4.numberVal as Value,
                v5.id as idQuantity, v5.numberVal as Quantity,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Position
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_TaskWork
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal=:pvTaskWork and v2.relobj=:relobjTaskWork 
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
    Store saveNormativePersonnel(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long pv = pms.getLong("pvTaskWork")
        if (pv == 0) {
            pv = apiMeta().get(ApiMeta).idPV("relcls", pms.getLong("linkCls"), "Prop_TaskWork")
            pms.put("pvTaskWork", pv)
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
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_TaskWork", "")
        Store stOwn = mdb.loadQuery("""
                select d.objorrelobj as own
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.prop=${map.get("Prop_TaskWork")} and v.propVal=${pv} 
                    and v.relobj=${pms.getLong("relobjTaskWork")} ${whe}
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
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_NormativePersonnel", "")
            par.put("cls", map.get("Cls_NormativePersonnel"))
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

            //2 Prop_TaskWork
            if (pms.getLong("relobjTaskWork") == 0)
                throw new XError("[TaskWork] не указан")
            else
                fillProperties(true, "Prop_TaskWork", pms)
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
        return loadNormativePersonnel(pms.getLong("relobjTaskWork"), "ru")
    }

    @DaoMethod
    Store loadSignMultiForSelect(Long objRelObj, String codProp) {
        List<Map<String, Object>> lst =  new ArrayList<>()
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_SignMulti", "")
        Store st = mdb.createStore("Obj.ObjList")
        mdb.loadQuery(st,"""
            select v1.obj as id, ov1.name
            from RelObj o
                left join DataProp d1 on d1.isObj=0 and d1.objorrelobj=o.id and d1.prop=${map.get("Prop_SignMulti")}
                inner join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where o.id=${objRelObj}
        """)
        //
        Set<Object> isd = st.getUniqueValues("id")
        Store stMulti = mdb.createStore("Obj.ObjList")
        mdb.loadQuery(stMulti,"""
            select o.id, o.cls, v.name, v.objParent as parent, null as pv
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and v.objParent in (0${isd.join(",")})
        """)
        //
        Long pv = apiMeta().get(ApiMeta).idPV("Cls", UtCnv.toLong(stMulti.get(0).get("cls")), codProp)

        for (StoreRecord r in stMulti) {
            r.set("pv", pv)
        }
        st.add(stMulti)
        //
        return st
    }

    @DaoMethod
    Store loadRelObjByUch1ForSelect(long uch1, String codRelTyp, String codProp) {
        if (uch1 == 0)
            throw new XError("Не указан [uch1]")
        if (codProp.isEmpty())
            throw new XError("Не указан [codProp]")
        if (codRelTyp.isEmpty())
            throw new XError("Не указан [codRelTyp]")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", codRelTyp, "")
        Store stMemb = loadSqlMeta("""
            select id from relclsmember 
            where relcls in (select id from Relcls where reltyp=${map.get(codRelTyp)})
            order by id
        """, "")

        Store stRO = mdb.loadQuery("""
            select o.id, o.relcls, ov1.name as name, null as pv
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join objver ov1 on ov1.ownerVer=r1.obj and ov1.lastVer=1
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
            where r2.obj=${uch1}
        """)

        if (stRO.size() == 0)
            return stRO

        Set<Object> idsRC = stRO.getUniqueValues("relcls")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codProp, "")
        Store stPV = loadSqlMeta("""
            select id, relcls from PropVal where prop=${map.get(codProp)}
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
    Store loadSign(Long id) {
        Map<String, Long> map
        String whe = "o.id=${id}"
        if (id == 0) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Sign", "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${"Typ_Sign"}")
            Store stTmp = loadSqlMeta("""
                select id from Cls where typ=${map.get("Typ_Sign")}
            """, "")
            Set<Object> idsCls = stTmp.getUniqueValues("id")
            whe = "o.cls in (0${idsCls.join(",")})"
        }
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        Store st = mdb.createStore("Obj.Sign")
        mdb.loadQuery(st,"""
            select o.id, o.cls, v.objParent as parent, v.name,
                v1.id as idUser, v1.propVal as pvUser, v1.obj as objUser, null as fullNameUser,
                v2.id as idCreatedAt, v2.dateTimeVal as CreatedAt,
                v3.id as idUpdatedAt, v3.dateTimeVal as UpdatedAt
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_User
                left join DataPropVal v1 on v1.dataprop=d1.id
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_CreatedAt
                left join DataPropVal v2 on v2.dataprop=d2.id
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_UpdatedAt
                left join DataPropVal v3 on v3.dataprop=d3.id
            where ${whe}
        """, map)
        //... Пересечение
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rUser = indUser.get(r.getLong("objUser"))
            if (rUser != null)
                r.set("fullNameUser", rUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveSign(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        if (pms.getString("name").isEmpty())
            throw new XError("Не указан [name]")
        if (pms.getLong("cls") == 0)
            throw new XError("Не указан [cls]")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_Sign")
        if (pms.getLong("cls") == map.get("Cls_SignVal")) {
            if (pms.getLong("parent") == 0)
                throw new XError("Не указан [parent]")
        } else if (pms.getLong("cls") == map.get("Cls_Sign")) {
            if (pms.getLong("parent") != 0)
                throw new XError("Указан [parent]")
        }
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        par.putIfAbsent("fullName", pms.getString("name"))

        if (mode.equalsIgnoreCase("ins")) {
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${pms.getLong("cls")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("Не указан [objUser]")
            //2 Prop_CreatedAt
            if (!pms.getString("CreatedAt").isEmpty())
                fillProperties(true, "Prop_CreatedAt", pms)
            else
                throw new XError("Не указан [CreatedAt]")
            //3 Prop_UpdatedAt
            if (!pms.getString("UpdatedAt").isEmpty())
                fillProperties(true, "Prop_UpdatedAt", pms)
            else
                throw new XError("Не указан [UpdatedAt]")
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            if (pms.getLong("id") == 0)
                throw new XError("Не указан [id]")
            //
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and o.id<>${pms.getLong("id")} and 
                    v.lastVer=1 and o.cls=${pms.getLong("cls")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)
            //
            own = pms.getLong("id")
            eu.updateEntity(par)
            pms.put("own", own)
            //1 Prop_User
            if (pms.getLong("idUser") > 0) {
                if (pms.getLong("objUser") > 0)
                    updateProperties( "Prop_User", pms)
                else
                    throw new XError("Не указан [objUser]")
            }else
                throw new XError("Не указан [idUser]")
            //2 Prop_UpdatedAt
            if (pms.getLong("idUpdatedAt") > 0) {
                if (!pms.getString("UpdatedAt").isEmpty())
                    updateProperties("Prop_UpdatedAt", pms)
                else
                    throw new XError("Не указан [UpdatedAt]")
            } else
                throw new XError("Не указан [idUpdatedAt]")
            //
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadSign(own)
    }

    @DaoMethod
    Store loadTaskForSelect(long objWork, String propCod) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Task", "")
        Long cls = map.get("Cls_Task")
        Long pv = apiMeta().get(ApiMeta).idPV("Cls", cls, propCod)

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_TaskWork", "")
        Store stMemb = loadSqlMeta("""
            select id from relclsmember 
            where relcls=${map.get("RC_TaskWork")}
            order by id
        """, "")

        Store st = mdb.createStore("Obj.ObjList")
        mdb.loadQuery(st,"""
            select r2.obj as id, null as cls, ov2.name as name, ov2.fullname as fullName, null as pv
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
                left join objver ov2 on ov2.ownerVer=r2.obj and ov2.lastVer=1
            where r1.obj=${objWork}
        """)

        for (StoreRecord r in st) {
            r.set("cls", cls)
            r.set("pv", pv)
        }
        return st
    }

    @DaoMethod
    Store loadTask(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Task", "")
        Store st = mdb.createStore("Obj.task")
        String whe = "o.id=${id}"
        if (id==0)
            whe = "o.cls=${map.get("Cls_Task")}"

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
    Store saveTask(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Task", "")
            String nm = pms.getString("name").trim().toLowerCase()
            Store st = mdb.loadQuery("""
                select v.name from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get("Cls_Task")} and lower(v.name)='${nm}' 
            """)
            if (st.size() > 0)
                throw new XError("[{0}] уже существует", nm)

            par.put("cls", map.get("Cls_Task"))
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
            //1 Prop_Description
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
            //1 Prop_Description
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
        return loadTask(own)
    }


    void validateForDeleteOwner(long owner, int isObj) {
        //---< check data in other DB
        if (isObj == 1) {
            Store stObj = mdb.loadQuery("""
                select o.cls, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${owner}
             """)
            if (stObj.size() > 0) {
                Store stTemp = mdb.loadQuery("""
                    select v.name
                    from RelObjMember m
                        left join RelObjVer v on v.ownerVer=m.relobj and v.lastVer=1
                    where m.obj=${owner}
                """)
                if (stTemp.size() > 0) {
                    String nm = stTemp.get(0).getString("name")
                    throw new XError("Существуют отношения объектов [${nm}]")
                }
                //
                stTemp = mdb.loadQuery("""
                    select name from ObjVer where lastVer=1 and objParent=${owner}
                """)
                if (stTemp.size() > 0) {
                    String nm = stTemp.getUniqueValues("name").join(", ")
                    throw new XError("Существуют дочерние объекты [${nm}]")
                }
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
                    //todo Проверить!
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "clientdata")
                    if (stData.size() > 0)
                        lstService.add("clientdata")
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
        } else if (isObj == 0) {
            Store stRelObj = mdb.loadQuery("""
                select o.relcls, v.name from RelObj o, RelObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${owner}
             """)
            if (stRelObj.size() > 0) {
                //
                List<String> lstService = new ArrayList<>()
                long relcls = stRelObj.get(0).getLong("relcls")
                String name = stRelObj.get(0).getString("name")
                Store stPV = loadSqlMeta("""
                    select id from PropVal where cls=${relcls}
                """, "")
                Set<Object> idsPV = stPV.getUniqueValues("id")
                if (stPV.size() > 0) {
                    Store stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "nsidata")
                    if (stData.size() > 0)
                        lstService.add("nsidata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "objectdata")
                    if (stData.size() > 0)
                        lstService.add("objectdata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "orgstructuredata")
                    if (stData.size() > 0)
                        lstService.add("orgstructuredata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "personnaldata")
                    if (stData.size() > 0)
                        lstService.add("personnaldata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "plandata")
                    if (stData.size() > 0)
                        lstService.add("plandata")
                    if (lstService.size() > 0) {
                        throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                    }
                }
            }
        } else {
            throw new XError("isObj is wrong")
        }
    }

    /*
        delete Owner without properties
    */
    @DaoMethod
    void deleteOwner(long id, int isObj) {
        //
        validateForDeleteOwner(id, isObj)
        //
        String tableName = isObj == 1 ? "Obj" : "RelObj"
        EntityMdbUtils eu = new EntityMdbUtils(mdb, tableName)
        eu.deleteEntity(id)
    }

    /*
        delete Owner with properties
    */
    @DaoMethod
    void deleteOwnerWithProperties(long id, int isObj) {
        //
        validateForDeleteOwner(id, isObj)
        //
        String tableName = isObj == 1 ? "Obj" : "RelObj"
        EntityMdbUtils eu = new EntityMdbUtils(mdb, tableName)
        mdb.execQueryNative("""
            delete from DataPropVal
            where dataProp in (select id from DataProp where isobj=${isObj} and objorrelobj=${id});
            delete from DataProp where id in (
                select id from dataprop
                except
                select dataProp as id from DataPropVal
            );
        """)
        if (tableName.equalsIgnoreCase("RelObj")) {
            try {
                mdb.execQueryNative("""
                    delete from RelObjMember
                    where relobj=${id};
                """)
            } finally {
                eu.deleteEntity(id)
            }
        } else
            eu.deleteEntity(id)
    }

    StoreRecord loadObjRec(long obj) {
        StoreRecord st = mdb.createStoreRecord("Obj.full")
        mdb.loadQueryRecord(st, """
            select o.*, v.name, v.fullName, v.objParent as parent from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id=:o
        """, [o: obj])
        return st
    }

    @DaoMethod
    Store loadObj(long cls) {
        Store st = mdb.createStore("Obj.full")
        mdb.loadQuery(st, """
            select o.*, v.name, v.fullName, v.objParent as parent from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls=:c
        """, [c: cls])
        return st
    }

    @DaoMethod
    Map<String, Long> getClsIds(String codCls) {
        if (codCls == "")
            return apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_%")
        else
            return apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codCls, "")
    }

    @DaoMethod
    Map<String, Object> idNameParent(long cls) {
        Map<String, Object> res = new HashMap<>()
        Store st = mdb.loadQuery("""
            select o.id, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.cls=:cls
        """, [cls: cls])
        if (st.size() == 0) {
            res.put("id", 0) as Map<String, Object>
            res.put("name", "") as Map<String, Object>
        } else {
            res.put("id", st.get(0).getLong("id"))
            res.put("name", st.get(0).getString("name"))
        }
        return res
    }

    @DaoMethod
    Store loadObjWithCls(String codTyp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", codTyp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codTyp}")

        Set<Object> setIdsCls = apiMeta().get(ApiMeta).setIdsOfCls(codTyp)
        if (setIdsCls.size() == 0)
            throw new XError("NotFoundCod@${codTyp}")
        String whe = setIdsCls.join(",")
        Store st = mdb.createStore("Obj.cust")
        mdb.loadQuery(st, """
            select o.id, v.objParent as parent, o.accesslevel, o.cls, v.name, null as nameCls, v.cmtver as cmt
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.cls in (${whe})
            order by o.ord
        """)
        Store stCls = apiMeta().get(ApiMeta).loadCls(codTyp)
        StoreIndex indCls = stCls.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indCls.get(r.getLong("cls"))
            if (rec != null) {
                r.set("nameCls", rec.getString("name"))
            }
        }
        return st
    }

    @DaoMethod
    Store insertObj(Map<String, Object> rec) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        long own = eu.insertEntity(rec)
        return loadObjWithClsRec(own)
    }

    @DaoMethod
    Store updateObj(Map<String, Object> rec) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        long id = UtCnv.toLong(rec.get("id"))
        eu.updateEntity(rec)
        return loadObjWithClsRec(id)
    }

    Store loadObjWithClsRec(long obj) {
        Store st = mdb.createStore("Obj.cust")
        mdb.loadQuery(st, """
            select o.id, v.objParent as parent, o.accesslevel, o.cls, v.name, null as nameCls, v.cmtver as cmt
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id=${obj}
            order by o.ord
        """)

        Store stCls = apiMeta().get(ApiMeta).recEntity("Cls", st.get(0).getLong("cls"))
        String nameCls = stCls.get(0).getString("name")
        for (StoreRecord r in st) {
            r.set("nameCls", nameCls)
        }
        return st
    }

    @DaoMethod
    Store loadClsForSelect(String codTyp) {
        return apiMeta().get(ApiMeta).loadClsForSelect(codTyp)
    }

    @DaoMethod
    Store loadDefects(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Defects", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Defects")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Defects")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store st = mdb.createStore("Obj.Defects")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idDefectsComponent, v1.propVal as pvDefectsComponent, v1.obj as objDefectsComponent, ov1.name as nameDefectsComponent,
                v2.id as idDefectsCategory, v2.propVal as pvDefectsCategory, null as fvDefectsCategory,
                v3.id as idDefectsIndex, v3.strVal as DefectsIndex,
                v4.id as idDefectsNote, v4.strVal as DefectsNote
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_DefectsComponent --1072
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on v1.obj=ov1.ownerver and ov1.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_DefectsCategory   --1074
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_DefectsIndex --1073
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_DefectsNote   --1075
                left join DataPropVal v4 on d4.id=v4.dataprop
            where ${whe}
        """, map)

        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_DefectsCategory", true)

        for (StoreRecord record in st) {
            record.set("fvDefectsCategory", mapPV.get(record.getLong("pvDefectsCategory")))
        }

        //mdb.outTable(st)
        return st
    }

    @DaoMethod
    Store loadComponents(long obj) {
        Set<Object> idsCls = apiMeta().get(ApiMeta).setIdsOfCls("Typ_Components")
        if (idsCls.size() == 0)
            throw new XError("NotFoundCod@Typ_Components")
        String whe = "o.cls in (${idsCls.join(",")})"
        if (obj > 0) whe = "o.id=${obj}"

        Store st = mdb.createStore("Obj.Components")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
            where ${whe}
        """)
        return st
    }

    @DaoMethod
    Store loadParameters(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Params", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Params")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Params")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store st = mdb.createStore("Obj.Parameters")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idParamsMeasure, v1.propVal as pvParamsMeasure, null as meaParamsMeasure,
                v2.id as idCollections, v2.propVal as pvCollections, v2.obj as objCollections,
                v3.id as idParamsDescription, v3.strVal as ParamsDescription
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.isObj=1 and d1.objorrelobj=o.id and d1.prop=:Prop_ParamsMeasure   --1105
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.isObj=1 and d2.objorrelobj=o.id and d2.prop=:Prop_Collections --1081
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.isObj=1 and d3.objorrelobj=o.id and d3.prop=:Prop_ParamsDescription   --1106
                left join DataPropVal v3 on d3.id=v3.dataprop
            where ${whe}
            order by o.id
        """, map)
        //Пересечение
        //Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("measure", "Prop_ParamsMeasure", true)
        Set<Object> pvs = st.getUniqueValues("pvParamsMeasure")
        Store stPV = apiMeta().get(ApiMeta).loadSql("""
            select m.id as mea, pv.id as pv, m.name from Measure m, PropVal pv 
            where pv.measure=m.id and pv.id in (0${pvs.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("pv")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indPV.get(r.getLong("pvParamsMeasure"))
            if (rec != null)
                r.set("meaParamsMeasure", rec.getLong("mea"))
        }
        //
        return st
    }

    @DaoMethod
    Store loadSourceCollections(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Collections", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Collections")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Collections")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store st = mdb.createStore("Obj.SourceCollections")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idDocumentNumber, v1.strVal as DocumentNumber,
                v2.id as idDocumentApprovalDate, v2.datetimeVal as DocumentApprovalDate,
                v3.id as idDocumentAuthor, v3.strVal as DocumentAuthor,
                v4.id as idDocumentStartDate, v4.datetimeVal as DocumentStartDate,
                v5.id as idDocumentEndDate, v5.datetimeVal as DocumentEndDate
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.isObj=1 and d1.objorrelobj=o.id and d1.prop=:Prop_DocumentNumber   --1082
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.isObj=1 and d2.objorrelobj=o.id and d2.prop=:Prop_DocumentApprovalDate --1083
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.isObj=1 and d3.objorrelobj=o.id and d3.prop=:Prop_DocumentAuthor   --1086
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.isObj=1 and d4.objorrelobj=o.id and d4.prop=:Prop_DocumentStartDate    --1084
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.isObj=1 and d5.objorrelobj=o.id and d5.prop=:Prop_DocumentEndDate  --1085
                left join DataPropVal v5 on d5.id=v5.dataprop
            where ${whe}
            order by o.id
        """, map)
        return st
    }

    @DaoMethod
    Map<String, Object> loadDepartmentsWithFile(long obj) {

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_LocationMulti", "")
        Store st = mdb.loadQuery("""
            select v.obj
            from DataProp d
            left join DataPropVal v on d.id=v.dataprop
            where d.isObj=1 and d.objOrRelObj=${obj} and d.prop=${map.get("Prop_LocationMulti")}
        """, "")
        Set<Object> ids = st.getUniqueValues("obj")

        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("departments", ids.join(","))
        //Files
        Store stDBFS = loadAttachedFiles(obj, "Prop_DocumentFiles")
        //
        mapRez.put("files", stDBFS)
        return mapRez
    }

    @DaoMethod
    Store loadAttachedFiles(long obj, String propCod) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", propCod, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${propCod}")
        map.put("id", obj)
        Store st = mdb.createStore("Obj.file")
        mdb.loadQuery(st, """
            select d.objorrelobj as obj, v.id as idDPV, v.fileVal, null as fileName, v.cmt
            from DataProp d, DataPropVal v 
            where d.id=v.dataprop and d.isobj=1 and d.objorrelobj=:id and d.prop=:${propCod}
        """, map)
        Set<Object> ids = st.getUniqueValues("fileVal")
        if (ids.isEmpty()) ids.add(0L)
        String whe = ids.join(",")
        Store stFS = apiMeta().get(ApiMeta).loadSql("""
            select id, originalfilename as filename from DbFileStorage where id in (${whe})
        """, "")
        StoreIndex indFS = stFS.getIndex("id")
        for (StoreRecord r : st) {
            StoreRecord rr = indFS.get(r.getLong("fileVal"))
            if (rr != null) {
                r.set("fileName", rr.getString("filename"))
            }
        }
        return st
    }

    @DaoMethod
    void saveDepartment(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Location", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Location")
        long cls = map.get("Cls_Location")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_LocationMulti", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Prop_LocationMulti")
        map.put("obj", UtCnv.toLong(params.get("obj")))


        Store stOld = mdb.loadQuery("""
            select v.id, v.obj
            from DataProp d
                left join DataPropVal v on d.id=v.dataprop
            where d.isObj=1 and d.objOrRelObj=:obj and d.prop=:Prop_LocationMulti --1080            
        """, map)
        Set<Long> idsOld = stOld.getUniqueValues("obj") as Set<Long>
        Set<Long> idsNew = UtCnv.toList(params.get("ids")) as Set<Long>
        Set<Long> idsNewLong = new HashSet<>()
        idsNew.forEach { idsNewLong.add(UtCnv.toLong(it)) }

        Set<Long> idsOldVal = new HashSet<>()
        //Deleting
        for (StoreRecord r in stOld) {
            if (!idsNewLong.contains(r.getLong("obj"))) {
                idsOldVal.add(r.getLong("id"))
            }
        }
        if (idsOldVal.size() > 0) {
            mdb.execQuery("""
                delete from DataPropVal where id in (${idsOldVal.join(",")});
                delete from DataProp
                where id in (
                    select id from DataProp
                    except
                    select dataprop as id from DataPropVal
                )
            """)
        }
        //
        //Adding
        Map<String, Object> pms = new HashMap<>()
        pms.put("own", UtCnv.toLong(params.get("obj")))
        //cls ?
        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("cls", "Prop_LocationMulti", false)
        //
        for (long obj in idsNewLong) {
            if (!idsOld.contains(obj)) {
                pms.put("objLocationMulti", obj)
                pms.put("pvLocationMulti", mapPV.get(cls))
                fillProperties(true, "Prop_LocationMulti", pms)
            }
        }
    }

    @DaoMethod
    Store loadObjList(String codClsOrTyp, String codProp, String model) {
        return apiObjectData().get(ApiObjectData).loadObjList(codClsOrTyp, codProp, model)
    }

    @DaoMethod
    Store loadObjForSelectFromObject(String codClsOrTyp) {
        Map<String, Long> map
        String sql
        if (codClsOrTyp.startsWith("Cls_")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codClsOrTyp, "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${codClsOrTyp}")
            sql = """
                select o.id, o.cls, v.name, v.objParent as parent, null as pv 
                from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get(codClsOrTyp)}
            """
        } else if (codClsOrTyp.startsWith("Typ_")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", codClsOrTyp, "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${codClsOrTyp}")
            Store stTmp = loadSqlMeta("""
                select id from Cls where typ=${map.get(codClsOrTyp)}
            """, "")
            Set<Object> idsCls = stTmp.getUniqueValues("id")
            sql = """
                select o.id, o.cls, v.objParent as parent, v.name, null as pv 
                from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls in (${idsCls.join(",")})
            """
        } else
            throw new XError("Неисвезстная сущность")


        return loadSqlService(sql, "", "objectdata")
    }

    @DaoMethod
    Store loadTypesObjects(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Set<Object> idsCls = apiMeta().get(ApiMeta).setIdsOfCls("Typ_ObjectTyp")
        if (idsCls.size() == 0)
            throw new XError("NotFoundCod@Typ_ObjectTyp")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls in (${idsCls.join(",")})"
        Store st = mdb.createStore("Obj.TypesObjects")
        mdb.loadQuery(st, """
            select o.id, v.objparent as parent, o.cls, v.name, null as nameCls,
                v2.id as idShape, v2.propVal as pvShape, null as fvShape
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Shape   --1006
                left join DataPropVal v2 on d2.id=v2.dataprop
            where ${whe}
        """, map)
        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Shape", true)

        Store stCls = loadSqlMeta("""
            select c.id, v.name
            from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.id in (${idsCls.join(",")})
        """, "")
        StoreIndex indCls = stCls.getIndex("id")

        long ind = 1
        for (StoreRecord record in st) {
            record.set("number", ind++)
            record.set("fvShape", mapPV.get(record.getLong("pvShape")))
            StoreRecord r = indCls.get(record.getLong("cls"))
            if (r != null)
                record.set("nameCls", r.getString("name"))
        }
        if (obj > 0) {
            Store stTmp = mdb.loadQuery("""
                select count(*) as cnt from Obj where cls in (${idsCls.join(",")})
            """)
            st.get(0).set("number", stTmp.get(0).getLong("cnt"))
        }

        return st
    }

    @DaoMethod
    Store loadProcessCharts(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Set<Object> idsCls = apiMeta().get(ApiMeta).setIdsOfCls("Typ_Work")
        if (idsCls.size() == 0)
            throw new XError("NotFoundCod@Typ_Work")
        String whe = "o.cls in (${idsCls.join(",")})"
        if (obj > 0) whe = "o.id=${obj}"
        Store st = mdb.createStore("Obj.ProcessCharts")
        mdb.loadQuery(st, """
            select o.id as obj, o.cls, v.name, v.fullName, null as nameCls,
                v2.id as idNumberSource, v2.strVal as NumberSource,
                v3.id as idCollections, v3.propVal as pvCollections, v3.obj as objCollections, null as nameCollections,
                v4.id as idPeriodType, v4.propVal as pvPeriodType, null as fvPeriodType, null as namePeriodType,
                v5.id as idPeriodicity, v5.numberVal as Periodicity
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_NumberSource
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_Collections
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join ObjVer ov3 on ov3.ownerVer=v3.obj and ov3.lastVer=1
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_PeriodType
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_Periodicity
                left join DataPropVal v5 on d5.id=v5.dataprop
            where ${whe}
            order by o.id
        """, map)
        //... Пересечение
        Set<Object> idsObjCollections = st.getUniqueValues("objCollections")
        Store stObjCollections = mdb.loadQuery("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObjCollections.join(",")})
        """)
        StoreIndex indCollections = stObjCollections.getIndex("id")
        //
        Set<Object> pvsPeriodType = st.getUniqueValues("pvPeriodType")
        Store stPeriodType = loadSqlMeta("""
            select p.id, p.factorVal, f.name
            from PropVal p
                left join Factor f on p.factorVal=f.id 
            where p.id in (0${pvsPeriodType.join(",")})   
        """, "")
        StoreIndex indPeriodType = stPeriodType.getIndex("id")
        //
        Store stCls = loadSqlMeta("""
            select c.id, v.name
            from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.id in (0${idsCls.join(",")})
        """, "")
        StoreIndex indCls = stCls.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recCollections = indCollections.get(r.getLong("objCollections"))
            if (recCollections != null)
                r.set("nameCollections", recCollections.getString("name"))

            StoreRecord recPeriodType = indPeriodType.get(r.getLong("pvPeriodType"))
            if (recPeriodType != null) {
                r.set("fvPeriodType", recPeriodType.getLong("factorVal"))
                r.set("namePeriodType", recPeriodType.getString("name"))
            }

            StoreRecord recCls = indCls.get(r.getLong("cls"))
            if (recCls != null)
                r.set("nameCls", recCls.getString("name"))
        }

        //mdb.outTable(st)
        return st
    }

    @DaoMethod
    Store saveParams(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Params", "")
        Map<String, Object> par = new HashMap<>(pms)
        par.put("cls", map.get("Cls_Params"))
        par.put("fullName", pms.get("name"))
        if (mode.equalsIgnoreCase("ins")) {
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_ParamsMeasure
            if (pms.getLong("meaParamsMeasure") > 0)
                fillProperties(true, "Prop_ParamsMeasure", pms)
            //2 Prop_Collections
            if (pms.getLong("objCollections") > 0)
                fillProperties(true, "Prop_Collections", pms)
            //3 Prop_ParamsDescription
            if (pms.containsKey("ParamsDescription"))
                fillProperties(true, "Prop_ParamsDescription", pms)
        } else {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_ParamsMeasure
            if (params.containsKey("idParamsMeasure"))
                updateProperties("Prop_ParamsMeasure", pms)
            else
                fillProperties(true, "Prop_ParamsMeasura", pms)
            //2 Prop_Collections
            if (params.containsKey("idCollections"))
                updateProperties("Prop_Collections", pms)
            else
                fillProperties(true, "Prop_Collections", pms)
            //3 Prop_ParamsDescription
            if (pms.getLong("idParamsDescription") > 0)
                updateProperties("Prop_ParamsDescription", pms)
            else
                fillProperties(true, "Prop_ParamsDescription", pms)
        }

        return loadParameters(own)
    }

    @DaoMethod
    double saveParamComponentValue(Map<String, Object> params) {

        params.put(UtCnv.toString(params.get("codProp")).split("_")[1], UtCnv.toDouble(params.get("val")))

        if (params.get("mode") == "ins") {
            fillProperties(false, UtCnv.toString(params.get("codProp")), params)
        } else {
            params.put("id"+UtCnv.toString(params.get("codProp")).split("_")[1], UtCnv.toLong(params.get("idVal")))

            updateProperties(UtCnv.toString(params.get("codProp")), params)
        }
        return UtCnv.toDouble(params.get("val"))
    }

    @DaoMethod
    void deletePropOfParamComponent(long id) {
        mdb.execQuery("""
            delete from DataPropVal where id=${id};
            with d as (
                select id from DataProp
                except
                select dataProp as id from DataPropVal
            )
            delete from DataProp where id in (select id from d);
        """)
    }

    @DaoMethod
    void savePropObjMulti( Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("id"))
        Boolean isObj = params.get("isObj")
        String codProp = params.get("codProp")
        List<Map<String, Object>> objLst = params.get("objOrRelObjLst") as List<Map<String, Object>>

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codProp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codProp}")

        Store stOld = mdb.loadQuery("""
            select v.id, v.obj
            from DataProp d
                left join DataPropVal v on d.id=v.dataprop
            where d.isObj=${UtCnv.toInt(isObj)} and d.objOrRelObj=${own} and d.prop=${map.get(codProp)}
        """)
        Set<Long> idsOld = stOld.getUniqueValues("obj") as Set<Long>
        Set<Long> idsNew = new HashSet<>()
        for (Map<String, Object> m in objLst) {
            idsNew.add(UtCnv.toLong(m.get("id")))
        }

        Set<Long> idsOldVal = new HashSet<>()
        //Deleting
        for (StoreRecord r in stOld) {
            if (!idsNew.contains(r.getLong("obj"))) {
                idsOldVal.add(r.getLong("id"))
            }
        }
        if (idsOldVal.size() > 0) {
            mdb.execQuery("""
                delete from DataPropVal where id in (${idsOldVal.join(",")});
                delete from DataProp
                where id in (
                    select id from DataProp
                    except
                    select dataprop as id from DataPropVal
                )
            """)
        }
        //
        Map<String, Object> pms = new HashMap<>()
        pms.put("own", own)
        String keyValue = codProp.split("_")[1]
        for (Map<String, Object> m in objLst) {
            if (!idsOld.contains(UtCnv.toLong(m.get("id")))) {
                pms.put("obj" + keyValue, UtCnv.toLong(m.get("id")))
                pms.put("pv" + keyValue, UtCnv.toLong(m.get("pv")))
                fillProperties(isObj, codProp, pms)
            }
        }
    }

    @DaoMethod
    Store saveDefects(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Defects", "")
        Map<String, Object> par = new HashMap<>(pms)
        par.put("cls", map.get("Cls_Defects"))
        par.put("fullName", pms.get("name"))
        if (mode.equalsIgnoreCase("ins")) {
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_DefectsIndex
            if (pms.getString("DefectsIndex") && pms.getString("DefectsIndex") != "")
                fillProperties(true, "Prop_DefectsIndex", pms)
            //2 Prop_DefectsNote
            if (pms.getString("DefectsNote") && pms.getString("DefectsNote") != "")
                fillProperties(true, "Prop_DefectsNote", pms)
            //3 Prop_DefectsCategory
            if (pms.getLong("fvDefectsCategory") > 0)
                fillProperties(true, "Prop_DefectsCategory", pms)
            //4 Prop_DefectsComponent
            if (pms.getLong("objDefectsComponent") > 0)
                fillProperties(true, "Prop_DefectsComponent", pms)
        } else {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_DefectsIndex
            if (params.containsKey("idDefectsIndex"))
                updateProperties("Prop_DefectsIndex", pms)
            else
                fillProperties(true, "Prop_DefectsIndex", pms)
            //2 Prop_DefectsNote
            if (params.containsKey("idDefectsNote"))
                updateProperties("Prop_DefectsNote", pms)
            else
                fillProperties(true, "Prop_DefectsNote", pms)
            //3 Prop_DefectsCategory
            if (pms.getLong("idDefectsCategory") > 0)
                updateProperties("Prop_DefectsCategory", pms)
            else
                fillProperties(true, "Prop_DefectsCategory", pms)
            //4 Prop_DefectsComponent
            if (pms.getLong("idDefectsComponent") > 0)
                updateProperties("Prop_DefectsComponent", pms)
            else
                fillProperties(true, "Prop_DefectsComponent", pms)
        }

        return loadDefects(own)
    }

    @DaoMethod
    Store saveComponents(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        params.put("fullName", pms.get("name"))
        if (mode.equalsIgnoreCase("ins"))
            own = eu.insertEntity(params)
        else {
            own = pms.getLong("id")
            eu.updateEntity(params)
        }

        return loadComponents(own)
    }

    @DaoMethod
    Store saveSourceCollections(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Collections", "")
        Map<String, Object> par = new HashMap<>(pms)
        par.put("cls", map.get("Cls_Collections"))
        par.put("fullName", pms.get("name"))
        if (mode.equalsIgnoreCase("ins")) {
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_DocumentNumber
            if (pms.containsKey("DocumentNumber"))
                fillProperties(true, "Prop_DocumentNumber", pms)
            //2 Prop_DocumentApprovalDate
            if (pms.containsKey("DocumentApprovalDate"))
                fillProperties(true, "Prop_DocumentApprovalDate", pms)

            //3 Prop_DocumentAuthor
            if (pms.containsKey("DocumentAuthor"))
                fillProperties(true, "Prop_DocumentAuthor", pms)

            //4 Prop_DocumentStartDate
            if (pms.containsKey("DocumentStartDate"))
                fillProperties(true, "Prop_DocumentStartDate", pms)

            //5 Prop_DocumentEndDate
            if (pms.containsKey("DocumentEndDate"))
                fillProperties(true, "Prop_DocumentEndDate", pms)

        } else {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_DocumentNumber
            if (pms.containsKey("idDocumentNumber"))
                updateProperties("Prop_DocumentNumber", pms)
            //2 Prop_DocumentApprovalDate
            if (pms.containsKey("idDocumentApprovalDate"))
                updateProperties("Prop_DocumentApprovalDate", pms)
            //3 Prop_DocumentAuthor
            if (pms.containsKey("idDocumentAuthor"))
                updateProperties("Prop_DocumentAuthor", pms)

            //4 Prop_DocumentStartDate
            if (pms.containsKey("idDocumentStartDate"))
                updateProperties("Prop_DocumentStartDate", pms)
            else {
                if (pms.containsKey("DocumentStartDate"))
                    fillProperties(true, "Prop_DocumentStartDate", pms)
            }
            //5 Prop_DocumentEndDate
            if (pms.containsKey("idDocumentEndDate"))
                updateProperties("Prop_DocumentEndDate", pms)
            else {
                if (pms.containsKey("DocumentEndDate"))
                    fillProperties(true, "Prop_DocumentEndDate", pms)
            }
        }

        return loadSourceCollections(own)
    }

    @DaoMethod
    void deleteTypesObjects(long id) {
        validateForDeleteOwner(id, 1)
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "", "Typ_Object%")
        Store stObj = mdb.loadQuery("""
            select o.cls, v.name from Obj o, ObjVer v 
            where o.id=v.ownerVer and v.lastVer=1 and o.id=${id}
        """)
        String name = stObj.get(0).getString("name")
        long cls = stObj.get(0).getLong("cls")
        //
        Store stCls = loadSqlMeta("""
            select c.id from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.typ in (${map.get("Typ_ObjectTyp")},${map.get("Typ_Object")})
                and v.name='${name}'
        """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        long clsObject = 0
        idsCls.forEach { Object i ->
            if (UtCnv.toLong(i) != cls)
                clsObject = UtCnv.toLong(i)
        }

        Store stTemp = loadSqlService("""
            select id from Obj where cls=${clsObject}
        """, "", "objectdata")
        if (stTemp.size() > 0)
            throw new XError("Существуют объекты класса [Cls_Object]")

        stTemp = loadSqlMeta("""
            select relcls
            from relclsmember where cls in (${idsCls.join(",")})
        """, "")
        Set<Object> idsRelCls = stTemp.getUniqueValues("relcls")
        stTemp = mdb.loadQuery("""
            select v.name
            from RelObj o, RelObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.relcls in (${idsRelCls.join(",")})
        """)

        if (stTemp.size() > 0) {
            String nm = stTemp.get(0).getString("name")
            throw new XError("Существуют отношения объектов [${nm}]")
        }
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_ObjectType", "")

        apiMeta().get(ApiMeta).execSql("""
            delete from relclsmember where relcls in (
                select relcls
                from relclsmember where cls=${cls}
            );
            delete from relclsver
            where ownerVer in (
                select id from relcls
                except
                select distinct relcls as id
                from relclsmember
            );
            delete from relcls
            where id in (
                select id from relcls
                except
                select ownerver as id
                from relclsver
            );
            delete from clsfactorval where cls in (${idsCls.join(",")});
            delete from Factor where parent=${map.get("Factor_ObjectType")} and name='${name}';
            delete from clsver where ownerVer in (${idsCls.join(",")});
            delete from cls where id in (${idsCls.join(",")});
        """)

        //
        mdb.execQueryNative("""
            delete from DataPropVal
            where dataProp in (select id from DataProp where isobj=1 and objorrelobj=${id});
            delete from DataProp where id in (
                select id from dataprop
                except
                select dataProp as id from DataPropVal
            );
        """)
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        eu.deleteEntity(id)
    }

    private long createFactorValAndCls(VariantMap params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_ObjectType", "")
        String nm = params.getString("name").trim()
        Store st = loadSqlMeta("""
            select name from Factor where parent=${map.get("Factor_ObjectType")} and name='${nm}'
        """, "")
        if (st.size() > 0)
            throw new XError("Значение фактора [${nm}] уже существует")
        //
        long idFV = apiMeta().get(ApiMeta).createFactorVal(map.get("Factor_ObjectType"), nm)
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "", "Typ_Object%")

        long clsObjectType = apiMeta().get(ApiMeta).createCls(map.get("Typ_ObjectTyp"), nm)
        apiMeta().get(ApiMeta).createClsFactorVal(clsObjectType, idFV)
        apiMeta().get(ApiMeta).createClsFactorVal(clsObjectType, 1001)  //todo Создать код
        // add to PropVal
        Store rTmp = loadSqlMeta("""
            select id, allItem from Prop where typ=${map.get("Typ_ObjectTyp")} and proptype=${FD_PropType_consts.typ}
        """, "")
        for (StoreRecord rec in rTmp) {
            if (rec.getBoolean("allItem")) {
                long prop = rec.getLong("id")
                apiMeta().get(ApiMeta).insertPropVal(prop, clsObjectType)
            }
        }
        //
        long clsObject = apiMeta().get(ApiMeta).createCls(map.get("Typ_Object"), nm)
        apiMeta().get(ApiMeta).createClsFactorVal(clsObject, idFV)
        apiMeta().get(ApiMeta).createClsFactorVal(clsObject, 1001)  //todo Создать код
        // add to PropVal
        rTmp = loadSqlMeta("""
            select id, allItem from Prop where typ=${map.get("Typ_Object")} and proptype=${FD_PropType_consts.typ}
        """, "")
        for (StoreRecord rec in rTmp) {
            if (rec.getBoolean("allItem")) {
                long prop = rec.getLong("id")
                apiMeta().get(ApiMeta).insertPropVal(prop, clsObject)
            }
        }
        //
        // Возвращает id класса clsObjectType
        return clsObjectType
    }

    private void createGroupRelCls(long clsObjectTyp) {
        Store stTmp = loadSqlMeta("select id from DataBase where modelname='nsidata'", "")
        if (stTmp.size() == 0)
            throw new XError("В таблице [DataBase] не указан сервис [nsidata]")
        long db = stTmp.get(0).getLong("id")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "", "RT_%")
        Map<String, Long> map1 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "", "Typ_%")
        apiMeta().get(ApiMeta).createGroupRelCls(map.get("RT_Components"), clsObjectTyp, 0, 0, map1.get("Typ_Components"), db)
        apiMeta().get(ApiMeta).createGroupRelCls(map.get("RT_Works"), 0, map1.get("Typ_Work"), clsObjectTyp, 0, db)
    }

    @DaoMethod
    Store saveTypesObjects(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        par.put("fullName", pms.get("name"))
        if (mode.equalsIgnoreCase("ins")) {
            long clsObjectType = createFactorValAndCls(pms)
            //
            createGroupRelCls(clsObjectType)
            //
            par.put("cls", clsObjectType)
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_Shape
            if (pms.getLong("fvShape") > 0)
                fillProperties(true, "Prop_Shape", pms)

        } else {
            own = pms.getLong("id")
            //eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_Shape
            if (params.containsKey("idShape"))
                updateProperties("Prop_Shape", pms)
            else
                fillProperties(true, "Prop_Shape", pms)
        }

        return loadTypesObjects(own)
    }

    @DaoMethod
    Store saveProcessCharts(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        //par.put("fullName", pms.get("fullName"))
        if (mode.equalsIgnoreCase("ins")) {
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_TechCard Deleted
            //2 Prop_NumberSource
            fillProperties(true, "Prop_NumberSource", pms)
            //3 Prop_Source
            fillProperties(true, "Prop_Collections", pms)
            //4 Prop_PeriodType
            fillProperties(true, "Prop_PeriodType", pms)
            //5 Prop_Periodicity
            fillProperties(true, "Prop_Periodicity", pms)
        } else {
            own = pms.getLong("obj")
            par.put("id", own)
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_TechCard   Deleted
            //2 Prop_NumberSource
            if (params.containsKey("idNumberSource"))
                updateProperties("Prop_NumberSource", pms)
            else
                fillProperties(true, "Prop_NumberSource", pms)
            //3 Prop_Source
            if (params.containsKey("idCollections"))
                updateProperties("Prop_Collections", pms)
            else
                fillProperties(true, "Prop_Collections", pms)
            //4 Prop_PeriodType
            if (params.containsKey("idPeriodType"))
                updateProperties("Prop_PeriodType", pms)
            else
                fillProperties(true, "Prop_PeriodType", pms)
            //5 Prop_PeriodType
            if (params.containsKey("idPeriodicity"))
                updateProperties("Prop_Periodicity", pms)
            else
                fillProperties(true, "Prop_Periodicity", pms)

        }
        return loadProcessCharts(own)
    }

    @DaoMethod
    long getIdRelTyp(codRelTyp) {
        Store st = loadSqlMeta("""
            select id from RelTyp where cod like '${codRelTyp}' 
        """, "")
        if (st.size() == 0)
            throw new XError("NotFoundCod@${codRelTyp}")

        return st.get(0).getLong("id")
    }

    private List<List<Object>> combAll(long relTyp) throws Exception {
        Store stRelCls = loadSqlMeta("""
            select id from RelCls where reltyp=${relTyp} order by ord
        """, "")

        List<List<Object>> lists = new ArrayList<>()

        for (StoreRecord r : stRelCls) {
            Store stMembCls = loadSqlMeta("""
                select * from relclsmember where relcls=${r.getLong("id")}
            """, "")

            List<Object> lst = new ArrayList<>()
            for (StoreRecord rr : stMembCls) {
                lst.add(rr.getLong("cls"))
            }
            lists.add(lst)
        }

        return lists

    };

    @DaoMethod
    void createGroupRelObj(long relTyp, List<List<List<Map<String, Object>>>> lists) {
        Store stRelCls = loadSqlMeta("""
            select id from RelCls where reltyp=${relTyp}
        """, "")

        Store stRelObj = mdb.loadQuery("""
            select id from relobj
            where relcls in (0${stRelCls.getUniqueValues("id").join(",")})
        """)

        Store stUch = mdb.loadQuery("""
            select relobj, obj 
            from relobjmember where relobj in (0${stRelObj.getUniqueValues("id").join(",")})
        """)
        List<List<Long>> allUch = new ArrayList<>()

        for (Object obj in stUch.getUniqueValues("relobj")) {
            long relobj = UtCnv.toLong(obj)
            Store stCur = stUch.findAll { it ->
                {
                    it['relobj'] == relobj
                }
            } as Store
            List<Long> lsCur = new ArrayList<>()
            for (StoreRecord rrr in stCur) {
                lsCur.add(rrr.getLong("obj"))
            }
            allUch.add(lsCur)
        }


        lists.forEach((List<List<Map<String, Object>>> ll) -> {
            List<List<Map<String, Object>>> lstUch = CartesianProduct.result(ll)
            lstUch.forEach((List<Map<String, Object>> uch) -> {
                List<Long> curUch = new ArrayList<>()
                curUch.add(uch.get(0).ent as Long)
                curUch.add(uch.get(1).ent as Long)

                if (!allUch.contains(curUch)) {
                    Map<String, Object> rec = new HashMap<>()
                    rec.put("relcls", uch.get(0).relcls)
                    String nm = "${uch.get(0).name} <=> ${uch.get(1).name}"
                    rec.put("name", nm)
                    rec.put("fullName", rec.get("name"))
                    EntityMdbUtils ue = new EntityMdbUtils(mdb, "RelObj")
                    long idRelObj = ue.insertEntity(rec)
                    //
                    rec = new HashMap<>()
                    rec.put("relobj", idRelObj)
                    rec.put("relclsmember", uch.get(0).rcm)
                    rec.put("cls", uch.get(0).cls)
                    rec.put("obj", uch.get(0).ent)
                    mdb.insertRec("RelObjMember", rec, true)
                    //
                    rec = new HashMap<>()
                    rec.put("relobj", idRelObj)
                    rec.put("relclsmember", uch.get(1).rcm)
                    rec.put("cls", uch.get(1).cls)
                    rec.put("obj", uch.get(1).ent)
                    mdb.insertRec("RelObjMember", rec, true)
                }
            })
        })
    }

    @DaoMethod
    Store loadObjRecOfROM(long rom) {
        Store st = mdb.createStore("Obj.ComponentsObject")
        return mdb.loadQuery(st, """
            select o.*, v.*
            from RelObjMember r
                left join Obj o on o.id=r.obj
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
            where r.id=${rom}
        """)
    }

    @DaoMethod
    Store loadRelObjRec(long id) {
        Store st = mdb.createStore("RelObj.full")
        return mdb.loadQuery(st, """
            select o.id, v.name, v.cmtVer as cmt
            from RelObj o, RelObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id=${id}
        """)
    }

    @DaoMethod
    Store editRelObj(Map<String, Object> rec) {
        mdb.execQuery("""
            update RelObjVer set name='${rec.get("name")}',fullName='${rec.get("name")}',cmtVer='${rec.get("cmt")}'
            where ownerver=${rec.get("id")} and lastVer=1
        """)
        return loadRelObjRec(UtCnv.toLong(rec.get("id")))
    }

    @DaoMethod
    Store editObj(Map<String, Object> rec) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        eu.updateEntity(rec)

        return mdb.loadQuery("""
            select ro.id, rv.name, rv.cmtver as cmt
            from Obj ro
            left join ObjVer rv on ro.id=rv.ownerver and rv.lastver=1
            where ro.id=${UtCnv.toLong(rec.get("id"))}
        """)
    }

    @DaoMethod
    Store loadComponentsObject_(long reltyp) {

        Store stRelCls = loadSqlMeta("""
            select id from RelCls where reltyp=${reltyp}
        """, "")
        String wheRelCls = "ro.relcls in (0," + stRelCls.getUniqueValues("id").join(",") + ")"

        return mdb.loadQuery("""
            select 
                ROW_NUMBER() OVER() AS number,
                ro.id, rv.name, rv.cmtver as cmt
            from RelObj ro
            left join RelObjVer rv on ro.id=rv.ownerver and rv.lastver=1
            where ${wheRelCls}
        """)
    }

    @DaoMethod
    Store loadAllRelObj(long relTyp) {
        Store st = mdb.createStore("Obj.ComponentsObject.full")
        Store stTmp = loadSqlMeta("""
            select id from RelCls where reltyp=${relTyp}
        """, "")
        String idsRelCls = stTmp.getUniqueValues("id").join(",")

        stTmp = mdb.loadQuery("""
            select id, relCls from RelObj where relcls in (0${idsRelCls})
        """)
        idsRelCls = stTmp.getUniqueValues("relCls").join(",")
        String idsRelObj = stTmp.getUniqueValues("id").join(",")

        Store stRelCls = loadSqlMeta("""
            select -c.id as id, null as parent, v.name, null as cmt, c.ord
            from RelCls c, RelClsVer v
            where c.id in (0${idsRelCls}) and c.id=v.ownerVer and v.lastVer=1
            order by c.ord
        """, "")

        st.add(stRelCls)

        Store stRelObj = mdb.loadQuery("""
            select ro.id, -ro.relcls as parent, rv.name, rv.cmtver as cmt, ro.ord
            from RelObj ro
            left join RelObjVer rv on ro.id=rv.ownerver and rv.lastver=1
            where ro.id in (0${idsRelObj})
        """)
        st.add(stRelObj)
        return st
    }

    @DaoMethod
    void deleteGroupRelObj(List<Long> params) {
        String idsRelObj = params.join(",")
        Store stROM = mdb.loadQuery("""
            select id from RelObjMember where relobj in (${idsRelObj})
        """)
        String idsROM = stROM.getUniqueValues("id").join(",")
        mdb.execQuery("""
            delete from RelObjMember where id in (${idsROM});
            delete from RelObjVer where ownerVer in (${idsRelObj});
            delete from RelObj where id in (${idsRelObj});
            delete from SysCod where entityType=2 and entityId in (${idsRelObj});
        """)
    }

    @DaoMethod
    Store loadParamsComponent(long relTyp) {
        Store st = mdb.createStore("Obj.Parameters.component")
        Store stTmp = loadSqlMeta("""
            select id from RelCls where reltyp=${relTyp}
        """, "")
        String idsRelCls = stTmp.getUniqueValues("id").join(",")

        stTmp = mdb.loadQuery("""
            select id, relCls from RelObj where relcls in (0${idsRelCls})
        """)
        idsRelCls = stTmp.getUniqueValues("relCls").join(",")
        String idsRelObj = stTmp.getUniqueValues("id").join(",")

        Store stRelCls = loadSqlMeta("""
            select -c.id as id, null as parent, v.name, null as cmt, c.ord
            from RelCls c, RelClsVer v
            where c.id in (0${idsRelCls}) and c.id=v.ownerVer and v.lastVer=1
            order by c.ord
        """, "")

        st.add(stRelCls)
        Store stRelObj = mdb.createStore("Obj.Parameters.component")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_ParamsLimit%")
        mdb.loadQuery(stRelObj, """
            select 
                ro.id, -ro.relcls as parent, rv.name, 
                v1.id as idParamsLimitMax, v1.numberval as ParamsLimitMax,
                v2.id as idParamsLimitMin, v2.numberval as ParamsLimitMin,
                v3.id as idParamsLimitNorm, v3.numberval as ParamsLimitNorm,
                null as objSignMulti, null as nameSignMulti,
                rv.cmtver as cmt, ro.ord
            from RelObj ro
            left join RelObjVer rv on ro.id=rv.ownerver and rv.lastver=1
            left join DataProp d1 on d1.isObj=0 and d1.objorrelobj=ro.id and d1.prop=:Prop_ParamsLimitMax   --1102
            left join DataPropVal v1 on d1.id=v1.dataprop
            left join DataProp d2 on d2.isObj=0 and d2.objorrelobj=ro.id and d2.prop=:Prop_ParamsLimitMin   --1103
            left join DataPropVal v2 on d2.id=v2.dataprop
            left join DataProp d3 on d3.isObj=0 and d3.objorrelobj=ro.id and d3.prop=:Prop_ParamsLimitNorm  --1104
            left join DataPropVal v3 on d3.id=v3.dataprop
            where ro.id in (0${idsRelObj})
        """, map)
        st.add(stRelObj)
        DataTreeNode dtn = UtData.createTreeIdParent(st, "id", "parent")
        long ind = 1
        UtData.scanTree(dtn, false, new ITreeNodeVisitor() {
            @Override
            void visitNode(DataTreeNode node) {
                node.record.set("number", ind++)
            }
        } as ITreeNodeVisitor)
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_SignMulti", "")
        Store stMulti = mdb.loadQuery("""
            select ro.id,
                string_agg (cast(v1.obj as varchar(2000)), ',' order by v1.obj) as lst,
                string_agg (cast(ov1.name as varchar(4000)), '; ' order by v1.obj) as lstName
            from RelObj ro 
                left join DataProp d1 on d1.isObj=0 and d1.objorrelobj=ro.id and d1.prop=:Prop_SignMulti
                inner join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where ro.id in (0${idsRelObj})
            group by ro.id
        """, map)
        StoreIndex indMulti = stMulti.getIndex("id")
        //
        for (StoreRecord record in st) {
            StoreRecord rec = indMulti.get(record.getLong("id"))
            if (rec != null) {
                record.set("objSignMulti", rec.getString("lst"))
                record.set("nameSignMulti", rec.getString("lstName"))
            }
        }
        //
        return st
    }

    @DaoMethod
    Store loadComponentsObject(long relTyp) {
        Store st = mdb.createStore("Obj.ComponentsObject.full")
        Store stTmp = loadSqlMeta("""
            select id from RelCls where reltyp=${relTyp}
        """, "")
        String idsRelCls = stTmp.getUniqueValues("id").join(",")

        stTmp = mdb.loadQuery("""
            select id, relCls from RelObj where relcls in (0${idsRelCls})
        """)
        idsRelCls = stTmp.getUniqueValues("relCls").join(",")
        String idsRelObj = stTmp.getUniqueValues("id").join(",")

        Store stRelCls = loadSqlMeta("""
            select -c.id as id, null as parent, v.name, null as cmt, c.ord
            from RelCls c, RelClsVer v
            where c.id in (0${idsRelCls}) and c.id=v.ownerVer and v.lastVer=1
            order by c.ord
        """, "")

        st.add(stRelCls)

        Store stRelObj = mdb.loadQuery("""
            select 
                ro.id, -ro.relcls as parent, rv.name, rv.cmtver as cmt, ro.ord
            from RelObj ro
            left join RelObjVer rv on ro.id=rv.ownerver and rv.lastver=1
            where ro.id in (0${idsRelObj})
        """)
        st.add(stRelObj)

        DataTreeNode dtn = UtData.createTreeIdParent(st, "id", "parent")
        long ind = 1
        UtData.scanTree(dtn, false, new ITreeNodeVisitor() {
            @Override
            void visitNode(DataTreeNode node) {
                node.record.set("number", ind++)
            }
        } as ITreeNodeVisitor)

        return st
    }

    @DaoMethod
    void saveRelObj(Map<String, Object> params) {
        String codRelTyp = UtCnv.toString(params.get("codRelTyp"))
        long uch1 = UtCnv.toLong(params.get("uch1"))
        long cls1 = UtCnv.toLong(params.get("cls1"))
        long uch2 = UtCnv.toLong(params.get("uch2"))
        long cls2 = UtCnv.toLong(params.get("cls2"))
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", codRelTyp, "")
        Store stTmp = loadSqlMeta("""
            select relcls from relclsmember
            where relcls in (
                select id from RelCls where reltyp=${map.get(codRelTyp)}
            )
            and cls=${cls1}
            and relcls in (
                select relcls from relclsmember
                where relcls in (
                    select id from RelCls where reltyp=${map.get(codRelTyp)}
                )
                and cls=${cls2}
            )
        """, "")

        if (stTmp.size() == 0)
            throw new XError("Отношение классов не найден")

        long relcls = stTmp.get(0).getLong("relcls")

        Store stRCM = loadSqlMeta("""
            select id 
            from RelClsMember
            where relcls=${relcls}
            order by id           
        """, "")
        long rcm1 = stRCM.get(0).getLong("id")
        long rcm2 = stRCM.get(1).getLong("id")

        String name = UtCnv.toString(params.get("name"))

        Map<String, Object> rec = new HashMap<>()
        rec.put("relcls", relcls)
        rec.put("name", name)
        rec.put("fullName", name)
        EntityMdbUtils ue = new EntityMdbUtils(mdb, "RelObj")
        long idRelObj = ue.insertEntity(rec)
        //
        rec = new HashMap<>()
        rec.put("relobj", idRelObj)
        rec.put("relclsmember", rcm1)
        rec.put("cls", cls1)
        rec.put("obj", uch1)
        mdb.insertRec("RelObjMember", rec, true)
        //
        rec = new HashMap<>()
        rec.put("relobj", idRelObj)
        rec.put("relclsmember", rcm2)
        rec.put("cls", cls2)
        rec.put("obj", uch2)
        mdb.insertRec("RelObjMember", rec, true)
    }

    @DaoMethod
    Store loadObjFromTyp(String codTyp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", codTyp, "")
        Store stTmp = loadSqlMeta("""
            select id from Cls where typ = ${map.get(codTyp)}
        """, "")
        Set<Object> idsCls = stTmp.getUniqueValues("id")
        Store st = mdb.loadQuery("""
            select o.id, o.cls, v.name, v.fullname
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls in (${idsCls.join(",")})
        """)
        return st
    }

    @DaoMethod
    Store loadUch2(String codRelTyp, long idUch1, String codTyp2) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", codRelTyp, "")

        Store stTmp = loadSqlMeta("""
            select id from RelCls where reltyp=${map.get(codRelTyp)}
        """, "")
        String idsRelCls = stTmp.getUniqueValues("id").join(",")

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", codTyp2, "")

        stTmp = loadSqlMeta("""
            select id from Cls where typ=${map.get(codTyp2)}
        """, "")
        String idsCls2 = stTmp.getUniqueValues("id").join(",")

        Store stRelObj = mdb.loadQuery("""
            select r2.obj
            from RelObj o
                inner join RelObjMember r1 on o.id=r1.relobj and r1.obj=(${idUch1})
                left join RelObjMember r2 on o.id=r2.relobj and r2.cls in (${idsCls2})
                left join ObjVer v1 on r1.obj=v1.ownerver and v1.lastver=1
                left join ObjVer v2 on r2.obj=v2.ownerver and v2.lastver=1
            where o.relcls in (${idsRelCls})
        """)
        //
        Set<Object> idsUch2 = stRelObj.getUniqueValues("obj")

        stTmp = loadSqlMeta("""
            select id from Cls where typ = ${map.get(codTyp2)}
        """, "")
        Set<Object> idsCls = stTmp.getUniqueValues("id")
        Store st = mdb.loadQuery("""
            select o.id, o.cls, v.name, v.fullname
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls in (${idsCls.join(",")})
                and o.id not in (0${idsUch2.join(",")})
        """)
        return st
    }


    @DaoMethod
    Store loadComponentsObject2(String codRelTyp, String codTyp1, String codTyp2) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", codRelTyp, "")

        Store stTmp = loadSqlMeta("""
            select id from RelCls where reltyp=${map.get(codRelTyp)}
        """, "")
        String idsRelCls = stTmp.getUniqueValues("id").join(",")
//Typ_ObjectTyp 1002 Typ_Components 1006
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "", "Typ_%")
        stTmp = loadSqlMeta("""
            select id from Cls where typ=${map.get(codTyp1)}
        """, "")
        String idsCls1 = stTmp.getUniqueValues("id").join(",")

        stTmp = loadSqlMeta("""
            select id from Cls where typ=${map.get(codTyp2)}
        """, "")
        String idsCls2 = stTmp.getUniqueValues("id").join(",")

        Store stRelObj = mdb.loadQuery("""
            select o.id as idRO, r1.obj as idROM1, r1.cls as clsROM1, v1.fullname as nameROM1, 
                r2.obj as idROM2, r2.cls as clsROM2, v2.name as nameROM2
            from RelObj o
                left join RelObjMember r1 on o.id=r1.relobj and r1.cls in (${idsCls1})
                left join RelObjMember r2 on o.id=r2.relobj and r2.cls in (${idsCls2})
                left join ObjVer v1 on r1.obj=v1.ownerver and v1.lastver=1
                left join ObjVer v2 on r2.obj=v2.ownerver and v2.lastver=1
            where o.relcls in (${idsRelCls})
        """)
        return stRelObj
    }


    @DaoMethod
    Store loadRelObjMember(long relobj) {
        //Store st = mdb.createStore("Obj.ComponentsObject")
        return mdb.loadQuery("""
            select r.id, v.name, v.cmtVer as cmt, o.id as obj
            from RelObjMember r
                left join Obj o on o.id=r.obj
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
            where relobj=${relobj}
        """)
    }

    @DaoMethod
    Store loadAllMembers(Map<String, Object> params) throws Exception {
        long reltyp = UtCnv.toLong(params.get("relTyp"))
        Store stRes = mdb.createStore("RelObjMember.all")

        Store stRC = loadSqlMeta("""
            select 'rc_'||c.id as id, null as parent, v.name, 'rc_'||c.id as cod, --null as cod, 
                null as cls, c.id as relcls, 0 as ent, c.ord
            from RelCls c, RelClsVer v
            where c.reltyp=${reltyp} and c.id=v.ownerver and v.lastver=1
            order by c.ord
        """, "")
        stRes.add(stRC)

        int i = 0
        for (StoreRecord r : stRC) {
            i++
            Store stCls = loadSqlMeta("""
                select 'c_'||r.cls||'_'||${i} as id, 'rc_'||c.id as parent, r.name, 'c_'||r.cls||'_'||${i} as cod,--null as cod, 
                    r.cls as cls, c.id as relcls, 0 as ent, r.id as ord
                from RelClsMember r, Relcls c
                where r.relcls=c.id and c.id=${r.getLong("relcls")}
                order by r.id
            """, "")
            stRes.add(stCls)
            for (StoreRecord rr : stCls) {
                Store stRO = mdb.createStore("RelObjMember.all")
                String prn = rr.getString("id")
                long rc = rr.getLong("ord")
                mdb.loadQuery(stRO, """
                    select 'o_'||o.id||'_'||${i} as id, '${prn}' as parent, v.name, o.cod, o.cls, ${rr.getLong("relcls")} as relcls, o.id as ent, o.ord,
                        '${rr.getString("parent")}_${rc}' as membs, ${rc} as rcm
                    from Obj o
                    left join ObjVer v on o.id=v.ownerver and v.lastver=1
                    where o.cls=${rr.getLong("cls")}
                    order by o.ord
                """)
                stRes.add(stRO)
            }
        }

        return stRes
    }

    @DaoMethod
    Store loadComponentDefect(String codTyp, String codProp) {
        Set<Object> idsCls = apiMeta().get(ApiMeta).setIdsOfCls(codTyp)
        Store st = mdb.loadQuery("""
            select o.id, o.cls, v.name as name, 0 as pv
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls in (0${idsCls.join(",")})
        """)

        Store stPV = loadSqlMeta("""
            select pv.id, pv.cls from PropVal pv, Prop p 
            where pv.prop=p.id and p.cod like '${codProp}'
        """, "")
        StoreIndex indPV = stPV.getIndex("cls")
        for (StoreRecord r in st) {
            StoreRecord rec = indPV.get(r.getLong("cls"))
            if (rec != null)
                r.set("pv", rec.getLong("id"))
        }

        return st
    }

    @DaoMethod
    Store loadMeasure(String codProp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codProp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codProp}")

        return loadSqlMeta("""
            select m.id, m.name, p.id as pv
            from PropVal p, Measure m
            where p.measure=m.id and p.prop=${map.get(codProp)}
        """, "")
    }

    @DaoMethod
    Store loadDepartments(String codTyp, String codProp) {
        //return loadObjTreeForSelect(codCls, codProp)
        return apiOrgStructureData().get(ApiOrgStructureData).loadObjTreeForSelect(codTyp, codProp)
    }

    @DaoMethod
    Store loadCollections(String codCls, String codProp) {
        return loadObjForSelect(codCls, codProp)
    }


    private Store loadObjTreeForSelect(String codCls, String codProp) {
        return apiNSIData().get(ApiNSIData).loadObjTreeForSelect(codCls, codProp)
    }

    private Store loadObjForSelect(String codCls, String codProp) {
        return apiNSIData().get(ApiNSIData).loadObjForSelect(codCls, codProp)
    }

//todo Delete!

    @DaoMethod
    Store loadFvCategory(String codFactor) {
        return loadFvForSelect(codFactor)
    }

    @DaoMethod
    Store loadFvSource(String codFactor) {
        return loadFvForSelect(codFactor)
    }

    @DaoMethod
    Store loadFvPeriodType(String codFactor) {
        return loadFvForSelect(codFactor)
    }

    @DaoMethod
    Store loadFvOt(String codFactor) {
        return loadFvForSelect(codFactor)
    }

    @DaoMethod
    Store loadFvForSelect(String codFactor) {
        return apiMeta().get(ApiMeta).loadFactorValsWithPV(codFactor)
    }

    @DaoMethod
    long getCls(long obj) {
        return loadObjRec(obj).getLong("cls")
    }

    @DaoMethod
    void deleteFileValue(Map<String, Object> rec) {
        String path
        try {
            path = mdb.getApp().bean(DataDirService.class).getPath("dbfilestorage")
        } catch (Exception e) {
            path = ""
            e.printStackTrace()
        }

        String bucketName = ""
        if (path == "") {
            try {
                Conf conf2 = mdb.getApp().getConf().getConf("datadir/minio")
                bucketName = conf2.getString("bucketName")
            } catch (Exception e) {
                bucketName = ""
                e.printStackTrace()
            }
        }

        if (path != "" && bucketName == "") {
            deleteFileValueFS(rec)
        } else if (path == "" && bucketName != "") {
            deleteFileValueMinio(rec)
        } else {
            throw new XError("FileStorage не настроен!")
        }
    }

    private void deleteFileValueFS(Map<String, Object> params) {
        long fileId = UtCnv.toLong(params.get("fileVal"))
        long id = UtCnv.toLong(params.get("idDPV"))

        try {
            DbFileStorageService dfsrv = apiMeta().get(ApiMeta).getDbFileStorageService()
            dfsrv.setModelName(UtCnv.toString(params.get("model")))
            dfsrv.removeFile(fileId)
        } finally {
            String sql = """
                delete from DataPropVal where id=${id};
                with d as (
                    select id from DataProp
                    except
                    select dataProp as id from DataPropVal
                )
                delete from DataProp where id in (select id from d);
            """
            execSql(sql, UtCnv.toString(params.get("model")))
        }
    }

    private static void deleteFileValueMinio(Map<String, Object> params) {
        throw new XError("MinIO не настроен!")
    }

    private void fillProperties(boolean isObj, String cod, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("own"))
        String keyValue = cod.split("_")[1]
        def objRef = UtCnv.toLong(params.get("obj" + keyValue))
        def propVal = UtCnv.toLong(params.get("pv" + keyValue))
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
            if (cod.equalsIgnoreCase("Prop_NumberSource") ||
                    cod.equalsIgnoreCase("Prop_NumberOt") ||
                    cod.equalsIgnoreCase("Prop_DefectsIndex") ||
                    cod.equalsIgnoreCase("Prop_DefectsNote") ||
                    cod.equalsIgnoreCase("Prop_DocumentNumber") ||
                    cod.equalsIgnoreCase("Prop_DocumentAuthor") ||
                    cod.equalsIgnoreCase("Prop_ParamsDescription")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib multistr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_DocumentApprovalDate") ||
                    cod.equalsIgnoreCase("Prop_DocumentStartDate") ||
                    cod.equalsIgnoreCase("Prop_DocumentEndDate") ||
                    cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }
        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Source") ||
                    cod.equalsIgnoreCase("Prop_PeriodType") ||
                    cod.equalsIgnoreCase("Prop_Shape") ||
                    cod.equalsIgnoreCase("Prop_DefectsCategory") ||
                    cod.equalsIgnoreCase("Prop_Position") ||
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
            if (cod.equalsIgnoreCase("Prop_ParamsMeasure") ||
                    cod.equalsIgnoreCase("Prop_Measure") ) {
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
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_StageLength") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMax") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMin") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitNorm") ||
                    cod.equalsIgnoreCase("Prop_Periodicity") ||
                    cod.equalsIgnoreCase("Prop_Value") ||
                    cod.equalsIgnoreCase("Prop_Quantity")) {
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
            if (cod.equalsIgnoreCase("Prop_DefectsComponent") ||
                    cod.equalsIgnoreCase("Prop_Collections") ||
                    cod.equalsIgnoreCase("Prop_LocationMulti") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_SignMulti") ||
                    cod.equalsIgnoreCase("Prop_TpService") ||
                    cod.equalsIgnoreCase("Prop_Material")) {
                if (objRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("obj", objRef)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // For RelTyp
        if ([FD_PropType_consts.reltyp].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_TaskWork")) {
                if (relRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("relobj", relRef)
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
        // For Attrib str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_NumberSource") ||
                    cod.equalsIgnoreCase("Prop_NumberOt") ||
                    cod.equalsIgnoreCase("Prop_DefectsIndex") ||
                    cod.equalsIgnoreCase("Prop_DefectsNote") ||
                    cod.equalsIgnoreCase("Prop_DocumentNumber") ||
                    cod.equalsIgnoreCase("Prop_DocumentAuthor") ||
                    cod.equalsIgnoreCase("Prop_ParamsDescription")) {
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
        // For Attrib multistr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null) {
                    sql = "update DataPropval set multiStrVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // For Attrib date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_DocumentApprovalDate") ||
                    cod.equalsIgnoreCase("Prop_DocumentStartDate") ||
                    cod.equalsIgnoreCase("Prop_DocumentEndDate") ||
                    cod.equalsIgnoreCase("Prop_CreatedAt") ||
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
            if (cod.equalsIgnoreCase("Prop_Source") ||
                    cod.equalsIgnoreCase("Prop_PeriodType") ||
                    cod.equalsIgnoreCase("Prop_Shape") ||
                    cod.equalsIgnoreCase("Prop_DefectsCategory") ||
                    cod.equalsIgnoreCase("Prop_Position") ||
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
            if (cod.equalsIgnoreCase("Prop_ParamsMeasure") ||
                    cod.equalsIgnoreCase("Prop_Measure")) {
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
            if (cod.equalsIgnoreCase("Prop_StartKm") ||
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_StageLength") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMax") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitMin") ||
                    cod.equalsIgnoreCase("Prop_ParamsLimitNorm") ||
                    cod.equalsIgnoreCase("Prop_Periodicity") ||
                    cod.equalsIgnoreCase("Prop_Value") ||
                    cod.equalsIgnoreCase("Prop_Quantity")) {
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
            if (cod.equalsIgnoreCase("Prop_DefectsComponent") ||
                    cod.equalsIgnoreCase("Prop_Collections") ||
                    cod.equalsIgnoreCase("Prop_LocationMulti") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_TpService") ||
                    cod.equalsIgnoreCase("Prop_Material")) {
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

    private void execSql(String sql, String model) {
        if (model.equalsIgnoreCase("userdata"))
            apiUserData().get(ApiUserData).execSql(sql)
        else if (model.equalsIgnoreCase("nsidata"))
            apiNSIData().get(ApiNSIData).execSql(sql)
        else
            throw new XError("Unknown model [${model}]")
    }

    @DaoMethod
    String getPathFile(long id) {

        DbFileStorageService dfsrv = apiMeta().get(ApiMeta).getDbFileStorageService()
        dfsrv.setModelName(UtCnv.toString("nsidata"))
        DbFileStorageItem dfsi = dfsrv.getFile(id)

        String pdf_dir = getApp().getAppdir() + File.separator + "frontend" + File.separator + "pdf"
        //String pdf_dir = getApp().getAppdir() + File.separator + "pdf"
        File fle = dfsi.getFile()

        File file = new File(UtFile.join(pdf_dir, dfsi.originalFilename))
        if (UtFile.exists(file))
            file.delete()

        try (InputStream ins = new FileInputStream(fle)) {
            Files.copy(ins, Paths.get(pdf_dir, dfsi.originalFilename))
        } catch (Exception e) {
            e.printStackTrace()
        }
        String pathFile = '/pdf/' + dfsi.originalFilename

        return pathFile

    }

/*
    private Store loadFvForSelect(String codFactor) {
        return apiMetaFish().get(ApiMetaFish).loadFvForSelect(codFactor)
    }*/

    @DaoMethod
    Store loadClsTree(Map<String, Object> params) {
        return apiMeta().get(ApiMeta).loadClsTree(params)
    }

    //-------------------------
    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }

    private Store loadSqlService(String sql, String domain, String model) {
        if (model.equalsIgnoreCase("userdata"))
            return apiUserData().get(ApiUserData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("nsidata"))
            return apiNSIData().get(ApiNSIData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("objectdata"))
            return apiObjectData().get(ApiObjectData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("personnaldata"))
            return apiPersonnalData().get(ApiPersonnalData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("orgstructuredata"))
            return apiOrgStructureData().get(ApiOrgStructureData).loadSql(sql, domain)
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

    @DaoMethod
    Map<String, Object> getCurUserInfo() {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        AuthUser au = authSvc.getCurrentUser()
        if (au == null) {
            throw new XError("NotLogined")
        }
        return au.getAttrs()
    }

    private long getUser() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        long au = authSvc.getCurrentUser().getAttrs().getLong("id")
        if (au == 0) {
            au = 1
            //throw new XError("notLogined")
        }
        return au
    }


}
