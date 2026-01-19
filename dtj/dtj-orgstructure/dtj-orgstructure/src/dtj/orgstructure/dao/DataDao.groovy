package dtj.orgstructure.dao

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
import jandcode.core.std.CfgService
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiMonitoringData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiRepairData
import tofi.api.dta.ApiResourceData
import tofi.api.dta.ApiUserData
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
    ApinatorApi apiIncidentData() {
        return app.bean(ApinatorService).getApi("incidentdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }

    /* =================================================================== */

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
                //
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
                """, "", "incidentdata")
                if (stData.size() > 0)
                    lstService.add("incidentdata")
                //
                stData = loadSqlService("""
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
                if (lstService.size()>0) {
                    throw new XError("${name} используется в ["+ lstService.join(", ") + "]")
                }

            }
        }
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
    List<Map<String, Object>> loadLocationByWorkAndSectionForSelect(long objWork, long objSection) {
        List<Map<String, Object>> lstRes = new ArrayList<>()
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        // Поиск Околоток по Источникам работы
        Store stTmp = loadSqlService("""
            select v.obj
            from DataProp d
                left join DataPropVal v on d.id=v.dataprop
            where d.isObj=1 and d.prop=${map.get("Prop_LocationMulti")} and d.objOrRelObj in (
                select v.obj from DataProp d, DataPropVal v
                where d.id=v.dataprop and d.objorrelobj=${objWork} and d.prop=${map.get("Prop_Collections")}
            )
        """, "", "nsidata")
        if (stTmp.size() == 0)
            return lstRes
        Set<Object> idsLocation = stTmp.getUniqueValues("obj")
        // Получение координаты места
        stTmp = loadSqlService("""
            select o.id,
                v1.numberVal * 1000 + (v3.numberVal - 1) * 100 + (v5.numberVal - 1) * 25 as dbeg,
                v2.numberVal * 1000 + (v4.numberVal - 1) * 100 + v6.numberVal * 25 as dend
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v6 on d6.id=v6.dataprop
            where o.id=${objSection}
        """, "", "objectdata")
        if (stTmp.size() == 0)
            return lstRes
        int dbeg = UtCnv.toInt(stTmp.get(0).get("dbeg"))
        int dend = UtCnv.toInt(stTmp.get(0).get("dend"))
        // Получение координаты Околотков
        Store stRez = mdb.loadQuery("""
            select o.id, o.cls, v.name,
                v1.numberVal * 1000 as dbeg,
                (v2.numberVal + 1) * 1000 as dend
            from Obj o
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v2 on d2.id=v2.dataprop
            where o.id in (0${idsLocation.join(",")})
        """)
        if (stRez.size() == 0)
            return lstRes
        // Получение Типы объектов по Работе
        stTmp = loadUch2("RT_Works", objWork, "Typ_ObjectTyp")
        Set<Object> idsTypObj = stTmp.getUniqueValues("id")
        StoreIndex indTypObj = stTmp.getIndex("id")
        // Получение Типы объектов по Околоткам
        stTmp = mdb.loadQuery("""
            select o.id,
                string_agg (cast(v1.obj as varchar(2000)), ',' order by v1.obj) as lst
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_ObjectTypeMulti")}
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj in (0${idsTypObj.join(",")})
            where o.id in (0${idsLocation.join(",")})
            group by o.id
        """)
        StoreIndex indLocation = stTmp.getIndex("id")
        //
        long pv = apiMeta().get(ApiMeta).idPV("Cls", UtCnv.toLong(stRez.get(0).get("cls")), "Prop_LocationClsSection")
        for (StoreRecord r in stRez) {
            if ((dbeg < r.getInt("dbeg") && r.getInt("dbeg") <= dend) || (dbeg < r.getInt("dend") && r.getInt("dend") <= dend)) {
                Map<String, Object> mapRes = r.getValues()
                mapRes.put("pv", pv)
                StoreRecord rec = indLocation.get(r.getLong("id"))
                if (rec != null) {
                    List<Map<String, Object>> lstOT = new ArrayList<>()
                    Set<Long> lst = rec.getString("lst").split(",") as Set<Long>
                    lst.forEach {   {
                        Map<String, Object> mapR = indTypObj.get(it).getValues()
                        lstOT.add(mapR)
                    }}
                    mapRes.put("objObjectTypeMulti", lstOT)
                }
                lstRes.add(mapRes)
            }
        }
        return lstRes
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

        Store stRelObj = loadSqlService("""
            select r2.obj
            from RelObj o
                inner join RelObjMember r1 on o.id=r1.relobj and r1.obj=(${idUch1})
                left join RelObjMember r2 on o.id=r2.relobj and r2.cls in (${idsCls2})
                left join ObjVer v1 on r1.obj=v1.ownerver and v1.lastver=1
                left join ObjVer v2 on r2.obj=v2.ownerver and v2.lastver=1
            where o.relcls in (${idsRelCls})
        """, "", "nsidata")
        //
        Set<Object> idsUch2 = stRelObj.getUniqueValues("obj")

        stTmp = loadSqlMeta("""
            select id from Cls where typ = ${map.get(codTyp2)}
        """, "")
        Set<Object> idsCls = stTmp.getUniqueValues("id")
        Store st = loadSqlService("""
            select o.id, o.cls, v.name, v.fullname
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls in (${idsCls.join(",")})
                and o.id not in (0${idsUch2.join(",")})
        """, "", "nsidata")
        return st
    }

    @DaoMethod
    Store loadLocation(long id) {

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Location", "")
        Store st = mdb.createStore("Obj.Location")

        Store stCls = loadSqlMeta("""
            select c.id , v.name
            from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get("Typ_Location")}
        """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        StoreIndex indCls = stCls.getIndex("id")

        String whe = "o.id=${id}"
        if (id==0)
            whe = "o.cls in (${idsCls.join(",")})"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        mdb.loadQuery(st, """
            select o.id, v.objParent as parent, o.cls, v.name, v.fullName, null as nameCls, prn.name as nameParent,
                v1.id as idAddress, v1.strVal as Address,
                v2.id as idPhone, v2.strVal as Phone,
                null as objObjectTypeMulti,
                v4.id as idStartKm, v4.numberVal as StartKm,
                v5.id as idFinishKm, v5.numberVal as FinishKm,
                v6.id as idStageLength, v6.numberVal as StageLength,
                v7.id as idRegion, v7.propVal as pvRegion, null as fvRegion, null as nameRegion,
                v8.id as idIsActive, v8.propVal as pvIsActive, null as fvIsActive, null as nameIsActive,
                v9.id as idCreatedAt, v9.dateTimeVal as CreatedAt,
                v10.id as idUpdatedAt, v10.dateTimeVal as UpdatedAt,
                v11.id as idDescription, v11.multiStrVal as Description
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join ObjVer prn on v.objParent=prn.ownerver and prn.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Address
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on v1.obj=ov1.ownerver and ov1.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Phone
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_StartKm
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_FinishKm
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_StageLength
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_Region
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_IsActive
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_CreatedAt
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_UpdatedAt
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_Description
                left join DataPropVal v11 on d11.id=v11.dataprop
            where ${whe}
        """, map)

        Map<String, Long> mapFV = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "", "Factor_")
        Store stFV = loadSqlMeta("""
            select id, name
            from Factor where parent in (${mapFV.get("Factor_Region")}, ${mapFV.get("Factor_IsActive")}) 
        """, "")
        StoreIndex indFV = stFV.getIndex("id")

        //
        Store stMulti = mdb.loadQuery("""
            select o.id,
                string_agg (cast(v1.obj as varchar(2000)), ',' order by v1.obj) as lst,
                string_agg (cast(ov1.name as varchar(4000)), ',' order by v1.obj) as lstName
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ObjectTypeMulti
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where ${whe}
            group by o.id
        """, map)
        StoreIndex indMulti = stMulti.getIndex("id")
        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Region", true)
        Map<Long, Long> mapPV2 = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_IsActive", true)
        mapPV.putAll(mapPV2)
        for (StoreRecord record in st) {
            StoreRecord rec = indMulti.get(record.getLong("id"))
            List<Long> lstMulti = new ArrayList<>()
            List<String> lstMultiName = new ArrayList<>()
            if (rec != null) {
                Store stObjList = loadSqlService("""
                    select o.id, o.cls, v.name, null as pv 
                    from Obj o, ObjVer v
                    where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${rec.getString("lst")})                    
                """, "", "nsidata")
                for (StoreRecord r in stObjList) {
                    lstMulti.add(r.getLong("id"))
                    lstMultiName.add(r.getString("name"))
                }
            }
            if (!lstMulti.isEmpty()) {
                record.set("objObjectTypeMulti", lstMulti.join(","))
                record.set("nameObjectTypeMulti", lstMultiName.join("; "))
            }
            StoreRecord rec2 = indCls.get(record.getLong("cls"))
            if (rec2 != null)
                record.set("nameCls", rec2.getString("name"))
            record.set("fvRegion", mapPV.get(record.getLong("pvRegion")))
            record.set("fvIsActive", mapPV.get(record.getLong("pvIsActive")))
            StoreRecord rFV = indFV.get(record.get("fvRegion"))
            if (rFV != null)
                record.set("nameRegion", rFV.getString("name"))
            rFV = indFV.get(record.get("fvIsActive"))
            if (rFV != null)
                record.set("nameIsActive", rFV.getString("name"))

        }
        //
        return st
    }

    private void saveObjectTypeMulti(long own, List<Map<String, Object>> objLst) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_ObjectTypeMulti", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Prop_ObjectTypeMulti")
        map.put("own", own)
        Store stOld = mdb.loadQuery("""
            select v.id, v.obj
            from DataProp d
                left join DataPropVal v on d.id=v.dataprop
            where d.isObj=1 and d.objOrRelObj=:own and d.prop=:Prop_ObjectTypeMulti          
        """, map)
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
        //Adding
        Map<String, Object> pms = new HashMap<>()
        pms.put("own", own)

        for (Map<String, Object> m in objLst) {
            idsNew.add(UtCnv.toLong(UtCnv.toLong(m.get("id"))))
            if (!idsOld.contains(m.get("id"))) {
                pms.put("objObjectTypeMulti", UtCnv.toLong(m.get("id")))
                pms.put("pvObjectTypeMulti", UtCnv.toLong(m.get("pv")))
                fillProperties(true, "Prop_ObjectTypeMulti", pms)
            }
        }
    }

    @DaoMethod
    Store saveLocation(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        List<Map<String, Object>> objMulti = pms.get("objObjectTypeMulti") as List<Map<String, Object>>
        pms.remove("objObjectTypeMulti")
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            par.put("fullName", par.get("name"))
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_Address
            if (pms.getString("Address") != "")
                fillProperties(true, "Prop_Address", pms)
            //2 Prop_Phone
            if (pms.getString("Phone") != "")
                fillProperties(true, "Prop_Phone", pms)

            //3 Prop_StartKm
            if (pms.getString("FinishKm") != "")
                fillProperties(true, "Prop_FinishKm", pms)

            //4 Prop_FinishKm
            if (pms.getString("StartKm") != "")
                fillProperties(true, "Prop_StartKm", pms)

            //5 Prop_StartPicket
            if (pms.getString("StageLength") != "")
                fillProperties(true, "Prop_StageLength", pms)
            //6 Prop_Region
            if (pms.getLong("fvRegion") > 0)
                fillProperties(true, "Prop_Region", pms)
            //7 Prop_IsActive
            if (pms.getLong("fvIsActive") > 0)
                fillProperties(true, "Prop_IsActive", pms)
            //8 Prop_CreatedAt
            if (pms.getString("CreatedAt") != "")
                fillProperties(true, "Prop_CreatedAt", pms)
            //9 Prop_UpdatedAt
            if (pms.getString("UpdatedAt") != "")
                fillProperties(true, "Prop_UpdatedAt", pms)
            //10 Prop_Description
            if (pms.getString("Description") != "")
                fillProperties(true, "Prop_Description", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.put("fullName", par.get("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_Address
            if (pms.containsKey("idAddress"))
                updateProperties("Prop_Address", pms)
            else {
                if (!pms.getString("Address").isEmpty())
                    fillProperties(true, "Prop_Address", pms)
            }
            //2 Prop_Phone
            if (pms.containsKey("idPhone"))
                updateProperties("Prop_Phone", pms)
            else {
                if (!pms.getString("Phone").isEmpty())
                    fillProperties(true, "Prop_Phone", pms)
            }
            //3 Prop_StartKm
            if (pms.containsKey("idStartKm"))
                updateProperties("Prop_StartKm", pms)
            else {
                if (!pms.getString("StartKm").isEmpty())
                    fillProperties(true, "Prop_StartKm", pms)
            }
            //4 Prop_FinishKm
            if (pms.containsKey("idFinishKm"))
                updateProperties("Prop_FinishKm", pms)
            else {
                if (!pms.getString("FinishKm").isEmpty())
                    fillProperties(true, "Prop_FinishKm", pms)
            }
            //5 Prop_StartPicket
            if (pms.containsKey("idStageLength"))
                updateProperties("Prop_StageLength", pms)
            else {
                if (!pms.getString("StageLength").isEmpty())
                    fillProperties(true, "Prop_StageLength", pms)
            }
            //6 Prop_Region
            if (pms.containsKey("idRegion"))
                updateProperties("Prop_Region", pms)
            else {
                if (pms.getLong("fvRegion") > 0)
                    fillProperties(true, "Prop_Region", pms)
            }
            //7 Prop_IsActive
            if (pms.containsKey("idIsActive"))
                updateProperties("Prop_IsActive", pms)
            else {
                if (pms.getLong("fvIsActive") > 0)
                    fillProperties(true, "Prop_IsActive", pms)
            }
            //8 Prop_CreatedAt
            if (pms.containsKey("idCreatedAt"))
                updateProperties("Prop_CreatedAt", pms)
            else {
                if (!pms.getString("CreatedAt").isEmpty())
                    fillProperties(true, "Prop_CreatedAt", pms)
            }

            //9 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                updateProperties("Prop_UpdatedAt", pms)
            else {
                if (!pms.getString("UpdatedAt").isEmpty())
                    fillProperties(true, "Prop_UpdatedAt", pms)
            }
            //10 Prop_Description
            if (pms.containsKey("idDescription"))
                updateProperties("Prop_Description", pms)
            else {
                if (!pms.getString("Description").isEmpty())
                    fillProperties(true, "Prop_Description", pms)
            }
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        if (objMulti != null) {
            saveObjectTypeMulti(own, objMulti)
        }
        //
        return loadLocation(own)
    }

    @DaoMethod
    Store loadDepartmentForSelect() {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_LocationSection", "")
        return mdb.loadQuery("""
            select o.id, v.name 
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls=:Cls_LocationSection
                and v.objparent not in (select id from Obj where cls=:Cls_LocationSection)
        """, map)
    }

    @DaoMethod
    Store loadObjForSelect(String codClsOrTyp) {
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

        Store st = mdb.loadQuery(sql)
        Set<Object> ids = st.getUniqueValues("id")
        for (StoreRecord r in st) {
            if (r.get("parent") != null) {
                if (!ids.contains(r.getLong("parent")))
                    r.set("parent", null)
            }
        }
        return st
    }

    /**
     *
     * @param codPropOrFactor: код фактора или код пропа
     * @return  Набор значений указанного фактора
     */
    @DaoMethod
    Store loadFactorValForSelect(String codPropOrFactor) {
        Map<String, Long> map
        String sql
        if (codPropOrFactor.startsWith("Prop_")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codPropOrFactor, "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${codPropOrFactor}")
            sql = """
                select fv.id, fv.name, p.id as pv, f.id as factor
                from propval p, factor fv, factor f 
                where fv.parent=f.id and p.factorval=fv.id and p.prop=${map.get(codPropOrFactor)}
            """
        } else if (codPropOrFactor.startsWith("Factor_")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", codPropOrFactor, "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${codPropOrFactor}")
            sql = """
                select f.id, f.name, p.id as pv, f.parent as factor
                from propval p, factor f 
                where p.factorval=f.id and f.parent=${map.get(codPropOrFactor)}
            """
        } else {
            throw new XError("Неисвезстная сущность")
        }
        return loadSqlMeta(sql, "")
    }

    /**
     *
     * @param codClsOr: Код класса или код типа
     * @param model:    Идентификатор сервиса (nsidata,plandata,personnaldata,...)
     * @return  Набор записей
     */
    @DaoMethod
    Store loadObjList(String codClsOrTyp, String codProp, String model) {
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
        Store st = loadSqlService(sql, "", model)
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codProp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codProp}")

        Store stPV = loadSqlMeta("""
            select id, cls  from propval p where prop=${map.get(codProp)}
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
    Store loadClsForSelect(String codTyp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", codTyp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codTyp}")

        return loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v where c.id=v.ownerVer and v.lastVer=1 and typ=${map.get(codTyp)}
        """, "")
    }

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
            if ( cod.equalsIgnoreCase("Prop_Address") ||
                    cod.equalsIgnoreCase("Prop_Phone")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_Region") ||
                    cod.equalsIgnoreCase("Prop_IsActive")) {
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
                    cod.equalsIgnoreCase("Prop_StageLength") ) {
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
            if (cod.equalsIgnoreCase("Prop_ObjectTypeMulti")) {
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
            if (cod.equalsIgnoreCase("Prop_Address") ||
                    cod.equalsIgnoreCase("Prop_Phone") ) {
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
            if ( cod.equalsIgnoreCase("Prop_Description")) {
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
            if ( cod.equalsIgnoreCase("Prop_CreatedAt") ||
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
            if ( cod.equalsIgnoreCase("Prop_Region") ||
                    cod.equalsIgnoreCase("Prop_IsActive")) {
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

        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_StartKm") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_StageLength")) {
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
            if (cod.equalsIgnoreCase("Prop_ObjectTypeMulti")) {
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
        else if (model.equalsIgnoreCase("orgstructuredata"))
            return apiOrgStructureData().get(ApiOrgStructureData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("personnaldata"))
            return apiPersonnalData().get(ApiPersonnalData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("inspectiondata"))
            return apiInspectionData().get(ApiInspectionData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSql(sql, domain)
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
