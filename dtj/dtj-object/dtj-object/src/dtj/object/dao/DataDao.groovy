package dtj.object.dao

import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.IVariantFieldsMapper
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.IStoreDictResolver
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
    ApinatorApi apiInspectioData() {
        return app.bean(ApinatorService).getApi("inspectiondata")
    }
    ApinatorApi apiClientData() {
        return app.bean(ApinatorService).getApi("clientdata")
    }
    ApinatorApi apiIncidentData() {
        return app.bean(ApinatorService).getApi("incidentdata")
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
                Store stData = loadSqlService("""
                    select id from DataPropVal
                    where propval in (${idsPV.join(",")}) and obj=${owner}
                """, "", "objectdata")
                if (stData.size() > 0)
                    lstService.add("objectdata")
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
    /**
     *
     * @param id Id Obj
     * Delete object with properties
     */
    @DaoMethod
    void deleteObjWithProperties(long id) {
        //
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
    void deleteComplexData(long id) {
        Store st = mdb.loadQuery("""
            select id from DataPropVal where parent=${id}
        """)
        if(st.size() == 0)
            throw new XError("Не найдено комплексное свойство по данному [id]")
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

    @DaoMethod
    Store saveObjectServed(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        //
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        if (mode.equalsIgnoreCase("ins")) {
            // find cls(linkCls)
            long linkCls = pms.getLong("linkCls")
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@Typ_Object")
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
                        select id from Cls where typ=${map.get("Typ_Object")}    --1011
                    )
                    group by c.cls
                ) t where t.fvlist in (select fv.fvlist from fv)
            """, "")

            long cls
            if (stTmp.size() > 0)
                cls = stTmp.get(0).getLong("cls")
            else {
                throw new XError("Не найден класс сответствующий классу {0}", linkCls)
            }

            par.put("cls", cls)
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_ObjectType
            if (pms.getLong("objObjectType") > 0)
                fillProperties(true, "Prop_ObjectType", pms)
            else
                throw new XError("[objObjectType] requered")
            //1 a Prop_Section
            if (pms.getLong("objSection") > 0)
                fillProperties(true, "Prop_Section", pms)
            else
                throw new XError("[objSection] requered")
            //2 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] requered")
            //3 Prop_FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] requered")
            //4 Prop_StartPicket
            if (pms.getLong("StartPicket") > 0)
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] requered")
            //5 Prop_FinishPicket
            if (pms.getLong("FinishPicket") > 0)
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] requered")
            //6 Prop_StartLink
            if (pms.getLong("StartLink") > 0)
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("[StartLink] requered")
            //7 Prop_FinishLink
            if (pms.getLong("FinishLink") > 0)
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("[FinishLink] requered")
            //8 Prop_PeriodicityReplacement
            if (pms.getLong("PeriodicityReplacement") > 0)
                fillProperties(true, "Prop_PeriodicityReplacement", pms)
            //9 Prop_Side
            if (pms.getLong("fvSide") > 0)
                fillProperties(true, "Prop_Side", pms)
            //10 Prop_Specs
            if (!pms.getString("Specs").isEmpty())
                fillProperties(true, "Prop_Specs", pms)
            //11 Prop_LocationDetails
            if (!pms.getString("LocationDetails").isEmpty())
                fillProperties(true, "Prop_LocationDetails", pms)
            //12 Prop_Number
            if (!pms.getString("Number").isEmpty())
                fillProperties(true, "Prop_Number", pms)
            //13 Prop_InstallationDate
            if (!pms.getString("InstallationDate").isEmpty())
                fillProperties(true, "Prop_InstallationDate", pms)
            //14 Prop_CreatedAt
            if (!pms.getString("CreatedAt").isEmpty())
                fillProperties(true, "Prop_CreatedAt", pms)
            else
                throw new XError("[CreatedAt] requered")
            //15 Prop_UpdatedAt
            if (!pms.getString("UpdatedAt").isEmpty())
                fillProperties(true, "Prop_UpdatedAt", pms)
            else
                throw new XError("[UpdatedAt] requered")
            //16 Prop_Description
            if (!pms.getString("Description").isEmpty())
                fillProperties(true, "Prop_Description", pms)
            //17 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[User] requered")
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 a Prop_Section
            if (pms.containsKey("idSection"))  {
                if (pms.getLong("objSection") > 0)
                    updateProperties("Prop_Section", pms)
                else
                    throw new XError("[Section] requered")
            }
            //2 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getLong("StartKm") > 0)
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("[StartKm] requered")
            } else {
                if (pms.getLong("StartKm") > 0)
                    fillProperties(true, "Prop_StartKm", pms)
            }
            //3 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getLong("FinishKm") > 0)
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("[FinishKm] requered")
            } else {
                if (pms.getLong("FinishKm") > 0)
                    fillProperties(true, "Prop_FinishKm", pms)
            }
            //4 Prop_StartPicket
            if (pms.containsKey("idStartPicket")) {
                if (pms.getLong("StartPicket") > 0)
                    updateProperties("Prop_StartPicket", pms)
                else
                    throw new XError("[StartPicket] requered")
            } else {
                if (pms.getLong("StartPicket") > 0)
                    fillProperties(true, "Prop_StartPicket", pms)
            }
            //5 Prop_FinishPicket
            if (pms.containsKey("idFinishPicket")) {
                if (pms.getLong("FinishPicket") > 0)
                    updateProperties("Prop_FinishPicket", pms)
                else
                    throw new XError("[FinishPicket] requered")
            } else {
                if (pms.getLong("FinishPicket") > 0)
                    fillProperties(true, "Prop_FinishPicket", pms)
            }
            //6 Prop_StartLink
            if (pms.containsKey("idStartLink")) {
                if (pms.getLong("StartLink") > 0)
                    updateProperties("Prop_StartLink", pms)
                else
                    throw new XError("[StartLink] requered")
            } else {
                if (pms.getLong("StartLink") > 0)
                    fillProperties(true, "Prop_StartLink", pms)
            }
            //7 Prop_FinishLink
            if (pms.containsKey("idFinishLink")) {
                if (pms.getLong("FinishLink") > 0)
                    updateProperties("Prop_FinishLink", pms)
                else
                    throw new XError("[FinishLink] requered")
            } else {
                if (pms.getLong("FinishLink") > 0)
                    fillProperties(true, "Prop_FinishLink", pms)
            }
            //8 Prop_PeriodicityReplacement
            if (pms.containsKey("idPeriodicityReplacement")) {
                updateProperties("Prop_PeriodicityReplacement", pms)
            } else {
                if (pms.getLong("PeriodicityReplacement") > 0)
                    fillProperties(true, "Prop_PeriodicityReplacement", pms)
            }
            //9 Prop_Side
            if (pms.containsKey("idSide")) {
                updateProperties("Prop_Side", pms)
            } else {
                if (pms.getLong("fvSide") > 0)
                    fillProperties(true, "Prop_Side", pms)
            }
            //10 Prop_Specs
            if (pms.containsKey("idSpecs")) {
                updateProperties("Prop_Specs", pms)
            } else {
                if (!pms.getString("Specs").isEmpty())
                    fillProperties(true, "Prop_Specs", pms)
            }
            //11 Prop_LocationDetails
            if (pms.containsKey("idLocationDetails")) {
                updateProperties("Prop_LocationDetails", pms)
            } else {
                if (!pms.getString("LocationDetails").isEmpty())
                    fillProperties(true, "Prop_LocationDetails", pms)
            }
            //12 Prop_Number
            if (pms.containsKey("idNumber")) {
                updateProperties("Prop_Number", pms)
            } else {
                if (!pms.getString("Number").isEmpty())
                    fillProperties(true, "Prop_Number", pms)
            }
            //13 Prop_InstallationDate
            if (pms.containsKey("idInstallationDate")) {
                updateProperties("Prop_InstallationDate", pms)
            } else {
                if (!pms.getString("InstallationDate").isEmpty())
                    fillProperties(true, "Prop_InstallationDate", pms)
            }
            //15 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                if (!pms.getString("UpdatedAt").isEmpty())
                    updateProperties("Prop_UpdatedAt", pms)
                else
                    throw new XError("[UpdatedAt] requered")
            } else {
                if (!pms.getString("UpdatedAt").isEmpty())
                    fillProperties(true, "Prop_UpdatedAt", pms)
            }
            //16 Prop_Description
            if (pms.containsKey("idDescription")) {
                updateProperties("Prop_Description", pms)
            } else {
                if (!pms.getString("Description").isEmpty())
                    fillProperties(true, "Prop_Description", pms)
            }
            //17 Prop_User
            if (pms.containsKey("idUser")) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[User] requered")
            } else {
                if (pms.getLong("objUser") > 0)
                    fillProperties(true, "Prop_User", pms)
            }
            //
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        return loadObjectServed(own)
    }

    @DaoMethod
    Store loadObjectServed(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        Store st = mdb.createStore("Obj.Served")
        //
        String whe = "o.id=${id}"
        if (id == 0) {
            Store stCls = loadSqlMeta("""
                select id from Cls where typ=${map.get("Typ_Object")}
            """, "")
            Set<Object> idsCls = stCls.getUniqueValues("id")
            //
            whe = "o.cls in (${idsCls.join(",")})"
        }
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.fullName,
                v1.id as idObjectType, v1.propVal as pvObjectType, v1.obj as objObjectType, null as nameObjectType,
                v2.id as idStartKm, v2.numberVal as StartKm,
                v3.id as idFinishKm, v3.numberVal as FinishKm,
                v4.id as idStartPicket, v4.numberVal as StartPicket,
                v5.id as idFinishPicket, v5.numberVal as FinishPicket,
                v6.id as idPeriodicityReplacement, v6.numberVal as PeriodicityReplacement,
                v7.id as idSide, v7.propVal as pvSide, null as fvSide,
                v8.id as idSpecs, v8.strVal as Specs,
                v9.id as idLocationDetails, v9.strVal as LocationDetails,
                v10.id as idNumber, v10.strVal as Number,
                v11.id as idInstallationDate, v11.dateTimeVal as InstallationDate,
                v12.id as idCreatedAt, v12.dateTimeVal as CreatedAt,
                v13.id as idUpdatedAt, v13.dateTimeVal as UpdatedAt,
                v14.id as idDescription, v14.multiStrVal as Description,
                v15.id as idSection, v15.propVal as pvSection, v15.obj as objSection, ov15.name as nameSection,
                v16.id as idStartLink, v16.numberVal as StartLink,
                v17.id as idFinishLink, v17.numberVal as FinishLink,
                v18.id as idUser, v18.propVal as pvUser, v18.obj as objUser, null as fullNameUser
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ObjectType
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_StartKm
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_FinishKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_StartPicket
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_FinishPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_PeriodicityReplacement
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_Side
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_Specs
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_LocationDetails
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_Number
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_InstallationDate
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_CreatedAt
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_UpdatedAt
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Description
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_Section
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join ObjVer ov15 on ov15.ownerVer=v15.obj and ov15.lastVer=1
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_StartLink
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_FinishLink
                left join DataPropVal v17 on d17.id=v17.dataprop
                left join DataProp d18 on d18.objorrelobj=o.id and d18.prop=:Prop_User
                left join DataPropVal v18 on d18.id=v18.dataprop
            where ${whe} order by o.id
        """, map)
        //... Пересечение
        Set<Object> idsObjectType = st.getUniqueValues("objObjectType")
        Store stObjectType = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and o.id in (0${idsObjectType.join(",")})    
        """, "", "nsidata")
        StoreIndex indObj = stObjectType.getIndex("id")
        //
        Set<Object> pvs = st.getUniqueValues("pvSide")
        Store stPV = apiMeta().get(ApiMeta).loadSql("""
            select fv.id as fv, pv.id as pv, fv.name from Factor fv, PropVal pv 
            where pv.factorval=fv.id and pv.id in (0${pvs.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("pv")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, v.name, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recObj = indObj.get(r.get("objObjectType"))
            if (recObj != null) {
                r.set("nameObjectType", recObj.getString("name"))
            }
            //
            StoreRecord recFV = indPV.get(r.getLong("pvSide"))
            if (recFV != null) {
                r.set("fvSide", recFV.getLong("fv"))
                r.set("nameSide", recFV.getString("name"))
            }
            //
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store loadObjectByTypObjAndCoordForSelect(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long objObjectType = pms.getLong("objObjectType")
        int beg = pms.getInt("beg")
        int end = pms.getInt("end")

        if (objObjectType == 0)
            throw new XError("Не указан [objObjectType]")
        if (beg == 0)
            throw new XError("Не указан [beg]")
        if (beg % 1000 > 0)
            throw new XError("[beg] не является кратным 1000")
        if (end == 0)
            throw new XError("Не указан [end]")
        if (end % 1000 > 0)
            throw new XError("[end] не является кратным 1000")
        if (beg >= end)
            throw new XError("[beg] не может быть больше или равно [end]")
        //
        Store st = mdb.createStore("Obj.Served.ForSelect")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        Store stCls = loadSqlMeta("""
                select id from Cls where typ=${map.get("Typ_Object")}
            """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        String whe = "o.cls in (${idsCls.join(",")})"
        Store stTmp = mdb.loadQuery("""
            select o.id, o.cls, v.name, v.fullName, null as pv,
                v2.numberVal as StartKm,
                v3.numberVal as FinishKm,
                v4.numberVal as StartPicket,
                v5.numberVal as FinishPicket,
                v6.numberVal as StartLink,
                v7.numberVal as FinishLink,
                v2.numberVal * 1000 + (v4.numberVal - 1) * 100 + (v6.numberVal - 1) * 25 as beg,
                v3.numberVal * 1000 + (v5.numberVal - 1) * 100 + v7.numberVal * 25 as end
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ObjectType
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj=${objObjectType}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_StartKm
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_FinishKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_StartPicket
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_FinishPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_StartLink
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_FinishLink
                left join DataPropVal v7 on d7.id=v7.dataprop
            where ${whe} order by o.id
        """, map)
        //
        if (stTmp.size() == 0)
            return st
        //
        idsCls = stTmp.getUniqueValues("cls")
        Store stPV = loadSqlMeta("""
            select id, cls  from propval where prop=${map.get("Prop_Object")} and cls in (0${idsCls.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("cls")

        for (StoreRecord r in stTmp) {
            if ((beg <= r.getInt("beg") && r.getInt("beg") < end) ||
                    (beg < r.getInt("end") && r.getInt("end") <= end) ||
                    (beg > r.getInt("beg") && r.getInt("end") > end)) {
                //
                if (beg > r.getInt("beg")) {
                    r.set("StartKm", UtCnv.toInt(beg / 1000))
                    if (beg == r.getInt("StartKm") * 1000) {
                        r.set("StartPicket", 1)
                        r.set("StartLink", 1)
                    } else {
                        throw new XError("[beg] не является кратным 1000")
                    }

                }
                //
                if (end < r.getInt("end")) {
                    r.set("FinishKm", UtCnv.toInt(end / 1000))
                    if (end == r.getInt("FinishKm") * 1000) {
                        r.set("FinishKm", r.getInt("FinishKm") - 1)
                        r.set("FinishPicket", 10)
                        r.set("FinishLink", 4)
                    } else {
                        throw new XError("[end] не является кратным 1000")
                        /*
                        r.set("FinishPicket", UtCnv.toInt((end - r.getInt("FinishKm") * 1000) / 100) + 1)
                        r.set("FinishLink", Math.ceil((end - r.getInt("FinishKm") * 1000 - (r.getInt("FinishPicket") - 1) * 100) / 25 as double))
                        if (r.getInt("FinishLink") == 0) {
                            r.set("FinishPicket", r.getInt("FinishPicket") - 1)
                            r.set("FinishLink", 4)
                        }
                        */
                    }
                }
                //
                StoreRecord rec = indPV.get(r.getLong("cls"))
                if (rec != null)
                    r.set("pv", rec.getLong("id"))
                //
                st.add(r)
            }
        }
        //
        return st
    }

    @DaoMethod
    Store loadObjectBySectionAndTypObjAndCoordForSelect(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long objSection = pms.getLong("objSection")
        long objObjectType = pms.getLong("objObjectType")
        int beg = pms.getInt("beg")
        int end = pms.getInt("end")

        if (objSection == 0)
            throw new XError("Не указан [objSection]")
        if (objObjectType == 0)
            throw new XError("Не указан [objObjectType]")
        if (beg == 0)
            throw new XError("Не указан [beg]")
        if (end == 0)
            throw new XError("Не указан [end]")
        if (beg >= end)
            throw new XError("[beg] не может быть больше или равно [end]")
        //
        Store st = mdb.createStore("Obj.Served.ForSelect")
        //
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        Store stCls = loadSqlMeta("""
                select id from Cls where typ=${map.get("Typ_Object")}
            """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")
        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        String whe = "o.cls in (${idsCls.join(",")})"
        Store stTmp = mdb.loadQuery("""
            select o.id, o.cls, v.name, v.fullName, null as pv,
                v2.numberVal as StartKm,
                v3.numberVal as FinishKm,
                v4.numberVal as StartPicket,
                v5.numberVal as FinishPicket,
                v6.numberVal as StartLink,
                v7.numberVal as FinishLink,
                v2.numberVal * 1000 + (v4.numberVal - 1) * 100 + (v6.numberVal - 1) * 25 as beg,
                v3.numberVal * 1000 + (v5.numberVal - 1) * 100 + v7.numberVal * 25 as end
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ObjectType
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj=${objObjectType}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_StartKm
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_FinishKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_StartPicket
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_FinishPicket
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_StartLink
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_FinishLink
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_Section
                inner join DataPropVal v8 on d8.id=v8.dataprop and v8.obj=${objSection}
            where ${whe} order by o.id
        """, map)
        //
        if (stTmp.size() == 0)
            return st
        //
        idsCls = stTmp.getUniqueValues("cls")
        Store stPV = loadSqlMeta("""
            select id, cls  from propval where prop=${map.get("Prop_Object")} and cls in (0${idsCls.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("cls")

        for (StoreRecord r in stTmp) {
            if (beg <= r.getInt("beg") && r.getInt("end") <= end) {
                StoreRecord rec = indPV.get(r.getLong("cls"))
                if (rec != null)
                    r.set("pv", rec.getLong("id"))
                //
                st.add(r)
            }
        }
        //
        return st
    }

    @DaoMethod
    List<Map<String, Object>> loadComplexObjectPassport(Long id) {
        if (id == 0)
            throw new XError("[id] не указан")
        List<Map<String, Object>> lstRes = new ArrayList<>()
        Store st = mdb.createStore("Obj.Complex.Passport")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_Passport")
        mdb.loadQuery(st, """
            select o.id,
                v1.id as idPassportComplex, v1.strVal as PassportComplex,
                v2.id as idPassportComponentParams, v2.propVal as pvPassportComponentParams,
                    v2.relObj as relobjPassportComponentParams, null as namePassportComponentParams,
                v3.id as idPassportMeasure, v3.propVal as pvPassportMeasure, null as meaPassportMeasure, null as namePassportMeasure,
                v4.id as idPassportVal, v4.numberVal as PassportVal
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_PassportComplex
                inner join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_PassportComponentParams
                inner join DataPropVal v2 on d2.id=v2.dataProp and v2.parent=v1.id
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_PassportMeasure
                inner join DataPropVal v3 on d3.id=v3.dataProp and v3.parent=v1.id
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_PassportVal
                inner join DataPropVal v4 on d4.id=v4.dataProp and v4.parent=v1.id
            where o.id=${id}
        """, map)
        if (st.size() == 0)
            return null
        //PropMulti
        Store stMulti = mdb.loadQuery("""
            select v1.parent, v1.obj, null as name
            from Obj o 
                left join DataProp d1 on d1.isObj=1 and d1.objorrelobj=o.id and d1.prop=:Prop_PassportSignMulti
                inner join DataPropVal v1 on d1.id=v1.dataprop
            where o.id=${id}
        """, map)
        //Пересечение
        Set<Object> idsSign = stMulti.getUniqueValues("obj")
        Store stSign = loadSqlService("""
            select o.id, o.cls, v.name
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsSign.join(",")})
        """, "", "nsidata")
        StoreIndex indSign = stSign.getIndex("id")
        //
        Set<Object> idsComponentParams = st.getUniqueValues("relobjPassportComponentParams")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_ParamsComponent", "")
        Store stMemb = loadSqlMeta("""
            select id from relclsmember 
            where relcls in (select id from Relcls where reltyp=${map.get("RT_ParamsComponent")})
            order by id
        """, "")
        Store stRO = loadSqlService("""
            select o.id, r1.obj as obj1, ov1.name as name1, r2.obj as obj2, ov2.name as name2
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join objver ov1 on ov1.ownerVer=r1.obj and ov1.lastVer=1
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
                left join objver ov2 on ov2.ownerVer=r2.obj and ov2.lastVer=1
            where o.id in (0${idsComponentParams.join(",")})
        """, "", "nsidata")

        Map<Long, Object> mapRO = new HashMap<>()
        for (StoreRecord r in stRO) {
            mapRO.put(r.getLong("id"), r.getValues())
        }
        //
        Set<Object> pvs = st.getUniqueValues("pvPassportMeasure")
        Store stPV = apiMeta().get(ApiMeta).loadSql("""
            select m.id as mea, pv.id as pv, m.name from Measure m, PropVal pv 
            where pv.measure=m.id and pv.id in (0${pvs.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("pv")
        //
        for (StoreRecord r in st) {
            r.set("namePassportComponentParams", mapRO.get(r.getLong("relobjPassportComponentParams"))["name1"])
            r.set("nameComponent", mapRO.get(r.getLong("relobjPassportComponentParams"))["name2"])
            r.set("objComponent", mapRO.get(r.getLong("relobjPassportComponentParams"))["obj2"])

            StoreRecord recMeasure = indPV.get(r.getLong("pvPassportMeasure"))
            if (recMeasure != null) {
                r.set("meaPassportMeasure", recMeasure.getLong("mea"))
                r.set("namePassportMeasure", recMeasure.getString("name"))
            }

            Map<String, Object> mapRec = r.getValues()
            List<Map<String, Object>> lstMulti = new ArrayList<>()
            for (StoreRecord rec in stMulti) {
                if (r.getLong("idPassportComplex") == rec.getLong("parent")) {
                    StoreRecord recSign = indSign.get(rec.getLong("obj"))
                    if (recSign != null) {
                        Map<String, Object> mapSign = recSign.getValues()
                        lstMulti.add(mapSign)
                    }
                }
            }
            mapRec.put("objPassportSignMulti", lstMulti)
            lstRes.add(mapRec)
        }
        //
        return lstRes
    }

    @DaoMethod
    void saveComplexObjectPassport(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own = pms.getLong("id")
        if (own == 0)
            throw new XError("[id] не указан")
        pms.put("own", own)
        //
        List<Map<String, Object>> objLst = params.get("objPassportSignMulti") as List<Map<String, Object>>
        Map<String, Object> parMulti = new HashMap<>()
        parMulti.put("id", own)
        parMulti.put("isObj", true)
        parMulti.put("codProp", "Prop_PassportSignMulti")
        parMulti.put("objOrRelObjLst", objLst)
        //
        if (mode.equalsIgnoreCase("ins")) {
            pms.remove("idComplex")
            pms.put("PassportComplex", "PassportComplex-" + own + "-" + pms.getString("objLinkToView"))
            mdb.startTran()
            try {
                //0 Parent
                fillProperties(true, "Prop_PassportComplex", pms)
                //1 Prop_PassportComponentParams
                if (pms.getLong("relobjPassportComponentParams") > 0)
                    fillProperties(true, "Prop_PassportComponentParams", pms)
                else
                    throw new XError("[relobjPassportComponentParams] не указан")
                //2 Prop_PassportMeasure
                if (pms.getLong("meaPassportMeasure") > 0)
                    fillProperties(true, "Prop_PassportMeasure", pms)
                else
                    throw new XError("[meaPassportMeasure] не указан")
                //3 Prop_PassportVal
                if (pms.getDouble("PassportVal") > 0)
                    fillProperties(true, "Prop_PassportVal", pms)
                else
                    throw new XError("[PassportVal] не указан")
                //4 Prop_PassportSignMulti
                if (objLst.size() != 0) {
                    parMulti.put("idComplex", pms.getLong("idComplex"))
                    savePropObjMulti(parMulti)
                } else
                    throw new XError("[objPassportSignMulti] не указан")
                mdb.commit()
            } catch (Exception e) {
                mdb.rollback(e)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            //1 Prop_PassportComponentParams
            if (!pms.containsKey("idPassportComplex"))
                throw new XError("[idPassportComplex] не указан")
            //1 Prop_PassportComponentParams
            if (pms.containsKey("idPassportComponentParams"))
                if (pms.getLong("relobjPassportComponentParams") == 0)
                    throw new XError("[relobjPassportComponentParams] не указан")
                else
                    updateProperties("Prop_PassportComponentParams", pms)
            //2 Prop_PassportMeasure
            if (pms.containsKey("idPassportMeasure"))
                if (pms.getLong("meaPassportMeasure") == 0)
                    throw new XError("[meaPassportMeasure] не указан")
                else
                    updateProperties("Prop_PassportMeasure", pms)
            //3 Prop_PassportVal
            if (pms.containsKey("idPassportVal"))
                if (pms.getDouble("PassportVal") == 0)
                    throw new XError("[PassportVal] не указан")
                else
                    updateProperties("Prop_PassportVal", pms)
            //4 Prop_PassportSignMulti
            if (objLst.size() != 0) {
                parMulti.put("idComplex", pms.getLong("idPassportComplex"))
                savePropObjMulti(parMulti)
            } else
                throw new XError("[objPassportSignMulti] не указан")
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
    }

    @DaoMethod
    void savePropObjMulti( Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("id"))
        if (own == 0)
            throw new XError("[id] не указан")
        Boolean isObj = params.get("isObj")
        if (!params.containsKey("isObj"))
            throw new XError("[isObj] не указан")
        String codProp = params.get("codProp")
        if (!params.containsKey("codProp") || codProp == "")
            throw new XError("[codProp] не указан")
        List<Map<String, Object>> objLst = params.get("objOrRelObjLst") as List<Map<String, Object>>

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codProp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codProp}")

        String whe = ""
        if (params.containsKey("idComplex"))
            whe = "and v.parent=${params.get("idComplex")}"

        Store stOld = mdb.loadQuery("""
            select v.id, v.obj
            from DataProp d
                left join DataPropVal v on d.id=v.dataprop
            where d.isObj=${UtCnv.toInt(isObj)} and d.objOrRelObj=${own} and d.prop=${map.get(codProp)} ${whe}
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
        if (UtCnv.toLong(params.get("idComplex")) > 0)
            pms.put("idComplex", UtCnv.toLong(params.get("idComplex")))
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
    Store loadSection(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Section", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Section")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Section")}"
        //
        Store st = mdb.createStore("Obj.Section")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st,"""
            select o.id, o.cls, v.name,
                v1.id as idStartKm, v1.numberVal as StartKm,
                v2.id as idFinishKm, v2.numberVal as FinishKm,
                v3.id as idStageLength, v3.numberVal as StageLength,
                v4.id as idClient, v4.propVal as pvClient, v4.obj as objClient, null as nameClient,
                v5.id as idUser, v5.propVal as pvUser, v5.obj as objUser, null as fullNameUser,
                v6.id as idCreatedAt, v6.dateTimeVal as CreatedAt,
                v7.id as idUpdatedAt, v7.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_StartKm
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_FinishKm
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_StageLength
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_Client
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_User
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_CreatedAt
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_UpdatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
            where ${whe}
        """, map)
        //... Пересечение
        Set<Object> idsClient = st.getUniqueValues("objClient")
        Store stClient = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id in (0${idsClient.join(",")})
        """, "", "clientdata")
        StoreIndex indClient = stClient.getIndex("id")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, v.name, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recClient = indClient.get(r.getLong("objClient"))
            if (recClient != null)
                r.set("nameClient", recClient.getString("name"))
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store loadStation(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Station", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Station")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Station")}"
        //
        Store st = mdb.createStore("Obj.Station")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.objParent as parent, ov.name as nameParent,
                v1.id as idStartKm, v1.numberVal as StartKm,
                v2.id as idStartPicket, v2.numberVal as StartPicket,
                v3.id as idFinishKm, v3.numberVal as FinishKm,
                v4.id as idFinishPicket, v4.numberVal as FinishPicket,
                v5.id as idUser, v5.propVal as pvUser, v5.obj as objUser, null as fullNameUser,
                v6.id as idCreatedAt, v6.dateTimeVal as CreatedAt,
                v7.id as idUpdatedAt, v7.dateTimeVal as UpdatedAt,
                v8.id as idStartLink, v8.numberVal as StartLink,
                v9.id as idFinishLink, v9.numberVal as FinishLink
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join ObjVer ov on ov.id=v.objParent and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_StartKm
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_StartPicket
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_FinishKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishPicket
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_User
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_CreatedAt
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_UpdatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_StartLink
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_FinishLink
                left join DataPropVal v9 on d9.id=v9.dataprop
            where ${whe} order by v1.numberVal
        """, map)
        //... Пересечение
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, v.name, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store loadStage(long obj) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Stage", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Cls_Stage")
        String whe = "o.id=${obj}"
        if (obj == 0)
            whe = "o.cls=${map.get("Cls_Stage")}"
        //
        Store st = mdb.createStore("Obj.Stage")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.objParent as parent, ov.name as nameParent,
                v1.id as idStartKm, v1.numberVal as StartKm,
                v2.id as idStartPicket, v2.numberVal as StartPicket,
                v3.id as idFinishKm, v3.numberVal as FinishKm,
                v4.id as idFinishPicket, v4.numberVal as FinishPicket,
                v5.id as idUser, v5.propVal as pvUser, v5.obj as objUser, null as fullNameUser,
                v6.id as idCreatedAt, v6.dateTimeVal as CreatedAt,
                v7.id as idUpdatedAt, v7.dateTimeVal as UpdatedAt,
                v8.id as idStartLink, v8.numberVal as StartLink,
                v9.id as idFinishLink, v9.numberVal as FinishLink,
                v10.id as idStageLength, v10.numberVal as StageLength
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join ObjVer ov on ov.id=v.objParent and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_StartKm
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_StartPicket
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_FinishKm
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FinishPicket
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_User
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_CreatedAt
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_UpdatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_StartLink
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_FinishLink
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_StageLength
                left join DataPropVal v10 on d10.id=v10.dataprop
            where ${whe} order by v1.numberVal
        """, map)
        //... Пересечение
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, v.name, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveSection(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        par.putIfAbsent("fullName", pms.getString("name"))
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Section", "")
            par.put("cls", map.get("Cls_Section"))
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("Не указан [StartKm]")
            //2 FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("Не указан [FinishKm]")
            //3 StageLength
            if (pms.getDouble("StageLength") > 0)
                fillProperties(true, "Prop_StageLength", pms)
            else
                throw new XError("Не указан [StageLength]")
            //4 Client
            if (pms.getLong("objClient") > 0)
                fillProperties(true, "Prop_Client", pms)
            else
                throw new XError("Не указан [Client]")
            //5 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[User] не указан")
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
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getLong("StartKm") > 0)
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("Не указан [StartKm]")
            }
            //2 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getLong("FinishKm") > 0)
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("Не указан [FinishKm]")
            }
            //3 Prop_StageLength
            if (pms.containsKey("idStageLength")) {
                if (pms.getDouble("StageLength") > 0)
                    updateProperties("Prop_StageLength", pms)
                else
                    throw new XError("Не указан [StageLength]")
            }
            //4 Prop_Client
            if (pms.containsKey("idClient")) {
                if (pms.getLong("objClient") > 0)
                    updateProperties("Prop_Client", pms)
                else
                    throw new XError("Не указан [Client]")
            }
            //5 Prop_User
            if (pms.containsKey("idUser")) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[User] не указан")
            }
            //6 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
            }
            //
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        return loadSection(own)
    }

    @DaoMethod
    Store saveStation(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        par.putIfAbsent("fullName", pms.getString("name"))
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Station", "")
            par.put("cls", map.get("Cls_Station"))
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("Не указан [StartKm]")
            //2 Prop_StartPicket
            if (pms.getLong("StartPicket") > 0)
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("Не указан [StartPicket]")
            //3 Prop_FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("Не указан [FinishKm]")
            //4 Prop_FinishPicket
            if (pms.getLong("FinishPicket") > 0)
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("Не указан [FinishPicket]")
            //5 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[User] не указан")
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
            //8 Prop_StartLink
            if (pms.getLong("StartLink") > 0)
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("Не указан [StartLink]")
            //9 Prop_FinishLink
            if (pms.getLong("FinishLink") > 0)
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("Не указан [FinishLink]")
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getLong("StartKm") > 0)
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("Не указан [StartKm]")
            }
            //2 Prop_StartPicket
            if (pms.containsKey("idStartPicket")) {
                if (pms.getLong("StartPicket") > 0)
                    updateProperties("Prop_StartPicket", pms)
                else
                    throw new XError("Не указан [StartPicket]")
            }
            //3 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getLong("FinishKm") > 0)
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("Не указан [FinishKm]")
            }
            //4 Prop_FinishPicket
            if (pms.containsKey("idFinishPicket")) {
                if (pms.getLong("FinishPicket") > 0)
                    updateProperties("Prop_FinishPicket", pms)
                else
                    throw new XError("Не указан [FinishPicket]")
            }
            //5 Prop_User
            if (pms.containsKey("idUser")) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[User] не указан")
            }
            //6 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
            }
            //7 Prop_StartLink
            if (pms.containsKey("idStartLink")) {
                if (pms.getLong("StartLink") > 0)
                    updateProperties("Prop_StartLink", pms)
                else
                    throw new XError("Не указан [StartLink]")
            }
            //8 Prop_FinishLink
            if (pms.containsKey("idFinishLink")) {
                if (pms.getLong("FinishLink") > 0)
                    updateProperties("Prop_FinishLink", pms)
                else
                    throw new XError("Не указан [FinishLink]")
            }
            //
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadStation(own)
    }

    @DaoMethod
    Store saveStage(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        Map<String, Object> par = new HashMap<>(pms)
        par.putIfAbsent("fullName", pms.getString("name"))
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Stage", "")
            par.put("cls", map.get("Cls_Stage"))
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_StartKm
            if (pms.getLong("StartKm") > 0)
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("Не указан [StartKm]")
            //2 Prop_StartPicket
            if (pms.getLong("StartPicket") > 0)
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("Не указан [StartPicket]")
            //3 Prop_FinishKm
            if (pms.getLong("FinishKm") > 0)
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("Не указан [FinishKm]")
            //4 Prop_FinishPicket
            if (pms.getLong("FinishPicket") > 0)
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("Не указан [FinishPicket]")
            //5 Prop_StageLength
            if (pms.getDouble("StageLength") > 0)
                fillProperties(true, "Prop_StageLength", pms)
            else
                throw new XError("Не указан [StageLength]")
            //6 Prop_User
            if (pms.getLong("objUser") > 0)
                fillProperties(true, "Prop_User", pms)
            else
                throw new XError("[User] не указан")
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
            //9 Prop_StartLink
            if (pms.getLong("StartLink") > 0)
                fillProperties(true, "Prop_StartLink", pms)
            else
                throw new XError("Не указан [StartLink]")
            //10 Prop_FinishLink
            if (pms.getLong("FinishLink") > 0)
                fillProperties(true, "Prop_FinishLink", pms)
            else
                throw new XError("Не указан [FinishLink]")
            //
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //1 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getLong("StartKm") > 0)
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("Не указан [StartKm]")
            }
            //2 Prop_StartPicket
            if (pms.containsKey("idStartPicket")) {
                if (pms.getLong("StartPicket") > 0)
                    updateProperties("Prop_StartPicket", pms)
                else
                    throw new XError("Не указан [StartPicket]")
            }
            //3 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getLong("FinishKm") > 0)
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("Не указан [FinishKm]")
            }
            //4 Prop_FinishPicket
            if (pms.containsKey("idFinishPicket")) {
                if (pms.getLong("FinishPicket") > 0)
                    updateProperties("Prop_FinishPicket", pms)
                else
                    throw new XError("Не указан [FinishPicket]")
            }
            //5 Prop_StageLength
            if (pms.containsKey("idStageLength")) {
                if (pms.getDouble("StageLength") > 0)
                    updateProperties("Prop_StageLength", pms)
                else
                    throw new XError("Не указан [StageLength]")
            }
            //6 Prop_User
            if (pms.containsKey("idUser")) {
                if (pms.getLong("objUser") > 0)
                    updateProperties("Prop_User", pms)
                else
                    throw new XError("[User] не указан")
            }
            //7 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
            }
            //8 Prop_StartLink
            if (pms.containsKey("idStartLink")) {
                if (pms.getLong("StartLink") > 0)
                    updateProperties("Prop_StartLink", pms)
                else
                    throw new XError("Не указан [StartLink]")
            }
            //9 Prop_FinishLink
            if (pms.containsKey("idFinishLink")) {
                if (pms.getLong("FinishLink") > 0)
                    updateProperties("Prop_FinishLink", pms)
                else
                    throw new XError("Не указан [FinishLink]")
            }
            //
        }
        else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }
        return loadStage(own)
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
        return apiObjectData().get(ApiObjectData).loadObjList(codClsOrTyp, codProp, model)
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
                order by o.cls, o.ord
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
                order by o.cls, o.ord
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

    @DaoMethod
    public void fillPropertiesForTest(boolean isObj, String cod, Map<String, Object> params) {
        fillProperties(isObj, cod, params)
    }

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
        // Complex
        if ([FD_PropType_consts.complex].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_PassportComplex")) {
                recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib Str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_Specs") ||
                    cod.equalsIgnoreCase("Prop_LocationDetails") ||
                    cod.equalsIgnoreCase("Prop_Number")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib Multi Str
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib Date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_InstallationDate") ||
                    cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_Side") ) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
                if (UtCnv.toLong(params.get("idComplex")) > 0)
                    recDPV.set("parent", params.get("idComplex"))
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_ParamsMeasure") ||
                    cod.equalsIgnoreCase("Prop_PassportMeasure")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
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
                    cod.equalsIgnoreCase("Prop_PeriodicityReplacement") ||
                    cod.equalsIgnoreCase("Prop_StageLength") ||
                    cod.equalsIgnoreCase("Prop_StartLink") ||
                    cod.equalsIgnoreCase("Prop_FinishLink") ||
                    cod.equalsIgnoreCase("Prop_PassportVal")) {
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
        // For Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ObjectType") ||
                    cod.equalsIgnoreCase("Prop_Section") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_Client") ||
                    cod.equalsIgnoreCase("Prop_PassportSignMulti")) {
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
        // For RelTyp
        if ([FD_PropType_consts.reltyp].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_PassportComponentParams")) {
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
        long relRef = mapProp.getLong("relobj"+keyValue)
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
            if (cod.equalsIgnoreCase("Prop_Specs") ||
                    cod.equalsIgnoreCase("Prop_LocationDetails") ||
                        cod.equalsIgnoreCase("Prop_Number")) {
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
            if ( cod.equalsIgnoreCase("Prop_InstallationDate") ||
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
            if ( cod.equalsIgnoreCase("Prop_Side")) {
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
            if ( cod.equalsIgnoreCase("Prop_ParamsMeasure") ||
                    cod.equalsIgnoreCase("Prop_PassportMeasure")) {
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
                    cod.equalsIgnoreCase("Prop_PeriodicityReplacement") ||
                    cod.equalsIgnoreCase("Prop_StageLength") ||
                    cod.equalsIgnoreCase("Prop_StartLink") ||
                    cod.equalsIgnoreCase("Prop_FinishLink") ||
                    cod.equalsIgnoreCase("Prop_PassportVal")) {
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
            if (cod.equalsIgnoreCase("Prop_ObjectType") ||
                    cod.equalsIgnoreCase("Prop_Section") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_Client") ||
                    cod.equalsIgnoreCase("Prop_PassportSignMulti")) {
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
        // For RelTyp
        if ([FD_PropType_consts.reltyp].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_PassportComponentParams")) {
                if (relRef > 0)
                    sql = "update DataPropval set propVal=${propVal}, relObj=${relRef}, timeStamp='${tmst}' where id=${idVal}"
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

    @DaoMethod
    Store findStationOfCoord(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_Sta%")

        String whe = "o.cls in (${map.get("Cls_Station")},${map.get("Cls_Stage")})"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")

        int beg = UtCnv.toInt(params.get('StartKm')) * 1000 + UtCnv.toInt(params.get('StartPicket')) * 100 + UtCnv.toInt(params.get('StartLink')) * 25
        //int end = UtCnv.toInt(params.get('FinishKm')) * 1000 + UtCnv.toInt(params.get('FinishPicket')) * 100 + UtCnv.toInt(params.get('FinishLink')) * 25

        String sql = """
            select o.id, o.cls, v.name, null as pv,
                v2.numberVal * 1000 + (v4.numberVal - 1) * 100 + v6.numberVal * 25 as beg,
                v3.numberVal * 1000 + (v5.numberVal - 1) * 100 + v7.numberVal * 25 as end
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
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v7 on d7.id=v7.dataprop
            where ${whe} and v2.numberVal * 1000 + v4.numberVal * 100 + v6.numberVal * 25 <= ${beg} 
                and v3.numberVal * 1000 + v5.numberVal * 100 + v7.numberVal * 25 >= ${beg}
        """
        Store st = mdb.loadQuery(sql)
        //mdb.outTable(st)
        if (st.size() == 1) {
            long idPV = apiMeta().get(ApiMeta).idPV("cls", st.get(0).getLong("cls"), "Prop_Section")
            st.get(0).set("pv", idPV)
            return st
        } else if (st.size() == 0) {
            throw new XError("По начальным координатам объекта не найдено место")
        } else {
            throw new XError("По начальным координатам объекта найдено несколько мест")
        }
    }

    @DaoMethod
    Store loadPeriodType() {
        return loadSqlMeta("""
            select id, text
            from FD_PeriodType
            where vis=1
            order by ord
        """, "")
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
            return apiInspectioData().get(ApiInspectionData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSql(sql, domain)
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
            return apiInspectioData().get(ApiInspectionData).loadSqlWithParams(sql, params, domain)
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
