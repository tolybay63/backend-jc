package dtj.object.dao

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
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
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
    ApinatorApi apiInspectioData() {
        return app.bean(ApinatorService).getApi("inspectiondata")
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
            if (pms.getString("StartKm") != "")
                fillProperties(true, "Prop_StartKm", pms)
            else
                throw new XError("[StartKm] requered")
            //3 Prop_FinishKm
            if (pms.getString("FinishKm") != "")
                fillProperties(true, "Prop_FinishKm", pms)
            else
                throw new XError("[FinishKm] requered")
            //4 Prop_StartPicket
            if (pms.getString("StartPicket") != "")
                fillProperties(true, "Prop_StartPicket", pms)
            else
                throw new XError("[StartPicket] requered")
            //5 Prop_FinishPicket
            if (pms.getString("FinishPicket") != "")
                fillProperties(true, "Prop_FinishPicket", pms)
            else
                throw new XError("[FinishPicket] requered")

            //6 Prop_PeriodicityReplacement
            if (pms.getString("PeriodicityReplacement") != "")
                fillProperties(true, "Prop_PeriodicityReplacement", pms)
            //7 Prop_Side
            if (pms.getLong("fvSide") > 0)
                fillProperties(true, "Prop_Side", pms)
            //8 Prop_Specs
            if (pms.getString("Specs") != "")
                fillProperties(true, "Prop_Specs", pms)
            //9 Prop_LocationDetails
            if (pms.getString("LocationDetails") != "")
                fillProperties(true, "Prop_LocationDetails", pms)
            //10 Prop_Number
            if (pms.getString("Number") != "")
                fillProperties(true, "Prop_Number", pms)
            //11 Prop_InstallationDate
            if (pms.getString("InstallationDate") != "")
                fillProperties(true, "Prop_InstallationDate", pms)
            //12 Prop_CreatedAt
            if (pms.getString("CreatedAt") != "")
                fillProperties(true, "Prop_CreatedAt", pms)
            //13 Prop_UpdatedAt
            if (pms.getString("UpdatedAt") != "")
                fillProperties(true, "Prop_UpdatedAt", pms)
            //14 Prop_Description
            if (pms.getString("Description") != "")
                fillProperties(true, "Prop_Description", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            eu.updateEntity(par)
            //
            pms.put("own", own)

            //1 Prop_ObjectType
            if (pms.containsKey("idObjectType"))
                updateProperties("Prop_ObjectType", pms)
            else {
                if (pms.containsKey("objObjectType"))
                    fillProperties(true, "Prop_ObjectType", pms)
            }

            //1 a Prop_Section
            if (pms.containsKey("idSection"))
                updateProperties("Prop_Section", pms)
            else {
                if (pms.containsKey("objSection"))
                    fillProperties(true, "Prop_Section", pms)
            }

            //2 Prop_StartKm
            if (pms.containsKey("idStartKm")) {
                if (pms.getString("StartKm") != "")
                    updateProperties("Prop_StartKm", pms)
                else
                    throw new XError("[StartKm] requered")
            }

            //3 Prop_FinishKm
            if (pms.containsKey("idFinishKm")) {
                if (pms.getString("FinishKm") != "")
                    updateProperties("Prop_FinishKm", pms)
                else
                    throw new XError("[FinishKm] requered")
            }

            //4 Prop_StartPicket
            if (pms.containsKey("idStartPicket")) {
                if (pms.getString("StartPicket") != "")
                    updateProperties("Prop_StartPicket", pms)
                else
                    throw new XError("[StartPicket] requered")
            }

            //5 Prop_FinishPicket
            if (pms.containsKey("idFinishPicket")) {
                if (pms.getString("FinishPicket") != "")
                    updateProperties("Prop_FinishPicket", pms)
                else
                    throw new XError("[FinishPicket] requered")
            }

            //6 Prop_PeriodicityReplacement
            if (pms.containsKey("idPeriodicityReplacement")) {
                updateProperties("Prop_PeriodicityReplacement", pms)
            } else {
                if (pms.getString("PeriodicityReplacement") != "")
                    fillProperties(true, "Prop_PeriodicityReplacement", pms)
            }

            //7 Prop_Side
            if (pms.containsKey("idSide")) {
                updateProperties("Prop_Side", pms)
            } else {
                if (pms.containsKey("fvSide"))
                    fillProperties(true, "Prop_Side", pms)
            }

            //8 Prop_Specs
            if (pms.containsKey("idSpecs")) {
                updateProperties("Prop_Specs", pms)
            } else {
                if (pms.getString("Specs") != "")
                    fillProperties(true, "Prop_Specs", pms)
            }

            //9 Prop_LocationDetails
            if (pms.containsKey("idLocationDetails")) {
                updateProperties("Prop_LocationDetails", pms)
            } else {
                if (pms.getString("LocationDetails") != "")
                    fillProperties(true, "Prop_LocationDetails", pms)
            }

            //10 Prop_Number
            if (pms.containsKey("idNumber")) {
                updateProperties("Prop_Number", pms)
            } else {
                if (pms.getString("Number") != "")
                    fillProperties(true, "Prop_Number", pms)
            }

            //11 Prop_InstallationDate
            if (pms.containsKey("idInstallationDate")) {
                updateProperties("Prop_InstallationDate", pms)
            } else {
                if (pms.getString("InstallationDate") != "")
                    fillProperties(true, "Prop_InstallationDate", pms)
            }

            //12 Prop_CreatedAt
            if (pms.containsKey("idCreatedAt")) {
                updateProperties("Prop_CreatedAt", pms)
            } else {
                if (pms.getString("CreatedAt") != "")
                    fillProperties(true, "Prop_CreatedAt", pms)
            }
            //13 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt")) {
                updateProperties("Prop_UpdatedAt", pms)
            } else {
                if (pms.getString("UpdatedAt") != "")
                    fillProperties(true, "Prop_UpdatedAt", pms)
            }
            //14 Prop_Description
            if (pms.containsKey("idDescription")) {
                updateProperties("Prop_Description", pms)
            } else {
                if (pms.getString("Description") != "")
                    fillProperties(true, "Prop_Description", pms)
            }
        } else {
            throw new XError("Нейзвестный режим сохранения ('ins', 'upd')")
        }

        return loadObjectServed(own)
    }

    @DaoMethod
    Store loadObjectServed(long id) {

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Object", "")
        Store st = mdb.createStore("Obj.Served")

        Store stCls = loadSqlMeta("""
            select id from Cls where typ=${map.get("Typ_Object")}
        """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")

        String whe = "o.id=${id}"
        if (id==0)
            whe = "o.cls in (${idsCls.join(",")})"

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
                v15.id as idSection, v15.propVal as pvSection, v15.obj as objSection, null as nameSection
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_ObjectType --1072
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_StartKm   --1074
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_FinishKm   --1074
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_StartPicket --1073
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_FinishPicket   --1075
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_PeriodicityReplacement   --1075
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_Side   --1075
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_Specs   --1075
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_LocationDetails   --1075
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_Number   --1075
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_InstallationDate   --1075
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_CreatedAt   --1075
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_UpdatedAt   --1075
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Description   --1075
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_Section   --1075
                left join DataPropVal v15 on d15.id=v15.dataprop
            where ${whe}
        """, map)

        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", true)

        Store stObj = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer    
        """, "", "nsidata")
        StoreIndex indObj = stObj.getIndex("id")

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_Side", "")
        Store stFV = loadSqlMeta("""
            select id, name
            from Factor where parent = ${map.get("Factor_Side")} 
        """, "")
        StoreIndex indFV = stFV.getIndex("id")

        for (StoreRecord record in st) {
            StoreRecord rObj = indObj.get(record.get("objObjectType"))
            if (rObj != null) {
                record.set("nameObjectType", rObj.getString("name"))
            }
            rObj = indObj.get(record.get("objSection"))
            if (rObj != null) {
                record.set("nameSection", rObj.getString("name"))
            }
            record.set("fvSide", mapPV.get(record.getLong("pvSide")))
            StoreRecord rFV = indFV.get(record.getLong("fvSide"))
            if (rFV != null)
                record.set("nameSide", rFV.getString("name"))
        }
        //
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
                select o.id, o.cls, v.name, v.fullname, null as pv 
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
                select o.id, o.cls, v.name, null as pv 
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
            if ( cod.equalsIgnoreCase("Prop_Specs") ||
                    cod.equalsIgnoreCase("Prop_LocationDetails") ||
                    cod.equalsIgnoreCase("Prop_Number")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
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
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                        cod.equalsIgnoreCase("Prop_PeriodicityReplacement")) {
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
            if (cod.equalsIgnoreCase("Prop_ObjectType") ||
                    cod.equalsIgnoreCase("Prop_Section")) {
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
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                        cod.equalsIgnoreCase("Prop_PeriodicityReplacement")) {
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
                    cod.equalsIgnoreCase("Prop_Section")) {
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

    @DaoMethod
    Store findStationOfCoord(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", "Typ_Section", "")

        Store stCls = loadSqlMeta("""
            select id from Cls where typ=${map.get("Typ_Section")}
        """, "")
        Set<Object> idsCls = stCls.getUniqueValues("id")

        String whe = "o.cls in (${idsCls.join(",")})"

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
        Store st = loadSqlServiceWithParams(sql, params, "", "nsidata")
        //mdb.outTable(st)
        if (st.size()==1) {
            long idPV = apiMeta().get(ApiMeta).idPV("cls", st.get(0).getLong("cls"), "Prop_Section")
            st.get(0).set("pv", idPV )
            return st
        } else
            throw new XError("Not Found")
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
