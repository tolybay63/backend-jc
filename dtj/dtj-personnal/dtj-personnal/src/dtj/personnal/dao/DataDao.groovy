package dtj.personnal.dao

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
import tofi.api.adm.ApiAdm
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

    @DaoMethod
    Store loadPersonnalLocationForSelect(long obj, String codProp) {
        Store st = mdb.createStore("Obj.PersonalLocationForSelect")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Personnel", "")
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map2.put("Cls_Personnel", map.get("Cls_Personnel"))
        map2.put("obj", obj)

        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.fullName, null as pv,
            v1.propVal as pvPosition, null as fvPosition, null as namePosition            
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Position
                left join DataPropval v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Location
                inner join DataPropval v2 on d2.id=v2.dataProp and v2.obj=:obj
            where o.cls=:Cls_Personnel    
        """, map2)

        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Position", true)
        Map<Long, Long> mapPV2 = apiMeta().get(ApiMeta).mapEntityIdFromPV("cls", codProp, false)
        for (StoreRecord r in st) {
            r.set("fvPosition", mapPV.get(r.getLong("pvPosition")))
            r.set("pv", mapPV2.get(r.getLong("cls")))
        }
        Set<Object> fvsPosition = st.getUniqueValues("fvPosition")
        Store stFV = loadSqlMeta("""
            select id, name from Factor where id in (0${fvsPosition.join(",")}) 
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("fvPosition"))
            if (rec != null)
                r.set("namePosition", rec.getString("name"))
        }
        //
        return st
    }

    @DaoMethod
    Store loadPersonnalByPosition(long pvPosition, String codProp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Personnel", "")
        if (map.isEmpty())
            throw new XError("Not found [Cls_Personnel]")

        long cls = map.get("Cls_Personnel")
        long pv = apiMeta().get(ApiMeta).idPV("cls", cls, codProp)

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("cls", cls)
        map.put("pv", pvPosition)
        Store st = mdb.createStore("Obj.PersonnalByPosition")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.fullName, ${pv} as pv,
                v14.propVal as pvPosition, null as fvPosition, null as namePosition,
                v15.obj as objLocation, v15.propVal as pvLocation, null as nameLocation
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_UserSecondName
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_UserFirstName
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_UserMiddleName
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Position
                inner join DataPropVal v14 on d14.id=v14.dataprop and v14.propVal=:pv     
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_Location
                left join DataPropVal v15 on d15.id=v15.dataprop
            where o.cls=:cls
        """, map)

        //
        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Position", true)

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_Position", "")

        Store stFV = apiMeta().get(ApiMeta).loadSql("""
            select id, name from Factor where parent = ${map.get("Factor_Position")} 
        """, "")
        StoreIndex indFV = stFV.getIndex("id")

        Store stObj = loadSqlService("""
            select o.id, v.name from Obj o, ObjVer v where o.id=v.ownerVer    
        """, "", "orgstructuredata")
        StoreIndex indObj = stObj.getIndex("id")
        //
        for (StoreRecord record in st) {
            record.set("fvPosition", mapPV.get(record.getLong("pvPosition")))
            StoreRecord rFV = indFV.get(record.getLong("fvPosition"))
            if (rFV != null)
                record.set("namePosition", rFV.getString("name"))

            StoreRecord rObj = indObj.get(record.getLong("objLocation"))
            if (rObj != null)
                record.set("nameLocation", rObj.getString("name"))
        }
        //
        return st
    }

    @DaoMethod
    Store loadPersonnal(long id) {
        return apiPersonnalData().get(ApiPersonnalData).loadPersonnal(id)
    }

    @DaoMethod
    Store savePersonnal(String mode, Map<String, Object> params) {
        Map<String, Object> par = new HashMap<>(params)
        validatePersonal(mode, params)

        long own = 0
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")

        String nm = UtCnv.toString(par.get("UserSecondName")) + " " + UtCnv.toString(par.get("UserFirstName")).charAt(0) + "."
        String fn = UtCnv.toString(par.get("UserSecondName")) + " " + UtCnv.toString(par.get("UserFirstName"))
        if (!UtCnv.toString(par.get("UserMiddleName")).isEmpty()) {
            nm += "" + UtCnv.toString(par.get("UserMiddleName")).charAt(0) + "."
            fn += " " + UtCnv.toString(par.get("UserMiddleName"))
        }
        par.put("name", nm)
        par.put("fullName", fn)

        if (mode.equalsIgnoreCase("ins")) {
            //
            long userId = 0
            if (params.containsKey("login")) {
                par.put("passwd", "123456")
                userId = regUser(par)
            }
            //
            try {
                Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Personnel", "")
                if (map.isEmpty())
                    throw new XError("Not found [Cls_Personnel]")
                par.put("cls", map.get("Cls_Personnel"))
                own = eu.insertEntity(par)
                params.put("own", own)
                //1 Prop_TabNumber
                fillProperties(true, "Prop_TabNumber", params)
                //2 Prop_UserSecondName
                fillProperties(true, "Prop_UserSecondName", params)
                //3 Prop_UserFirstName
                fillProperties(true, "Prop_UserFirstName", params)
                //4 Prop_UserMiddleName
                if (!UtCnv.toString(params.get("UserMiddleName")).isEmpty())
                    fillProperties(true, "Prop_UserMiddleName", params)
                //5 Prop_UserDateBirth
                fillProperties(true, "Prop_UserDateBirth", params)
                //6 Prop_UserEmail
                fillProperties(true, "Prop_UserEmail", params)
                //7 Prop_UserPhone
                fillProperties(true, "Prop_UserPhone", params)
                //8 Prop_DateEmployment
                if (!UtCnv.toString(params.get("DateEmployment")).isEmpty())
                    fillProperties(true, "Prop_DateEmployment", params)
                //9 Prop_DateDismissal
                if (!UtCnv.toString(params.get("DateDismissal")).isEmpty())
                    fillProperties(true, "Prop_DateDismissal", params)
                //10 Prop_CreatedAt
                if (!UtCnv.toString(params.get("CreatedAt")).isEmpty())
                    fillProperties(true, "Prop_CreatedAt", params)
                else
                    throw new XError("[CreatedAt] not specified")
                //11 Prop_UpdatedAt
                fillProperties(true, "Prop_UpdatedAt", params)
                //12 Prop_UserId
                if (userId > 0) {
                    params.put("UserId", userId)
                    fillProperties(true, "Prop_UserId", params)
                }
                //13 Prop_UserSex
                fillProperties(true, "Prop_UserSex", params)
                //14 Prop_Position
                fillProperties(true, "Prop_Position", params)
                //15 Prop_Location
                fillProperties(true, "Prop_Location", params)
                //16 Prop_Location
                fillProperties(true, "Prop_Location", params)
                //17 Prop_User
                fillProperties(true, "Prop_User", params)
            } catch (Exception e) {
                e.printStackTrace()
                if (userId > 0)
                    deleteAuthUser(userId)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            own = UtCnv.toLong(params.get("id"))
            eu.updateEntity(par)
            //
            params.put("own", own)
            //1 Prop_TabNumber
            updateProperties("Prop_TabNumber", params)
            //2 Prop_UserSecondName
            updateProperties("Prop_UserSecondName", params)
            //3 Prop_UserFirstName
            updateProperties("Prop_UserFirstName", params)
            //4 Prop_UserMiddleName
            if (UtCnv.toLong(params.get("idUserMiddleName")) > 0) {
                updateProperties("Prop_UserMiddleName", params)
            } else {
                if (!UtCnv.toString(params.get("UserMiddleName")).isEmpty())
                    fillProperties(true, "Prop_UserMiddleName", params)
            }
            //5 Prop_UserDateBirth
            updateProperties("Prop_UserDateBirth", params)
            //6 Prop_UserEmail
            updateProperties("Prop_UserEmail", params)
            //7 Prop_UserPhone
            updateProperties("Prop_UserPhone", params)
            //8 Prop_DateEmployment
            if (UtCnv.toLong(params.get("idDateEmployment")) > 0)
                updateProperties("Prop_DateEmployment", params)
            else {
                if (!UtCnv.toString(params.get("DateEmployment")).isEmpty())
                    fillProperties(true, "Prop_DateEmployment", params)
            }
            //9 Prop_DateDismissal
            if (UtCnv.toLong(params.get("idDateDismissal")) > 0)
                updateProperties("Prop_DateDismissal", params)
            else {
                if (!UtCnv.toString(params.get("DateDismissal")).isEmpty())
                    fillProperties(true, "Prop_DateDismissal", params)
            }
            //11 Prop_UpdatedAt
            updateProperties("Prop_UpdatedAt", params)
            //13 Prop_UserSex
            updateProperties("Prop_UserSex", params)
            //14 Prop_Position
            updateProperties("Prop_Position", params)
            //15 Prop_Location
            updateProperties("Prop_Location", params)
            //15 Prop_User
            if (UtCnv.toLong(params.get("idUser")) > 0)
                updateProperties("Prop_User", params)
            else
                fillProperties(true, "Prop_User", params)
            //16 Prop_IsActive
            if (UtCnv.toLong(params.get("idIsActive")) > 0)
                updateProperties("Prop_IsActive", params)
            else {
                if (UtCnv.toLong(params.get("fvIsActive")) > 0)
                    fillProperties(true, "Prop_IsActive", params)
            }
        } else {
            throw new XError("Unknown mode")
        }
        return loadPersonnal(own)
    }

    @DaoMethod
    void deleteObjWithProperties(long id, boolean isUser) {
        //
        validateForDeleteOwner(id)
        //
        if (isUser) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UserId", "")
            if (map.isEmpty())
                throw new XError("Not found [Prop_UserId]")
            Store st = mdb.loadQuery("""
                select v.strVal as userId
                from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=1 and d.objOrRelObj=${id} and d.prop=${map.get("Prop_UserId")}
            """)
            if (st.size() == 0)
                throw new XError("Не найден Объект или его значение свойства Prop_UserId")
            long userId = st.get(0).getLong("userId")
            deleteAuthUser(userId)
        }
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
                """, "", "objectdata")
                if (stData.size() > 0)
                    lstService.add("objectdata")
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
                if (lstService.size() > 0) {
                    throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                }

            }
        }
    }

    private static void validatePersonal(String mode, Map<String, Object> params) {
        if (mode == "ins" && params.containsKey("login")) {
            if (UtCnv.toString(params.get("login")).isEmpty())
                throw new XError("[login] not specified")
        }
        if (UtCnv.toString(params.get("UserEmail")).isEmpty())
            throw new XError("[UserEmail] not specified")
        if (UtCnv.toString(params.get("TabNumber")).isEmpty())
            throw new XError("[TabNumber] not specified")
        if (UtCnv.toString(params.get("UserSecondName")).isEmpty())
            throw new XError("[UserSecondName] not specified")
        if (UtCnv.toString(params.get("UserFirstName")).isEmpty())
            throw new XError("[UserFirstName] not specified")
        if (UtCnv.toString(params.get("UserDateBirth")).isEmpty())
            throw new XError("[UserDateBirth] not specified")
        if (UtCnv.toString(params.get("UpdatedAt")).isEmpty())
            throw new XError("[UpdatedAt] not specified")
        if (UtCnv.toLong(params.get("fvUserSex")) == 0)
            throw new XError("[UserSex] not specified")
        if (UtCnv.toLong(params.containsKey("fvPosition")) == 0)
            throw new XError("[Position] not specified")
        if (UtCnv.toLong(params.containsKey("objLocation")) == 0)
            throw new XError("[Location] not specified")
        if (UtCnv.toLong(params.containsKey("objUser")) == 0)
            throw new XError("[User] not specified")
    }

    private long regUser(Map<String, Object> params) {
        Map<String, Object> rec = new HashMap<>()
        rec.put("login", params.get("login"))
        rec.put("passwd", params.get("passwd"))
        rec.put("accessLevel", 1)
        rec.put("email", params.get("UserEmail"))
        if (params.containsKey("UserPhone"))
            rec.put("phone", params.get("UserPhone"))
        rec.put("name", params.get("name"))
        rec.put("fullName", params.get("fullName"))
        return apiAdm().get(ApiAdm).regUser(rec)
    }

    private void deleteAuthUser(long id) {
        apiAdm().get(ApiAdm).deleteAuthUser(id)
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
                if (params.get(keyValue) != null) {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt") ||
                    cod.equalsIgnoreCase("Prop_DateEmployment") ||
                    cod.equalsIgnoreCase("Prop_DateDismissal") ||
                    cod.equalsIgnoreCase("Prop_UserDateBirth")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_UserSex") ||
                    cod.equalsIgnoreCase("Prop_Position") ||
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
            if (cod.equalsIgnoreCase("Prop_StartKm")) { // template
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
            if (cod.equalsIgnoreCase("Prop_Location") ||
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
        // For Attrib
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_TabNumber") ||
                    cod.equalsIgnoreCase("Prop_UserSecondName") ||
                    cod.equalsIgnoreCase("Prop_UserFirstName") ||
                    cod.equalsIgnoreCase("Prop_UserMiddleName") ||
                    cod.equalsIgnoreCase("Prop_UserEmail") ||
                    cod.equalsIgnoreCase("Prop_UserPhone") ||
                    cod.equalsIgnoreCase("Prop_UserId")) {
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
                    cod.equalsIgnoreCase("Prop_DateEmployment") ||
                    cod.equalsIgnoreCase("Prop_DateDismissal") ||
                    cod.equalsIgnoreCase("Prop_UserDateBirth")) {
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
            if (cod.equalsIgnoreCase("Prop_UserSex") ||
                    cod.equalsIgnoreCase("Prop_Position") ||
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
            if (cod.equalsIgnoreCase("Prop_Location") ||
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
        if (au == 0)
            au = 1//throw new XError("notLogined")
        return au
    }

}
