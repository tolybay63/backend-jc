package tofi.api.dta.impl

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.adm.ApiAdm
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

class ApiPersonnalDataImpl extends BaseMdbUtils implements ApiPersonnalData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiAdm() {
        return app.bean(ApinatorService).getApi("adm")
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

    @Override
    Store infoUser(Map<String, Long> mapCods, long authuser, String idsCls, String idsUser) {
        String whe = "dv.strval='${authuser}' and o.cls in (${idsCls})"
        if (authuser == 0)
            whe = "o.cls in (${idsCls})"
        if (idsUser.contains(","))
            whe = "o.id in (${idsUser})"

        Store st= mdb.createStore("UserInfo")
        mdb.loadQuery(st, """
            select o.id, o.cls, dv.authUser as authUser,
                dv1.strVal || ' ' || dv2.strVal || case when dv3.strVal <> '' then ' '||dv3.strVal else '' end as name,
                dv1.strVal as UserSecondName, dv2.strVal as UserFirstName, case when dv3.strVal <> '' then dv3.strVal else '' end as UserMiddleName 
            from Obj o
                left join ObjVer v on o.id=v.ownerver and v.lastVer=1
                left join DataProp d on d.isobj=1 and d.objorrelobj=o.id and d.prop=:Prop_UserId
                left join DataPropVal dv on d.id=dv.dataprop
                left join DataProp d1 on d1.isobj=1 and d1.objorrelobj=o.id and d1.prop=:Prop_UserSecondName
                left join DataPropVal dv1 on d1.id=dv1.dataprop
                left join DataProp d2 on d2.isobj=1 and d2.objorrelobj=o.id and d2.prop=:Prop_UserFirstName
                left join DataPropVal dv2 on d2.id=dv2.dataprop
                left join DataProp d3 on d3.isobj=1 and d3.objorrelobj=o.id and d3.prop=:Prop_UserMiddleName
                left join DataPropVal dv3 on d3.id=dv3.dataprop
            where ${whe}
        """, mapCods)
        //
        return st
    }

    @Override
    Store loadSql(String sql, String domain) {
        if (domain.isEmpty())
            return mdb.loadQuery(sql)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql)
        }
    }

    @Override
    Store loadSqlWithParams(String sql, Map<String, Object> params, String domain) {
        if (domain.isEmpty())
            return mdb.loadQuery(sql, params)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql, params)
        }
    }

    @Override
    long getClsOrRelCls(long owner, int isObj) {
        if (isObj==1) {
            Store stTmp =  mdb.loadQuery("select cls from Obj where id=:id", [id: owner])
            if (stTmp.size()>0)
                return stTmp.get(0).getLong("cls")
            else
                return 0
        } else {
            Store stTmp =  mdb.loadQuery("select relcls from RelObj where id=:id", [id: owner])
            if (stTmp.size()>0)
                return stTmp.get(0).getLong("relcls")
            else
                return 0
        }
    }

    @Override
    boolean is_exist_entity_as_data(long entId, String entName, String propVal) {
        if (entName.equalsIgnoreCase("obj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=1 and v.propVal in (0${propVal}) and v.obj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("relobj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=0 and v.propVal in (0${propVal}) and v.relobj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("factorVal")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and v.propVal=${propVal} and v.obj is null and v.relobj is null
                limit 1
            """).size() > 0
        }
        throw new XError("Not known Entity")
    }

    @Override
    boolean is_exist_entity_as_dataOld(long entId, String entName, long propVal) {
        if (entName.equalsIgnoreCase("obj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=1 and v.propVal=${propVal} and v.obj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("relobj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=0 and v.propVal=${propVal} and v.relobj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("factorVal")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and v.propVal=${propVal} and v.obj is null and v.relobj is null
                limit 1
            """).size() > 0
        }
        throw new XError("Not known Entity")
    }

    @Override
    boolean checkExistOwners(long clsORrelcls, boolean isObj) {
        if (isObj)
            return mdb.loadQueryNative("""
                select id from Obj where cls=${clsORrelcls} limit 1
            """).size() > 0
        else
            return mdb.loadQueryNative("""
                select id from RelObj where relcls=${clsORrelcls} limit 1
            """).size() > 0
    }

    @Override
    Store loadPersonnal(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Personnel", "")
        if (map.isEmpty())
            throw new XError("Not found [Cls_Personnel]")

        String whe = "o.id=${id}"
        if (id == 0)
            whe = "o.cls=${map.get("Cls_Personnel")}"

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store st = mdb.createStore("Obj.Personnal")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.fullName,
                v1.id as idTabNumber, v1.strVal as TabNumber,
                v2.id as idUserSecondName, v2.strVal as UserSecondName,
                v4.id as idUserFirstName, v4.strVal as UserFirstName,
                v5.id as idUserMiddleName, v5.strVal as UserMiddleName,
                v6.id as idUserEmail, v6.strVal as UserEmail,
                v7.id as idUserPhone, v7.strVal as UserPhone,
                v8.id as idUserDateBirth, v8.dateTimeVal as UserDateBirth,
                v9.id as idDateEmployment, v9.dateTimeVal as DateEmployment,
                v10.id as idDateDismissal, v10.dateTimeVal as DateDismissal,
                v11.id as idCreatedAt, v11.dateTimeVal as CreatedAt,
                v12.id as idUpdatedAt, v12.dateTimeVal as UpdatedAt,
                v13.id as idUserSex, v13.propVal as pvUserSex, null as fvUserSex, null as nameUserSex,
                v14.id as idPosition, v14.propVal as pvPosition, null as fvPosition, null as namePosition,
                v15.id as idLocation, v15.obj as objLocation, v15.propVal as pvLocation, null as nameLocation,
                v16.id as idIsActive, v16.propVal as pvIsActive, null as fvIsActive, null as nameIsActive,
                v17.id as idUser, v17.obj as objUser, v17.propVal as pvUser, null as nameUser
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_TabNumber
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_UserSecondName
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_UserFirstName
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_UserMiddleName
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=:Prop_UserEmail
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_UserPhone
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UserDateBirth
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_DateEmployment
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_DateDismissal
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_CreatedAt
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_UpdatedAt
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_UserSex
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_Position
                left join DataPropVal v14 on d14.id=v14.dataprop     
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_Location
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=:Prop_IsActive
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=:Prop_User
                left join DataPropVal v17 on d17.id=v17.dataprop
            where ${whe}
            order by o.id
        """, map)
        // UserId
        Map<String, Long> mapUId = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UserId", "")
        Store stUser = mdb.loadQuery("""
            select o.id, ov.fullname, v.strVal as userId, null as login
            from Obj o
            left join ObjVer ov on ov.ownerVer=o.id and ov.lastVer=1
            left join DataProp d on d.isObj=1 and d.objorrelobj=o.id and d.prop=${mapUId.get("Prop_UserId")}
            left join DataPropVal v on d.id=v.dataprop
            where ${whe}
        """)
        Store stAU = apiAdm().get(ApiAdm).loadSql("""
            select id, login from AuthUser
        """, "")
        StoreIndex indAU = stAU.getIndex("id")
        for (StoreRecord r in stUser) {
            StoreRecord rec = indAU.get(r.getLong("userId"))
            if (rec != null)
                r.set("login", rec.getString("login"))
        }

        StoreIndex indUser = stUser.getIndex("id")
        //
        Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_Position", true)
        Map<Long, Long> mapPV2 = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_UserSex", true)
        Map<Long, Long> mapPV3 = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_IsActive", true)
        mapPV.putAll(mapPV2)
        mapPV.putAll(mapPV3)

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_Sex", "")
        String wheFV = "${map.get("Factor_Sex")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_Position", "")
        wheFV = wheFV + ",${map.get("Factor_Position")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_IsActive", "")
        wheFV = wheFV + ",${map.get("Factor_IsActive")}"

        Store stFV = apiMeta().get(ApiMeta).loadSql("""
            select id, name from Factor where parent in (${wheFV}) 
        """, "")
        StoreIndex indFV = stFV.getIndex("id")

        Store stObj = loadSqlService("""
            select o.id, v.name from Obj o, ObjVer v where o.id=v.ownerVer    
        """, "", "orgstructuredata")
        StoreIndex indObj = stObj.getIndex("id")
        //
        for (StoreRecord record in st) {
            record.set("fvUserSex", mapPV.get(record.getLong("pvUserSex")))
            record.set("fvPosition", mapPV.get(record.getLong("pvPosition")))
            record.set("fvIsActive", mapPV.get(record.getLong("pvIsActive")))
            //
            StoreRecord rFV = indFV.get(record.getLong("fvUserSex"))
            if (rFV != null)
                record.set("nameUserSex", rFV.getString("name"))
            rFV = indFV.get(record.getLong("fvPosition"))
            if (rFV != null)
                record.set("namePosition", rFV.getString("name"))
            rFV = indFV.get(record.getLong("fvIsActive"))
            if (rFV != null)
                record.set("nameIsActive", rFV.getString("name"))
            //
            StoreRecord rObj = indObj.get(record.getLong("objLocation"))
            if (rObj != null)
                record.set("nameLocation", rObj.getString("name"))
            StoreRecord rUsr = indUser.get(record.getLong("id"))
            if (rUsr != null && !rUsr.getString("login").isEmpty())
                record.set("login", rUsr.getString("login"))
            rUsr = indUser.get(record.getLong("objUser"))
            if (rUsr != null)
                record.set("fullNameUser", rUsr.getString("fullname"))
        }
        //
        return st
    }

    @Override
    Long saveNotification(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own = 0
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        params.putIfAbsent("fullName", pms.getString("name"))
        //
        if (mode.equalsIgnoreCase("ins")) {
            Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Notification", "")
            params.put("cls", map.get("Cls_Notification"))
            own = eu.insertEntity(params)
            pms.put("own", own)
            //1 Prop_Personnel
            if (pms.getLong("objPersonnel") > 0)
                fillProperties(true, "Prop_Personnel", pms)
            else
                throw new XError("Не указан [objPersonnel]")
            //2 Prop_Description
            if (!pms.getString("Description").isEmpty())
                fillProperties(true, "Prop_Description", pms)
            else
                throw new XError("Не указан [Description]")
            //3 Prop_TimeSending
            if (!pms.getString("TimeSending").isEmpty())
                fillProperties(true, "Prop_TimeSending", pms)
            else
                throw new XError("Не указан [TimeSending]")
            //
        } else {
            throw new XError("Unknown mode")
        }
        return own
    }

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

        // Attrib multiStr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib dttm
        if ([FD_AttribValType_consts.dttm].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_TimeSending") ||
                    cod.equalsIgnoreCase("Prop_TimeReceiving") ||
                    cod.equalsIgnoreCase("Prop_TimeReading")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }
        // For Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Personnel")) {
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

        long au = 1
        recDPV.set("authUser", au)
        if (params.containsKey("inputType"))
            recDPV.set("inputType", params.get("inputType"))
        else
            recDPV.set("inputType", FD_InputType_consts.app)
        long idDPV = mdb.getNextId("DataPropVal")
        recDPV.set("id", idDPV)
        recDPV.set("ord", idDPV)
        recDPV.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        mdb.insertRec("DataPropVal", recDPV, false)
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
        else
            throw new XError("Unknown model [${model}]")
    }

}
