package tofi.api.dta.impl

import jandcode.commons.error.XError
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
import tofi.api.mdl.ApiMeta
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
            whe = "o.cls=(${map.get("Cls_Personnel")})"

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
                v10.id as idDateDateDismissal, v10.dateTimeVal as DateDismissal,
                v11.id as idCreatedAt, v11.dateTimeVal as CreatedAt,
                v12.id as idUpdatedAt, v12.dateTimeVal as UpdatedAt,
                v13.id as idUserSex, v13.propVal as pvUserSex, null as fvUserSex, null as nameUserSex,
                v14.id as idPosition, v14.propVal as pvPosition, null as fvPosition, null as namePosition,
                v15.id as idLocation, v15.obj as objLocation, v15.propVal as pvLocation, null as nameLocation
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
            where ${whe}
        """, map)
        // UserId
        Map<String, Long> mapUId = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UserId", "")
        Store stUser = mdb.loadQuery("""
            select o.id, v.strVal as userId, null as login
            from Obj o
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
        mapPV.putAll(mapPV2)

        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_Sex", "")
        String wheFV = "${map.get("Factor_Sex")}"
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "Factor_Position", "")
        wheFV = wheFV + ",${map.get("Factor_Position")}"

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
            StoreRecord rFV = indFV.get(record.getLong("fvUserSex"))
            if (rFV != null)
                record.set("nameUserSex", rFV.getString("name"))
            rFV = indFV.get(record.getLong("fvPosition"))
            if (rFV != null)
                record.set("namePosition", rFV.getString("name"))

            StoreRecord rObj = indObj.get(record.getLong("objLocation"))
            if (rObj != null)
                record.set("nameLocation", rObj.getString("name"))
            StoreRecord rUsr = indUser.get(record.getLong("id"))
            if (rUsr != null)
                record.set("login", rUsr.getString("login"))
        }
        //
        return st
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
