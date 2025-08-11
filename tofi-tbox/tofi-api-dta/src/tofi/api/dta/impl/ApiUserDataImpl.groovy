package tofi.api.dta.impl

import jandcode.commons.UtCnv
import jandcode.commons.UtString
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiMonitoringData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiUserData
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiUserDataImpl extends BaseMdbUtils implements ApiUserData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }


    @Override
    Store infoUser(Map<String, Long> mapCods, long authuser, String idsCls, String idsUser = "0") {
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
    Store loadGroupUsers() {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_UsersGroup", "")
        String sql = """
            select o.id, v.name, o.cls, v.objParent as parent, o.ord, 1 as group, false as checked
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls=:Cls_UsersGroup
        """
        Store st = mdb.loadQuery(sql, map)
        Set<Object> idsOwn = st.getUniqueValues("id")
        if (idsOwn.isEmpty()) idsOwn.add(0L)

        Store stUsers = getUsersGroup(idsOwn)
        st.add(stUsers)
        return st
    }

    private Store getUsersGroup(Set<Object> idsOwn) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UsersGroup", "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@Prop_UsersGroup")

        String whe = UtString.join(idsOwn, ",")
        Store stTmp = mdb.loadQuery("""
            select v.id, d.objorrelobj as parent, v.obj, 0 as cls, '' as name
            from DataProp d, DataPropVal v
            where d.id=v.dataprop and d.prop=:Prop_UsersGroup 
                and d.objorrelobj in (${whe})
        """, map)

        String idsUser = UtString.join(stTmp.getUniqueValues("obj"), ",")
        if (idsUser.isEmpty()) idsUser = "0"

        Map<String, Long> mapCods = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_")
        Set<Object> setCls = apiMeta().get(ApiMeta).setIdsOfCls("Typ_Users")
        if (setCls.empty) setCls.add(0L)

        Store stUsers = infoUser(mapCods, 0, UtString.join(setCls, ","), idsUser)

        StoreIndex stInd = stUsers.getIndex("id")

        for (StoreRecord r in stTmp) {
            StoreRecord rec = stInd.get(r.getLong("obj"))
            if (rec != null) {
                r.set("cls", rec.getLong("cls"))
                r.set("name", rec.getString("name"))
            }
        }
        return stTmp
    }

    @Override
    Store addUsersGroup(long obj, boolean isOwn) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UsersGroup", "")
        map.put("obj", obj)
        if (isOwn) {
            Store st = mdb.loadQuery("""
                select distinct v.obj, o.cls, ov.name  
                from DataProp d, DataPropVal v, Obj o, ObjVer ov
                where d.id=v.dataprop and d.prop=:Prop_UsersGroup and d.objorrelobj=:obj
                    and v.obj=o.id and o.id=ov.ownerVer and ov.lastVer=1
            """, map)
            return st
        } else {
            Store stLeaf = mdb.loadQuery("""
                WITH RECURSIVE r AS (
                    select o.id 
                    from Obj o 
                        left join Objver v on o.id=v.ownerver and v.lastver=1
                    where o.id=:obj
                    union
                    select o.id 
                    from Obj o 
                        left join Objver v on o.id=v.ownerver and v.lastver=1
                        join r
                          ON v.objparent = r.id
                )
                SELECT * FROM r
            """, [obj: obj])

            String whe = stLeaf.getUniqueValues("id").join(",")
            if (whe.isEmpty()) whe = "0"

            Store st = mdb.loadQuery("""
                select distinct v.obj, o.cls, ov.name  
                from DataProp d, DataPropVal v, Obj o, ObjVer ov
                where d.id=v.dataprop and d.prop=:Prop_UsersGroup and d.objorrelobj in (${whe})
                    and v.obj=o.id and o.id=ov.ownerVer and ov.lastVer=1
            """, map)
            return st
        }
    }

    @Override
    long getFKFromTable(String tableName, long idTable, String fkField) {
        return mdb.loadQuery("""
            select ${fkField} from ${tableName} where id=${idTable}
        """).get(0).getLong(fkField)
    }


    @Override
    Map<Long, String> mapUserNameFromObj(Map<String, Long> mapCods, Set<Object> idsUser) {
        if (idsUser.empty) idsUser.add(0L)
        String whe = "o.id in (${UtString.join(idsUser, ",")})"
        Store st = mdb.loadQuery("""
            select o.id, dv1.strVal || ' ' || dv2.strVal || case when dv3.strVal <> '' then ' '||dv3.strVal else '' end as name 
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

        Map<Long, String> mapRez = new HashMap<>()
        for (StoreRecord r in st) {
            mapRez.put(r.getLong("id"), r.getString("name"))
        }
        return mapRez
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
        if (domain.isEmpty()) {
            return mdb.loadQuery(sql, params)
        } else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql, params)
        }
    }

    @Override
    long insertRecToTable(String tableName, Map<String, Object> params, boolean generateId) {
        Store st = mdb.createStore(tableName)
        StoreRecord r = st.add(params)
        if (generateId)
            return mdb.insertRec(tableName, r, generateId)
        else {
            long id = mdb.getNextId(tableName)
            r.set("id", id)
            r.set("ord", id)
            r.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
            //
            return mdb.insertRec(tableName, r, false);
        }
    }

    @Override
    void execSql(String sql) {
        mdb.execQuery(sql)
    }

    @Override
    void updateTable(String tableName, Map<String, Object> params) {
        Store st = mdb.createStore(tableName)
        StoreRecord r = st.add(params)
        mdb.updateRec(tableName, r)
    }

    @Override
    void deleteTable(String tableName, long id) {
        mdb.deleteRec(tableName, id)
    }

    @Override
    long createOwner(Map<String, Object> params) {
        String tabl = "RelObj"
        if (UtCnv.toBoolean(params.get("isObj")))
            tabl = "Obj"
        EntityMdbUtils ent = new EntityMdbUtils(mdb, tabl)
        if (UtCnv.toString(params.get("mode"))=="ins")
            return ent.insertEntity(params)
        else {
            ent.updateEntity(params)
            return UtCnv.toLong(params.get("id"))
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
    void deleteEntity(long entId, String tableName) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, tableName)
        eu.deleteEntity(entId)
    }
}
