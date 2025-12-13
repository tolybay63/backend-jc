package tofi.api.dta.impl

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.IVariantMap
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiNSIData
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiNSIDataImpl extends BaseMdbUtils implements ApiNSIData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
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
    void attachFile(Map<String, Object> rec) {
        IVariantMap par = new VariantMap(rec)
        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        if (par.isValueNull("fileVal")) {
            throw new XError("filevalue is required")
        }
        String propCod = par.getString("propCod")

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", propCod, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${propCod}")
        Map<String, Object> params = new HashMap<>()
        params.put("isObj", par.getLong("isObj"))
        params.put("dbeg", par.getString("dbeg"))
        params.put("dend", par.getString("dend"))
        params.put("objorrelobj", par.getLong("objorrelobj"))
        params.put("prop", map.get(propCod))
        params.put("fileVal", par.getLong("fileVal"))
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        long au = authSvc.getCurrentUser().getAttrs().getLong("id")
        if (au == 0) au= 1
            //throw new XError("notLogined")
        long id = insertRecToTable("DataProp", params, true)

        params.put("dataProp", id)
        params.put("authUser", au)
        params.put("inputType", FD_InputType_consts.app)
        //params.put("cmt", par.getString("cmt"))
        insertRecToTable("DataPropVal", params, false)
    }

    @Override
    Store loadObjForSelect(String codCls, String codProp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codCls, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codCls}")

        Store st = mdb.loadQuery("""
            select o.id, v.name, 0 as pv 
            from Obj o
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
            where o.cls=${map.get(codCls)}
            order by v.name
        """)

        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get(codCls), codProp)
        for (StoreRecord r in st) {
            r.set("pv", pv)
        }
        return st
    }

    @Override
    Store loadObjTreeForSelect(String codCls, String codProp) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codCls, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codCls}")

        Store st = mdb.loadQuery("""
            select o.id, v.name, v.objParent as parent, 0 as pv 
            from Obj o
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
            where o.cls=${map.get(codCls)}
            order by v.name
        """)

        long pv = apiMeta().get(ApiMeta).idPV("cls", map.get(codCls), codProp)
        for (StoreRecord r in st) {
            r.set("pv", pv)
        }
        return st
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
