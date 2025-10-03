package tofi.api.dta.impl

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiClientData
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiUserData
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiObjectDataImpl extends BaseMdbUtils implements ApiObjectData {

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
    ApinatorApi apiIncidentData() {
        return app.bean(ApinatorService).getApi("incidentdata")
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
    void updateTable(String tableName, Map<String, Object> params) {
        Store st = mdb.createStore(tableName)
        StoreRecord r = st.add(params)
        mdb.updateRec(tableName, r)
    }

    @Override
    void execSql(String sql) {
        mdb.execQueryNative(sql)
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
    void deleteOwnerWithProperties(long id, int isObj) {
        //
        Store stObj = mdb.loadQuery("""
            select o.id
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and v.objParent=${id}
        """)
        if (stObj.size()>0)
            throw new XError("Существуют дочерние элементы")

        validateForDeleteOwner(id, isObj)
        //
        String tableName = isObj==1 ? "Obj" : "RelObj"
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

    @Override
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
                select o.id, o.cls, v.name, v.fullname, null as pv 
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

        Store res = mdb.createStore("Obj.ObjList")
        for (StoreRecord r in st) {
            if (r.getLong("pv") > 0)
                res.add(r)
        }
        return res
    }

    void validateForDeleteOwner(long owner, int isObj) {
        //---< check data in other DB
        if (isObj==1) {
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
                    if (lstService.size() > 0) {
                        throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                    }
                }
            }
        } else if (isObj==0) {
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
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
        else
            throw new XError("Unknown model [${model}]")
    }



}
