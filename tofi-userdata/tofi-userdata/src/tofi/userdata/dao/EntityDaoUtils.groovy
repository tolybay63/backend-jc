package tofi.userdata.dao

import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.ModelService
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.dta.model.utils.UtPeriod
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.*
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService;


@CompileStatic
class EntityDaoUtils extends EntityMdbUtils {
    Mdb mdb
    String tableName

    public EntityDaoUtils(Mdb mdb, String tableName) {
        super(mdb, tableName)
        this.mdb = mdb
        this.tableName = tableName
    }

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    public void saveProfile(Map<String, Object> params) {
        //создаем Объект
        params.put("dbeg", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE))
        String mn = params.get("UserMiddleName").toString() != "" ? params.get("UserMiddleName").toString().substring(0,1) + "." : ""
        String fn = params.get("UserSecondName").toString() + " " + params.get("UserFirstName").toString().substring(0,1) + "." + mn
        params.put("fullName", fn)
        long own = insertEntity(params)
        params.put("own", own)
        //заполняем значения свойств Объекта
        //1. Идентификатор пользователя Prop_UserId-1046
        fillProp(true, "Prop_UserId", params)
        //2. Фамилия 1041
        fillProp(true, "Prop_UserSecondName", params)
        //3. Имя 1042
        fillProp(true, "Prop_UserFirstName", params)
        //4. Отчество 1043
        fillProp(true, "Prop_UserMiddleName", params)
        //5. Пол 1044
        fillProp(true, "Prop_UserSex", params)
        //6. Дата рождения
        fillProp(true, "Prop_UserDateBirth", params)
        //7. email
        fillProp(true, "Prop_UserEmail", params)
        //8. phone
        fillProp(true, "Prop_UserPhone", params)
    }

    private void fillProp(boolean isObj, String cod, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("own"))
        String keyValue = cod.split("_")[1]
        // val = params.get(keyValue)
        long userId = UtCnv.toLong(params.get("UserId"))
        params.put("UserId", userId)
        //...
        long objRef = UtCnv.toLong(params.get("obj"))
        long propVal = UtCnv.toLong(params.get("propVal"))

        Store stProp = apiMeta().get(ApiMeta).getPropInfo(cod)
        //
        long prop = stProp.get(0).getLong("id")
        long propType = stProp.get(0).getLong("proptype")
        long attribValType = stProp.get(0).getLong("attribValType")

        StoreRecord recDP = mdb.createStoreRecord("DataProp")
        recDP.set("isObj", isObj)
        recDP.set("objOrRelObj", own)
        recDP.set("prop", prop)
        if (stProp.get(0).getLong("statusFactor") > 0) {
            long fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
            recDP.set("status", fv)
        }

        if (stProp.get(0).getLong("providerTyp") > 0) {
            //
            // provider
            //
        }
        if (stProp.get(0).getBoolean("dependperiod")) {
            //
            recDP.set("periodType", FD_PeriodType_consts.year)
        }

        long idDP = mdb.insertRec("DataProp", recDP, true)
        //
        StoreRecord recDPV = mdb.createStoreRecord("DataPropVal")
        recDPV.set("dataProp", idDP)
        //
        // For Attrib - str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_UserId") || cod.equalsIgnoreCase("Prop_UserSecondName") ||
                    cod.equalsIgnoreCase("Prop_UserFirstName") || cod.equalsIgnoreCase("Prop_UserMiddleName") ||
                    cod.equalsIgnoreCase("Prop_UserEmail") || cod.equalsIgnoreCase("Prop_UserPhone")) {
                if (params.get(keyValue) != null)
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // For Attrib - dt
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_UserDateBirth")) {
                if (params.get(keyValue) != null)
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_UserSex")) {
                long pv = apiMeta().get(ApiMeta).idPV("factorVal", UtCnv.toLong(params.get(keyValue)), cod)
                recDPV.set("propVal", pv)
/*                if (propVal > 0)
                    recDPV.set("propVal", propVal)*/
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // for Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_UsersGroup")) {
                recDPV.set("propVal", propVal)
                recDPV.set("obj", objRef)
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
            recDPV.set("dbeg", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE))
            recDPV.set("dend", "3333-12-31")
        }

        long au = getUser();
        recDPV.set("authUser", au)
        recDPV.set("inputType", FD_InputType_consts.model)
        long idDPV = mdb.getNextId("DataPropVal")
        recDPV.set("id", idDPV)
        recDPV.set("ord", idDPV)
        recDPV.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        mdb.insertRec("DataPropVal", recDPV, false)

    }

    private long getUser() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        long au = authSvc.getCurrentUser().getAttrs().getLong("id");
        if (au == 0)
            throw new XError("notLogined");
        return au;
    }

    public void updateProfile(Map<String, Object> params) {
        for (it in params) {
            if (!it.key.equalsIgnoreCase("UserId") && it.key.endsWith("Id")) {
                def key2 = it.key.substring(0, it.key.length() - 2)
                updProp(it.key, key2, params)
            }
        }
    }

    private void updProp(String keyId, String keyValue, Map<String, Object> params) {
        long id = UtCnv.toLong(params.get(keyId))
        def value = UtCnv.toString(params.get(keyValue))
        if (keyId.equalsIgnoreCase("UserSexId")) {
            value = apiMeta().get(ApiMeta).idPV("factorVal", UtCnv.toLong(params.get("UserSex")), "Prop_UserSex")
        }

        def fld = "strVal"
        if (keyId.equalsIgnoreCase("UserDateBirthId"))
            fld = "dateTimeVal"
        if (keyId.equalsIgnoreCase("UserSexId"))
            fld = "propVal"
        String sql = """
            update DataPropval set ${fld}=:v where id=:id 
        """
        if (id > 0)
            mdb.execQuery(sql, [id: id, v: value])
        else
            fillProp(true, "Prop_" + keyValue, params)
    }

    private Store loadGroupUserRec(long id) {
        return mdb.loadQuery("""
            select o.id, o.cls, v.name, v.objParent as parent
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id=:id
        """, [id: id])
    }

    public Store insertGroup(Map<String, Object> params) {
        Map<String, Long> map = apiMeta()
                .get(ApiMeta)
                .getIdFromCodOfEntity("Cls", "Cls_UsersGroup", "")
        params.put("cls", map.get("Cls_UsersGroup"))
        params.put("fullName", params.get("name"))
        long id = insertEntity(params)
        //
        return loadGroupUserRec(id)
    }

    public Store updateGroup(Map<String, Object> params) {
        long id = UtCnv.toLong(params.get("id"))
        updateEntity(params)
        return loadGroupUserRec(id)
    }

    public void deleteGroup(long id) {
        //Существует ли подпапка?
        Store st = mdb.loadQueryNative("""
            select o.id
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and v.objparent=${id}
        """)
        if (st.size() > 0)
            throw new XError("hasChild")

        st = mdb.loadQueryNative("""
            select v.obj
            from DataProp d, DataPropVal v
            where d.id=v.dataprop and d.objorrelobj=${id}
        """)
        if (st.size() > 0)
            throw new XError("groupNotEmpty")
        //
        deleteEntity(id)
    }

    void saveGroupUsers(long obj, /*long cls, */List<Map<String, Object>> lst) {
        Map<String, Object> params = new HashMap<>()
        params.put("own", obj)
        //long pv = apiMeta().get(ApiMeta).idPV("cls", cls, "Prop_UsersGroup")
        //params.put("propVal", pv)
        //old values
        Map<String, Long> mapCods = getIdFromCodOfEntity("Prop", "Prop_UsersGroup", "")
        Store stOld = mdb.loadQuery("""
            select v.obj, v.id
            from DataProp d, DataPropVal v
            where d.id=v.dataprop and d.prop=:Prop_UsersGroup 
                and d.objorrelobj = ${obj}
        """, mapCods)
        Set<Object> oldUsers = stOld.getUniqueValues("obj")

        Set<Object> idsUser = new HashSet<>()

        //Adding
        for (Map<String, Object> map in lst) {
            idsUser.add(UtCnv.toLong(map.get("userobj")))
            if (!oldUsers.contains(UtCnv.toLong(map.get("userobj")))) {
                long cls = UtCnv.toLong(map.get("cls"))
                long pv = apiMeta().get(ApiMeta).idPV("cls", cls, "Prop_UsersGroup")
                params.put("propVal", pv)
                params.put("obj", map.get("userobj"))
                fillProp(true, "Prop_UsersGroup", params)
            }
        }

        //Deleting
        for (StoreRecord r in stOld) {
            if (!idsUser.contains(r.getLong("obj"))) {
                mdb.execQueryNative("""
                    delete from DataPropVal where id=${r.getLong("id")};
                    delete from DataProp where id in (
                        select id from dataprop
                        except
                        select dataProp as id from DataPropVal
                    );
                """)
            }
        }
    }

    private Map<String, Long> getIdFromCodOfEntity(String entity, String cod, String prefix) {
        return apiMeta().get(ApiMeta).getIdFromCodOfEntity(entity, cod, prefix)
    }

}
