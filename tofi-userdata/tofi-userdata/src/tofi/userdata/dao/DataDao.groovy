package tofi.userdata.dao

import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.UtString
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiUserData
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_AttribValType_consts
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

    @DaoMethod
    Store loadUsersGroupAll(long obj) {
        Map<String, Long> mapCods = getIdFromCodOfEntity("Prop", "", "Prop_")
        Set<Object> setCls = apiMeta().get(ApiMeta).setIdsOfCls("Typ_Users")
        if (setCls.empty) setCls.add(0L)

        //
        Store stAll = apiUserData().get(ApiUserData).infoUser(mapCods, 0, UtString.join(setCls, ","), "0")

        Store stTmp = mdb.loadQuery("""
            select v.obj
            from DataProp d, DataPropVal v
            where d.id=v.dataprop and d.prop=:Prop_UsersGroup 
                and d.objorrelobj = ${obj}
        """, mapCods)

        Set<Object> idsUser = stTmp.getUniqueValues("obj")

        //
        for (StoreRecord r : stAll) {
            if (idsUser.contains(r.getLong("id"))) {
                r.set("checked", true)
            }
        }
        //
        return stAll
    }

    private Store getUsersGroup(Set<Object> idsOwn) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_UsersGroup", "")

        String whe = UtString.join(idsOwn, ",")
        Store stTmp = mdb.loadQuery("""
            select v.id, d.objorrelobj as parent, v.obj, 0 as cls, '' as name
            from DataProp d, DataPropVal v
            where d.id=v.dataprop and d.prop=:Prop_UsersGroup 
                and d.objorrelobj in (${whe})
        """, map)

        String idsUser = UtString.join(stTmp.getUniqueValues("obj"), ",")
        if (idsUser.isEmpty()) idsUser = "0"

        Map<String, Long> mapCods = getIdFromCodOfEntity("Prop", "", "Prop_")
        Set<Object> setCls = apiMeta().get(ApiMeta).setIdsOfCls("Typ_Users")
        if (setCls.empty) setCls.add(0L)

        Store stUsers = apiUserData().get(ApiUserData).infoUser(mapCods, 0, UtString.join(setCls, ","), idsUser)
        //mdb.outTable(stUsers)
        //mdb.outTable(stTmp)
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

    @DaoMethod
    Store loadGroupUsers() {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_UsersGroup", "")
        String sql = """
            select o.id, v.name, o.cls, v.objParent as parent, o.ord, 1 as group
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


    @DaoMethod
    Map<String, Object> getCurUserInfo() {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        AuthUser au = authSvc.getCurrentUser()
        if (au == null) {
            throw new XError("NotLogined")
        }
        return au.getAttrs()
    }

    @DaoMethod
    void analizeFlatTable(String ft_code) {
        Map<String, Store> mapInfo = apiMeta().get(ApiMeta).infoFlatTable(ft_code)
        Store stFt = mapInfo.get("stFt")
        Store stProp = mapInfo.get("stProp")

        Set<Object> sAll = stProp.getUniqueValues("s")
        boolean notS = sAll.size() == 1 && sAll.contains(0L)
        Set<Object> qAll = stProp.getUniqueValues("q")
        boolean notQ = qAll.size() == 1 && qAll.contains(0L)
        Set<Object> setMetaProps = stProp.getUniqueValues("prop")

        for (StoreRecord r : stFt) {
            StringBuilder sb = new StringBuilder()

            String nm = r.getString("nametable").toLowerCase()
            Store stTmp = mdb.loadQuery("""
                            select tablename
                            from pg_catalog.pg_tables
                            where schemaname='public' and tablename like :nm
                    """, Map.of("nm", nm))
            boolean isExist = stTmp.size() > 0

            Store stDataProps = mdb.loadQuery("""
                        select column_name
                        from information_schema.columns
                        where table_name like :nm and column_name like 'p_%'
                             and column_name not like 'periodtype'
                    """, Map.of("nm", nm))

            Set<Object> setDataProps = stDataProps.getUniqueValues("column_name")

            String sql
            if (!isExist) {

                sb.append("create table ").append(nm)
                        .append("(")
                        .append("id int8 NOT NULL,")
                        .append("isobj int2 NULL,")
                        .append("\"owner\" int8 NULL,")
                if (!notS)
                    sb.append("status int8 NULL,")
                if (!notQ)
                    sb.append("provider int8 NULL,")

                sb.append("periodType int8 NULL,")
                        .append("dbeg date NULL,")
                        .append("dend date NULL,")
                //prop
                for (StoreRecord rr : stProp) {
                    long pt = rr.getLong("pt")
                    long avt = rr.getLong("avt")
                    if (rr.getLong("flattable") == r.getLong("flattable")) {
                        if (pt == FD_PropType_consts.typ || pt == FD_PropType_consts.factor || pt == FD_PropType_consts.reltyp)
                            sb.append("p_")
                                    .append(rr.getString("prop"))
                                    .append(" int8 NULL,")
                        else if (pt == FD_PropType_consts.meter || pt == FD_PropType_consts.rate)
                            sb.append("p_")
                                    .append(rr.getString("prop"))
                                    .append(" float8 NULL,")
                        else if (pt == FD_PropType_consts.attr && (
                                avt == FD_AttribValType_consts.str || avt == FD_AttribValType_consts.mask ||
                                        avt == FD_AttribValType_consts.multistr))
                            sb.append("p_")
                                    .append(rr.getString("prop"))
                                    .append(" varchar(1000) NULL,")
                        else if (pt == FD_PropType_consts.attr && (avt == FD_AttribValType_consts.dt))
                            sb.append("p_")
                                    .append(rr.getString("prop"))
                                    .append(" date NULL,")
                        else if (pt == FD_PropType_consts.attr && (avt == FD_AttribValType_consts.dttm || avt == FD_AttribValType_consts.tm))
                            sb.append("p_")
                                    .append(rr.getString("prop"))
                                    .append(" timestamp NULL,")
                        else if (pt == FD_PropType_consts.attr && avt == FD_AttribValType_consts.file)
                            sb.append("p_")
                                    .append(rr.getString("prop"))
                                    .append(" float8 NULL,")
                    }
                }
                sb.append(");")
                sql = sb.toString().replace(",);", ");")
                //
                sb = new StringBuilder()
                sb.append(sql)
                sb.append("alter table ").append(nm)
                        .append(" add constraint pk_")
                        .append(nm).append(" primary key (id);\n")
                sb.append("create sequence g_")
                        .append(nm).append(" start with 1000 increment by 1;")
                //
                //System.out.println("sql 2")
                //System.out.println(sb.toString())
                //
                try {
                    mdb.execQueryNative(sb.toString())
                } catch (Exception e) {
                    //throw new XError("Error on create flat table [" + nm + "]")
                    throw new XError(e.getMessage())
                }
            } else { //not isExist
                //deleting
                sb = new StringBuilder()
                for (StoreRecord rr : stDataProps) {
                    String fld = rr.getString("column_name")
                    long prop = UtCnv.toLong(fld.split("_")[1])
                    if (!setMetaProps.contains(prop)) {
                        sb.append("alter table ").append(nm).append(" drop column ").append(fld).append(";\n")
                    }
                }
                //Adding
                StringBuilder sb2 = new StringBuilder()
                for (StoreRecord rr : stProp) {
                    long pt = rr.getLong("pt")
                    long avt = rr.getLong("avt")
                    String prop = "p_" + rr.getString("prop")
                    if (rr.getLong("flattable") == r.getLong("flattable")) {
                        if (!setDataProps.contains(prop)) {
                            if (pt == FD_PropType_consts.typ || pt == FD_PropType_consts.factor || pt == FD_PropType_consts.reltyp)
                                sb2.append(" ADD COLUMN ").append(prop).append(" int8 NULL,")
                            else if (pt == FD_PropType_consts.meter || pt == FD_PropType_consts.rate)
                                sb2.append(" ADD COLUMN ").append(prop).append(" float8 NULL,")
                            else if (pt == FD_PropType_consts.attr && (
                                    avt == FD_AttribValType_consts.str || avt == FD_AttribValType_consts.mask ||
                                            avt == FD_AttribValType_consts.multistr))
                                sb2.append(" ADD COLUMN ").append(prop).append(" varchar(1000) NULL,")
                            else if (pt == FD_PropType_consts.attr && (avt == FD_AttribValType_consts.dt))
                                sb2.append(" ADD COLUMN ").append(prop).append(" date NULL,")
                            else if (pt == FD_PropType_consts.attr && (avt == FD_AttribValType_consts.dttm || avt == FD_AttribValType_consts.tm))
                                sb2.append(" ADD COLUMN ").append(prop).append(" timestamp NULL,")
                            else if (pt == FD_PropType_consts.attr && avt == FD_AttribValType_consts.file)
                                sb2.append(" ADD COLUMN ").append(prop).append(" float8 NULL,")
                        }
                    }
                }
                if (!sb2.toString().isEmpty()) {
                    sb2.append(";")
                    sql = "ALTER TABLE " + nm + sb2.toString().replace(",;", ";")
                    sb.append(sql)
                }
                if (!sb.toString().isEmpty()) {
                    try {
                        mdb.execQueryNative(sb.toString())
                    } catch (Exception e) {
                        throw new XError(e.getMessage())
                    }
                }
            }
        }
    } //stFt

    @DaoMethod
    Map<String, Object> existsProfile(long userId) {
        Map<String, Long> map = getIdFromCodOfEntity("Prop", "", "Prop_")
        long pu = map.get("Prop_UserId", 0L) // 1046 //Prop_UserId
        long ps = map.get("Prop_UserSecondName", 0L)
        long pf = map.get("Prop_UserFirstName", 0L)
        long pm = map.get("Prop_UserMiddleName", 0L)

        String sql = """
            select o.own, obj.cls, o.UserId,
                v1.strval as sn, 
                v2.strval as fn, 
                v3.strval as mn 
            from (
                    select d.objorrelobj as own, v.strVal as userId 
                    from DataProp d
                        left join DataPropVal v on d.id=v.dataprop 
                    where d.isobj=1 and d.prop=:pu and v.authuser=:ui
                 ) o
                    left join Obj on obj.id=o.own
                    left join DataProp d1 on d1.isobj=1 and d1.objorrelobj=o.own and d1.prop=:ps
                    left join DataPropVal v1 on d1.id=v1.dataprop 
                    left join DataProp d2 on d2.isobj=1 and d2.objorrelobj=o.own and d2.prop=:pf
                    left join DataPropVal v2 on d2.id=v2.dataprop 
                    left join DataProp d3 on d3.isobj=1 and d3.objorrelobj=o.own and d3.prop=:pm
                    left join DataPropVal v3 on d3.id=v3.dataprop
        """
        Store st = mdb.loadQuery(sql, [pu: pu, ui: userId, ps: ps, pf: pf, pm: pm])
        Map<String, Object> res = new HashMap<String, Object>()
        if (st.size() > 0) {
            res.put("exists", true) as Map<String, Object>
            String mn = st.get(0).getString("mn").isEmpty() ? "" : " " + st.get(0).getString("mn")
            String nm = st.get(0).getString("sn") + " " + st.get(0).getString("fn") + mn
            res.put("name", nm) as Map<String, Object>
        } else {
            res.put("exists", false) as Map<String, Object>
            res.put("name", "") as Map<String, Object>
        }
        return res
    }

    @DaoMethod
    Store loadProfile(long userId) {
        Map<String, Long> mapIdFromCod = getIdFromCodOfEntity("Prop", "", "Prop_")
        mapIdFromCod.put("UserId", userId)
        Store st = mdb.createStore("Account")
        String sql = """
            select o.own, obj.cls, o.authUser,
                coalesce (v1.id, 0) as UserSecondNameId, v1.strval as UserSecondName, 
                coalesce (v2.id, 0) as UserFirstNameId, v2.strval as UserFirstName, 
                coalesce (v3.id, 0) as UserMiddleNameId, v3.strval as UserMiddleName, 
                coalesce (v4.id, 0) as UserDateBirthId, v4.datetimeVal as UserDateBirth, 
                coalesce (v5.id, 0) as UserEmailId, v5.strval as UserEmail,
                coalesce (v6.id, 0) as UserPhoneId, v6.strval as UserPhone,
                coalesce (v7.id, 0) as UserSexId, coalesce (v7.propval, 0) as UserSexPropval, 0 as UserSex            
            from (
                    select d.objorrelobj as own, v.authuser 
                    from DataProp d
                        left join DataPropVal v on d.id=v.dataprop 
                    where d.isobj=1 and d.prop=:Prop_UserId and v.strval=:UserId
                 ) o
                    left join Obj on obj.id=o.own
                    left join DataProp d1 on d1.isobj=1 and d1.objorrelobj=o.own and d1.prop=:Prop_UserSecondName
                    left join DataPropVal v1 on d1.id=v1.dataprop 
                    left join DataProp d2 on d2.isobj=1 and d2.objorrelobj=o.own and d2.prop=:Prop_UserFirstName
                    left join DataPropVal v2 on d2.id=v2.dataprop 
                    left join DataProp d3 on d3.isobj=1 and d3.objorrelobj=o.own and d3.prop=:Prop_UserMiddleName
                    left join DataPropVal v3 on d3.id=v3.dataprop
                    left join DataProp d4 on d4.isobj=1 and d4.objorrelobj=o.own and d4.prop=:Prop_UserDateBirth
                    left join DataPropVal v4 on d4.id=v4.dataprop
                    left join DataProp d5 on d5.isobj=1 and d5.objorrelobj=o.own and d5.prop=:Prop_UserEmail
                    left join DataPropVal v5 on d5.id=v5.dataprop
                    left join DataProp d6 on d6.isobj=1 and d6.objorrelobj=o.own and d6.prop=:Prop_UserPhone
                    left join DataPropVal v6 on d6.id=v6.dataprop
                    left join DataProp d7 on d7.isobj=1 and d7.objorrelobj=o.own and d7.prop=:Prop_UserSex
                    left join DataPropVal v7 on d7.id=v7.dataprop
        """

        mdb.loadQuery(st, sql, mapIdFromCod)
        if (st.size() == 0) {
            throw new XError("NotCreatedProfile")
        }

        long fv = getFactorValFromPropVal(st.get(0).getLong("UserSexPropval"), "Prop_UserSex")
        st.get(0).set("UserSex", fv)
        //
        return st
    }

    @DaoMethod
    Store loadUsers(String codTyp) {
        Set<Object> idsCls = apiMeta().get(ApiMeta).setIdsOfCls(codTyp)
        return mdb.loadQuery("""
            select o.id, v.name, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.cls in (0${idsCls.join(",")})
        """)
    }

    @DaoMethod
    void deleteOwnerWithProperties(long id, int isObj) {
        String tableName = isObj == 1 ? "Obj" : "RelObj"
        //
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

    private Map<String, Long> getIdFromCodOfEntity(String entity, String cod, String prefix) {
        return apiMeta().get(ApiMeta).getIdFromCodOfEntity(entity, cod, prefix)
    }

    private long getFactorValFromPropVal(long propVal, String codProp) {
        Map<Long, Long> mapPV_Entity = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", codProp, true)
        return mapPV_Entity.get(propVal)
    }


}
