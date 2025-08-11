package tofi.api.mdl.impl

import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.ApiMetaKPI
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.api.mdl.utils.EntityMdbUtils
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiMetaKPIImpl extends BaseMdbUtils implements ApiMetaKPI {
    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    @Override
    Store loadFactorVals(long factor) {
        Store st = mdb.createStore("Factor")
        mdb.loadQuery(st,"""
            select *
            from Factor
            where parent = '${factor}'
            order by ord
        """)
        return st
    }

    @Override
    Store insertFactorVal(Map<String, Object> rec) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Factor")
        long id = eu.insertEntity(rec)
        //add to PropVal
        long fac = UtCnv.toLong(rec.get("parent"))
        if (fac > 0) {
            Store rTmp = mdb.loadQuery("select id, allItem from Prop where factor=:f and proptype=:pt",
                    Map.of("f", fac, "pt", FD_PropType_consts.factor))
            if (rTmp.size() > 0) {
                if (rTmp.get(0).getBoolean("allItem")) {
                    long prop = rTmp.get(0).getLong("id")
                    mdb.insertRec("PropVal", Map.of("prop", prop, "factorVal", id), true)
                }
            }
        }
        //

        Store st = mdb.createStore("Factor")
        mdb.loadQuery(st, "select * from factor where id=:id", Map.of("id", id))
        return st
    }

    @Override
    Store updateFactorVal(Map<String, Object> rec) {
        long id = UtCnv.toLong(rec.get("id"))
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение")
        }
        //
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Factor")
        eu.updateEntity(rec)
        //
        // Загрузка записи
        Store st = mdb.createStore("Factor")
        mdb.loadQuery(st, "select * from Factor where id=:id", Map.of("id", id))
        return st
    }

    @Override
    void deleteFactorVal(Map<String, Object> rec) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Factor")
        eu.deleteEntity(rec)
    }

    @Override
    void changeOrdFV(Map<String, Object> params) {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        AuthUser au = authSvc.getCurrentUser()
        long al = au.getAttrs().getLong("accesslevel")

        Map<String, Object> rec = UtCnv.toMap(params.get("rec"))
        boolean up = UtCnv.toBoolean(params.get("up"))
        long factor = UtCnv.toLong(rec.get("parent"))
        long id1 = UtCnv.toLong(rec.get("id"))
        long ord1 = UtCnv.toLong(rec.get("ord"))
        long id2, ord2

        Store st = mdb.loadQuery("""
                    select * from Factor where parent=:factor and accessLevel <= :al order by ord
                """, Map.of("factor", factor, "al", al))
        int k = 0;  //искомая позиция
        for (int i = 0; i < st.size(); i++) {
            if (st.get(i).getLong("id") == id1) {
                k = i
                break
            }
        }
        if (up) {
            id2 = st.get(k - 1).getLong("id")
            ord2 = st.get(k - 1).getLong("ord")
        } else {
            id2 = st.get(k + 1).getLong("id")
            ord2 = st.get(k + 1).getLong("ord")
        }
        //
        mdb.execQuery("""
                    update Factor set ord=:ord2 where id=:id1;
                    update Factor set ord=:ord1 where id=:id2;
                """, Map.of("id1", id1, "id2", id2, "ord1", ord1, "ord2", ord2))
    }

    @Override
    StoreRecord propAndMeasureInfo(String codProp) {
        return mdb.loadQuery("""
            select p.id, p.cod, p.name, p.proptype, p.statusfactor, p.providertyp, p.isdependvalueonperiod,
                p.measure, p.digit, m.name as measureName, m.kfrombase as koef 
            from Prop p
                left join Measure m on m.id=p.measure
            where p.cod like '${codProp}'
        """).get(0)
    }

    //todo Анализировать!
    @Override
    Map<String, Object> idClsAndMeasureName(String CodCls, String codProp) {
        Store stTmp = mdb.loadQuery("select id from Cls where cod like '${CodCls}'")
        if (stTmp.size()==0)
            throw new XError("NotFoundCod@${CodCls}")
        stTmp = mdb.loadQuery("select id from Prop where cod like '${codProp}'")
        if (stTmp.size()==0)
            throw new XError("NotFoundCod@${codProp}")

        return mdb.loadQuery("""
            select c.id, m.name as measureName
            from typchargr t
                left join typchargrprop t2 on t.id=t2.typchargr
                left join prop p on t2.prop=p.id
                left join Measure m on m.id=p.measure
                inner join Cls c on t.typ=c.typ 
            where c.cod like '${CodCls}' and p.cod like '${codProp}'
        """).get(0).values
    }

    @Override
    Map<String, Object> idRelClsAndMeasureInfo(String codRelCls) {
        Store stTmp = mdb.loadQuery("select id from RelCls where cod like '${codRelCls}'")
        if (stTmp.size()==0)
            throw new XError("NotFoundCod@${codRelCls}")

        Store st = mdb.loadQuery("""
            select p.cod, c.id, m.name, p.minval, p.maxval, p.digit
            from reltypchargr t
                left join reltypchargrprop t2 on t.id=t2.reltypchargr
                left join prop p on t2.prop=p.id
                left join Measure m on m.id=p.measure
                inner join RelCls c on t.relcls=c.id 
            where c.cod like '${codRelCls}' and p.proptype = ${FD_PropType_consts.meter}
        """)
        if (st.size()==0)
            throw new XError("Не найден свойство [Измеритель]")

        Map<String, Object> mapRes = new HashMap<>()
        Map<String, Object> map1 = new HashMap<>()
        mapRes.put("relcls", st.get(0).getLong("id"))
        for (StoreRecord r in st) {
            Map<String, Object> map = new HashMap<>()
            map.put("name", r.getString("name"))
            if (!r.isValueNull("minval"))
                map.put("minval", r.getDouble("minval"))
            if (!r.isValueNull("maxval"))
                map.put("maxval", r.getDouble("maxval"))
            if (!r.isValueNull("digit"))
                map.put("digit", r.getInt("digit"))
            map1.put(r.getString("cod"), map)
        }
        mapRes.putAll(map1)
        return mapRes
    }

    @Override
    Store loadClsForSelect(String codTyp, String codCls) {
        return mdb.loadQuery("""
            select c.id, v.name
            from Cls c, ClsVer v, Typ t
            where c.id=v.ownerver and c.typ=t.id and t.cod like '${codTyp}' and c.cod not like '${codCls}'
            order by c.ord
        """)
    }

    @Override
    Set<Object> setClsWithoutCurCodCls(String codCls) {
        Store st = mdb.loadQuery("""
            select id 
            from cls
            where typ in (
                select typ from Cls where cod like '${codCls}'
                ) and cod <> '${codCls}'
        """)
        Set<Object> setRes =  st.getUniqueValues("id")
        if (setRes.isEmpty()) setRes.add(0L)
        //
        return setRes
    }

    @Override
    Store loadRelClsMember(long relcls) {
        return mdb.loadQuery("""
            select * from relclsmember where relcls=${relcls} order by id
        """)
    }

    @Override
    Map<String, Long> getIdsPropFromRTCG(String codRelCls) {
        Map<String, Long> mapRC = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", codRelCls, "")
        Store st = mdb.loadQuery("""
            select p.id, p.cod  
            from typchargr t
                left join typchargrprop t2 on t.id=t2.typchargr
                left join prop p on t2.prop=p.id
                inner join Cls c on t.typ=c.typ
            where c.id=${mapRC.get(codRelCls)}
        """)
        Map<String, Long> mapRes = new HashMap<>()
        if (st.size()==0)
            throw new XError("Харгруппа пустая для [${codRelCls}]")
        for (StoreRecord r in st) {
            if (!["Prop_DateOfCreation","Prop_DateStart","Prop_DateCompletion","Prop_Detail",
                  "Prop_Operation","Prop_Executor","Prop_Intern","Prop_TimeSpentActual","Prop_Order"]
                    .contains(r.getString("cod"))) {
                throw new XError("NotFoundCod@${r.getString("cod")}")
            }
            mapRes.put(r.getString("cod"), r.getLong("id"))
        }
        return mapRes
    }
}
