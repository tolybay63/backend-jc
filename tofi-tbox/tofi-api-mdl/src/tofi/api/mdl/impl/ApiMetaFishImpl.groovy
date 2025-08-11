package tofi.api.mdl.impl


import jandcode.commons.error.XError
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.ApiMetaFish
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiMetaFishImpl extends BaseMdbUtils implements ApiMetaFish {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    @Override
    Map<String, Object> getMeasureInfo(String codRelTyp, boolean isRel) {
        Store stTmp
        if (isRel)
            stTmp = mdb.loadQuery("select id from RelTyp where cod like '${codRelTyp}'")
        else
            stTmp = mdb.loadQuery("select id from Typ where cod like '${codRelTyp}'")

        if (stTmp.size()==0)
            throw new XError("NotFoundCod@${codRelTyp}")
        //
        String sql
        if (isRel)
            sql = """
                select p.cod, c.id, m.name, p.minval, p.maxval, p.digit
                from reltypchargr t
                    left join reltypchargrprop t2 on t.id=t2.reltypchargr
                    left join prop p on t2.prop=p.id
                    left join Measure m on m.id=p.measure
                    inner join RelCls c on t.relcls=c.id 
                where p.proptype = ${FD_PropType_consts.meter} and c.id in (
                    select id from relcls where reltyp in (select id from reltyp where cod like '${codRelTyp}')
                )
            """
        else
            sql = """
                select p.cod, c.id, m.name, p.minval, p.maxval, p.digit
                from typchargr t
                    left join typchargrprop t2 on t.id=t2.typchargr
                    left join prop p on t2.prop=p.id
                    left join Measure m on m.id=p.measure
                    left join Typ t3 on t3.id=t.typ
                where p.proptype = ${FD_PropType_consts.meter} and t3.cod like '${codRelTyp}'
            """
        Store st = mdb.loadQuery(sql)
        if (st.size()==0)
            throw new XError("Не найден свойство [Измеритель]")

        Map<String, Object> mapRes = new HashMap<>()
        Map<String, Object> map1 = new HashMap<>()

        st.forEach {StoreRecord r ->
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
    long idRelCls(long cls1, long cls2) {
        Store st = mdb.loadQuery("""
            select r.relcls from relclsmember r
                left join relclsmember r2 on r.relcls=r2.relcls and r2.cls=${cls2}
            where r.membertype=3 and r.cls=${cls1}
            order by r2.id
        """)
        if (st.size()==0)
            throw new XError("Not Found relcl[${cls1},${cls2}]")

        return st.get(0).getLong("relcls")
    }

    @Override
    Store loadRelClsMember(long relcls) {
        return mdb.loadQuery("""
            select * from relclsmember where relcls=${relcls} order by id
        """)
    }

    @Override
    Set<Object> idsChildClses(String codTyp) {
        Store st = mdb.loadQuery("""
            select cls
            from clsfactorval
            where cls in (select c.id from Cls c, Typ t where c.typ=t.id and t.cod like 'Typ_WaterBodies')
            group by cls
            having count(*) > 1
        """)
        Set<Object> ids = st.getUniqueValues("cls")
        if (ids.isEmpty()) ids.add(0L)
        return ids
    }

    @Override
    Store loadChildClsForSelect(String codTyp) {
        String ids = idsChildClses(codTyp).join(",")
        return mdb.loadQuery("""
            select c.id, v.name from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.id in (${ids}) 
            order by v.name
        """)
    }

    @Override
    Store loadFvForSelect(String codFactor) {
        Store st = mdb.loadQuery("""
            select fv.id, fv.name, 0 as pv
            from Factor fv
                inner join Factor f on fv.parent=f.id
            where f.cod like '${codFactor}'
            order by fv.ord
        """)

        if (st.size()==0)
            throw new XError("NotFoundCod@${codFactor}")

        Map<Long, Long> map = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", false)

        for (StoreRecord rec in st) {
            rec.set("pv", map.get(rec.getLong("id")))
        }
        return st
    }
}
