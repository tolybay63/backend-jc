package tofi.api.mdl.impl

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.dbm.dict.DictService
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.api.mdl.utils.ClsTreeUtils
import tofi.api.mdl.utils.UtPeriod
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService

class ApiMetaImpl extends BaseMdbUtils implements ApiMeta {

    @Override
    Map<Long, String> loadDict(String dictName) {
        long al = getAccessLevel()
        String wheAL = ""
        if (dictName.equalsIgnoreCase("FD_AccessLevel"))
            wheAL = "and id <= ${al}"

        Store st = mdb.loadQuery("""
            select * from ${dictName} where vis=1 ${wheAL} order by ord
        """)
        Map<Long, String> map = [:]
        for (StoreRecord r in st) {
            map.put(r.getLong("id"), r.getString("text"))
        }
        //
        return map
    }

    @Override
    Store loadDictAsStore(String dictName) {
        long al = getAccessLevel()
        String wheAL = ""
        if (dictName.equalsIgnoreCase("FD_AccessLevel"))
            wheAL = "and id <= ${al}"
        return mdb.loadQuery("""
            select * from ${dictName} where vis=1 ${wheAL} order by ord
        """)
    }

    @Override
    Store loadClsTree(Map<String, Object> params) {
        long typ = UtCnv.toLong(params.get("typ"))
        if (typ == 0) {
            String typCod = UtCnv.toString(params.get("typCod"))
            Store st = mdb.loadQuery("select id from typ where lower(cod) like :c",
                    Map.of("c", typCod.toLowerCase()));
            if (st.size() == 0)
                throw new XError("NotFoundCod@${typCod}");
            typ = st.get(0).getLong("id");
            params.put("typ", typ);
        }

        boolean typNodeVisible = true;
        long typId = UtCnv.toLong(params.get("typ"));
        if (params.get("typNodeVisible") != null)
            typNodeVisible = UtCnv.toBoolean( params.get("typNodeVisible"));

        Store dsTyp = loadFltTyp(params);
        Store dsResult = mdb.createStore("ClsTree");
        Store dsRes = mdb.createStore("ClsTree");

        if (typId > 0) {
            if (typNodeVisible) {
                dsResult.add(dsTyp);
            }
        } else {
            dsResult.add(dsTyp);
        }

        //Store dsCls
        //Store dsClsPrt
        Map<String, Object> param = new HashMap<>();
        for (StoreRecord rTyp : dsTyp) {
            param.put("own", 1L);
            param.put("typ", rTyp.get("ent"));

            Store dsCls = loadFltClsCpy(param);
            dsResult.add(dsCls);
            dsCls.clear();

            if (rTyp.getLong("typParent") > 0) {
                param.put("own", 0);
                param.put("typ", rTyp.get("ent"));
                Store dsClsPrt = loadFltClsCpy(param);
                dsResult.add(dsClsPrt);
                dsClsPrt.clear();
            }

            ClsTreeUtils clsTreeUtils = new ClsTreeUtils(mdb);
            dsResult = clsTreeUtils.MakeTreeCls(rTyp, dsResult, typNodeVisible);
            dsRes.add(dsResult);
        }
        //
        var ids = dsRes.getUniqueValues("id");

        for (StoreRecord r: dsRes) {
            if (!ids.contains(r.getString("parent"))) {
                r.set("parent", null);
            }
        }
        //mdb.outTable(dsRes)
        return dsRes;
    }
    //
    private Store loadFltTyp(Map<String, Object> params) {
        long al = getAccessLevel()

        String sql = """
            select * from
            (
              select c.id as ent,0 as typ,v.name, v.fullName,
                    c.accessLevel, c.cod, 0 as kind, -1 as isOwn, c.parent as typParent,
                    't_'||c.id as id, null as parent, c.isOpenness, c.cmt
              from Typ c, TypVer v
              where c.id=v.ownerVer and v.lastVer=1 and c.accessLevel <= ${al}
            ) t
            /**/where 0=0
            order by cod
        """;

        long typId = UtCnv.toLong(params.get("typ"));
        if (typId > 0) {
            sql = """
                        select * from
                        (
                          select c.id as ent,0 as typ,v.name, v.fullName,
                                c.accessLevel, c.cod, 0 as kind, -1 as isOwn, c.parent as typParent,
                                't_'||c.id as id, null as parent, c.isOpenness, c.cmt
                          from Typ c, TypVer v
                          where c.id=v.ownerVer and v.lastVer=1  and c.id=:typId and c.accessLevel <= ${al}
                        ) t
                        /**/where 0=0
                        order by cod
                    """;
        }

        Store st = mdb.createStore("ClsTree");
        mdb.loadQuery(st, sql, Map.of("typId", typId));
        return st;
    }

    private Store loadFltClsCpy(Map<String, Object> params) {
        ClsTreeUtils clsTreeUtils = new ClsTreeUtils(mdb);
        return clsTreeUtils.loadFltCls(params);
    }

    private long getAccessLevel() throws Exception {
        long al = mdb.getApp().bean(AuthService.class)
                .getCurrentUser()
                .getAttrs()
                .getLong("accesslevel")
        if (al == 0)
            throw new XError("notLogined")
        return al
    }

    @Override
    Map<String, Long> getIdFromCodOfEntity(String entity, String cod, String prefixcod) {
        String sql = "select id, cod from ${entity} where cod like '${cod}'"
        if (!prefixcod.isEmpty())
            sql = "select id, cod from ${entity} where cod like '${prefixcod}%'"
        Store st = mdb.loadQuery(sql)
        if (st.size()==0) {
            String cd = cod.isEmpty() ? prefixcod : cod
            throw new XError("NotFoundCod@${cd}")
        }
        Map<String, Long> map = new HashMap<>()
        for (StoreRecord r in st) {
            map.put(r.getString("cod"), r.getLong("id"))
        }
        return map
    }

    @Override
    Map<String, Long> getIdFromCodOfProp(long cls) {
        Store st = mdb.loadQuery("""
            select p.id, p.cod
            from typchargr t
                left join typchargrprop t2 on t.id=t2.typchargr
                left join prop p on t2.prop=p.id
                inner join Cls c on t.typ=c.typ 
            where c.id=${cls}
        """)
        Map<String, Long> map = new HashMap<>()
        for (StoreRecord r in st) {
            map.put(r.getString("cod"), r.getLong("id"))
        }
        return map
    }

    @Override
    Set<Object> setIdsOfCls(String codTyp) {
        long al = getAccessLevel()
        Store st = mdb.loadQuery("""
            select c.id from Cls c, Typ t
            where c.typ=t.id and t.cod like :cod and c.accessLevel <= :al
        """, [cod: codTyp, al: al])
        return st.getUniqueValues("id")
    }

    @Override
    Set<Object> setIdsOfRelCls(String codRelTyp) {
        long al = getAccessLevel()
        Store st = mdb.loadQuery("""
            select c.id from RelCls c, RelTyp t
            where c.reltyp=t.id and t.cod like :cod and c.accessLevel <= :al
        """, [cod: codRelTyp, al: al])
        return st.getUniqueValues("id")
    }

    @Override
    Map<Long, Long> mapEntityIdFromPV(String entity, boolean keyIsPropVal) {
        Map<Long, Long> res = [:]

        Store st = mdb.loadQuery("""
                select id, ${entity} from PropVal where ${entity} is not null
            """)
        for (StoreRecord r in st) {
            if (keyIsPropVal)
                res.put(r.getLong("id"), r.getLong(entity))
            else
                res.put(r.getLong(entity), r.getLong("id"))
        }
        return res
    }

    @Override
    Map<Long, String> mapPropValArrFromCls(String clsORrelcls) {
        Map<Long, String> res = [:]
        Store st = mdb.loadQuery("""
            select ${clsORrelcls}, string_agg (cast(id as varchar(3000)), ',' order by id) as lst
            from PropVal 
            where ${clsORrelcls} is not null
            group by ${clsORrelcls}
        """)
        for (StoreRecord r in st) {
            res.put(r.getLong(clsORrelcls), r.getString("lst"))
        }

        return res
    }

    @Override
    Map<Long, String> mapFvNameFromId() {
        Map<Long, String> res = [:]
        Store st = mdb.loadQuery("""
            select pv.factorVal, f.name from PropVal pv, Factor f 
            where pv.factorVal=f.id
        """)
        for (StoreRecord r in st) {
            res.put(r.getLong("factorVal"), r.getString("name"))
        }
        return res
    }

    @Override
    Store storePropValForSelectFV(String codProp) {
        Store st = mdb.loadQuery("""
            select f.id as fv, pv.factorval as id, f.name, pv.id as propval
            from propval pv
                left join Prop p on pv.prop=p.id
                left join Factor f on pv.factorval=f.id
            where p.cod like '${codProp}'
        """)
        if (st.size() == 0)
            throw new XError("NotFoundPossibleValues@${codProp}");
        return st
    }

    @Override
    Store storeFVfromPropVal() {
        Store st = mdb.loadQuery("""
            select pv.factorval, pv.id as propval
            from propval pv
                left join Prop p on pv.prop=p.id
            where p.propType=${FD_PropType_consts.factor}
        """)
        return st
    }


    @Override
    long idPV(String entity, long idEntity, String codProp) {
        Store st = mdb.loadQuery("""
            select pv.id from PropVal pv, Prop p
            where pv.prop=p.id and pv.${entity}=${idEntity} and p.cod like '${codProp}'
        """)
        if (st.size() == 0)
            throw new XError("NotFoundPossibleValues@${codProp}");
        else
            return st.get(0).getLong("id")
    }

    @Override
    Store getPvFromCls(Set<Object> idsCls, String codProp) {
        return mdb.loadQuery("""
            select pv.cls, pv.id as propVal from PropVal pv, Prop p
            where pv.prop=p.id and pv.cls in (0${idsCls.join(",")}) and p.cod like '${codProp}'
        """)
    }

    @Override
    Store getPropInfo(String codProp) {
        Store stProp = mdb.createStore("Prop.cust")
        mdb.loadQuery(stProp, """
                select p.id, p.cod, p.proptype, a.attribvaltype, p.isuniq, p.isdependvalueonperiod as dependperiod,
                    p.statusfactor, p.providertyp, m.kfrombase as koef, p.digit  
                from Prop p
                    left join Attrib a on a.id=p.attrib
                    left join Measure m on m.id=p.measure
                where p.cod like :c         
            """, [c: codProp])
        if (stProp.size() == 0) {
            throw new XError("NotFoundPropCod@" + codProp)
        }
        return stProp
    }

    @Override
    Store getStoreFromFK(String tableName, String fkField, long fkValue, String ordField) {
        return mdb.loadQuery("""
            select * from ${tableName} where ${fkField}=${fkValue} order by ${ordField}
        """)
    }

    @Override
    Store getClsFromTypCharGr(long typId, boolean isMultiProp) {
        String whe = "t2.multiprop is null"
        if (isMultiProp)
            whe = "t2.multiprop is not null"
        return mdb.loadQuery("""
            select distinct cf.cls 
            from typchargr t
                inner join typchargrprop t2 on t.typ=${typId} and ${whe}
                inner join clsfactorval cf on cf.factorval=t.factorval
                    and cf.cls in (select id from cls where typ=${typId})
            where t.id=t2.typchargr and t.typ=${typId}
        """)
    }

    @Override
    long getFKFromTable(String tableName, long idTable, String fkField) {
        return mdb.loadQuery("""
            select ${fkField} from ${tableName} where id=${idTable}
        """).get(0).getLong(fkField)
    }

    @Override
    long getDefaultStatus(long prop) {
        Store st = mdb.loadQuery("""
            select factorVal from PropStatus where prop=${prop} and isDefault is true
        """)
        if (st.size() == 0)
            throw new XError("Not found default status")

        return st.get(0).getLong("factorVal")
    }

    @Override
    Store recEntity(String entity, long id) {
        return mdb.loadQuery("""
            select * from ${entity} t, ${entity}Ver v where t.id=v.ownerVer and v.lastVer=1 and t.id=${id}
        """)
    }

    @Override
    Store loadCls(String codTyp) {
        return mdb.loadQuery("""
            select c.id, c.cod, v.name, c.accessLevel
            from Cls c, ClsVer v, Typ t 
            where c.id=v.ownerver and t.id=c.typ and t.cod like '${codTyp}'
            order by c.id
        """)
    }

    @Override
    Store loadFactorVals(String codFactor) {
        Store st = mdb.createStore("Factor.select")
        mdb.loadQuery(st,"""
            select fv.*
            from Factor f
                left join Factor fv on f.id=fv.parent
            where f.cod like '${codFactor}'
            order by fv.ord
        """)
        if (st.size()==0) {
            if (st.size() == 0)
                throw new XError("NotFoundCod@${codFactor}");
        }
        return st
    }

    @Override
    Map<String, Store> infoFlatTable(String ft_cod) {
        Store stFt = mdb.loadQuery("""
                    select distinct p.flattable, f.nametable
                    from typchargr t
                        left join typchargrprop p on t.id=p.typchargr
                        left join flattable f on p.flattable=f.id
                        inner join (
                            select distinct t.id
                            from cls c
                                left join "database" d on d.id=c."database"
                                left join Typ t on t.id=c.typ
                                where d.cod like :ft_cod
                            ) a on t.typ=a.id
                    where p.flattable is not null
                    union all
                    select distinct p.flattable, f.nametable
                    from reltypchargr t
                        left join reltypchargrprop p on t.id=p.reltypchargr
                        left join flattable f on p.flattable=f.id
                        inner join (
                            select distinct c.id
                            from relcls c
                                left join "database" d on d.id=c."database"
                                left join RelTyp t on t.id=c.reltyp
                                where d.cod like :ft_cod
                            ) a on t.relcls=a.id
                    where p.flattable is not null
                """, Map.of("ft_cod", ft_cod))

        Store stProp = mdb.loadQuery("""
                    select * from (
                    select distinct p.flattable, p.prop, pp.proptype as pt,
                    coalesce (a.attribvaltype,0) as avt, coalesce (pp.statusfactor,0) as s, coalesce (pp.providertyp,0) as q
                    from typchargr t
                        left join typchargrprop p on t.id=p.typchargr
                        left join prop pp on pp.id=p.prop
                        left join attrib a on pp.attrib=a.id
                        inner join (
                            select distinct t.id
                            from cls c
                                left join "database" d on d.id=c."database"
                                left join Typ t on t.id=c.typ
                                where d.cod like :ft_cod
                            ) aa on t.typ=aa.id
                    where p.flattable is not null
                    union all
                    select distinct p.flattable, p.prop, pp.proptype as pt,
                    coalesce (a.attribvaltype,0) as avt, coalesce (pp.statusfactor,0) as s, coalesce (pp.providertyp,0) as q
                    from reltypchargr t
                        left join reltypchargrprop p on t.id=p.reltypchargr
                        left join prop pp on pp.id=p.prop
                        left join attrib a on pp.attrib=a.id
                        inner join (
                            select distinct c.id
                            from relcls c
                                left join "database" d on d.id=c."database"
                                left join RelTyp t on t.id=c.reltyp
                                where d.cod like :ft_cod
                            ) aa on t.relcls=aa.id
                    where p.flattable is not null
                    ) tt
                    order by flattable, prop
                """, Map.of("ft_cod", ft_cod))
        Map<String, Store> mapRez = new HashMap<>()
        mapRez.put("stFt", stFt)
        mapRez.put("stProp", stProp)
        return mapRez
    }

    //todo Удалить после проверки (есть loadCls)
    @Override
    Store loadClsForSelect(String codTyp) {
        return mdb.loadQuery("""
            select c.id, v.name
            from Cls c, ClsVer v, Typ t
            where c.id=v.ownerver and c.typ=t.id and t.cod like '${codTyp}'
            order by c.ord
        """)
    }

    @Override
    Map<String, Object> measureInfo() {
        Store st = mdb.loadQuery("""
            select p.id, p.cod, m.name, m.kfrombase as kfc, p.digit 
            from prop p
                left join Measure m on m.id=p.measure
            where p.proptype=${FD_PropType_consts.meter} and p.cod like 'Prop_%'
        """)
        Map<String, Object> mapRes = new HashMap<>()
        for (StoreRecord r in st) {
            Map<String, Object> map = new HashMap<>()
            map.put("name", r.getString("name"))
            map.put("kfc", r.getDouble("kfc"))
            if (!r.isValueNull("digit"))
                map.put("digit", r.getInt("digit"))
            mapRes.put(r.getString("cod"), map)
        }
        return mapRes
    }

    @Override
    Map<Long, String> mapFVforSelect(String codFactor) {
        Store st = mdb.loadQuery("""
            select fv.id, fv.name
            from Factor fv
                left join Factor f on fv.parent=f.id and f.cod like '${codFactor}'
            where fv.parent is not null
        """)
        Map<Long, String> mapRes = new HashMap<>()
        for (StoreRecord r in st) {
            mapRes.put(r.getLong("id"), r.getString("name"))
        }
        return mapRes
    }

    @Override
    Map<String, Long> getIdsFromCodOfEntity(String Entity, String cods) {
        Store st = mdb.loadQuery("""
            select id, cod from ${Entity} where cod in (${cods})
        """)
        if (st.size()==0)
            throw new XError("NotFoundCod@${cods}")
        Map<String, Long> map = new HashMap<>()
        for(StoreRecord r in st) {
            map.put(r.getString("cod"), r.getLong("id"))
        }
        return map
    }

    @Override
    Store loadSql(String sql, String domain) {
        if (domain=="")
            return mdb.loadQuery(sql)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql)
        }
    }

    @Override
    Store loadSqlWithParams(String sql, String domain, Map<String, Object> params) {
        if (domain=="")
            return mdb.loadQuery(sql, params)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql, params)
        }
    }

    @Override
    Map<String, Object> getPeriodInfo(XDate dt, long periodType) {
        if (dt == null)
            dt = XDate.today()

        UtPeriod utPeriod = new UtPeriod()
        String d1 = utPeriod.calcDbeg(dt, periodType, 0).toString(XDateTimeFormatter.ISO_DATE)
        String d2 = utPeriod.calcDend(dt, periodType, 0).toString(XDateTimeFormatter.ISO_DATE)
        DictService dc = mdb.getModel().bean(DictService.class)
        Store dd = dc.loadDictData("FD_PeriodType").getData()
        Map<Long, String> mapPeriod = new HashMap<>()
        dd.forEach(r -> {
            mapPeriod.put(r.getLong("id"), r.getString("text"))
        });
        return Map.of("dbeg", d1, "dend", d2, "periodTypeName", mapPeriod.get(periodType))
    }

    @Override
    DbFileStorageService getDbFileStorageService() {
        return mdb.getModel().bean(DbFileStorageService.class)
    }

    @Override
    double getKfromBase(long propId) {
        return mdb.loadQuery("select m.kFromBase from Prop p, Measure m where p.measure=m.id and p.id=:p",
        [p: propId]).get(0).getDouble("kFromBase")
    }

    @Override
    String ListClsParents(long typ, long cls) {
        ClsTreeUtils cu = new ClsTreeUtils(mdb)
        return cu.ListClsParents(typ, cls)
    }

}
