package tofi.mdl.model.dao.typ

import jandcode.commons.UtCnv
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.mdl.model.utils.UtEntityTranslate

class ClsTreeMdbUtils {
    Mdb mdb;

    public ClsTreeMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb
    }

/*
*  Возвращает дерево-классов с учетом наследования
*
*  1. loadClsTree([typ:0]) - Все типы со своими классами
*  2. loadClsTree([typ:5000]) - Тип (id=5000) и все его классы
*  3. loadClsTree([typ:5000, typNodeVisible:false]) - Все классы типа с id=5000, без родителя
*
* */

    Store loadClsTree(Map<String, Object> params) throws Exception {
        boolean typNodeVisible = true
        long typId = UtCnv.toLong(params["typ"])
        String lang = UtCnv.toString(params.get("lang"))
        //
        Store stCls = mdb.createStore("Cls.lang")
        mdb.loadQuery(stCls, """
            select *, v.id as verId
            from Cls c, ClsVer v--, TableLang l
            where c.id=v.ownerVer and v.lastVer=1 and c.typ=:t
                --and l.nameTable='ClsVer' and l.idTable=v.id and l.lang=:lang
        """, [t: typId, lang: lang])
        UtEntityTranslate ut = new UtEntityTranslate(mdb)
        ut.getTranslatedStore(stCls, "Cls", UtCnv.toString(params.get("lang")), true)
        //

        if (params["typNodeVisible"] != null)
            typNodeVisible = params["typNodeVisible"]

        Store dsTyp = loadFltTyp(params)
        Store dsResult = mdb.createStore("ClsTree")
        Store dsRes = mdb.createStore("ClsTree")

        if (typId > 0) {
            if (typNodeVisible) {
                dsResult.add(dsTyp)
            }
        } else {
            dsResult.add(dsTyp)
        }

        //Store dsCls
        //Store dsClsPrt
        Map<String, Object> param = new HashMap<>()
        param.put("lang", params.get("lang"))
        for (StoreRecord rTyp in dsTyp) {
            param.put("own", 1L)
            param.put("typ", rTyp.get("ent"))

            Store dsCls = loadFltCls(param)
            dsResult.add(dsCls)
            dsCls.clear()

            if (rTyp.get("typParent") > 0) {
                param.put("own", 0)
                param.put("typ", rTyp.get("ent"))
                Store dsClsPrt = loadFltCls(param)
                dsResult.add(dsClsPrt)
                dsClsPrt.clear()
            }

            dsResult = MakeTreeCls(rTyp, dsResult, typNodeVisible)
            dsRes.add(dsResult)
        }
        //
        def ids = dsRes.getUniqueValues("id")

        for (StoreRecord r in dsRes) {
            if (!ids.contains(r.getString("parent"))) {
                r.set("parent", null)
            }
        }
        //mdb.outTable(dsRes)
        return dsRes
    }

    protected Store loadFltTyp(Map<String, Object> params) throws Exception {

        String whe1 = ""
        long typId = UtCnv.toLong(params.get("typ"))
        if (typId > 0)
            whe1 = " and c.id=${typId}"

        String sFld = "'t_'||c.id as id"
        String lang = UtCnv.toString(params.get("lang"))
        String sql = """
            select * from
            (
              select c.id as ent, 0 as typ, l.name, l.fullName, 
                    c.accessLevel, c.cod, 0 as kind, -1 as isOwn, c.parent as typParent,
                    ${sFld}, null as parent, c.isOpenness, l.cmt
              from Typ c, TypVer v, TableLang l
              where c.id=v.ownerVer and v.lastVer=1 ${whe1}
                and l.nameTable='TypVer' and l.idTable=v.id and l.lang='${lang}'
            ) t
            /**/where 0=0
            order by cod
        """

        Store st = mdb.createStore("ClsTree")
        mdb.loadQuery(st, sql)
        return st
    }

    protected Store loadFltCls(Map<String, Object> params) throws Exception {

        long typId = UtCnv.toLong(params.get("typ"))
        int own = UtCnv.toInt(params.get("own"))
        String lang = UtCnv.toString(params.get("lang"))

        String whe2 = " and c.typ=${typId}"
        String whe3 = " and t.id=${typId}"

        String sFld1 = "'c_'||c.id as id, 't_'||c.typ as parent"
        String sFld2 = "'t_'||c.typ||'_'||c.id as id, 't_'||t.id as parent"
        String sql

        if (own == 1) {
            sql = """
                select * from
                (
                    select c.id as ent,c.typ as typ, l.name, l.fullName,
                        c.accessLevel, c.dataBase, c.cod, 1 as kind, 1 as isOwn, ${sFld1},
                        0 as typParent, c.ord, c.isOpenness, l.cmt
                    from Cls c, ClsVer v, TableLang l
                    where c.id=v.ownerVer and v.lastVer=1 ${whe2}
                        and l.nameTable='ClsVer' and idTable=v.id and l.lang='${lang}'
                ) t
                /**/where 0=0
                order by isOwn desc, ord
            """
        } else {
            sql = """
                select * from
                (
                    select c.id as ent,t.id as typ,v.name, v.fullName,
                        c.accessLevel, c.dataBase, c.cod as cod,1 as kind,0 as isOwn, ${sFld2},
                        tp.id as typParent, c.ord, c.isOpenness, c.cmt
                    from Typ t, TypVer vt, Typ tp, TypVer vtp, Cls c, ClsVer v, TableLang l
                    where t.id=vt.ownerVer and vt.LastVer=1 and t.parent=tp.id and
                        tp.id=vtp.ownerVer and vtp.LastVer=1 and
                        tp.id=c.typ and c.id=v.ownerVer and v.lastVer=1 and l.nameTable='ClsVer' and idTable=v.id and l.lang='${lang}' and
                        not c.id in (select clsOrObjCls from TypParentNot where typ=t.id and clsOrObjCls is not null and obj is null) ${whe3}
                ) t
                /**/where 0=0
                order by isOwn desc, ord
            """
        }

        Store st = mdb.createStore("ClsTree")
        mdb.loadQuery(st, sql)
        return st
    }

    private Store MakeTreeCls(StoreRecord rTyp, Store dsCls, boolean typNodeVisible) {
        Store dsC = mdb.createStore("ClsTemp")
        Store dsResult = mdb.createStore("ClsTree")

        String sFld1 = "'c_'||id as id_, 't_'||typ as parent"
        String sFld2 = "'t_'||typ||'_'||id as id_, 't_'||typ as parent"

        mdb.loadQuery(dsC, """
              select ${sFld1}, '' as sfv, 0 as isDone, id as id, 1 as isOwn
              from Cls where typ=:t
            """, [t: rTyp.get("ent")])

        if (rTyp.getLong("typParent") != 0) {
            mdb.loadQuery(dsC, """
              select ${sFld2}, '' as sfv, 0 as isDone, id as id, 0 as isOwn
              from Cls where typ=:t  and not id in (select clsOrObjCls from TypParentNot where typ=:t and clsOrObjCls > 0)
            """, [t: rTyp.get("typParent")])
        }

        Store dsCFV = mdb.createStore("ClsFV.factorVal")
        int maxLev = 0
        int minLev = 1000
        for (r in dsC) {
            dsCFV.clear()
            mdb.loadQuery(dsCFV, """
                  select factorVal from ClsFactorVal where cls=:cls
                  order by factorVal
               """, [cls: r.get("id")])

            def idsFVS = dsCFV.getUniqueValues("factorVal")
            r.set("sfv", idsFVS.join(","))
            r.set("cnt", idsFVS.size())

            if (r.getInt("isOwn") == 0)
                r.set("parent", "t_".concat(rTyp.getString("ent")))

            def s_fv = r.getString("sfv").split(",")
            if (s_fv.size() > maxLev)
                maxLev = s_fv.size()
            if (s_fv.size() < minLev)
                minLev = s_fv.size()
        }
        for (r in dsC) {
            if (r.getInt("cnt") == minLev) {
                r.set("isDone", 1)
            }
            r.set("id", r.get("id_"))
        }
//

        dsC.getRecords().sort((a, b) -> {
            a.getInt("cnt") == b.getInt("cnt") ? 0 : a.getInt("cnt") < b.getInt("cnt") ? -1 : 1
        })

        Store dsDop = mdb.createStore("ClsTemp")
        Store dsRes = mdb.createStore("ClsTemp")

        for (r in dsC) {
            if (r.getInt("isDone") != 1)
                continue;
            Set s_fv = r.getString("sfv").split(",")
            boolean is1Lev = true
            for (r1 in dsC) {
                if (r1.getInt("isDone") == 1)
                    continue;
                Set s_fv1 = r1.getString("sfv").split(",")

                if (s_fv1.containsAll(s_fv)) {
                    is1Lev = false
                    break;
                }
            }
            if (is1Lev)
                r.set("isDone", 1)
        }

        def lenFVs = dsC.getUniqueValues("cnt")

        for (StoreRecord r in dsC) {
            if (r.getLong("cnt") == lenFVs[0]) {
                dsRes.add(r);
            }
        }
        //
        int k = 1
        while (k < lenFVs.size()) {
            dsDop.clear()
            for (r1 in dsC) {
                if (r1.getInt("cnt") != lenFVs[k])
                    continue;

                Set s_fv1 = r1.getString("sfv").split(",")

                int kk = k - 1
                while (true) {
                    for (r in dsRes) {
                        if (r.getInt("cnt") != lenFVs[kk])
                            continue;
                        Set s_fv = r.getString("sfv").split(",")
                        if (s_fv1.containsAll(s_fv)) {
                            r1.set("isDone", 1);
                            def r2 = dsDop.add(r1);
                            r2.set("id", r.get("id") + "_" + r2.get("id"));
                            r2.set("parent", r.get("id"));
                        }
                    }
                    if (r1.getInt("isDone") == 1) {
                        break;
                    } else {
                        if (kk < 1) {
                            r1.set("isDone", 1);
                            def r2 = dsDop.add(r1);
                            break;
                        } else {
                            kk = kk - 1;
                        }
                    }
                } //while
            }
            for (def r in dsDop) {
                dsRes.add(r)
            }

            dsRes.getRecords().sort((a, b) -> {
                a.getInt("cnt") == b.getInt("cnt") ? 0 : a.getInt("cnt") < b.getInt("cnt") ? -1 : 1
            })
            k = k + 1
        }
        for (def r in dsRes) {
            StoreRecord r1 = getRecordCls(r.getString("id_"), dsCls);
            if (r1) {
                r1.set("id", r.get("id"));
                r1.set("parent", r.get("parent"));
                dsResult.add(r1);
            }
        }

        if (typNodeVisible)
            dsResult.add(rTyp);

        return dsResult;
    }

    private StoreRecord getRecordCls(String id, Store st) {
        Store ds = mdb.createStore("ClsTree")
        StoreRecord res = null;
        for (StoreRecord r in st) {
            if (id.equals(r.getString("id"))) {
                res = ds.add(r)
                break;
            }
        }
        return res;
    }

    /*
        Возвращает список дочерних классов класса cls, если curCls = true, то включается cls
    */

    public String listIdChilds(long typ, long cls, boolean curCls) {

        if (cls == 0 || typ == 0)
            return "0";

        String s;
        if (curCls)
            s = cls.toString()
        else
            s = "0";

        TypDao daoTyp = mdb.createDao(TypDao.class) as TypDao
        //StoreRecord rP = daoTyp.loadRec(Map.of("id", typ))
        StoreRecord rP = daoTyp.loadRec(typ)
        String wh;
        if (rP.get("parent"))
            wh = "typ in (${typ}, ${rP.get("parent")})"
        else
            wh = "typ=${typ}"

        Store dsC = mdb.createStore("ClsTemp")
        mdb.loadQuery(dsC, "select id from Cls where ${wh}")

        Store dsCFV = mdb.createStore("ClsFV.factorVal")
        def maxLev = 0
        def minLev = 1000
        for (r in dsC) {
            dsCFV.clear()
            mdb.loadQuery(dsCFV, """
                  select factorVal from ClsFactorVal where cls=:cls
                  order by factorVal
               """, [cls: r.getLong("id")])

            def idsFVS = dsCFV.getUniqueValues("factorVal")
            r.set("sfv", idsFVS.join(","))
            r.set("cnt", idsFVS.size())
        }

        dsC.getRecords().sort((a, b) -> {
            a.getInt("cnt") == b.getInt("cnt") ? 0 : a.getInt("cnt") < b.getInt("cnt") ? -1 : 1
        })

        StoreRecord r0 = mdb.createStore("ClsTemp").add()
        for (r in dsC) {
            if (r.getLong("id") == cls) {
                r0 = r
                break;
            }
        }
        Set s_fv0 = r0.getString("sfv").split(",")
        for (r in dsC) {
            if (r.getLong("id") == cls)
                continue;
            Set s_fv1 = r.getString("sfv").split(",")

            if (s_fv1.containsAll(s_fv0))
                s = s.concat(",").concat(r.getString("id"))
        }
        return s;
    }
    //

    /*
     *   Возвращает список классов типа typ, которые имеет общий родительский класс с классом cls
     *   исключая дочерних классов класса cls
     *
 `  */

    public String ListClsParents(long typ, long cls) {

        if (cls == 0 || typ == 0)
            return "0";

        String s = cls.toString();
        TypDao daoTyp = mdb.createDao(TypDao.class) as TypDao
        //StoreRecord rP = daoTyp.loadRec(Map.of("id", typ))
        StoreRecord rP = daoTyp.loadRec(typ)

        String wh;
        if (rP.get("parent"))
            wh = "typ in (${typ}, ${rP.get("parent")})"
        else
            wh = "typ=${typ}"

        Store dsC = mdb.createStore("ClsTemp")
        mdb.loadQuery(dsC, "select id from Cls where ${wh}")

        Store dsCFV = mdb.createStore("ClsFV.factorVal")
        def maxLev = 0
        def minLev = 1000
        for (r in dsC) {
            dsCFV.clear()
            mdb.loadQuery(dsCFV, """
                  select factorVal from ClsFactorVal where cls=:cls
                  order by factorVal
               """, [cls: r.getLong("id")])

            def idsFVS = dsCFV.getUniqueValues("factorVal")
            r.set("sfv", idsFVS.join(","))
            r.set("cnt", idsFVS.size())
            r.set("isDone", 0)
        }

        dsC.getRecords().sort((a, b) -> {
            a.getInt("cnt") == b.getInt("cnt") ? 0 : a.getInt("cnt") < b.getInt("cnt") ? -1 : 1
        })

        StoreRecord r0 = mdb.createStore("ClsTemp").add()
        for (r in dsC) {
            if (r.getLong("id") == cls) {
                r0 = r
                break;
            }
        }
        Store dsCls = mdb.createStore("ClsTemp")
        Set s_fv0 = r0.getString("sfv").split(",")

        for (r in dsC) {
            if (r.getLong("id") == cls)
                continue;
            Set s_fv1 = r.getString("sfv").split(",")

            if (s_fv0.containsAll(s_fv1))
                dsCls.add(r);
        }

        if (dsCls.size() == 0)
            return s
        else {
            s = s + "," + dsCls.getUniqueValues("id").join(",")    //UtString.join(ut.uniqueValues(dsCls, "id"), ",");
            Set setCls = s.split(",")
            for (c in dsCls) {
                s_fv0 = c.getString("sfv").split(",")
                for (r in dsC) {
                    if ((r.getInt("isDone") == 1) || setCls.contains(r.get("id")))
                        continue;
                    Set s_fv1 = r.getString("sfv").split(",")
                    if (s_fv1.containsAll(s_fv0)) {
                        s = s + "," + r.getString("id")
                        r.set("isDone", 1)
                    }
                }
            }
        }
        return s;
    }

}
