package tofi.api.mdl.utils

import jandcode.commons.UtCnv
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord

class ClsTreeUtils {
    Mdb mdb;

    ClsTreeUtils(Mdb mdb) throws Exception {
        this.mdb = mdb
    }

    Store MakeTreeCls(StoreRecord rTyp, Store dsCls, boolean typNodeVisible) {
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

    Store loadFltCls(Map<String, Object> params) throws Exception {

        long typId = UtCnv.toLong(params.get("typ"))
        int own = UtCnv.toInt(params.get("own"))

        String whe2 = " and c.typ=${typId}"
        String whe3 = " and t.id=${typId}"

        String sFld1 = "'c_'||c.id as id, 't_'||c.typ as parent"
        String sFld2 = "'t_'||c.typ||'_'||c.id as id, 't_'||t.id as parent"
        String sql = ""

        if (own == 1) {
            sql = """
                select * from
                (
                    select c.id as ent,c.typ as typ,v.name, v.fullName,
                        c.accessLevel, c.dataBase, c.cod, 1 as kind, 1 as isOwn, ${sFld1},
                        0 as typParent, c.ord, c.isOpenness, c.cmt
                    from Cls c, ClsVer v
                    where c.id=v.ownerVer and v.lastVer=1 ${whe2}
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
                    from Typ t, TypVer vt, Typ tp, TypVer vtp, Cls c, ClsVer v
                    where t.id=vt.ownerVer and vt.LastVer=1 and t.parent=tp.id and
                        tp.id=vtp.ownerVer and vtp.LastVer=1 and
                        tp.id=c.typ and c.id=v.ownerVer and v.lastVer=1 and
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

    /*
     *   Возвращает список классов типа typ, которые имеет общий родительский класс с классом cls
     *   исключая дочерних классов класса cls
     *
 `  */
    String ListClsParents(long typ, long cls) {

        if (cls == 0 || typ == 0)
            return "0"

        String s = cls.toString()
        Store st = mdb.createStore("Typ.full")
        StoreRecord rP = mdb.loadQuery(st, """
            select * from Typ t, TypVer v
            where t.id=v.ownerVer and v.lastVer=1 and t.id=${typ}
        """).get(0)


        String wh
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

        dsC.getRecords().sort((a,b)-> {
            a.getInt("cnt")==b.getInt("cnt") ? 0 : a.getInt("cnt") < b.getInt("cnt") ? -1: 1
        })

        StoreRecord r0 = mdb.createStore("ClsTemp").add()
        for (r in dsC) {
            if (r.getLong("id") == cls) {
                r0 = r
                break
            }
        }
        Store dsCls = mdb.createStore("ClsTemp")
        Set s_fv0 = r0.getString("sfv").split(",")

        for (r in dsC) {
            if (r.getLong("id") == cls)
                continue;
            Set s_fv1 = r.getString("sfv").split(",")

            if (s_fv0.containsAll(s_fv1))
                dsCls.add(r)
        }

        if (dsCls.size() == 0)
            return s
        else {
            s = s + "," + dsCls.getUniqueValues("id").join(",")
            Set setCls = s.split(",")
            for (c in dsCls) {
                s_fv0 = c.getString("sfv").split(",")
                for (r in dsC) {
                    if ((r.getInt("isDone") == 1) || setCls.contains(r.get("id")))
                        continue
                    Set s_fv1 = r.getString("sfv").split(",")
                    if (s_fv1.containsAll(s_fv0)) {
                        s = s + "," + r.getString("id")
                        r.set("isDone", 1)
                    }
                }
            }
        }
        return s
    }

    private StoreRecord getRecordCls(String id, Store st) {
        Store ds = mdb.createStore("ClsTree")
        StoreRecord res = null;
        for (StoreRecord r in st) {
            if (id.equalsIgnoreCase(r.getString("id"))) {
                res = ds.add(r)
                break;
            }
        }
        return res;
    }



}
