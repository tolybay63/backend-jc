package tofi.mdl.model.dao.scale;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScaleMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public ScaleMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public Store load(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("Scale");
        return mdb.loadQuery(st, "select * from Scale where 0=0");
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        //
        long id = insertEntity(rec);
        //
        Store st = mdb.createStore("Scale");
        mdb.loadQuery(st, "select * from Scale where id=:id", Map.of("id", id));
        //
        return st;
    }

    public Store update(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        //
        Store st = mdb.createStore("Scale");
        mdb.loadQuery(st, "select * from Scale where id=:id", Map.of("id", id));
        //
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    //-------------------------------ScaleVal
    public Store loadScaleVal(long scale) throws Exception {
        Store st = mdb.createStore("ScaleVal.full");
        mdb.loadQuery(st, "select * from ScaleVal where scale=:s", Map.of("s", scale));

        for (StoreRecord r : st) {
            String lgr = r.getBoolean("isMinValOpen") ? "(" : "[";
            String rgr = r.getBoolean("isMaxValOpen") ? ")" : "]";
            r.set("name", lgr + r.getString("minVal") + ", " + r.getString("maxVal") + rgr);
        }
        return st;
    }

    protected Store loadScaleValRec(long id) throws Exception {
        Store st = mdb.createStore("ScaleVal.full");
        mdb.loadQuery(st, "select * from ScaleVal where id=:id", Map.of("id", id));
        StoreRecord r = st.get(0);
        String lgr = r.getBoolean("isMinValOpen") ? "(" : "[";
        String rgr = r.getBoolean("isMaxValOpen") ? ")" : "]";
        r.set("name", lgr + r.getString("minVal") + ", " + r.getString("maxVal") + rgr);
        //
        return st;
    }

    protected int getIndex(Store st, Map<String, Object> rec) throws Exception {
        int k = -1;
        for (int i=0; i < st.size(); i++) {
            if (st.get(i).getLong("id")==UtCnv.toLong(rec.get("id"))) {
                k = i;
                break;
            }
        }
        return k;
    }

    protected void validScaleVal(Map<String, Object> rec, String mode) throws Exception {
        if (UtCnv.toDouble(rec.get("minVal")) > UtCnv.toDouble(rec.get("maxVal"))) {
            throw new XError("minValLargerMaxVal");
        }
        Store st = mdb.createStore("ScaleVal");
        mdb.loadQuery(st, "select * from ScaleVal where scale=:s", Map.of("s", rec.get("scale")));
        if (mode.equals("ins")) {
            if (st.size() > 0) {
                if (st.get(st.size() - 1).getDouble("minVal") >= UtCnv.toDouble(rec.get("maxVal"))) {
                    throw new XError("predValLargerCurVal");
                } else if ( st.get(st.size() - 1).getDouble("maxVal") > UtCnv.toDouble(rec.get("minVal")) ||
                        ( !st.get(st.size() - 1).getBoolean("isMaxValOpen") && !UtCnv.toBoolean(rec.get("isMinValOpen")) &&
                                st.get(st.size() - 1).getDouble("maxVal") == UtCnv.toDouble(rec.get("minVal"))) ) {
                    throw new XError("intervalsIntersect");
                }
            }
        } else {
            if (st.size() > 1) {
                int ind = getIndex(st, rec);
                if (ind == st.size()-1) {
                    if (st.get(ind - 1).getDouble("minVal") >= UtCnv.toDouble(rec.get("maxVal"))) {
                        throw new XError("predValLargerCurVal");
                    } else if (st.get(ind - 1).getDouble("maxVal") > UtCnv.toDouble(rec.get("minVal")) ||
                            ( !st.get(ind - 1).getBoolean("isMaxValOpen") && !UtCnv.toBoolean(rec.get("isMinValOpen")) &&
                                    st.get(ind - 1).getDouble("maxVal") == UtCnv.toDouble(rec.get("minVal"))) ) {
                        throw new XError("intervalsIntersect");
                    }
                } else if (ind == 0) {
                    if (UtCnv.toDouble(rec.get("minVal")) >= st.get(ind+1).getDouble("maxVal")) {
                        throw new XError("predValLargerCurVal");
                    } else if (st.get(ind + 1).getDouble("maxVal") < UtCnv.toDouble(rec.get("minVal")) ||
                            ( !st.get(ind + 1).getBoolean("isMinValOpen") && !UtCnv.toBoolean(rec.get("isMaxValOpen")) &&
                                    st.get(ind + 1).getDouble("minVal") == UtCnv.toDouble(rec.get("maxVal"))) ) {
                        throw new XError("intervalsIntersect");
                    }
                } else {
                    if (st.get(0).getDouble("minVal") >= UtCnv.toDouble(rec.get("maxVal"))) {
                        throw new XError("predValLargerCurVal");
                    }
                    if (st.get(ind-1).getDouble("maxVal") > UtCnv.toDouble(rec.get("minVal")) ||
                            ( !st.get(ind - 1).getBoolean("isMaxValOpen") && !UtCnv.toBoolean(rec.get("isMinValOpen")) &&
                                    st.get(ind - 1).getDouble("maxVal") == UtCnv.toDouble(rec.get("minVal"))) ) {
                        throw new XError("intervalsIntersect");
                    }
                    if (UtCnv.toDouble(rec.get("maxVal")) > st.get(ind+1).getDouble("minVal") ||
                            ( !st.get(ind + 1).getBoolean("isMinValOpen") && !UtCnv.toBoolean(rec.get("isMaxValOpen")) &&
                                    st.get(ind + 1).getDouble("minVal") == UtCnv.toDouble(rec.get("maxVal")))) {
                        throw new XError("intervalsIntersect");
                    }
                }
            }
        }

    }

    public Store insertScaleVal(Map<String, Object> rec) throws Exception {
        validScaleVal(rec, "ins");
        long id = mdb.insertRec("ScaleVal", rec);
        //
        return loadScaleValRec(id);
    }

    public Store updateScaleVal(Map<String, Object> rec) throws Exception {
        validScaleVal(rec, "upd");
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        mdb.updateRec("ScaleVal", rec);
        //
        return loadScaleValRec(id);
    }

    public void deleteScaleVal(Map<String, Object> rec) throws Exception {
        mdb.deleteRec("ScaleVal", UtCnv.toLong(rec.get("id")));
    }


    //--------------------------ScaleAsgn
    public Store loadScaleAsgn(long scale) throws Exception {
        Store st = mdb.createStore("ScaleAsgn");
        return mdb.loadQuery(st, "select * from ScaleAsgn where scale=:s and current_date between dbeg and dend",
                Map.of("s", scale));
    }

    protected Store loadScaleAsgnRec(long id) throws Exception {
        Store st = mdb.createStore("ScaleAsgn");
        return mdb.loadQuery(st, "select * from ScaleAsgn where id=:id", Map.of("id", id));
    }

    public Store insertScaleAsgn(Map<String, Object> rec) throws Exception {
        //System.out.println(rec);
        mdb.outMap(rec);
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        mdb.outMap(rec);

        long id = insertEntity(rec);
        //
        return loadScaleAsgnRec(id);
    }

    public Store updateScaleAsgn(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        //
        return loadScaleAsgnRec(id);
    }

    public void deleteScaleAsgn(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    //ScaleValAsgn
    public Store loadScaleValAsgnUpd(long scale, long scaleAsgn, String nmAsgn) throws Exception {
        Store st = mdb.createStore("ScaleValAsgn.full");
        return mdb.loadQuery(st, """
            select a.id, s.id as scaleval, :a as scaleAsgn,
            case when a.scalevalcolor is null then '#000000' else a.scalevalcolor end as scalevalcolor,
            case when isminvalopen=1 then '(' else '[' end || s.minval || ', '|| s.maxval || case when ismaxvalopen=1 then ')' else ']' end as val,
            case when isminvalopen=1 then '(' else '[' end || s.minval || ', '|| s.maxval || case when ismaxvalopen=1 then ')' else '] => '||:nm end as name,
            case when isminvalopen=1 then '(' else '[' end || s.minval || ', '|| s.maxval || case when ismaxvalopen=1 then ')' else '] => '||:nm end as fullName,
            case when a.id>0 then true else false end as checked
            from scaleval s
            left join scaleValAsgn a on s.id=a.scaleVal and a.scaleAsgn=:a
            where scale =:s
            order by val
        """, Map.of("s", scale, "a", scaleAsgn, "nm", nmAsgn));
    }

    public Store loadScaleValAsgn(long scale, long scaleAsgn) throws Exception {
        Store st = mdb.createStore("ScaleValAsgn.full");
        return mdb.loadQuery(st, """
            select a.id, s.id as scaleval, :a as scaleAsgn, a.scalevalcolor, a.name, a.fullName,
            case when isminvalopen=1 then '(' else '[' end || s.minval || ', '|| s.maxval || case when ismaxvalopen=1 then ')' else ']' end as val
            from scaleval s
            inner join scaleValAsgn a on s.id=a.scaleVal and a.scaleAsgn=:a
            where scale =:s
            order by val
        """, Map.of("s", scale, "a", scaleAsgn));
    }


    public void updateScaleValAsgn(long scale, long scaleAsgn, List<Map<String, Object>> data) throws Exception {
        //old values
        Store stOld = mdb.loadQuery("""
            select id from ScaleValAsgn where scaleAsgn=:a
                and scaleVal in (select id from ScaleVal where scale=:s)
        """,Map.of("s", scale, "a", scaleAsgn));
        Set<Object> setOldIds = stOld.getUniqueValues("id");
        //all values
        Set<Object> setAllIds = new HashSet<>();
        data.forEach(d-> {
            setAllIds.add(UtCnv.toLong(d.get("id")));
        });
        //Deleting
        setOldIds.forEach(id-> {
            if (!setAllIds.contains(id)) {
                try {
                    mdb.deleteRec("ScaleValAsgn", UtCnv.toLong(id));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        //Saving
        data.forEach(d-> {
            if ( !setOldIds.contains(UtCnv.toLong(d.get("id"))) ) {
                try {   //ins
                    StoreRecord r = mdb.createStoreRecord("ScaleValAsgn", d);
                    mdb.insertRec("ScaleValAsgn", r, true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try {   //upd
                    StoreRecord r = mdb.createStoreRecord("ScaleValAsgn", d);
                    mdb.updateRec("ScaleValAsgn", r);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
