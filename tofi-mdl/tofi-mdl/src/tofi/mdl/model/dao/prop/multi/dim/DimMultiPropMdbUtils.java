package tofi.mdl.model.dao.prop.multi.dim;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreField;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.consts.FD_DimMultiPropType_consts;
import tofi.mdl.consts.FD_MultiValEntityType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DimMultiPropMdbUtils extends EntityMdbUtils {

    Mdb mdb;
    String tableName;

    public DimMultiPropMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public Store newRec(long propGr) throws Exception {
        Store st = mdb.createStore("DimMultiProp");
        StoreRecord r = st.add();
        r.set("dimMultiPropGr", propGr);
        r.set("accessLevel", FD_AccessLevel_consts.common);
        r.set("dimMultiPropType", FD_DimMultiPropType_consts.stat);
        return st;
    }

    public Store loadDimMultiProp(long propGr, String lang) throws Exception {

        Store st = mdb.createStore("DimMultiProp.lang");
        String sql = "select * from DimMultiProp where dimMultiPropGr=:gr";
        mdb.loadQuery(st, sql, Map.of("gr", propGr));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "DimMultiProp", lang);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = insertEntity(rec);
        Store st = mdb.createStore("DimMultiProp");
        mdb.loadQuery(st, "select * from DimMultiProp where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        // Загрузка записи
        Store st = mdb.createStore("DimMultiProp");

        mdb.loadQuery(st, "select * from DimMultiProp where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    //DimMultiPropItem
    public Map<String, Object> loadDimMultiPropItem(long dimMultiProp) throws Exception {
        Store st = mdb.createStore("DimMultiPropItem.full");

        Store stTitle = mdb.loadQuery("""
                    select id, title from DimMultiPropName where dimMultiProp=:dmp order by ord
                """, Map.of("dmp", dimMultiProp));

        boolean isDopCols = stTitle.size() > 0;


        String sql = """
                    select *,
                    case
                        when multiEntityType = 1 then 'meter'
                        when multiEntityType = 2 then 'factorval'
                        when multiEntityType = 3 then 'obj'
                        when multiEntityType = 4 then 'relobj'
                        when multiEntityType = 5 then 'formula'
                        when multiEntityType = 6 then 'alg'
                        when multiEntityType = 7 then 'attr_str'
                        when multiEntityType = 8 then 'attr_mask'
                        when multiEntityType = 9 then 'attr_date'
                    end as entityType
                    from DimMultiPropItem where dimMultiProp=:dmp
                """;
        mdb.loadQuery(st, sql, Map.of("dmp", dimMultiProp));

        if (!isDopCols) {
            return Map.of("rows", st, "titles", stTitle);
        }
        //
        Set<Object> ids = st.getUniqueValues("id");
        String where = "(0" + UtString.join(ids, ",") + ")";
        Store stDMPIN = mdb.loadQuery("""
            select *, dimMultiPropItem || '_' || dimMultiPropName as key
            from DimMultiPropItemName where dimMultiPropItem in
        """+ where);
        StoreIndex indDMPIN = stDMPIN.getIndex("key");

        for (Object o : ids) {
            long id = UtCnv.toLong(o);
            for (StoreRecord r : stTitle) {
                String key = id + "_" + r.getString("id");
                StoreRecord rec = indDMPIN.get(key);
                if (rec == null) {  //add
                    mdb.insertRec("DimMultiPropItemName",
                            Map.of("dimMultiPropItem", id, "dimMultiPropName", r.getLong("id")),
                            true);
                }
            }
        }
        //
        for (StoreRecord r : stTitle) {
            StoreField fld = st.addField("col_" + r.getString("id"), "string", 150);
            fld.setTitle(r.getString("title"));
        }

        for (StoreRecord r : st) {
            for (StoreField fld : r.getFields()) {
                if (fld.getName().startsWith("col_")) {
                    String idDMPIN = fld.getName().split("_")[1];
                    StoreRecord record = indDMPIN.get(r.getString("id")+"_"+idDMPIN);
                    String nm = null;
                    if (record != null) {
                        nm = record.getString("name");
                    }
                    r.set(fld.getName(), nm);
                }
            }
        }

        //
        return Map.of("rows", st, "titles", stTitle);
    }

    private Store loadDimMultiPropItemRec(long id, long dmp) throws Exception {
        Store st = mdb.createStore("DimMultiPropItem.full");

        Store stTitle = mdb.loadQuery("""
                    select id, title from DimMultiPropName where dimMultiProp=:dmp order by ord
                """, Map.of("dmp", dmp));

        boolean isDopCols = stTitle.size() > 0;


        for (StoreRecord r : stTitle) {
            StoreField fld = st.addField("col_" + r.getString("id"), "string", 150);
            fld.setTitle(r.getString("title"));
        }

        String sql = """
                    select *,
                    case
                        when multiEntityType = 1 then 'meter'
                        when multiEntityType = 2 then 'factorval'
                        when multiEntityType = 3 then 'obj'
                        when multiEntityType = 4 then 'relobj'
                        when multiEntityType = 5 then 'formula'
                        when multiEntityType = 6 then 'alg'
                        when multiEntityType = 7 then 'attr_str'
                        when multiEntityType = 8 then 'attr_mask'
                        when multiEntityType = 9 then 'attr_date'
                    end as entityType
                    from DimMultiPropItem where id=:id
                """;
        mdb.loadQuery(st, sql, Map.of("id", id));
        //
        Store stDMPIN = mdb.loadQuery("""
            select *, dimMultiPropItem || '_' || dimMultiPropName as key
            from DimMultiPropItemName where dimMultiPropItem=:id
        """,Map.of("id", id));
        StoreIndex indDMPIN = stDMPIN.getIndex("key");
        //
        for (StoreRecord r : st) {
            for (StoreField fld : r.getFields()) {
                if (fld.getName().startsWith("col_")) {
                    String idDMPIN = fld.getName().split("_")[1];
                    StoreRecord record = indDMPIN.get(r.getString("id")+"_"+idDMPIN);
                    String nm = null;
                    if (record != null) {
                        nm = record.getString("name");
                    }
                    r.set(fld.getName(), nm);
                }
            }
        }
        //
        return st;
    }

    public Store newRecDimMultiPropItem(long propDim) throws Exception {
        Store st = mdb.createStore("DimMultiPropItem.full");
        StoreRecord r = st.add();
        r.set("dimMultiProp", propDim);
        r.set("multiEntityType", FD_MultiValEntityType_consts.meter);
        return st;
    }


    public Store insertDimItem(Map<String, Object> params) throws Exception {
        Map<Long, String> mapCols = new HashMap<>();
        for (String c : params.keySet()) {
            if (c.startsWith("col_")) {
                mapCols.put(UtCnv.toLong(c.split("_")[1]), UtCnv.toString(params.get(c)));
            }
        }

        for (Long c : mapCols.keySet()) {
            params.remove("col_"+c);
        }
        //
        long id = insertEntity(params);
        //
        long dmp = UtCnv.toLong(params.get("dimMultiProp"));
        for (Map.Entry<Long, String> entry : mapCols.entrySet()) {
            mdb.insertRec("DimMultiPropItemName", Map.of("dimMultiPropItem", id,
                    "dimMultiPropName", entry.getKey(), "name", entry.getValue()), true);
        }
        //
        return loadDimMultiPropItemRec(id, dmp);
    }

    public Store updateDimItem(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        long dmp = UtCnv.toLong(params.get("dimMultiProp"));
        Map<Long, String> mapCols = new HashMap<>();
        for (String c : params.keySet()) {
            if (c.startsWith("col_")) {
                mapCols.put(UtCnv.toLong(c.split("_")[1]), UtCnv.toString(params.get(c)));
            }
        }

        for (Long c : mapCols.keySet()) {
            params.remove("col_"+c);
        }

        updateEntity(params);
        for (Map.Entry<Long, String> entry : mapCols.entrySet()) {
            mdb.execQuery("""
                update DimMultiPropItemName set name=:nm where dimMultiPropItem=:dmpi and dimMultiPropName=:dmpn
            """, Map.of("nm", entry.getValue(), "dmpi", id, "dmpn", entry.getKey()));
        }
        return loadDimMultiPropItemRec(id, dmp);
    }

    public void deleteDimItem(Map<String, Object> params) throws Exception {
        Store stTmp = mdb.loadQuery("select id from DimMultiPropItemMeter where dimMultiPropItem=:id",
                Map.of("id", UtCnv.toLong(params.get("id"))));
        if (stTmp.size() == 0) {
            mdb.execQuery("delete from DimMultiPropItemName where dimMultiPropItem=:id",
                    Map.of("id", UtCnv.toLong(params.get("id"))));
        }
        deleteEntity(params);
    }
    ////

    public Store loadClsForSelect() throws Exception {
        return mdb.loadQuery("""
                    select 't_'||t.id as id, null as parent, v.name, v.fullname, -t.id as cls, null as ent
                    from Typ t, TypVer v
                    where t.id=v.ownerVer and v.lastVer=1
                    union all
                    select 'c_'||c.id as id, 't_'||c.typ as parent, v.name, v.fullname, c.id as cls, 'cls' as ent
                    from Cls c, ClsVer v
                    where c.id=v.ownerVer and v.lastVer=1
                """);

    }

    public Store loadObjForSelect(long cls) throws Exception {
        throw new XError("Запрос на другую базу");

//todo Запрос на данные
/*        return mdb.loadQuery("""
            select 't_'||t.id as id, null as parent, v.name, null::bigint as obj
            from Typ t, TypVer v
            where t.id=v.ownerVer and v.lastVer=1
            union all
            select 'c_'||c.id as id, 't_'||c.typ as parent, v.name, null::bigint as obj
            from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1
            union all
            select 'o_'||o.id as id, 'c_'||o.cls as parent, v.name, o.id as obj
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1
        """);*/

    }

    public Store loadRelClsForSelect() throws Exception {
        return mdb.loadQuery("""
                    select 't_'||t.id as id, null as parent, v.name, -t.id as relcls, null as ent
                    from RelTyp t, RelTypVer v
                    where t.id=v.ownerVer and v.lastVer=1
                    union all
                    select 'c_'||c.id as id, 't_'||c.reltyp as parent, v.name, c.id as relcls, 'relcls' as ent
                    from RelCls c, RelClsVer v
                    where c.id=v.ownerVer and v.lastVer=1
                """);
    }


    public Store loadRelObjForSelect(long relCls) throws Exception {
        throw new XError("Запрос на другую базу");
//todo Запрос на данные
/*
        return mdb.loadQuery("""
            select 'r_'||r.id as id, null as parent, v.name, null as relobj
            from RelTyp r, RelTypVer v
            where r.id=v.ownerVer and v.lastVer=1
            union all
            select 'o_'||o.id as id, 'r_'||o.reltyp as parent, v.name, o.id as relobj
            from RelObj o, RelObjVer v
            where o.id=v.ownerVer and v.lastVer=1
        """);
*/

    }

    public Store loadProp() throws Exception {
        return mdb.loadQuery("""
                    select id, name from prop
                    where proptype in (1,5,6)
                """);
    }

    public Store loadDimMultiPropItemAttrib(long dimMultiPropItem) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemAttrib");
        return mdb.loadQuery(st, """
                    select * from DimMultiPropItemAttrib
                    where dimMultiPropItem=:id
                """, Map.of("id", dimMultiPropItem));
    }

    private Store loadDimMultiPropItemAttribRec(long id) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemAttrib");
        return mdb.loadQuery(st, """
                    select * from DimMultiPropItemAttrib
                    where id=:id
                """, Map.of("id", id));
    }

    public Store insDimMultiPropItemAttrib(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemAttrib");
        StoreRecord r = st.add(params);
        long id = mdb.insertRec("DimMultiPropItemAttrib", r, true);
        return loadDimMultiPropItemAttribRec(id);
    }

    public Store updDimMultiPropItemAttrib(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemAttrib");
        StoreRecord r = st.add(params);
        long id = r.getLong("id");
        mdb.updateRec("DimMultiPropItemAttrib", r);
        return loadDimMultiPropItemAttribRec(id);
    }

    public void deleteDimMultiPropItemAttrib(long id) throws Exception {
        mdb.deleteRec("DimMultiPropItemAttrib", id);
    }

    //
    public Store loadDimMultiPropItemMeter(long dimMultiPropItem) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeter.full");
        return mdb.loadQuery(st, """
                    select d.*, mea.name as measureName
                    from DimMultiPropItemMeter d
                        left join Measure mea on d.measure=mea.id
                    where d.dimMultiPropItem=:id
                """, Map.of("id", dimMultiPropItem));
    }

    private Store loadDimMultiPropItemMeterRec(long id) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeter.full");
        return mdb.loadQuery(st, """
                    select d.*, m.name as measureName from DimMultiPropItemMeter d
                    left join Measure m on d.measure=m.id
                    where d.id=:id
                """, Map.of("id", id));
    }

    public Store insDimMultiPropItemMeter(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeter");
        StoreRecord r = st.add(params);
        long id = mdb.insertRec("DimMultiPropItemMeter", r, true);
        return loadDimMultiPropItemMeterRec(id);
    }

    public Store updDimMultiPropItemMeter(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeter");
        StoreRecord r = st.add(params);
        long id = r.getLong("id");
        mdb.updateRec("DimMultiPropItemMeter", r);
        return loadDimMultiPropItemMeter(id);
    }

    public void deleteDimMultiPropItemMeter(long id) throws Exception {
        mdb.deleteRec("DimMultiPropItemMeter", id);
    }

    //
    public Store loadMeasure() throws Exception {
        Store st = mdb.createStore("Measure");
        return mdb.loadQuery(st, """
                    select * from Measure where 0=0
                """);
    }

    //Factor
    public Store loadDimMultiPropItemFactor(long dimMultiPropItem) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemFactor.full");
        return mdb.loadQuery(st, """
                    select d.*, f.name
                    from DimMultiPropItemFactor d
                        left join Factor f on d.factor=f.id and f.parent is null
                    where d.dimMultiPropItem=:id
                """, Map.of("id", dimMultiPropItem));
    }

    private Store loadDimMultiPropItemFactorRec(long id) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemFactor.full");
        return mdb.loadQuery(st, """
                    select d.*, f.name
                    from DimMultiPropItemFactor d
                        left join Factor f on d.factor=f.id and f.parent is null
                    where d.id=:id
                """, Map.of("id", id));
    }

    public Store insDimMultiPropItemFactor(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemFactor");
        StoreRecord r = st.add(params);
        long id = mdb.insertRec("DimMultiPropItemFactor", r, true);
        return loadDimMultiPropItemFactorRec(id);
    }

    public Store updDimMultiPropItemFactor(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemFactor");
        StoreRecord r = st.add(params);
        long id = r.getLong("id");
        mdb.updateRec("DimMultiPropItemFactor", r);
        return loadDimMultiPropItemFactor(id);
    }

    public void deleteDimMultiPropItemFactor(long id) throws Exception {
        mdb.deleteRec("DimMultiPropItemFactor", id);
    }

    public Store loadFactors() throws Exception {
        Store st = mdb.createStore("Factor.select");
        return mdb.loadQuery(st, """
                    select * from Factor where parent is null
                """);
    }

    //Cls
    public Store loadDimMultiPropItemCls(long dimMultiPropItem) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemCls.full");
        return mdb.loadQuery(st, """
                    select d.*, c.name
                    from DimMultiPropItemCls d
                        left join ClsVer c on d.cls=c.ownerVer and c.lastVer=1
                    where d.dimMultiPropItem=:id
                """, Map.of("id", dimMultiPropItem));
    }

    private Store loadDimMultiPropItemClsRec(long id) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemCls.full");
        return mdb.loadQuery(st, """
                    select d.*, c.name
                    from DimMultiPropItemCls d
                        left join ClsVer c on d.cls=c.ownerVer and c.lastVer=1
                    where d.id=:id
                """, Map.of("id", id));
    }

    public Store insDimMultiPropItemCls(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemCls");
        StoreRecord r = st.add(params);
        long id = mdb.insertRec("DimMultiPropItemCls", r, true);
        return loadDimMultiPropItemClsRec(id);
    }

    public Store updDimMultiPropItemCls(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemCls");
        StoreRecord r = st.add(params);
        long id = r.getLong("id");
        mdb.updateRec("DimMultiPropItemCls", r);
        return loadDimMultiPropItemCls(id);
    }

    public void deleteDimMultiPropItemCls(long id) throws Exception {
        mdb.deleteRec("DimMultiPropItemCls", id);
    }

    //RelCls
    public Store loadDimMultiPropItemRelCls(long dimMultiPropItem) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemRelCls.full");
        return mdb.loadQuery(st, """
                    select d.*, c.name
                    from DimMultiPropItemRelCls d
                        left join RelClsVer c on d.relCls=c.ownerVer and c.lastVer=1
                    where d.dimMultiPropItem=:id
                """, Map.of("id", dimMultiPropItem));
    }

    private Store loadDimMultiPropItemRelClsRec(long id) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemRelCls.full");
        return mdb.loadQuery(st, """
                    select d.*, c.name
                    from DimMultiPropItemRelCls d
                        left join RelClsVer c on d.relCls=c.ownerVer and c.lastVer=1
                    where d.id=:id
                """, Map.of("id", id));
    }

    public Store insDimMultiPropItemRelCls(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemRelCls");
        StoreRecord r = st.add(params);
        long id = mdb.insertRec("DimMultiPropItemRelCls", r, true);
        return loadDimMultiPropItemRelClsRec(id);
    }

    public Store updDimMultiPropItemRelCls(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemRelCls");
        StoreRecord r = st.add(params);
        long id = r.getLong("id");
        mdb.updateRec("DimMultiPropItemRelCls", r);
        return loadDimMultiPropItemRelClsRec(id);
    }

    public void deleteDimMultiPropItemRelCls(long id) throws Exception {
        mdb.deleteRec("DimMultiPropItemRelCls", id);
    }

    //Measure
    public Store loadDimMultiPropItemMeasure(long dimMultiPropItem) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeasure.full");
        return mdb.loadQuery(st, """
                    select d.*, c.name
                    from DimMultiPropItemMeasure d
                        left join Measure c on c.parent is null and d.measure=c.id
                    where d.dimMultiPropItem=:id
                """, Map.of("id", dimMultiPropItem));
    }

    private Store loadDimMultiPropItemMeasureRec(long id) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeasure.full");
        return mdb.loadQuery(st, """
                    select d.*, c.name
                    from DimMultiPropItemMeasure d
                        left join Measure c on c.parent is null and d.measure=c.id
                    where d.id=:id
                """, Map.of("id", id));
    }

    public Store insDimMultiPropItemMeasure(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeasure");
        StoreRecord r = st.add(params);
        long id = mdb.insertRec("DimMultiPropItemMeasure", r, true);
        return loadDimMultiPropItemMeasureRec(id);
    }

    public Store updDimMultiPropItemMeasure(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("DimMultiPropItemMeasure");
        StoreRecord r = st.add(params);
        long id = r.getLong("id");
        mdb.updateRec("DimMultiPropItemMeasure", r);
        return loadDimMultiPropItemMeasureRec(id);
    }

    public void deleteDimMultiPropItemMeasure(long id) throws Exception {
        mdb.deleteRec("DimMultiPropItemMeasure", id);
    }

    public Store loadDimMultiPropMoreCols(long dimMultiProp) throws Exception {
        return mdb.loadQuery("""
                    select id, title from DimMultiPropName where dimMultiProp=:dim order by ord
                """, Map.of("dim", dimMultiProp));
    }

    public Store insertMoreCols(Map<String, Object> params) throws Exception {
        long id = mdb.insertRec("DimMultiPropName", params, true);
        mdb.execQuery("""
                    update DimMultiPropName set ord=:id where id=:id
                """, Map.of("id", id));
        Store st = mdb.createStore("DimMultiPropName");
        mdb.loadQuery(st, "select id, title from DimMultiPropName where id=:id", Map.of("id", id));
        return st;
    }

    public Store updateMoreCols(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        mdb.updateRec("DimMultiPropName", params);
        Store st = mdb.createStore("DimMultiPropName");
        mdb.loadQuery(st, "select id, title from DimMultiPropName where id=:id", Map.of("id", id));
        return st;
    }

    public void deleteMoreCols(long id) throws Exception {
        try {
            mdb.execQuery("""
                        delete from DimMultiPropItemName where dimMultiPropName=:id;
                    """, Map.of("id", id));
        } finally {
            mdb.deleteRec("DimMultiPropName", id);
        }

    }

    public void changeOrdMoreCols(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        boolean up = UtCnv.toBoolean(params.get("up"));
        long dmp = UtCnv.toLong(params.get("dimMultiProp"));
        long id1 = UtCnv.toLong(rec.get("id"));
        long ord1 = UtCnv.toLong(rec.get("ord"));
        long id2;
        long ord2;

        Store st = mdb.loadQuery("""
                    select * from DimMultiPropName where dimMultiProp=:dmp order by ord
                """, Map.of("dmp", dmp));
        int k = 0;  //искомая позиция
        for (int i = 0; i < st.size(); i++) {
            if (st.get(i).getLong("id") == id1) {
                k = i;
                break;
            }
        }
        if (up) {
            id2 = st.get(k - 1).getLong("id");
            ord2 = st.get(k - 1).getLong("ord");
        } else {
            id2 = st.get(k + 1).getLong("id");
            ord2 = st.get(k + 1).getLong("ord");
        }
        //
        mdb.execQuery("""
                    update DimMultiPropName set ord=:ord2 where id=:id1;
                    update DimMultiPropName set ord=:ord1 where id=:id2;
                """, Map.of("id1", id1, "id2", id2, "ord1", ord1, "ord2", ord2));
    }

    public Store loadPropForMultiPropItem() throws Exception {
        return mdb.loadQuery("""
            select id, name, parent, 0 as prop
            from PropGr
            where id in (
                select distinct propgr
                from prop
                where proptype in (1,5,6)
            )
            union all
            select id, name, propgr as parent, id as prop
            from Prop
            where propGr in (
                select id
                from PropGr
                where id in (
                    select distinct propgr
                    from prop
                    where prop.proptype in (1,5,6)
                    )
            ) and proptype in (1,5,6)
        """);
    }

}

