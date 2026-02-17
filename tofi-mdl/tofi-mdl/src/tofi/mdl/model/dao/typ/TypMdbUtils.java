package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.commons.UtJson;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_PropType_consts;
import tofi.mdl.consts.FD_StorageType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;
import tofi.mdl.model.utils.UtMeterSoft;

import java.util.*;
import java.util.stream.Stream;


public class TypMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public TypMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }


    /**
     * Загрузка Typ с пагинацией
     *
     * @param params Map
     * @return Map
     */
    public Map<String, Object> loadTypPaginate(Map<String, Object> params) throws Exception {
        String lang = UtCnv.toString(params.get("lang"));
        //count
        String sqlCount = """
            select count(*) as cnt
            from Typ t
                left join TypVer v on t.id=v.ownerVer and v.lastVer=1
                --left join TableLang l on l.nameTable='TypVer' and l.idTable=v.id and l.lang=:lang -----------DELETE
            where 0=0
        """;
        SqlText sqlText = mdb.createSqlText(sqlCount);
        sqlText.setSql(sqlCount);
        String filter = UtCnv.toString(params.get("filter")).trim();
        String textFilter = "name like '%" + filter + "%' or fullName like '%" + filter + "%' or cod like '%" + filter + "%' or cmt like '%" + filter + "%'";
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        int total = mdb.loadQuery(sqlText, Map.of("lang", lang)).get(0).getInt("cnt");
        int lm = UtCnv.toInt(params.get("rowsPerPage")) == 0 ? total : UtCnv.toInt(params.get("rowsPerPage"));
        int offset = (UtCnv.toInt(params.get("page")) - 1) * lm;
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", lm);
        // load
        String sqlLoad = """
            select *, v.id as verId
            from Typ t
                left join TypVer v on t.id=v.ownerVer and v.lastVer=1
                -- left join TableLang l on l.nameTable='TypVer' and l.idTable=v.id and l.lang=:lang -----------DELETE
            where 0=0
        """;
        sqlText = mdb.createSqlText(sqlLoad);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("sortBy")).trim().isEmpty()) {
            String orderBy = UtCnv.toString(params.get("sortBy"));
            if (UtCnv.toBoolean(params.get("descending"))) {
                orderBy = orderBy + " desc";
            }
            sqlText = sqlText.replaceOrderBy(orderBy);
        }

        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        Store stLoad = mdb.createStore("Typ.lang");
        mdb.loadQuery(stLoad, sqlText, Map.of("lang", lang, "offset", offset, "limit", lm));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        stLoad = ut.getTranslatedStore(stLoad,"Typ", lang, true);
        //
        return Map.of("store", stLoad, "meta", meta);
    }

    /**
     * Delete Type
     *
     * @param rec Map
     */

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Type
     *
     * @param params Map
     * @return Store
     */
    public Store update(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntity(params);
        //
        // Загрузка записи
        return loadRec(params);
    }

    /**
     * Insert Type
     *
     * @param params Map
     * @return Store
     */
    public Store insert(Map<String, Object> params) throws Exception {
        //
        long id = insertEntity(params);
        //
        params.put("id", id);
        return loadRec(params);
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        Store st = mdb.createStore("Typ.lang");
        mdb.loadQuery(st, """
                    select *, v.id as verId from Typ t, TypVer v where t.id=v.ownerVer and v.lastVer=1 and t.id=:id
                """, Map.of("id", id));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st,"Typ", lang, true);
    }

    public Store loadTypParent(Map<String, Object> params) throws Exception {
        String lang = UtCnv.toString(params.get("lang"));
        Store st = mdb.createStore("Typ.lang");
        mdb.loadQuery(st, """
            select *
            from Typ
            where not (id in (select id from Typ where parent>0))
            order by ord
        """);

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Typ", lang, true);
    }

    //TypVer

    public Store loadVer(long typ, String lang) throws Exception {
        Store st = mdb.createStore("TypVer.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId
            from TypVer v
                left join TableLang l on l.nameTable='TypVer' and l.idTable=v.id and l.lang=:lang
            where ownerVer=:typ
            order by dend desc
         """, Map.of("typ", typ, "lang", lang));
        return st;
    }

    public Store loadVerRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("TypVer.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId
            from TypVer v
                left join TableLang l on l.nameTable='TypVer' and l.idTable=v.id and l.lang=:lang
            where v.id=:id
         """, Map.of("id", id, "lang", lang));
        return st;
    }


    /**
     * Insert TypeVer
     *
     * @param params Map
     * @return Store
     * @throws Exception onError
     */
    public Store insertVer(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntityVer(rec);
        //
        return loadVerRec(id, UtCnv.toString(rec.get("lang")));
    }

    /**
     * Update TypeVer
     *
     * @param params
     * @return
     * @throws Exception
     */
    public Store updateVer(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntityVer(rec);
        // Загрузка записи
        return loadVerRec(id, UtCnv.toString(rec.get("lang")));
    }

    public void deleteVer(Map<String, Object> rec) throws Exception {
        deleteEntityVer(rec);
    }

    //TypCharGr

    public Store loadTypCharGr(long id, String lang) throws Exception {
        String whe = " 0=0 order by tcg.ord";
        if (id > 0)
            whe = " tcg.id=" + id + " order by tcg.ord";

        Store st = mdb.createStore("TypCharGr.lang");
        mdb.loadQuery(st, """
                    select tcg.*, ltc.name, ltc.fullName, ltc.cmt,
                        ltv."name" as typName, lf."name" as fvName, t1.dbs, t1.dbNames
                    from typchargr tcg
                    left join TableLang ltc on ltc.nameTable='TypCharGr' and ltc.idTable=tcg.id and ltc.lang=:lang
                    inner join (
                        select t.typ, t.factorval,
                            string_agg (distinct  cast(t.db as varchar(4000)), ',' ) as dbs,
                            string_agg (distinct  cast(t.name as varchar(4000)), ', ' ) as dbNames
                        from (
                            select distinct c.typ, cfv.factorval, c."database" as db, ld.name
                            from cls c
                            inner join clsfactorval cfv on c.id=cfv.cls
                            left join TableLang ld on ld.nameTable='DataBase' and ld.idTable=c.database and ld.lang=:lang
                            where 0=0
                            order by c.typ, cfv.factorval
                        ) t
                        group by t.typ, t.factorval
                    ) t1 on tcg.typ=t1.typ and tcg.factorval=t1.factorval
                    left join TypVer tv on tv.ownerver=tcg.typ and tv.lastVer=1
                    left join TableLang ltv on ltv.nameTable='TypVer' and ltv.idTable=tv.id and ltv.lang=:lang
                    left join Factor fv on fv.id=tcg.factorval
                    left join TableLang lf on lf.nameTable='Factor' and lf.idTable=fv.id and lf.lang=:lang
                    where
                """+whe, Map.of("lang", lang));
        return st;
    }

    protected Store loadTypCharGrRec(long id, String lang) throws Exception {
        return loadTypCharGr(id, lang);
    }

    public Store loadTypCharGrInfo(long id, String lang) throws Exception {
        Store st = mdb.createStore("TypCharGr.info");
        mdb.loadQuery(st, """
            select tcg.id, tcg.cod, tcg.name as tcgName, t.modelName, t.dbs, t.dbTitle
            from typchargr tcg
            left join (
                select t.typ, t.factorval,
                    string_agg (distinct  cast(t.modelName as varchar(2000)), ', ' ) as modelName,
                    string_agg (distinct  cast(t.db as varchar(2000)), ',' ) as dbs,
                    string_agg (distinct  cast(t.name as varchar(2000)), ', ' ) as dbTitle
                from (
                    select distinct c.typ, cfv.factorval, c."database" as db, ld.name, d.modelName
                    from cls c
                    inner join clsfactorval cfv on c.id=cfv.cls
                    left join "database" d on c.database=d.id
                    left join TableLang ld on ld.nameTable='DataBase' and ld.idTable=d.id and ld.lang=:lang
                    where 0=0
                    order by c.typ, cfv.factorval
                    ) t
                    group by t.typ, t.factorval
            ) t on tcg.typ=t.typ and tcg.factorval=t.factorval
            where tcg.id=:id
        """, Map.of("id", id, "lang", lang));
        return st;
    }

    //todo Delete!
/*    public Store loadTypCharGr2(long typ) throws Exception {
        Store st = mdb.createStore("TypCharGr");
        mdb.loadQuery(st, "select * from TypCharGr where typ=:typ", Map.of("typ", typ));
        return st;
    }*/


    public Store loadTypClustFactorVal(long typ, String mode, String lang) throws Exception {
        Store st = mdb.createStore("Factor.cfv");
        String sql = """
                    with f as (
                        select factor as id from typclusterfactor where typ=:typ
                    )
                    select * from Factor
                    where id in (select id from f) or parent in (select id from f)
                        and id not in (select factorVal from TypCharGr where typ=:typ)
                """;
        if (mode.equals("upd")) {
            sql = """
                            with f as (
                                select factor as id from typclusterfactor where typ=:typ
                            )
                            select * from Factor
                            where id in (select id from f) or parent in (select id from f)
                    """;
        }
        mdb.loadQuery(st, sql, Map.of("typ", typ));
        st.forEach((r)-> {
            if (r.getLong("parent")==0)
                r.set("key", 0);
            else
                r.set("key", r.getLong("id"));
        });
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Factor", lang);
    }

    public Store insertTypCharGr(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);
        //
        return loadTypCharGrRec(id, UtCnv.toString(rec.get("lang")));
    }

    public Store updateTypCharGr(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        //
        return loadTypCharGrRec(id, UtCnv.toString(rec.get("lang")));
    }

    public void deleteTypCharGr(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    //TypCharGrProp
    public Store loadTypCharGrProp(Map<String, Object> params) throws Exception {
        //TypCharGrProp
        long typCharGr = UtCnv.toLong(params.get("typCharGr"));
        long typ = UtCnv.toLong(params.get("typORrel"));
        boolean isFlat = UtCnv.toBoolean(params.get("isFlat"));
        String lang = UtCnv.toString(params.get("lang"));
        Store st = mdb.createStore("TypCharGrProp.prop");
        String sql = """
            select
                tcp.id as typCharGrProp, typCharGrProp_measure, lpm.name as p_measure, propVal_measure,
                lm.name as pv_measure, storageType, 'p_'||p.id as id,
                case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                p.id as prop, p.propgr, p.propType, p.cod, lp.name, flatTable
            from typchargrprop tcp
                left join prop p on p.id=tcp.prop
                left join TableLang lp on lp.nameTable='Prop' and lp.idTable=p.id and lp.lang=:lang
                left join propVal pv on pv.id=tcp.propval_measure
                left join Measure m on m.id=pv.measure
                left join TableLang lm on lm.nameTable='Measure' and lm.idTable=m.id and lm.lang=:lang
                left join prop pm on pm.id=tcp.typchargrprop_measure
                left join TableLang lpm on lpm.nameTable='Prop' and lpm.idTable=pm.id and lpm.lang=:lang
            where tcp.typchargr=:tcg and tcp.prop is not null
        """;
        if (typ > 0) {
            sql = """
                        with tcg as (
                            select distinct id from typchargr where typ=:typ
                        )
                        select distinct 'p_'||p.id as id,
                            case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                            p.id as prop, p.propgr, p.propType, p.cod, p.name, flatTable
                        from typchargrprop tcp
                            left join prop p on p.id=tcp.prop
                        where tcp.prop is not null
                            and tcp.typchargr in (select id from tcg)
                    """;
            if (isFlat)
                sql = sql + " and tcp.flatTable is not null";
            else
                sql = sql + " and tcp.flatTable is null";

        }
        mdb.loadQuery(st, sql, Map.of("tcg", typCharGr, "typ", typ, "lang", lang));

        //mdb.outTable(st);


        // PropGr
        Store stGr = mdb.createStore("TypCharGrProp.propGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr
                    from PropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");

        //setParents for Meter, Factor... without MeterRate
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            if (r1.getString("parent").startsWith("p_"))
                continue;
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("propGr") == r2.getLong("propGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            idsGr.add(recGr.getString("id").substring(2));
            Object parentGr = recGr.get("parent");
            while (parentGr != "") {
                idsGr.add(UtCnv.toString(parentGr).substring(2));
                recGr = indStGr.get(parentGr);
                parentGr = recGr.get("parent");
            }
        }

        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        stGr = mdb.createStore("TypCharGrProp.prop");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, cod
                    from PropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        stGr = ut.getTranslatedStore(stGr,"PropGr", lang);
        //
        stGr.add(st);
        //mdb.outTable(stGr);

        Store stGrAll = mdb.createStore("TypCharGrProp.prop");
        String sqlGrAll = """
            select 'g_'||p.id as id, 'g_'||parent as parent, p.id as propGr,
                null as prop, null as propType, cod, lp.name, false as checked
            from PropGr p
                left join TableLang lp on lp.nameTable='PropGr' and lp.idTable=p.id and lp.lang=:lang
            where 0=0
            union all
            select 'p_'||p.id as id,
                case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent, p.propgr,
                p.id as prop, p.propType, p.cod, lp.name, false as checked
            from Prop p
                left join TableLang lp on lp.nameTable='Prop' and lp.idTable=p.id and lp.lang=:lang
            where 0=0
        """;
        mdb.loadQuery(stGrAll, sqlGrAll, Map.of("lang", lang));
        StoreIndex indStAll = stGrAll.getIndex("id");
        //mdb.outTable(stGrAll);


        //Analize parents
        Set<Object> ids = stGr.getUniqueValues("id");
        Set<Object> setPropGr = new HashSet<>();
        Set<Object> setProp = new HashSet<>();
        for (StoreRecord r : stGr) {
            if (r.getString("parent").isBlank())
                continue;
            if (!ids.contains(r.getString("parent"))) {
                setPropGr.add(r.getLong("propGr"));
                setProp.add(r.getLong("prop"));
            }
        }
        if (setProp.isEmpty()) {
            Stream<StoreRecord> strm = stGr.getRecords().stream().filter(r -> r.getLong("propType") == FD_PropType_consts.complex);
            List<String> lst = new ArrayList<>();
            List<String> lstTmp1 = new ArrayList<>();
            strm.forEach(s -> lst.add(s.getString("id")));
            //
            stGr.getRecords().forEach(r -> {
                if (lst.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                    lstTmp1.add(r.getString("id"));
                }
            });
            // ToDo
            List<String> lstTmp2 = new ArrayList<>();
            stGr.getRecords().forEach(r -> {
                if (lstTmp1.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                    lstTmp2.add(r.getString("id"));
                }
            });
            List<String> lstTmp3 = new ArrayList<>();
            stGr.getRecords().forEach(r -> {
                if (lstTmp2.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                    lstTmp3.add(r.getString("id"));
                }
            });
            //Last 4 lev
            stGr.getRecords().forEach(r -> {
                if (lstTmp3.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                }
            });

            return stGr;
        }

        String whGr = String.join(",", UtCnv.toString(setPropGr))
                .replace("[", "(").replace("]", ")");
        if (whGr.equals("()")) whGr = "(0)";
        Store stPath = mdb.createStore("DomainPath");
        mdb.loadQuery(stPath, "select id, parent, '' as path from Prop where propGr in " + whGr);
        UtMeterSoft utMeterSoft = new UtMeterSoft(mdb, 0, lang, false);
        utMeterSoft.setPath(stPath);
        //mdb.outTable(stPath);
        StoreIndex indStProp = stPath.getIndex("id");
        StoreIndex indStGrProp = stGr.getIndex("id");

        Store stGrCopy = mdb.createStore("TypCharGrProp.prop");
        Set<Object> idsPropGrDop = new HashSet<>();

        for (Object it : setPropGr) {
            String gr = "g_" + UtCnv.toString(it);
            for (StoreRecord r : stGr) {
                if (!setProp.contains(r.getLong("prop"))) continue;
                StoreRecord rec = indStProp.get(r.getLong("prop"));
                List<String> lstPrts = List.of(rec.getString("path").split(","));
                for (String el : lstPrts) {
                    boolean ok = false;
                    if (ids.contains("p_" + el)) {
                        //StoreRecord recGr = indStGrProp.get(UtCnv.toLong(el));
                        r.set("parent", "p_" + el);
                        ok = true;
                        break;
                    }
                    if (!ok) {
                        StoreRecord recGr = indStGrProp.get(gr);
                        if (recGr != null)
                            r.set("parent", recGr.getString("parent"));
                        else {
                            recGr = indStAll.get(gr);
                            while (true) {
                                if (recGr.getString("id").startsWith("g_"))
                                    break;
                                recGr = indStAll.get(recGr.getString("parent"));
                            }
                            r.set("parent", recGr.getString("parent"));
                            idsPropGrDop.add(recGr.getLong("propGr"));
                        }
                    }
                }
            }
        }

        //Defined groups for PropMeter
        whe = String.join(",", UtCnv.toString(idsPropGrDop))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        mdb.loadQuery(stGrCopy, sqlGr);
        //
        stGr.add(stGrCopy);

        //mdb.outTable(stGrCopy);
        //mdb.outTable(stGr);


        return stGr;
    }

    public Store loadTypCharGrPropForUpd(long typCharGr, String lang) throws Exception {
        //old values
        Store stOld = loadTypCharGrProp(Map.of("typCharGr", typCharGr, "lang", lang));
        StoreIndex indStOld = stOld.getIndex("id");

        Store st = mdb.createStore("TypCharGrProp.prop.checked");
        String sql = """
            select 'g_'||p.id as id, 'g_'||parent as parent, p.id as propGr,
            null as prop, null as propType, cod, lp.name, false as checked
            from PropGr p
                left join TableLang lp on lp.nameTable='PropGr' and lp.idTable=p.id and lp.lang=:lang
            where 0=0
            union all
            select 'p_'||p.id as id,
            case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent, p.propgr,
            p.id as prop, p.propType, p.cod, lp.name, false as checked
            from Prop p
                left join TableLang lp on lp.nameTable='Prop' and lp.idTable=p.id and lp.lang=:lang
            where 0=0
        """;
        mdb.loadQuery(st, sql, Map.of("lang", lang));
        List<String> lst = new ArrayList<>();
        for (StoreRecord r : st) {
            if (r.getLong("propType") == FD_PropType_consts.complex)
                lst.add(r.getString("id"));
            StoreRecord rec = indStOld.get(r.getString("id"));
            if (rec != null) {
                r.set("checked", true);
                r.set("typCharGrProp", rec.getLong("typCharGrProp"));
            }
        }
        //
        List<String> lstTmp1 = new ArrayList<>();
        st.getRecords().forEach(r -> {
            if (lst.contains(r.getString("parent"))) {
                r.set("isItem", true);
                lstTmp1.add(r.getString("id"));
            }
        });
        //ToDo
        List<String> lstTmp2 = new ArrayList<>();
        st.getRecords().forEach(r -> {
            if (lstTmp1.contains(r.getString("parent"))) {
                r.set("isItem", true);
                lstTmp2.add(r.getString("id"));
            }
        });
        List<String> lstTmp3 = new ArrayList<>();
        st.getRecords().forEach(r -> {
            if (lstTmp2.contains(r.getString("parent"))) {
                r.set("isItem", true);
                lstTmp3.add(r.getString("id"));
            }
        });
        //Last 4 lev
        st.getRecords().forEach(r -> {
            if (lstTmp3.contains(r.getString("parent"))) {
                r.set("isItem", true);
            }
        });

        return st;
    }

    public String saveTypCharGrProps(Map<String, Object> params) throws Exception {
        long typCharGr = UtCnv.toLong(params.get("typCharGr"));
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data");

        //Old Props in TypCharGrProp
        Store stOld = mdb.loadQuery("""
                    select c.id, c.prop, p.cod from TypCharGrProp c, Prop p
                    where c.prop=p.id and c.typCharGr=:g
                """, Map.of("g", typCharGr));
        Set<Object> oldPropIds = stOld.getUniqueValues("prop");
        Set<Object> oldIds = stOld.getUniqueValues("id");

        //String wheProps = UtString.join(oldPropIds, ",");
        //if (wheProps.equals("()")) wheProps = "(0)";

        //todo Old Props in CharGrProp with Data
        Set<Object> oldPropsData = new HashSet<>();
        /*
        делаем запрос и получаем oldPropsData
        */
        // пока
        oldPropsData = Set.of(0);

        //allIds = oldIds + newIds
        Set<Object> allIds = new HashSet<>();
        for (Map<String, Object> map : lstData) {
            allIds.add(UtCnv.toLong(map.get("typCharGrProp")));
        }
        //Deleting
        Set<String> codsPropForInfo = new HashSet<>();
        for (StoreRecord r : stOld) {
            if (!allIds.contains(r.getLong("id"))) {
                if (oldPropsData.contains(r.getLong("prop"))) {
                    codsPropForInfo.add(r.getString("cod"));
                } else {
                    try {
                        mdb.deleteRec("TypCharGrProp", r.getLong("id"));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        // Saving
        Store st = mdb.createStore("TypCharGrProp");
        for (Map<String, Object> map : lstData) {
            if (!oldIds.contains(UtCnv.toLong(map.get("typCharGrProp")))) {
                StoreRecord r = st.add(map);
                r.set("id", null);
                r.set("prop", UtCnv.toLong(map.get("prop")));
                r.set("typCharGr", typCharGr);
                r.set("storageType", FD_StorageType_consts.std);
                mdb.insertRec("TypCharGrProp", r, true);
            }
        }
        return String.join(",", codsPropForInfo);
    }

    //TypCharGrMultiProp
    public Store loadTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        long typCharGr = UtCnv.toLong(params.get("typCharGr"));
        long typ = UtCnv.toLong(params.get("typORrel"));

        Store st = mdb.createStore("TypCharGrProp.multiProp");
        String sql = """
                    select tcp.id as typCharGrProp, storageType,
                    'p_'||p.id as id, 'g_'||p.multiPropGr as parent, p.id as multiProp, p.multiPropGr, p.cod, p.name
                    from typchargrprop tcp
                    left join MultiProp p on p.id=tcp.multiProp
                    where tcp.typchargr=:tcg and tcp.multiProp is not null
                """;

        if (typ > 0)
            sql = """
                    with tcg as (
                        select distinct id from typchargr where typ=:typ
                    )
                    select 'p_'||p.id as id, 'g_'||p.multiPropGr as parent,
                    p.id as multiProp, p.multiPropGr, p.cod, p.name
                    from typchargrprop tcp
                    left join MultiProp p on p.id=tcp.multiProp
                    where tcp.multiProp is not null and tcp.typchargr in (select id from tcg)
                    """;


        mdb.loadQuery(st, sql, Map.of("tcg", typCharGr, "typ", typ));

        // MultiPropGr
        Store stGr = mdb.createStore("TypCharGrProp.multiPropGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as multiPropGr
                    from MultiPropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");
        //setParents
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("multiPropGr") == r2.getLong("multiPropGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            if (recGr != null) {
                idsGr.add(recGr.getString("id").substring(2));
                Object parentGr = recGr.get("parent");
                while (parentGr != "") {
                    idsGr.add(UtCnv.toString(parentGr).substring(2));
                    recGr = indStGr.get(parentGr);
                    parentGr = recGr.get("parent");
                }
            }
        }
        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        stGr = mdb.createStore("TypCharGrProp.multiProp");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as multiPropGr, cod, name
                    from MultiPropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        stGr.add(st);
        return stGr;
    }

    public Store loadTypCharGrMultiPropForUpd(long typCharGr) throws Exception {
        //old values
        Store stOld = loadTypCharGrMultiProp(Map.of("typCharGr", typCharGr));
        StoreIndex indStOld = stOld.getIndex("id");
        Store st = mdb.createStore("TypCharGrProp.multiProp.checked");
        String sql = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as multiPropGr,
                    null as multiProp, cod, name, false as checked
                    from MultiPropGr where 0=0
                    union all
                    select 'p_'||p.id as id, 'g_'||p.multiPropGr as parent, p.multiPropGr,
                    p.id as multiProp, p.cod, p.name, false as checked
                    from MultiProp p
                    where 0=0
                """;
        mdb.loadQuery(st, sql);
        for (StoreRecord r : st) {
            StoreRecord rec = indStOld.get(r.getString("id"));
            if (rec != null) {
                r.set("checked", true);
                r.set("typCharGrProp", rec.getLong("typCharGrProp"));
            }
        }

        //
        //mdb.outTable(stOld);
        //mdb.outTable(st);
        //
        return st;
    }

    public String saveTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        long typCharGr = UtCnv.toLong(params.get("typCharGr"));
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data");

        //Old multiProps in TypCharGrProp
        Store stOld = mdb.loadQuery("""
                    select c.id, c.multiProp, p.cod from TypCharGrProp c, MultiProp p
                    where c.multiProp=p.id and c.typCharGr=:g
                """, Map.of("g", typCharGr));
        Set<Object> oldPropIds = stOld.getUniqueValues("multiProp");
        Set<Object> oldIds = stOld.getUniqueValues("id");

        //todo Old Props in TypCharGrProp with Data
        Set<Object> oldPropsData = new HashSet<>();
        /*
        делаем запрос и получаем oldPropsData
        */
        // пока
        oldPropsData = Set.of(0);
        //allIds = oldIds + newIds
        Set<Object> allIds = new HashSet<>();
        for (Map<String, Object> map : lstData) {
            allIds.add(UtCnv.toLong(map.get("typCharGrProp")));
        }

        //Deleting
        Set<String> codsPropForInfo = new HashSet<>();
        for (StoreRecord r : stOld) {
            if (!allIds.contains(r.getLong("id"))) {
                if (oldPropsData.contains(r.getLong("multiProp"))) {
                    codsPropForInfo.add(r.getString("cod"));
                } else {
                    try {
                        mdb.deleteRec("TypCharGrProp", r.getLong("id"));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        // Saving
        Store st = mdb.createStore("TypCharGrProp");
        for (Map<String, Object> map : lstData) {
            if (!oldIds.contains(UtCnv.toLong(map.get("typCharGrProp")))) {
                StoreRecord r = st.add(map);
                r.set("id", null);
                r.set("multiProp", UtCnv.toLong(map.get("multiProp")));
                r.set("typCharGr", typCharGr);
                r.set("storageType", FD_StorageType_consts.std);
                mdb.insertRec("TypCharGrProp", r, true);
            }
        }
        return String.join(",", codsPropForInfo);
    }

    public void updateTypCharGrMultiProp(Map<String, Object> rec) throws Exception {
        mdb.updateRec("TypCharGrProp", Map.of("id", UtCnv.toLong(rec.get("typCharGrProp")),
                "storageType", UtCnv.toLong(rec.get("storageType"))));
        //
    }

    public void updateTypCharGrProp(Map<String, Object> rec) throws Exception {

        Long vft = null;
        if (UtCnv.toLong(rec.get("flatTable")) > 0)
            vft = UtCnv.toLong(rec.get("flatTable"));

        Map<String, Long> map = new HashMap<>();
        map.put("id", UtCnv.toLong(rec.get("typCharGrProp")));
        map.put("storageType", UtCnv.toLong(rec.get("storageType")));
        map.put("flatTable", vft);

        if (UtCnv.toLong(rec.get("typCharGrProp_measure")) > 0)
            map.put("typCharGrProp_measure", UtCnv.toLong(rec.get("typCharGrProp_measure")));
        if (UtCnv.toLong(rec.get("propVal_measure")) > 0)
            map.put("propVal_measure", UtCnv.toLong(rec.get("propVal_measure")));
        //
        mdb.updateRec("TypCharGrProp", map);

        // if ComplexType for childs...
        if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.complex) {
            List<Map<String, Object>> lst = UtJson.fromJson(UtCnv.toString(rec.get("children")), List.class);

            for (Map<String, Object> rc : lst) {
                map = new HashMap<>();
                map.put("id", UtCnv.toLong(rc.get("typCharGrProp")));
                map.put("storageType", UtCnv.toLong(rc.get("storageType")));
                map.put("flatTable", vft);

                if (UtCnv.toLong(rc.get("typCharGrProp_measure")) > 0)
                    map.put("typCharGrProp_measure", UtCnv.toLong(rc.get("typCharGrProp_measure")));
                if (UtCnv.toLong(rc.get("propVal_measure")) > 0)
                    map.put("propVal_measure", UtCnv.toLong(rc.get("propVal_measure")));
                //
                mdb.updateRec("TypCharGrProp", map);
            }
        }
        //

    }

    public Store loadPropMeasure(long typCharGr) throws Exception {

        Store st = mdb.createStore("TypCharGrProp.prop");
        String sql = """
                    select tcp.id as typCharGrProp,
                    'p_'||p.id as id, case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                    p.id as prop, p.propgr, p.propType, p.cod, p.name
                    from typchargrprop tcp
                    left join prop p on p.id=tcp.prop
                    where tcp.typchargr=:tcg and tcp.prop is not null and p.proptype=:pt
                """;
        mdb.loadQuery(st, sql, Map.of("tcg", typCharGr, "pt", FD_PropType_consts.measure));

        //mdb.outTable(st);


        // PropGr
        Store stGr = mdb.createStore("TypCharGrProp.propGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, 0 as prop
                    from PropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");

        //setParents for Meter, Factor... without MeterRate
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            if (r1.getString("parent").startsWith("p_"))
                continue;
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("propGr") == r2.getLong("propGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            if (recGr != null) {
                idsGr.add(recGr.getString("id").substring(2));
                Object parentGr = recGr.get("parent");
                while (parentGr != "") {
                    idsGr.add(UtCnv.toString(parentGr).substring(2));
                    recGr = indStGr.get(parentGr);
                    parentGr = recGr.get("parent");
                }
            }
        }

        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";

        stGr = mdb.createStore("TypCharGrProp.prop");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, 0 as prop, cod, name
                    from PropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        stGr.add(st);

        mdb.outTable(stGr);

        return stGr;
    }

    public Store loadMeasure(long prop, String lang) throws Exception {
        return mdb.loadQuery("""
            select m.id, l.name, m.parent, v.id as propval
            from measure m, TableLang l, PropVal v
            where prop=:prop and m.id=v.measure and l.nameTable='Measure' and l.idTable=m.id and l.lang=:lang
        """, Map.of("prop", prop, "lang", lang));
    }


}
