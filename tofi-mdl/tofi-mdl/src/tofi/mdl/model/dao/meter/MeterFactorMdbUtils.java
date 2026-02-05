package tofi.mdl.model.dao.meter;

import jandcode.commons.UtCnv;
import jandcode.commons.UtLang;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.dao.factor.FactorDao;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static jandcode.commons.UtCnv.toLong;

public class MeterFactorMdbUtils {
    Mdb mdb;

    MeterFactorMdbUtils(Mdb mdb) {
        this.mdb = mdb;
    }

    public Store load(Map<String, Object> params) throws Exception {
        long meterId = toLong(params.get("meter"));
        // for Translate
        Store stFactor = mdb.createStore("Factor.lang");
        mdb.loadQuery(stFactor, """
                    select f.*, '' as name, '' as fullName, '' as cmt
                    from Factor f, MeterFactor mf
                    where mf.meter=:id and f.id = mf.factor
                """, Map.of("id", meterId));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        ut.getTranslatedStore(stFactor, "Factor", lang);
        //

        Store st = mdb.createStore("MeterFactor.tree");
        mdb.loadQuery(st, """
                    select mf.id as id, mf.meter, f.id as factor, l.name, l.fullName, f.cod,
                    mf.ordDim, mf.ordFactorInDim
                    from Factor f, MeterFactor mf, TableLang l
                    where mf.meter=:id and f.id = mf.factor and l.nameTable='Factor' and f.id = l.idTable and l.lang=:lang
                    order by mf.ordDim, mf.ordFactorInDim
                """, Map.of("id", meterId, "lang", lang));
        //
        // дополняем измерениями
        //String pathKey = mdb.getApp().getAppdir() + File.separator + "triple-cab-444511-s7-d2a53763df4c.json";

        Store res = mdb.createStore("MeterFactor.tree");
        StoreRecord lastDimRec = null;
        String dimension = "Измерение";
        if (Objects.equals(lang, "kk")) {
            dimension = "Өлшем"; //tr.translateText(dimension, "ru", "kk");
        }
        if (Objects.equals(lang, "en-US")) {
            dimension = "Measurement"; //tr.translateText(dimension, "ru", "en-US");
        }
        long id = -1;
        for (StoreRecord r : st) {
            int curDim = r.getInt("ordDim");
            if (lastDimRec == null || lastDimRec.getInt("ordDim") != curDim) {
                lastDimRec = res.add(Map.of("id", id--, "cod", dimension + " " + curDim, "ordDim", curDim));
            }
            StoreRecord rc = res.add(r);
            rc.setValue("parent", lastDimRec.getLong("id"));
        }
        //
        return res;
    }

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("MeterFactor");
        return st.add();
    }

    public void insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long factor = UtCnv.toLong(rec.get("factor"));
        if (factor == 0) {
            throw new XError(UtLang.t("Поле [Фактор] обязательно"));
        } else {
            long ordd = UtCnv.toLong(rec.get("ordDim"));
            if (ordd == 0) {
                throw new XError(UtLang.t("Поле [Порядковый номер измерения] обязательно"));
            } else {
                // определяем размер исходный таблицы
                Store st = mdb.loadQuery("""
                            select * from MeterFactor where meter=:meterId order by ordDim, ordFactorInDim
                        """, Map.of("meterId", UtCnv.toLong(rec.get("meter"))));
                long ordfactor = 1;
                long maxorddim = 1;
                if (st.size() > 0) {
                    int i = 0;
                    for (StoreRecord r : st) {
                        if (r.getLong("factor") != factor) {
                            i++;
                        }
                    }
                    //
                    Store fs = mdb.loadQuery("""
                                select factor from MeterFactor where meter = :meter and ordDim = :ordDim
                            """, Map.of("meter", rec.get("meter"), "ordDim", ordd));

                    List<Long> factors = new ArrayList<>();
                    for (StoreRecord r : fs)
                        factors.add(r.getLong("factor"));
                    String text = testFR(factor, factors, true, UtCnv.toString(params.get("lang")));
                    if (!Objects.equals(text, "")) throw new XError(UtLang.t(text));
                    st = mdb.loadQuery("""
                                select max(ordDim) as maxd from MeterFactor where meter=:meterId
                            """, Map.of("meterId", UtCnv.toLong(rec.get("meter"))));
                    maxorddim = st.get(0).getLong("maxd");
                    if (ordd <= maxorddim) {
                        st = mdb.loadQuery("""
                                    select max(ordFactorInDim) as maxf from MeterFactor where meter=:meterId and ordDim=:ordd
                                """, Map.of("meterId", UtCnv.toLong(rec.get("meter")), "ordd", ordd));
                        ordfactor = st.get(0).getLong("maxf") + 1;
                    }
                }
                if (ordd <= maxorddim) {
                    Store factors = mdb.loadQuery("""
                                select distinct id from Factor
                                where parent is not null and id in (select distinct factorVal from MeterRateFV
                                    where meterRate in (select distinct id from MeterRate where meter=:meterId))
                            """, Map.of("meterId", UtCnv.toLong(rec.get("meter"))));

                    st = mdb.loadQuery("""
                                select factor from MeterFactor where meter = :meterId and ordDim = :ordd and factor != :factorId
                            """, Map.of("meterId", rec.get("meter"), "ordd", ordd, "factorId", factor));

                    if (factors.size() > 0) {
                        long k = 0;
                        for (StoreRecord r : st) {
                            for (StoreRecord rf : factors) {
                                if (r.getLong("factor") == rf.getLong("factor")) {
                                    k += 1;
                                }
                            }
                        }
                        if (k > 0) {
                            throw new XError("Добавление фактора в это измерение невозможно, так как существуют показатели, образованные из элементов этого измерения");
                        }
                    }
                }
                //добавляем новую запись
                mdb.insertRec("MeterFactor", Map.of("meter", rec.get("meter"), "factor", factor,
                        "ordDim", ordd, "ordFactorInDim", ordfactor), true);
            }
        }
    }

    public String testFR(long factorId, List<Long> factors, boolean flag, String lang) throws Exception {

        for (long fac : factors) {
            String errorText;
            long factor = toLong(fac);
            Store fr = mdb.loadQuery("""
                        select count(*) as cnt from factorvalrel
                        where (factor1 in
                            (select id from Factor where parent=:factorId) and factor2 in (select id from Factor where parent=:factor))
                            or (factor2 in (select id from Factor where parent=:factorId) and factor1 in (select id from Factor where parent=:factor))
                    """, Map.of("factorId", factorId, "factor", factor));

            Store fv1 = mdb.loadQuery("select count(*) as cnt from Factor where parent=:factorId",
                    Map.of("factorId", factorId));
            Store fv2 = mdb.loadQuery("select count(*) as cnt from Factor where parent=:factor",
                    Map.of("factor", factor));

            if (fr.get(0).getLong("cnt") == fv1.get(0).getLong("cnt") * fv2.get(0).getLong("cnt")) {
                FactorDao factorDao = mdb.createDao(FactorDao.class);
                StoreRecord f1 = factorDao.loadRec(Map.of("id", factorId, "lang", lang)).get(0);
                StoreRecord f2 = factorDao.loadRec(Map.of("id", factor, "lang", lang)).get(0);
                String text1 = "Добавляемый фактор ["+f1.getString("name") + "] не совместим с фактором ["+f2.getString("name") + "]";
                String text2 = "Перемещаемый фактор ["+f1.getString("name") + "] не совместим с фактором ["+f2.getString("name") + "]";

                if (lang.equals("kk")) {
                    text1 = "Қосылған фактор ["+f1.getString("name") + "] ["+f2.getString("name") + "] факторымен үйлесімді емес";
                    text2 = "Жылжытылған фактор ["+f1.getString("name") + "] ["+f2.getString("name") + "] факторымен үйлесімді емес";
                }
                if (lang.equals("en-US")) {
                    text1 = "The added factor ["+ f1.getString("name") + "] is not compatible with the factor ["+f2.getString("name")+"]";
                    text2 = "The moving factor ["+ f1.getString("name") + "] is not compatible with the factor ["+f2.getString("name")+"]";
                }

                if (flag)
                    errorText = text1;
                else
                    errorText = text2;

                return errorText;
            }
        }
        return "";
    }

    public void delFactor(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long meterId = UtCnv.toLong(rec.get("meter"));
        long factorId = UtCnv.toLong(rec.get("factor"));

        Store st = mdb.loadQuery("""
                    select * from meterratefv
                    where meterrate in (select id from meterrate where meter=:meterId) and
                        factorval in (select id from factor  where parent=:factorId)
                """, Map.of("meterId", meterId, "factorId", factorId));
        if (st.size() > 0) {
            MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(mdb);
            Store ds = mdbUtils.load(Map.of("meter", meterId));
            throw new XError("Показатель [" + ds.get(0).getString("cod") + "] [" + ds.get(0).getString("name") + "] образован от значения данного фактора");
        } else {
            st = mdb.loadQuery("""
                        select ordDim from MeterFactor where meter=:meterId and factor=:factorId
                    """, Map.of("meterId", meterId, "factorId", factorId));
            long orddim = st.get(0).getLong("ordDim");
            // удаляем запись
            mdb.execQuery("""
                        delete from MeterFactor where meter=:meterId and factor=:factorId
                    """, Map.of("meterId", meterId, "factorId", factorId));
            //
            st = mdb.loadQuery("""
                        select * from MeterFactor where meter=:meterId and ordDim=:ordd
                    """, Map.of("meterId", meterId, "ordd", orddim));
            if (st.size() == 0) {
                st = mdb.loadQuery("""
                            select * from MeterFactor where meter=:meterId and ordDim>:ordd
                        """, Map.of("meterId", meterId, "ordd", orddim));
                for (StoreRecord r : st) {
                    mdb.updateRec("MeterFactor",
                            Map.of("id", r.getLong("id"), "ordDim", r.getLong("ordDim") - 1));
                }
            } else {
                long ordfactor = 1;
                for (StoreRecord r : st) {
                    mdb.updateRec("MeterFactor",
                            Map.of("id", r.getLong("id"), "ordFactorInDim", ordfactor));
                    ordfactor += 1;
                }
            }
        }
    }


    public Store factors(Map<String, Object> params) throws Exception {
        long meter = UtCnv.toLong(params.get("meter"));
        Store st = mdb.createStore("Factor.lang");
        String sql = """
                    select * from Factor
                    where parent is null and id not in (
                        select factor from MeterFactor where meter=:m
                    )
                """;
        mdb.loadQuery(st, sql, Map.of("m", meter));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st, "Factor", lang);
    }

    public void changeOrd(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        boolean up = UtCnv.toBoolean(params.get("up"));
        String lang = UtCnv.toString(params.get("lang"));
        long meter = UtCnv.toLong(rec.get("meter"));
        long factor = UtCnv.toLong(rec.get("factor"));
        long ordd = UtCnv.toLong(rec.get("ordDim"));
        Store st = mdb.loadQuery("""
                    select * from MeterFactor where meter=:meter order by ordDim, ordFactorInDim
                """, Map.of("meter", meter));
        int k = 0;  //искомая позиция
        for (int i = 0; i < st.size(); i++) {
            if (st.get(i).getLong("factor") == UtCnv.toLong(rec.get("factor"))) {
                k = i;
                break;
            }
        }
        if (up) {
            if (st.get(k - 1).getLong("ordDim") == ordd) {
                long tmp = st.get(k - 1).getLong("ordFactorInDim");
                st.get(k - 1).set("ordFactorInDim", st.get(k).getLong("ordFactorInDim"));
                st.get(k).set("ordFactorInDim", tmp);
            } else {
                //check!
                long od = st.get(k - 1).getLong("ordDim");
                List<Long> factors = new ArrayList<Long>();
                for (int i = k - 1; i >= 0; i--) {
                    if (st.get(i).getLong("ordDim") == od) {
                        factors.add(st.get(i).getLong("factor"));
                    } else {
                        break;
                    }
                }
                String text = testFR(factor, factors, false, lang);
                if (!Objects.equals(text, "")) throw new XError(UtLang.t(text));
                //
                st.get(k).set("ordDim", st.get(k - 1).getLong("ordDim"));
                st.get(k).set("ordFactorInDim", st.get(k - 1).getLong("ordFactorInDim") + 1);
            }
            mdb.updateRec("MeterFactor", st.get(k - 1));
            mdb.updateRec("MeterFactor", st.get(k));
        } else {
            if (st.get(k + 1).getLong("ordDim") == ordd) {
                long tmp = st.get(k + 1).getLong("ordFactorInDim");
                st.get(k + 1).set("ordFactorInDim", st.get(k).getLong("ordFactorInDim"));
                st.get(k).set("ordFactorInDim", tmp);
            } else {
                //check!
                long od = st.get(k + 1).getLong("ordDim");
                List<Long> factors = new ArrayList<Long>();
                for (int i = k + 1; i < st.size(); i++) {
                    if (st.get(i).getLong("ordDim") == od) {
                        factors.add(st.get(i).getLong("factor"));
                    } else {
                        break;
                    }
                }
                String text = testFR(factor, factors, false, lang);
                if (!Objects.equals(text, "")) throw new XError(UtLang.t(text));
                //
                st.get(k).set("ordDim", st.get(k + 1).getLong("ordDim"));
                st.get(k).set("ordFactorInDim", st.get(k + 1).getLong("ordFactorInDim"));
                st.get(k + 1).set("ordFactorInDim", st.get(k + 1).getLong("ordFactorInDim") + 1);
            }
            mdb.updateRec("MeterFactor", st.get(k));
            mdb.updateRec("MeterFactor", st.get(k + 1));
        }
    }

    public void newDimension(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long meter = UtCnv.toLong(rec.get("meter"));
        long factor = UtCnv.toLong(rec.get("factor"));
        long ordd = UtCnv.toLong(rec.get("ordDim"));
        long maxDim = UtCnv.toLong(params.get("maxDim"));
        String lang = UtCnv.toString(params.get("lang"));
        Store st = mdb.loadQuery("""
                    select * from MeterFactor where meter=:meter and ordDim=:ordd
                """, Map.of("meter", meter, "ordd", ordd));
        if (st.size() == 1) {
            String txt = "Создано максимальное количество измерений";
            if (lang.equals("kk"))
                txt = "Өлшемдердің ең көп саны жасалды";
            if (lang.equals("en-US"))
                txt = "Maximum number of dimensions created";

            throw new XError(txt);
        }
        mdb.updateRec("MeterFactor", Map.of("id", UtCnv.toLong(rec.get("id")),
                "meter", meter, "factor", factor, "ordDim", maxDim, "ordFactorInDim", 1));
    }
    //

}
