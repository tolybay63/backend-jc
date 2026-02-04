package tofi.mdl.model.utils

import jandcode.commons.error.XError
import jandcode.commons.variant.IVariantMap
import jandcode.commons.variant.VariantMap
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord

class UtEntityTranslate extends BaseMdbUtils {
    Mdb mdb

    UtEntityTranslate(Mdb mdb) {
        this.mdb = mdb
    }

    Store getTranslatedStore(Store st, String table, String lang, boolean hasVer=false) {
        if (lang.isEmpty())
            throw new XError("lang is empty")

        Store stLang = null
        if (!hasVer) {
            stLang = mdb.loadQuery("""
                select id, name, fullName, cmt, lang, idTable || '_' || lang as keyExact, idTable as key
                from TableLang where nameTable='${table}'
                order by lang desc
            """)
        }
        if (hasVer) {
            stLang = mdb.loadQuery("""
                select l.id, l.name, l.fullName, l.cmt, lang, idTable || '_' || lang as keyExact, idTable as key
                from TableLang l, ${table}Ver t where l.nameTable='${table}Ver' and t.id=l.idtable
                order by lang desc
            """)
        }
        StoreIndex indExact = stLang.getIndex("keyExact")
        StoreIndex indOther = stLang.getIndex("key")
        for (StoreRecord r : st) {
            StoreRecord rec
            if (hasVer)
                rec = indExact.get(r.getString("verId")+"_"+lang)
            else
                rec = indExact.get(r.getString("id")+"_"+lang)

            if (rec != null) {
                if (r.findField("name") != null)
                    r.set("name", rec.getString("name"))
                if (r.findField("fullName") != null)
                    r.set("fullName", rec.getString("fullName"))
                if (r.findField("cmt") != null) {
                    r.set("cmt", rec.getString("cmt"))

/*
                    if (r.getString("cmt").isEmpty() && !rec.getString("cmt").isEmpty() && rec.getString("lang") != lang) {
                        String s = tr.translateText(rec.getString("cmt"), rec.getString("lang"), lang)
                        r.set("cmt", s)
                    } else {
                        r.set("cmt", rec.getString("cmt"))
                    }
*/
                }

/*                StoreRecord recLg = mdb.createStoreRecord("TableLang", r)
                //StoreRecord recLg = stLg.add(r)
                recLg.set("nameTable", table)
                recLg.set("id", rec.getLong("id"))
                recLg.set("idTable", r.getLong("id"))
                if (hasVer) {
                    recLg.set("nameTable", table + 'Ver')
                    recLg.set("idTable", r.getLong("verId"))
                }
                recLg.set("lang", lang)
                mdb.updateRec("TableLang", recLg)*/


            } else {

                if (hasVer)
                    rec = indOther.get(r.getString("verId"))
                else
                    rec = indOther.get(r.getString("id"))

                if (rec != null) {
                    Translator tr = new Translator(mdb)
                    if (r.findField("name") != null) {
                        String s = tr.translateText(rec.getString("name"), rec.getString("lang"), lang)
                        r.set("name", s)
                    }
                    if (r.findField("fullName") != null) {
                        String s = tr.translateText(rec.getString("fullName"), rec.getString("lang"), lang)
                        r.set("fullName", s)
                    }
                    if (r.findField("cmt") != null) {
                        String s = tr.translateText(rec.getString("cmt"), rec.getString("lang"), lang)
                        r.set("cmt", s)
                    }
                    //
                    Store stLg = mdb.createStore("TableLang")
                    StoreRecord recLg = stLg.add(r)
                    recLg.set("id", 0)
                    recLg.set("nameTable", table)
                    recLg.set("idTable", r.getLong("id"))
                    if (hasVer) {
                        recLg.set("nameTable", table + 'Ver')
                        recLg.set("idTable", r.getLong("verId"))
                    }
                    recLg.set("lang", lang)
                    mdb.insertRec("TableLang", recLg, true)
                    //
                }
            }
        }
        return st
    }


    void deleteFromTableLang(String table, long id) {
        mdb.execQuery("""
            delete from TableLang where nameTable='${table}' and idTable=${id}
        """)
    }

    void insertToTableLang(Map<String, Object> params) {
        IVariantMap par = new VariantMap(params)
        if (par.getString("name").isEmpty())
            throw new XError("lang is empty")
        // table, idInTable, lang, name, fullName, cmt
        Store st = mdb.createStore("TableLang")
        StoreRecord rLang = st.add()
        long idLang = mdb.getNextId("TableLang")
        rLang.set("id", idLang)
        rLang.set("nameTable", par.getString("table"))
        rLang.set("idTable", par.getLong("id"))
        rLang.set("lang", par.getString("lang"))
        if (par.containsKey("name"))
            rLang.set("name", par.getString("name"))
        if (par.containsKey("fullName"))
            rLang.set("fullName", par.getString("fullName"))
        if (par.containsKey("cmt"))
            rLang.set("cmt", par.getString("cmt"))
        //
        mdb.insertRec("TableLang", rLang, false)
    }

}
