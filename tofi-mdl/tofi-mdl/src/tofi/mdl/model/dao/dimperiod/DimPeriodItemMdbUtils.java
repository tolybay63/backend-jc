package tofi.mdl.model.dao.dimperiod;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.dao.dimperiod.helpers.DataStoreToDataTreeNode;
import tofi.mdl.model.dao.dimperiod.helpers.IPeriodElement;
import tofi.mdl.model.dao.dimperiod.helpers.IStoreLoader;
import tofi.mdl.model.dao.dimperiod.helpers.PeriodItemTree;
import tofi.mdl.model.utils.tree.DataTreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DimPeriodItemMdbUtils {
    Mdb mdb;
    String domainResult;

    public DimPeriodItemMdbUtils(Mdb mdb, String domainResult) {
        this.mdb = mdb;
        this.domainResult = domainResult;
    }

    public Store load(Map<String, Object> params) throws Exception {
        long dimperiod = UtCnv.toLong(params.get("dimperiod"));
        StoreRecord dimperiodRec = mdb.createStoreRecord(domainResult);
        mdb.loadQueryRecord(dimperiodRec, "select * from DimPeriod where id=:id", Map.of("id", dimperiod));

        PeriodItemTree treeBuilder = new PeriodItemTree();
        treeBuilder.setDomain(domainResult);
        treeBuilder.setUt(mdb);
        treeBuilder.setPeriodTml(dimperiodRec.getLong("periodNameTml"));
        if (params.containsKey("currDate")) {
            String dte = UtCnv.toString(params.get("currDate"));
            treeBuilder.setCurrDate(dte);
        }

        treeBuilder.setPeriodElement(new IPeriodElement() {


            @Override
            public List<StoreRecord> getItemChildRec(String link) throws Exception {
                Long lParent = null;

                if (link != null && !link.isEmpty())
                    lParent = Long.parseLong(PeriodItemTree.extractPeriodItemId(link));

                Store st = mdb.createStore("DimPeriodItem.tree");
                loadDimPeriodItemChilds(st, dimperiod, lParent);
                return st.getRecords();
            }

            @Override
            public StoreRecord getItemRec(String itemLink) throws Exception {
                Store rec = mdb.createStore("DimPeriodItem.tree");
                mdb.loadQuery(rec, """
                            select * from DimPeriodItem dpi
                                left join FD_PeriodType pt on dpi.periodType=pt.id
                            where dpi.id=:id
                        """, Map.of("id", Long.parseLong(itemLink)));
                return rec.get(0);
            }

            @Override
            public List<Long> getNotInList(String link) throws Exception {
                long id = Long.parseLong(link);
                Store st = mdb.loadQuery("""
                            select t.* from
                            DimPeriodItemNotIn t
                            where t.dimperiodItem=:dpi
                        """, Map.of("dpi", id));
                List<Long> list = new ArrayList<Long>();
                for (StoreRecord rec : st) {
                    list.add(rec.getLong("numb"));
                }
                return list;
            }
        });

        return treeBuilder.loadPeriodTree(params);
    }

    protected void loadDimPeriodItemChilds(Store st, long dimperiod, Long lParent) throws Exception {
        if (lParent == null) {
            String sql = """
                        select * from DimPeriodItem dpi
                            left join FD_PeriodType pt on dpi.periodType=pt.id
                        where dpi.dimperiod=:dp and dpi.parent is null
                        order by dpi.ord
                    """;
            mdb.loadQuery(st, sql, Map.of("dp", dimperiod));
        } else {
            String sql = """
                        select * from DimPeriodItem dpi
                            left join FD_PeriodType pt on dpi.periodType=pt.id
                        where dpi.dimperiod=:dp and dpi.parent=:node
                        order by dpi.ord
                    """;
            mdb.loadQuery(st, sql, Map.of("dp", dimperiod, "node", lParent));
        }

        for (StoreRecord r : st) {
            Store ch = mdb.loadQuery("select id from DimPeriodItem where parent=:prt limit 1",
                    Map.of("prt", r.getLong("id")));
            if (ch.size() > 0) {
                r.setValue("leaf", false);
            } else {
                r.setValue("leaf", true);
            }
        }
    }

    public DataTreeNode loadTreeNode(Map<String, Object> params) throws Exception {
        DataStoreToDataTreeNode transformer = new DataStoreToDataTreeNode(mdb.createStore("DimPeriodItem.view"));
        return transformer.transform(params, new IStoreLoader() {
            public Store loadInitialStore(Map<String, Object> p) throws Exception {
                return load(p);
            }
        });
    }


}
