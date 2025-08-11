package tofi.api.mdl.utils

import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.dbm.domain.Domain
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord

class UtMeterSoft {
    Mdb mdb;
    public String domainResult = "MeterRate.soft.tree";
    public long meter = 0;
    public boolean withNameMeter = true;

    UtMeterSoft(Mdb mdb, long meter, boolean withNameMeter = true) {
        this.mdb = mdb;
        this.meter = meter;
        this.withNameMeter = withNameMeter;
    }

    public void setPath(Store st) throws Exception {
        Domain dn = mdb.createDomain(st);
        Store stCpy = mdb.createStore(dn);
        st.copyTo(stCpy);
        for (StoreRecord r : st) {
            List<Long> lstParent = new ArrayList<>();
            if (r.getLong("parent") > 0) {
                long prt = r.getLong("parent");
                while (true) {
                    StoreRecord rP = getParentRecord(stCpy, prt);
                    if (rP == null) {
                        throw new XError("Error!");
                    }
                    lstParent.add(rP.getLong("id"));
                    prt = rP.getLong("parent");
                    if (rP.getLong("parent")==0) break;
                }
            } else {
                lstParent.add(0L);
            }
            r.set("path", String.join(",", UtCnv.toString(lstParent)
                    .replace("[","").replace("]","").replace(" ","")));
        }
    }

    protected static StoreRecord getParentRecord(Store st, long recordParent) {
        Map<Long, StoreRecord> map = new HashMap<>();
        for (StoreRecord r : st) {
            if (r.getLong("id")==recordParent) {
                map.put(recordParent, r);
                break;
            }
        }
        return map.get(recordParent);
    }

}
