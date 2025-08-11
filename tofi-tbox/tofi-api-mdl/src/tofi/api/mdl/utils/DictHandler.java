package tofi.api.mdl.utils;

import jandcode.core.dbm.dict.BaseDictHandlerLoadable;
import jandcode.core.dbm.dict.Dict;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreField;
import jandcode.core.store.StoreRecord;

import java.util.HashMap;
import java.util.Map;

public class DictHandler extends BaseDictHandlerLoadable {

    @Override
    public void loadDict(Mdb mdb, Dict dict, Store data) throws Exception {
        Store st = dict.createStore();
        String dictname = dict.getName();
        mdb.loadQuery(st, "select * from " + dictname + " order by ord");
        for (StoreRecord r : st) {
            Map<String, Object> map = new HashMap<>();
            for (StoreField f : r.getFields()) {
                map.put(f.getName(), r.getValue(f.getName()));
            }
            data.add(map);
        }
    }
}
