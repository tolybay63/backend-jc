package tofi.api.mdl.utils.dimPeriod.helpers;

import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.api.mdl.utils.UtData;
import tofi.api.mdl.utils.tree.DataTreeNode;


import java.util.HashMap;
import java.util.Map;

public class DataStoreToDataTreeNode {
    private final Store resultStore;

    public DataStoreToDataTreeNode(Store st) {
        resultStore = st;
    }

    /**
     * переводит DataStore в DataTreeNode
     *
     * @param params
     * @param storeLoader
     * @return
     */
    public DataTreeNode transform(Map<String, Object> params, IStoreLoader storeLoader) throws Exception {
        loadAllStore(params, storeLoader);
        return UtData.createTreeIdParent(resultStore, "id", "parent");
    }

    /**
     * Рекурсивно возвращает все сторе дерева
     *
     * @param params
     * @param storeLoader
     */
    private void loadAllStore(Map<String, Object> params, IStoreLoader storeLoader) throws Exception {
        Store store = storeLoader.loadInitialStore(params);
        store.copyTo(resultStore);

        for (StoreRecord rec : store) {
            Map<String, Object> p = new HashMap<>(params);
            p.remove("node");
            p.put("node", rec.get("id"));
            rec.setValue("countPeriod", null);
            rec.setValue("lagCurrentDate", null);
            if (!rec.getBoolean("leaf"))
                loadAllStore(p, storeLoader);
        }
    }
}
