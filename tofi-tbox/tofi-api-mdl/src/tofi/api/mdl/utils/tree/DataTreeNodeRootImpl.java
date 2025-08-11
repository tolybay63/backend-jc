package tofi.api.mdl.utils.tree;

import jandcode.core.store.Store;

public class DataTreeNodeRootImpl extends DataTreeNodeImpl {

    private final Store store;

    public DataTreeNodeRootImpl(Store store) {
        super(null);
        this.store = store;
    }

    public Store getStore() {
        return this.store;
    }
}
