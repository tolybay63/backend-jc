package tofi.api.mdl.utils.tree;

import jandcode.commons.error.XError;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.ArrayList;
import java.util.List;

public class DataTreeNodeImpl implements DataTreeNode {
    private final StoreRecord rec;
    private DataTreeNode parent;
    private List<DataTreeNode> childs;

    public DataTreeNodeImpl(StoreRecord rec) {
        this.rec = rec;
    }

    @Override
    public Store getStore() {
        if (this.rec != null) {
            return this.rec.getStore();
        }
        return null;
    }

    @Override
    public StoreRecord getRecord() {
        return this.rec;
    }

    @Override
    public DataTreeNode getParent() {
        return this.parent;
    }

    @Override
    public boolean hasChilds() {
        return childs != null && childs.size() > 0;
    }

    @Override
    public DataTreeNode addChild(StoreRecord rec) {
        if (rec == null) {
            throw new XError("Запись для узла дерева не может быть null");
        }
        return addChild(new DataTreeNodeImpl(rec));
    }

    @Override
    public DataTreeNode addChild(DataTreeNode node) {
        ((DataTreeNodeImpl) node).parent = this;
        getChilds().add(node);
        return node;
    }

    @Override
    public List<DataTreeNode> getChilds() {
        if (childs == null) {
            childs = new ArrayList<DataTreeNode>();
        }
        return childs;
    }

    @Override
    public int getLevel() {
        int res = 0;
        DataTreeNodeImpl cur = this;
        while (cur != null) {
            if (cur.parent != null) {
                res++;
            }
            cur = (DataTreeNodeImpl) cur.parent;
        }
        return res;
    }
}
