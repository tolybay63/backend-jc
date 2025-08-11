package tofi.mdl.model.utils;

import jandcode.core.store.Store;
import jandcode.core.store.StoreField;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.tree.DataTreeNode;
import tofi.mdl.model.utils.tree.DataTreeNodeImpl;
import tofi.mdl.model.utils.tree.DataTreeNodeRootImpl;
import tofi.mdl.model.utils.tree.ITreeNodeVisitor;

import java.util.*;

public class UtData {


    /**
     * Создает пустой DataTreeNode
     *
     * @param rec запись для узла. Можно передавать null, если для корневого узла не
     *            нужна запись
     */
    public static DataTreeNode createTreeNode(StoreRecord rec) {
        return new DataTreeNodeImpl(rec);
    }

    /**
     * Создает корневой DataTreeNode для store
     *
     * @param store store для корневого узла
     */
    public static DataTreeNode createTreeNodeRoot(Store store) {
        return new DataTreeNodeRootImpl(store);
    }

    /**
     * Построение дерева по id-parent.
     *
     * @param store           для какого store
     * @param idFieldName     имя поля id
     * @param parentFieldName имя поля parent
     * @return корневой узел дерева (без записи)
     */
    public static DataTreeNode createTreeIdParent(Store store, String idFieldName, String parentFieldName) {
        DataTreeNode root = createTreeNodeRoot(store);
        //
        StoreField idField = store.getField(idFieldName);
        StoreField parentField = store.getField(parentFieldName);
        //

        ArrayList<DataTreeNode> lst = new ArrayList<DataTreeNode>();
        HashMap<Object, DataTreeNode> map = new HashMap<Object, DataTreeNode>();

        for (StoreRecord r : store) {
            DataTreeNode node = createTreeNode(r);
            lst.add(node);
            map.put(r.getValue(idField.getName()), node);
        }

        // добавляем в списки дочерних
        for (DataTreeNode node : lst) {
            DataTreeNode p = map.get(node.getRecord().getValue(parentField.getName()));
            Objects.requireNonNullElse(p, root).addChild(node);
        }

        // дерево готово...
        return root;
    }


    /**
     * Сканирование дерева
     *
     * @param root        с какого начинать
     * @param includeRoot вызывать ли visitor для root
     * @param visitor     интерфейс визитера
     */
    public static void scanTree(DataTreeNode root, boolean includeRoot, ITreeNodeVisitor visitor) {
        if (includeRoot) {
            visitor.visitNode(root);
        }

        if (root.hasChilds()) {
            Iterator<DataTreeNode> var3 = root.getChilds().iterator();

            while (var3.hasNext()) {
                DataTreeNode node = (DataTreeNode) var3.next();
                scanTree(node, true, visitor);
            }
        }
    }

    public static Set<Store> getTreeStores(DataTreeNode root) {
        final HashSet<Store> res = new HashSet<>();
        scanTree(root, true, new ITreeNodeVisitor() {
            public void visitNode(DataTreeNode node) {
                if (node.getRecord() != null) {
                    Store st = node.getRecord().getStore();
                    if (!res.contains(st)) {
                        res.add(st);
                    }
                }
            }
        });
        return res;
    }

}
