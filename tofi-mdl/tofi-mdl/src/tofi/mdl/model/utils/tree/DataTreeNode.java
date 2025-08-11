package tofi.mdl.model.utils.tree;

import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.List;

/**
 * Узел дерева
 */
public interface DataTreeNode {

    /**
     * DataStore, ассоциированное с этим узлом дерева.
     * Если узел не корневой, то это store из записи.
     *
     * @return
     */
    Store getStore();

    /**
     * Запись, ассоциированная с этим узлом дерева
     *
     * @return
     */
    StoreRecord getRecord();

    /**
     * Родительский узел.
     */
    DataTreeNode getParent();

    /**
     * Есть ли дочерние
     */
    boolean hasChilds();

    /**
     * Добавить дочерний
     *
     * @param rec запись для дочернего
     * @return узел
     */
    DataTreeNode addChild(StoreRecord rec);

    /**
     * Добавить дочерний
     *
     * @param node узел для дочернего
     * @return добавленный узел
     */
    DataTreeNode addChild(DataTreeNode node);

    /**
     * Дочерние узлы. Всегда не null.
     * Можно удалять и сортировать. Нельзя добавлять (для добавления используйте addChild).
     * Для проверки на наличие дочерних используйте
     * {@link DataTreeNode#hasChilds()}, что бы избежать не нужного расхода памяти.
     */
    List<DataTreeNode> getChilds();

    /**
     * Возвращает уровень, на котором находится узел.
     * Для корневого возвращается 0
     */
    int getLevel();


}
