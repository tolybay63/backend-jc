package tofi.mdl.model.utils.tree;

public interface ITreeNodeVisitor {


    /**
     * Вызывается для каждого узла
     *
     * @param node узел дерева
     */
    void visitNode(DataTreeNode node);

}
