package tofi.api.mdl.utils.tree;

public interface ITreeNodeVisitor {


    /**
     * Вызывается для каждого узла
     *
     * @param node узел дерева
     */
    public void visitNode(DataTreeNode node);

}
