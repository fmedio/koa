package net.rhapso.koa.tree;

class InsertionResult {
    final NodeRef newRoot;
    final boolean didUpdate;

    InsertionResult(NodeRef newRoot, boolean didUpdate) {
        this.newRoot = newRoot;
        this.didUpdate = didUpdate;
    }
}
