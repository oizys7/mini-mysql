# 优化待办清单

系统功能完善后再处理的性能优化项。

## B+Tree 分裂路径优化

**问题**: `findAndInsertToParent()` 在节点分裂时从根开始 O(N) 遍历找父节点。

**位置**: `BPlusTree.java:298-358`

**优化方案**: 在 `findLeafNode` 下沉时用 `ArrayDeque<int[]>` 栈记录路径（parentPageId + childIndex），分裂时直接从栈顶回溯。

**预期效果**: 分裂操作从 O(N) 降为 O(logN)，N 为树中节点总数。

**优先级**: 低 — 当前数据规模（学习项目）无感知差异，树高极少超过 3 层。
