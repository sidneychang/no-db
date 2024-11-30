package index

type RbTreeColor bool

var RED RbTreeColor = true
var BLACK RbTreeColor = false

type RbTreeKeyType int
type RbTreeValueType interface{}

type RbTreeNode struct {
	Color  RbTreeColor
	Parent *RbTreeNode
	Left   *RbTreeNode
	Right  *RbTreeNode
	Key    RbTreeKeyType
	Value  RbTreeValueType
}

type RbTree struct {
	Root *RbTreeNode
	// 定义哨兵节点，即为红黑树中的空节点
	sentinel *RbTreeNode
	NodeNum  int
}

func NewRbTree() *RbTree {
	sentinel := &RbTreeNode{}
	sentinel.Left = sentinel
	sentinel.Right = sentinel
	sentinel.Parent = sentinel
	sentinel.Key = -1
	sentinel.Value = nil

	return &RbTree{
		Root:     sentinel,
		sentinel: sentinel,
		NodeNum:  0,
	}
}

func (rbTree *RbTree) LeftRotate(node *RbTreeNode) {
	tmpNode := node.Right
	node.Right = tmpNode.Left
	if tmpNode.Left != rbTree.sentinel {
		tmpNode.Left.Parent = node
	}
	tmpNode.Parent = node.Parent
	if node.Parent == rbTree.sentinel {
		rbTree.Root = tmpNode
	} else if node == node.Parent.Left {
		node.Parent.Left = tmpNode
	} else {
		node.Parent.Right = tmpNode
	}
	tmpNode.Left = node
	node.Parent = tmpNode
}

func (rbTree *RbTree) RightRotate(node *RbTreeNode) {
	tmpNode := node.Left
	node.Left = tmpNode.Right
	if tmpNode.Left != rbTree.sentinel {
		tmpNode.Parent = node
	}
	tmpNode.Parent = node.Parent
	if node.Parent == rbTree.sentinel {
		rbTree.Root = tmpNode
	} else if node == node.Parent.Left {
		node.Parent.Left = tmpNode
	} else {
		node.Parent.Right = tmpNode
	}
	tmpNode.Left = node
	node.Parent = tmpNode
}

func (rbTree *RbTree) InsertNewNode(node *RbTreeNode) {
	newNodeParent := rbTree.sentinel
	tmpNode := rbTree.Root
	for tmpNode != rbTree.sentinel {
		newNodeParent = tmpNode
		if node.Key < newNodeParent.Key {
			tmpNode = tmpNode.Left
		} else if node.Key > newNodeParent.Key {
			tmpNode = tmpNode.Right
		} else {
			return
		}
	}

	node.Parent = newNodeParent

	if newNodeParent == rbTree.sentinel {
		rbTree.Root = node
	} else if node.Key < newNodeParent.Key {
		newNodeParent.Left = node
	} else {
		newNodeParent.Right = node
	}

	node.Left = rbTree.sentinel
	node.Right = rbTree.sentinel
	node.Color = RED

	rbTree.InsertFixUp(node)
	rbTree.NodeNum++

}

func (rbTree *RbTree) InsertFixUp(node *RbTreeNode) {
	for node != rbTree.Root && node.Parent.Color != BLACK {
		if node.Parent == node.Parent.Parent.Left {
			uncleNode := node.Parent.Parent.Right
			if uncleNode.Color == RED {
				node.Parent.Color = BLACK
				uncleNode.Color = BLACK
				node.Parent.Parent.Color = RED
				node = node.Parent.Parent
			} else {
				if node == node.Parent.Right {
					node = node.Parent
					rbTree.LeftRotate(node)
				}
				node.Parent.Color = BLACK
				node.Parent.Parent.Color = RED
				rbTree.RightRotate(node.Parent.Parent)
			}
		} else {
			uncleNode := node.Parent.Parent.Left
			if uncleNode.Color == RED {
				node.Parent.Color = BLACK
				uncleNode.Color = BLACK
				node.Parent.Parent.Color = RED
				node = node.Parent.Parent
			} else {
				if node == node.Parent.Left {
					node = node.Parent
					rbTree.RightRotate(node)
				}
				node.Parent.Color = BLACK
				node.Parent.Parent.Color = RED
				rbTree.LeftRotate(node.Parent.Parent)
			}
		}
	}
	rbTree.Root.Color = BLACK
}

func (rbTree *RbTree) Delete(key RbTreeKeyType) {
	node := rbTree.getNode(key)
	sentinelNode := rbTree.sentinel
	if node == sentinelNode {
		return
	}
	willDeleteNode := sentinelNode
	willDeleteChildNode := sentinelNode

	if node.Left == sentinelNode || node.Right == sentinelNode {
		willDeleteNode = node
	} else {
		willDeleteNode = node.FindMinNodeBy(sentinelNode)
	}

	if willDeleteNode.Left != sentinelNode {
		willDeleteChildNode = willDeleteNode.Left
	} else if willDeleteNode.Right != sentinelNode {
		willDeleteChildNode = willDeleteNode.Right
	}

	willDeleteChildNode.Parent = willDeleteNode.Parent

	if willDeleteNode.Parent == sentinelNode {
		rbTree.Root = willDeleteChildNode
	} else if willDeleteNode == willDeleteNode.Parent.Left {
		willDeleteNode.Parent.Left = willDeleteChildNode
	} else {
		willDeleteNode.Parent.Right = willDeleteChildNode
	}

	if willDeleteNode != node {
		node.Key = willDeleteNode.Key
		node.Value = willDeleteNode.Value
	}

	if willDeleteNode.Color == BLACK {
		rbTree.DeleteFixUp(willDeleteChildNode)
	}

	willDeleteNode = nil
	rbTree.NodeNum--

}

func (rbTree *RbTree) DeleteFixUp(node *RbTreeNode) {
	for node != rbTree.Root && node.Color == BLACK {
		if node == node.Parent.Left {
			brotherNode := node.Parent.Right
			if brotherNode.Color == RED {
				brotherNode.Color = BLACK
				node.Parent.Color = RED
				rbTree.LeftRotate(node.Parent)
				brotherNode = node.Parent.Right
			}

			if brotherNode.Left.Color == BLACK && brotherNode.Right.Color == BLACK {
				brotherNode.Color = RED
				node = node.Parent
			} else {
				if brotherNode.Right.Color == BLACK {
					brotherNode.Left.Color = BLACK
					brotherNode.Color = RED
					rbTree.RightRotate(brotherNode)
					brotherNode = node.Parent.Right
				}
				brotherNode.Color = node.Parent.Color
				node.Parent.Color = BLACK
				brotherNode.Right.Color = BLACK
				rbTree.LeftRotate(node.Parent)
				node = rbTree.Root
			}
		} else {
			brotherNode := node.Parent.Left
			if brotherNode.Color == RED {
				brotherNode.Color = BLACK
				node.Parent.Color = RED
				rbTree.RightRotate(node.Parent)
				brotherNode = node.Parent.Left
			}
			if brotherNode.Left.Color == BLACK && brotherNode.Right.Color == BLACK {
				brotherNode.Color = RED
				node = node.Parent
			} else {
				if brotherNode.Left.Color == BLACK {
					brotherNode.Right.Color = BLACK
					brotherNode.Color = RED
					rbTree.LeftRotate(brotherNode)
					brotherNode = node.Parent.Left
				}
				brotherNode.Color = node.Parent.Color
				node.Parent.Color = BLACK
				brotherNode.Left.Color = BLACK
				rbTree.RightRotate(node.Parent)
				node = rbTree.Root
			}
		}
	}
	node.Color = BLACK
}

func (rbTree *RbTree) getNode(key RbTreeKeyType) *RbTreeNode {
	node := rbTree.Root
	for node != rbTree.sentinel {
		if node.Key > key {
			node = node.Right
		} else if node.Key < key {
			node = node.Left
		} else {
			return node
		}
	}
	return rbTree.sentinel
}

func (node *RbTreeNode) FindMinNodeBy(rbTreeNilNode *RbTreeNode) *RbTreeNode {
	newNode := node
	for newNode.Left != rbTreeNilNode {
		newNode = newNode.Left
	}
	return newNode
}

func (node *RbTreeNode) FindMaxNodeBy(rbTreeNilNode *RbTreeNode) *RbTreeNode {
	newNode := node
	for newNode.Right != rbTreeNilNode {
		newNode = newNode.Left
	}
	return newNode
}

func (rbTree *RbTree) findmaxkey(key RbTreeKeyType) RbTreeValueType {
	node := rbTree.Root
	targetNode := rbTree.sentinel
	for node != rbTree.sentinel {
		if node.Key > key {
			targetNode = node
			node = node.Left
		} else if node.Key < key {
			node = node.Right
		} else {
			targetNode = node
			break
		}
	}
	return targetNode.Value
}
