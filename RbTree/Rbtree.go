package rbtree

import (
	"bytes"
)

type RbTreeColor bool

var RED RbTreeColor = true
var BLACK RbTreeColor = false

type RbTreeKeyType interface {
	Compare(other RbTreeKeyType) int
}
type RbTreeValueType interface{}

type Uint32Key struct {
	Value uint32
}

// Compare 实现了 RbTreeKeyType 接口的 Compare 方法
func (k Uint32Key) Compare(other RbTreeKeyType) int {
	otherUint32, ok := other.(Uint32Key)
	if !ok {
		// 无法比较不同类型的键
		return -2 // -2 表示类型不匹配
	}
	if k.Value < otherUint32.Value {
		return -1
	} else if k.Value > otherUint32.Value {
		return 1
	}
	return 0
}

// BytesKey 是 []byte 类型的键的包装
type BytesKey struct {
	Value []byte
}

// Compare 实现了 RbTreeKeyType 接口的 Compare 方法
func (k BytesKey) Compare(other RbTreeKeyType) int {
	otherBytes, ok := other.(BytesKey)
	if !ok {
		// 无法比较不同类型的键
		return -2 // -2 表示类型不匹配
	}
	if bytes.Compare(k.Value, otherBytes.Value) < 0 {
		return -1
	} else if bytes.Compare(k.Value, otherBytes.Value) > 0 {
		return 1
	}
	return 0
}

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
	Sentinel *RbTreeNode
	NodeNum  int
}

func NewRbTree() *RbTree {
	Sentinel := &RbTreeNode{}
	Sentinel.Left = Sentinel
	Sentinel.Right = Sentinel
	Sentinel.Parent = Sentinel
	Sentinel.Key = nil
	Sentinel.Value = nil

	return &RbTree{
		Root:     Sentinel,
		Sentinel: Sentinel,
		NodeNum:  0,
	}
}

func (rbTree *RbTree) NewRbTreeNode(key RbTreeKeyType, value RbTreeValueType) *RbTreeNode {
	return &RbTreeNode{
		Color:  RED,
		Parent: rbTree.Sentinel,
		Left:   rbTree.Sentinel,
		Right:  rbTree.Sentinel,
		Key:    key,
		Value:  value,
	}
}

func (rbTree *RbTree) LeftRotate(node *RbTreeNode) {
	tmpNode := node.Right
	node.Right = tmpNode.Left
	if tmpNode.Left != rbTree.Sentinel {
		tmpNode.Left.Parent = node
	}
	tmpNode.Parent = node.Parent
	if node.Parent == rbTree.Sentinel {
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
	if tmpNode.Left != rbTree.Sentinel {
		tmpNode.Parent = node
	}
	tmpNode.Parent = node.Parent
	if node.Parent == rbTree.Sentinel {
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
	newNodeParent := rbTree.Sentinel
	tmpNode := rbTree.Root
	for tmpNode != rbTree.Sentinel {
		newNodeParent = tmpNode
		if node.Key.Compare(newNodeParent.Key) == -1 {
			tmpNode = tmpNode.Left
		} else if node.Key.Compare(newNodeParent.Key) == 1 {
			tmpNode = tmpNode.Right
		} else {
			return
		}
	}

	node.Parent = newNodeParent

	if newNodeParent == rbTree.Sentinel {
		rbTree.Root = node
	} else if node.Key.Compare(newNodeParent.Key) == -1 {
		newNodeParent.Left = node
	} else {
		newNodeParent.Right = node
	}

	node.Left = rbTree.Sentinel
	node.Right = rbTree.Sentinel
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

func (rbTree *RbTree) DeleteByKey(key RbTreeKeyType) {
	node := rbTree.GetNode(key)
	sentinelNode := rbTree.Sentinel
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

func (rbTree *RbTree) DeleteByNode(node *RbTreeNode) {
	sentinelNode := rbTree.Sentinel
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

func (rbTree *RbTree) GetNode(key RbTreeKeyType) *RbTreeNode {
	node := rbTree.Root
	for node != rbTree.Sentinel {
		if node.Key.Compare(key) == 1 {
			node = node.Right
		} else if node.Key.Compare(key) == -1 {
			node = node.Left
		} else {
			return node
		}
	}
	return rbTree.Sentinel
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

func (rbTree *RbTree) FindMaxKey(key RbTreeKeyType) *RbTreeNode {
	node := rbTree.Root
	targetNode := rbTree.Sentinel
	for node != rbTree.Sentinel {
		if node.Key.Compare(key) == 1 {
			targetNode = node
			node = node.Left
		} else if node.Key.Compare(key) == -1 {
			node = node.Right
		} else {
			targetNode = node
			break
		}
	}
	return targetNode
}
