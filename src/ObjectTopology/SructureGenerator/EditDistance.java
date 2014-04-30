package ObjectTopology.SructureGenerator;

import java.util.Enumeration;

import simpack.accessor.tree.SimpleTreeAccessor;
import simpack.api.ITreeNode;
import simpack.api.impl.AbstractDistanceConversion;
import simpack.exception.InvalidElementException;
import simpack.measure.tree.TreeEditDistance;
import simpack.util.conversion.CommonDistanceConversion;
import simpack.util.conversion.WorstCaseDistanceConversion;
import simpack.util.tree.TreeNode;
import simpack.util.tree.comparator.AlwaysTrueTreeNodeComparator;
import simpack.util.tree.comparator.NamedTreeNodeComparator;
import simpack.util.tree.comparator.NodeComparator;
import simpack.util.tree.comparator.TypedTreeNodeComparator;

public class EditDistance {

    private ITreeNode builtNodeTree(int[][] structure, int column, ITreeNode currentNode, int numLeaves){
        
        ITreeNode node = currentNode;
        
        for(int j = 0; structure.length-1 > j ; j++){
            
            if(structure[column][j] == 1){
                if(j < numLeaves){
                    TreeNode tn = new TreeNode(j);
                    node.add(tn);
                }
                else{
                    TreeNode tn = new TreeNode(j);
                    node.add(tn);
                    builtNodeTree(structure, j, tn, numLeaves);
                }
            }
        }

        return node;
        
    }
    
    public double getDistance(int[][] structure1, int[][] structure2) throws InvalidElementException{
        
        int numLeaves = (structure1.length+1)/2;
        int initialColumn = numLeaves+(numLeaves-2);
        
        ITreeNode tree1 = builtNodeTree(structure1, initialColumn, new TreeNode(-1), numLeaves);
        ITreeNode tree2 = builtNodeTree(structure2, initialColumn, new TreeNode(-1), numLeaves);
        
        TreeEditDistance ted = new TreeEditDistance(
                new SimpleTreeAccessor(tree1),
                new SimpleTreeAccessor(tree2),
                new NamedTreeNodeComparator(),
                new CommonDistanceConversion());
        
        int maxNodeNumber = Math.max(getNodeNumber(tree1,1), getNodeNumber(tree2,1));
        
        ted.calculate();
        
        return ted.getTreeEditDistance()/maxNodeNumber;
    }
    
    private int getNodeNumber(ITreeNode tree, int currentChildren){
        
        int nodeNumber = tree.getChildCount() + currentChildren;
        Enumeration<ITreeNode> children = tree.children(); 
        
        while (children.hasMoreElements()) {
            ITreeNode node = children.nextElement();
            if(node.isLeaf())
                continue;
            else
                nodeNumber = getNodeNumber(node, nodeNumber);
        }
        
        return nodeNumber;
        
    }

}
