package ObjectTopology.SructureGenerator;

import java.util.Enumeration;

import simpack.accessor.tree.SimpleTreeAccessor;
import simpack.api.ITreeNode;
import simpack.exception.InvalidElementException;
import simpack.measure.tree.TreeEditDistance;
import simpack.util.conversion.CommonDistanceConversion;
import simpack.util.conversion.LogarithmicDistanceConversion;
import simpack.util.conversion.WorstCaseDistanceConversion;
import simpack.util.tree.TreeNode;
import simpack.util.tree.comparator.AlwaysTrueTreeNodeComparator;
import simpack.util.tree.comparator.NamedTreeNodeComparator;
import simpack.util.tree.comparator.NodeComparator;
import simpack.util.tree.comparator.TypedTreeNodeComparator;

public class TesteEditDistance {

    private static ITreeNode tree1;
    private static ITreeNode tree2;

    private static ITreeNode t1n1, t1n2, t1n3, t1n4, t1n5;

    private static ITreeNode t2n1, t2n2, t2n3, t2n4, t2n5, t2n6, t2n7;

    private static TreeEditDistance calc, calc1;    

    public static void testWorstCaseDistanceConversion() {
      
        try {
            calc = new TreeEditDistance(new SimpleTreeAccessor(tree1),
                    new SimpleTreeAccessor(tree2),
                    new NamedTreeNodeComparator(),
                    new WorstCaseDistanceConversion());
            
            calc.calculate();
            
            System.out.println("testWorstCaseDistanceConversion: " + calc.getTreeEditDistance());

        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (InvalidElementException e) {
            e.printStackTrace();
        }
    }

    public static void testCommonDistanceConversion() {

        try {
            calc = new TreeEditDistance(new SimpleTreeAccessor(tree1),
                    new SimpleTreeAccessor(tree2),
                    new NamedTreeNodeComparator(),
                    new CommonDistanceConversion());
            
            calc.calculate();
            
            System.out.println("testCommonDistanceConversion(): " + calc.getTreeEditDistance());

        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (InvalidElementException e) {
            e.printStackTrace();
        }
    }

    public static void testLogarithmicDistanceConversion() {
       
        try {
            calc = new TreeEditDistance(new SimpleTreeAccessor(tree1),
                    new SimpleTreeAccessor(tree2),
                    new NamedTreeNodeComparator(),
                    new LogarithmicDistanceConversion());
            
            calc.calculate();
            
            System.out.println("testLogarithmicDistanceConversion(): " + calc.getTreeEditDistance());

        } catch (NullPointerException e) {
            e.printStackTrace();
        } catch (InvalidElementException e) {
            e.printStackTrace();
        }
    }
    
    private static ITreeNode generateSampleT1() {
        t1n1 = new TreeNode(new String("t1n1"));
        t1n2 = new TreeNode(new String("t1n2"));
        t1n3 = new TreeNode(new String("t1n3"));
        t1n4 = new TreeNode(new String("t1n4"));
        t1n5 = new TreeNode(new String("t1n5"));

        // left
        t1n2.add(t1n3);
        t1n2.add(t1n4);

        // root
        t1n1.add(t1n2);
        t1n1.add(t1n5);

        return t1n1;
    }

    /**
     * Constructs the sample tree T1 on page 56 (Fig. 2.1) from "Algorithms on
     * trees and graphs" by Gabriel Valiente
     * 
     * @return sample tree T2
     */
    private static ITreeNode generateSampleT2() {
        t1n1 = new TreeNode(new String("t1n1alt"));
        t1n2 = new TreeNode(new String("t1n2"));
        t1n3 = new TreeNode(new String("t1n3"));
        t1n4 = new TreeNode(new String("t1n4"));
        t1n5 = new TreeNode(new String("t1n5"));

        // left
        t1n2.add(t1n3);
        t1n1.add(t1n4);

        // root
        t1n4.add(t1n2);
        t1n4.add(t1n5);

        return t1n1;
    }    

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        tree1 = generateSampleT1();
        tree2 = generateSampleT2();
        
        System.out.println(getChildNodes(tree1, 1));
        testWorstCaseDistanceConversion();
        testCommonDistanceConversion();
        testLogarithmicDistanceConversion();
    }
    
    private static int getChildNodes(ITreeNode tree, int currentChildren){
        
        int nodeNumber = tree.getChildCount() + currentChildren;
        Enumeration<ITreeNode> children = tree.children(); 
        
        while (children.hasMoreElements()) {
            ITreeNode node = children.nextElement();
            if(node.isLeaf())
                continue;
            else
                nodeNumber = getChildNodes(node, nodeNumber);
        }
        
        return nodeNumber;
        
    }

}
