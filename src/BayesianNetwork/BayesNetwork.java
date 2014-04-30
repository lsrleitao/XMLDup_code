package BayesianNetwork;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import DuplicateDetection.ConfigObj;
import DuplicateDetection.StringMatching;

/**
 * @author Lu�s Leit�o
 */
public class BayesNetwork {
    // Formulas para os 3 tipos de ValueNode
    private static String SINGLE_NODE = "AND";
    private static String MCHILDREN_LEVEL1_NODE = "AVG";
    private static String MCHILDREN_LEVEL2_NODE = "OR";
    // Formula para o ChildSetNode
    private static String CHILDSETNODE = "AVG";
    // Formula para o ValueSetNode
    private static String VALUESETNODE = "AVG";

    private ValueNode _redeBayes;

    private StringMatching _sm = new StringMatching();

    public BayesNetwork(ConfigObj co, Node n1, Node n2) {

        _redeBayes = buildBayesNetwork(co, n1, n2);
    }

    public BayesNetwork() {
    }

    private List<ObjBayes> getAttributes(Node n1, Node n2, ConfigObj co) {

        String[] attributesName = co.getAttributes();
        List<ObjBayes> res = new ArrayList<ObjBayes>();

        NamedNodeMap a1;
        NamedNodeMap a2;

        if (n1 == null)
            a1 = null;
        else
            a1 = n1.getAttributes();

        if (n2 == null)
            a2 = null;
        else
            a2 = n2.getAttributes();

        ValueLeafNode vln;
        HiddenValueLeafNode hvln;

        String attr1 = "";
        String attr2 = "";
        Node attributeNode1 = null;
        Node attributeNode2 = null;
        String attributeName;

        int size = attributesName.length;
        for (int i = 0; size > i; i++) {

            if (a1 != null && a2 != null) {
                attributeName = attributesName[i];
                attributeNode1 = a1.getNamedItem(attributeName);
                attributeNode2 = a2.getNamedItem(attributeName);

                if (attributeNode1 != null && attributeNode2 != null) {
                    attr1 = attributeNode1.getTextContent();
                    attr2 = attributeNode2.getTextContent();
                }
            }

            if (a1 == null || a2 == null || attributeNode1 == null || attributeNode2 == null
                    || _sm.isEmptyString(attr1) || _sm.isEmptyString(attr2)) {
                hvln = new HiddenValueLeafNode(new ArrayList<ObjBayes>(), co.getSimCut(), co
                        .getDefaultProb(), "FINAL");
                res.add(hvln);
            } else {
                vln = new ValueLeafNode(new ArrayList<ObjBayes>(), co.getSimCut(), co
                        .getSimMeasure(), attr1, attr2, "FINAL", co.getPruningFactor());
                res.add(vln);
            }
        }
        return res;
    }

    private List<ObjBayes> mergeNodes(List<ObjBayes> src, List<ObjBayes> dst) {
        int size = src.size();
        for (int i = 0; size > i; i++) {
            dst.add(src.get(i));
        }
        return dst;
    }

    private ValueNode buildBayesNetwork(ConfigObj estrut, Node node1, Node node2) {

        String nodeName = estrut.getNodeName();
        List<ObjBayes> lst = new ArrayList<ObjBayes>();

        ChildSetNode csn = new ChildSetNode(
                assembleNetwork(estrut.getChildrenList(), node1, node2), CHILDSETNODE);

        lst.add(csn);
        ValueNode vn = new ValueNode(nodeName, lst, SINGLE_NODE, ValueNode.Generic);

        return vn;
    }

    /**
     * Builds the bayesian network
     * 
     * @param estrut The configuration structure with the configuration parameters
     * @param node1 The node to be compared
     * @param node2 The node to be compared
     * @return The List that represents the bayesian network
     */
    private List<ObjBayes> assembleNetwork(List<ConfigObj> estrut, Node node1, Node node2) {

        List<ConfigObj> st = estrut;
        List<ObjBayes> lt = new ArrayList<ObjBayes>();
        List<ObjBayes> lt_hidden_aux = new ArrayList<ObjBayes>();
        List<ObjBayes> lt_mc_aux = new ArrayList<ObjBayes>();
        List<ObjBayes> lt_comparable_aux = new ArrayList<ObjBayes>();
        List<Node> validElem1;
        List<Node> validElem2;

      
        //List<List<Node>> nl = obtemNivel(node1);
        //List<List<Node>> nl2 = obtemNivel(node2);        
        //nl = preencheNivel(nl, estrut);
        //nl2 = preencheNivel(nl2, estrut);

        //Alternativa a soluçao de cima. Parece nao ser mais rapido, apesar de
        //a implementaçao ser mais simples.
        Map<String,List<Node>> nl_on = obtemNivel2(node1);
        Map<String,List<Node>> nl2_on = obtemNivel2(node2);
        List<List<Node>> nl = preencheNivel2(nl_on, estrut);
        List<List<Node>> nl2 = preencheNivel2(nl2_on, estrut);
        
        
        ConfigObj co;
        List<Node> typeLst1;
        List<Node> typeLst2;

        int stSize = estrut.size();
        for (int i = 0; i < stSize; i++) {

            co = st.get(i);
            typeLst1 = nl.get(i);
            typeLst2 = nl2.get(i);

            boolean isLeaf = co.isLeaf();

            if (isLeaf) {
                validElem1 = getValidNodes(typeLst1, co);
                validElem2 = getValidNodes(typeLst2, co);
            } else {
                validElem1 = typeLst1;
                validElem2 = typeLst2;
            }

            // criar nova rede daqui para baixo.

            /*
             * System.out.println("NodeName: " + co.getNodeName());
             * System.out.println("size1: " + validElem1);
             * System.out.println("size2: " + validElem2);
             */

            ValueNode vn = null;

            int validElemSize1 = validElem1.size();
            int validElemSize2 = validElem2.size();

            if (validElemSize1 == 0 || validElemSize2 == 0) {// System.out.println("Hidden");
                vn = buildHiddenValueLeafNode(co, getAttributes(null, null, co));
            } else if (validElemSize1 == 1 && validElemSize2 == 1) {// System.out.println("SingleNode");
                vn = buildSingleValueNode(validElem1.get(0), validElem2.get(0), co);
            } else if (validElemSize1 > 1 || validElemSize2 > 1) {// System.out.println("Multiple");
                vn = buildMultipleChildrenLeaf2(validElem1, validElem2, co);
            }

            if (vn.isHidden())
                lt_hidden_aux.add(vn);
            else
                /* if(vn.isGenericNode() || vn.isMultipleChildren()) */
                lt_comparable_aux.add(vn);

            /*
             * else lt_mc_aux.add(vn);
             */

            // lt.add(vn);

        }

        //Junta os nós comparaveis com os que não apresentam conteudo e, como tal, vão assumir o valor da probabilidade de ausencia
        //Este nós sem conteudo são colocados na rede à frente dos nós com conteudo por o seu valor já ser conhecido e não apresentar necessidade de calculo de similaridade
        //Notar que a ordem definida pela estratégia de ordenação pode não ser respeitada se existirem nós sem conteudo nos objectos
        //Neste caso a ordem é respeitada apenas individualmente para cada lista
        lt = mergeNodes(lt_comparable_aux, lt_hidden_aux);
        // lt = mergeNodes(lt_mc_aux, lt);

        // System.out.println("rede "+lt);
        return lt;
    }

    private ValueNode buildHiddenValueLeafNode(ConfigObj co, List<ObjBayes> attributes) {
        List<ObjBayes> lst_aux = new ArrayList<ObjBayes>();
        List<ObjBayes> l_aux2 = new ArrayList<ObjBayes>();
        if (!co.considerOnlyAttributes() || attributes.size() == 0) {
            HiddenValueLeafNode hvln = new HiddenValueLeafNode(new ArrayList<ObjBayes>(), co
                    .getSimCut(), co.getDefaultProb(), "FINAL");
            lst_aux.add(hvln);
        }
        lst_aux = mergeNodes(attributes, lst_aux);
        ValueSetNode vsn = new ValueSetNode(lst_aux, VALUESETNODE);

        l_aux2.add(vsn);
        ValueNode vn = new ValueNode(co.getNodeName(), l_aux2, SINGLE_NODE, ValueNode.Hidden);

        return vn;
    }

    private ValueNode buildSingleValueNode(Node n1, Node n2, ConfigObj co) {

        List<ObjBayes> attributes = getAttributes(n1, n2, co);
        ValueNode vn;

        if (n1 == null || n2 == null || co.isDeadEndNode()) {
            vn = buildHiddenValueLeafNode(co, attributes);
        } else {

            List<ObjBayes> lst_aux = new ArrayList<ObjBayes>();
            List<ObjBayes> l_aux2 = new ArrayList<ObjBayes>();

            lst_aux = mergeNodes(attributes, lst_aux);

            if (co.isLeaf() && !co.considerOnlyAttributes()) {
                ValueLeafNode vln = new ValueLeafNode(new ArrayList<ObjBayes>(), co.getSimCut(), co
                        .getSimMeasure(), n1.getTextContent(), n2.getTextContent(), "FINAL", co.getPruningFactor());
                lst_aux.add(vln);
            }

            // No meio destes if's para garantir que o valueSetNode aparece
            // antes do childSetNode.
            if (lst_aux.size() != 0) {
                ValueSetNode vsn = new ValueSetNode(lst_aux, VALUESETNODE);
                l_aux2.add(vsn);
            }

            if (!co.isLeaf()) {
                ChildSetNode csn = new ChildSetNode(assembleNetwork(co.getChildrenList(), n1, n2),
                        CHILDSETNODE);
                l_aux2.add(csn);
            }

            vn = new ValueNode(co.getNodeName(), l_aux2, SINGLE_NODE, ValueNode.Generic);
        }

        return vn;
    }

    //Esta funcao controi os filhos multiplos comparando os que sao em menor numero em primeiro lugar
    private ValueNode buildMultipleChildrenLeaf2(List<Node> l1, List<Node> l2, ConfigObj co) {

        if (l1.get(0) == null || l2.get(0) == null)
            return buildSingleValueNode(null, null, co);

        List<ObjBayes> lst_aux = new ArrayList<ObjBayes>();
        
        
        List<Node> mcl1 = new ArrayList<Node>(l1);
        List<Node> mcl2 = new ArrayList<Node>(l2);
        
        int size1 = l1.size();
        int size2 = l2.size();
        
        if(size1 > size2){
            mcl1 = new ArrayList<Node>(l2);
            mcl2 = new ArrayList<Node>(l1);      
            size1 = l2.size();
            size2 = l1.size();
        }
        
        for (int i = 0; size1 > i; i++) {
            List<ObjBayes> lst2_aux = new ArrayList<ObjBayes>();
            for (int j = 0; size2 > j; j++) {
                lst2_aux.add(buildSingleValueNode(mcl1.get(i), mcl2.get(j), co));
            }
            ValueNode vn = new ValueNode(co.getNodeName(), lst2_aux, MCHILDREN_LEVEL2_NODE,
                    ValueNode.Generic);
            lst_aux.add(vn);
        }
        ValueNode vn = new ValueNode(co.getNodeName(), lst_aux, MCHILDREN_LEVEL1_NODE,
                ValueNode.MultipleChildren);

        return vn;
    }
    
    private ValueNode buildMultipleChildrenLeaf(List<Node> l1, List<Node> l2, ConfigObj co) {

        if (l1.get(0) == null || l2.get(0) == null)
            return buildSingleValueNode(null, null, co);

        List<ObjBayes> lst_aux = new ArrayList<ObjBayes>();
        
        int size1 = l1.size();
        int size2 = l2.size();
        
        for (int i = 0; size1 > i; i++) {
            List<ObjBayes> lst2_aux = new ArrayList<ObjBayes>();
            for (int j = 0; size2 > j; j++) {
                lst2_aux.add(buildSingleValueNode(l1.get(i), l2.get(j), co));
            }
            ValueNode vn = new ValueNode(co.getNodeName(), lst2_aux, MCHILDREN_LEVEL2_NODE,
                    ValueNode.Generic);
            lst_aux.add(vn);
        }
        ValueNode vn = new ValueNode(co.getNodeName(), lst_aux, MCHILDREN_LEVEL1_NODE,
                ValueNode.MultipleChildren);

        return vn;
    }

    /**
     * Normalizes the NodeList obtained by using getChildNodes(). Used to eliminate the
     * empty positions between the child nodes when the database file contains newlines,
     * tabs, etc
     * 
     * @param nl The node list
     * @return The normalized list
     */
    private List<Node> normalizaFilhos(NodeList nl) {

        List<Node> filhos = new ArrayList<Node>();
        Node n;

        int nlSize = nl.getLength();
        for (int i = 0; nlSize > i; i++) {
            n = nl.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {

                // if(nl.item(i).getChildNodes().getLength() != 0)//com esta
                // linha, folhas com conteudo vazio não são consideradas, caso
                // existam mais do mesmo tipo com conteudo.
                filhos.add(n);
            }

        }
        // System.out.println("filhos "+filhos);
        return filhos;

    }

    /**
     * Creates a list with the children of a given node. Each position of the list
     * contains another list. This inner list is used because of the cases where multiple
     * children of the same type may occur
     * 
     * @param n The given node
     * @return The resulting list containing the corresponding level of children
     */
    private List<List<Node>> obtemNivel(Node n) {

        List<Node> nl = normalizaFilhos(n.getChildNodes());
        List<List<Node>> nivel = new ArrayList<List<Node>>();
        List<Node> nivel_aux = new ArrayList<Node>();
        Node nd;

        Node nd2;

        int i = 0;
        int j = 1;

        int nlSize = nl.size();
        while (nlSize > i) {

            nd = nl.get(i);
            nivel_aux.add(nd);

            while (nlSize > j) {

                nd2 = nl.get(j);
                if (nd.getNodeName().equals(nd2.getNodeName())) {

                    nivel_aux.add(nd2);
                    nl.remove(j);//ineficiente
                    nlSize = nl.size();
                    j--;

                }

                j++;

            }

            nivel.add(nivel_aux);
            nivel_aux = new ArrayList<Node>();
            i++;
            j = i + 1;
        }
        // System.out.println("nivel: "+nivel);
        return nivel;

    }
    
    private Map<String,List<Node>> obtemNivel2(Node n) {

        List<Node> nl = normalizaFilhos(n.getChildNodes());
        List<Node> nivel_aux = new ArrayList<Node>();
 
        Map<String,List<Node>> nodeIndex = new HashMap<String,List<Node>>();

        Node nd;
        String ndName;
        int nlSize = nl.size();
        for(int i = 0; nlSize > i; i++) {
            nd = nl.get(i);
            ndName = nd.getNodeName();
            nivel_aux = nodeIndex.get(ndName);
            if(nivel_aux != null){
                nivel_aux.add(nd);
                nodeIndex.put(ndName, nivel_aux);          
            }
            else{
                List<Node> l_aux = new ArrayList<Node>();
                l_aux.add(nd);
                nodeIndex.put(ndName, l_aux);
            }
        }
           
        return nodeIndex;

    }

    // Esta fun��o recebe uma lista produzida pela fun�ao obtemNivel() e,
    // recorrendo � estrutura carregada do ficheiro de configura�ao, coloca as
    // sublistas que cont�m os elementos pela ordem apresentada no ficheiro de
    // configura�ao.
    // Nos locais onde faltam n�s, esta coloca uma lista vazia.
    /**
     * Receives a given level of an object produced in obtemNivel() and sorts the list
     * elements according to the order defined in the configuration structure. This way
     * the algorithm deals with objects with different order of occurrence of their
     * elements as long as they belong to the same level. If an element has the useFlag on
     * and it shows no presence in the level, it is inserted anyway for aplication of the
     * defaultProb.
     * 
     * @param nivel The list with some given level of the object
     * @param estrutura The configuration structure
     * @return The list with the elements in the sorted positions
     */
    private List<List<Node>> preencheNivel(List<List<Node>> nivel, List<ConfigObj> estrutura) {

        List<List<Node>> lt_aux = new ArrayList<List<Node>>();
        List<Node> lt_aux2 = new ArrayList<Node>();
        int flg = 0;
        String nodeName;
        List<Node> nl;

        int stSize = estrutura.size();
        for (int i = 0; i < stSize; i++) {
            nodeName = estrutura.get(i).getNodeName();
            for (int j = 0; j < nivel.size(); j++) {
                nl = nivel.get(j);
                if (nodeName.equals(nl.get(0).getNodeName())) {
                    lt_aux.add(nl);
                    flg = 1;
                    break;
                }

            }

            if (flg == 0) {
                Node n = null;
                lt_aux2.add(n);
                lt_aux.add(lt_aux2);
                lt_aux2 = new ArrayList<Node>();
            }

            flg = 0;
        }

        return lt_aux;
    }
    
    private List<List<Node>> preencheNivel2(Map<String,List<Node>> nivel, List<ConfigObj> estrutura) {

        List<List<Node>> lt_aux = new ArrayList<List<Node>>();
        List<Node> lt_aux2 = new ArrayList<Node>();
        String nodeName;

        int stSize = estrutura.size();
        for (int i = 0; i < stSize; i++) {        
            nodeName = estrutura.get(i).getNodeName();
            lt_aux2 = nivel.get(nodeName);
            if(lt_aux2 != null){
                lt_aux.add(lt_aux2);
            }
            else{
                Node n = null;
                lt_aux2 = new ArrayList<Node>();
                lt_aux2.add(n);
                lt_aux.add(lt_aux2);
            }
        }

        return lt_aux;
    }

    private boolean hasMatchingAttributes(Node n, ConfigObj co) {

        String[] attributesName = co.getAttributes();
        NamedNodeMap a = n.getAttributes();

        if (n == null || a == null)
            return false;

        Node n_aux;
        int size = attributesName.length;
        for (int i = 0; size > i; i++) {
            n_aux = a.getNamedItem(attributesName[i]);
            if (n_aux == null)
                continue;
            else if (!_sm.isEmptyString(n_aux.getTextContent())) {
                return true;
            }
        }
        return false;
    }

    private List<Node> getValidNodes(List<Node> lst, ConfigObj co) {
        StringMatching sm = new StringMatching();
        Node n;
        List<Node> res = new ArrayList<Node>();

        int lstSize = lst.size();
        for (int i = 0; lstSize > i; i++) {
            n = lst.get(i);
            if (n == null)
                continue;
            if (hasMatchingAttributes(n, co) || !sm.isEmptyString(n.getTextContent()))
                res.add(n);
        }
        return res;
    }

    /**
     * @return The bayesian network
     */
    public ValueNode getBayesNetwork() {
        return _redeBayes;
    }

}
