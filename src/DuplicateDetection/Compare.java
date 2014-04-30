package DuplicateDetection;

import java.util.List;
import java.util.ArrayList;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import BayesianNetwork.BayesNetwork;
import BayesianNetwork.Formula;
import BayesianNetwork.ValueNode;
import ObjectTopology.XMLTransformer.XMLToXML;
import RDB.DataBase;

/**
 * @author Lu�s Leit�o
 */

public class Compare extends
        FileHandler {

    // Lista que guarda a compara��o por pares de duplicados ordenada
    private List<ObjSort> _dup = new ArrayList<ObjSort>();
    private DataBase _db;
    private SortingAlgos _sa = new SortingAlgos(_dup);

    public Compare() {

        _db = DataBase.getSingletonObject(false);
        XMLDup._duplicatesTotal = _db.getExistingDuplicates();
    }

    /**
     * Compares a pair of nodes from the database
     * 
     * @param indexni The index of the first node
     * @param indexnj The index of the second node
     * @param configGeneral The class which contains the configuration defined by the user
     * @param ni The first node
     * @param nj The second node
     * @return 1 or 0 if the nodes are considered to be duplicates or not
     */
    public double compareNodes(int indexni,
                               int indexnj,
                               ConfigGeneral configGeneral,
                               Node ni,
                               Node nj) {
        
        
         /*System.out.println("Compare "+ indexni + " and " + indexnj);
         if(indexni == 0 && indexnj == 81){
             System.out.println("N1: "+
                     ni.getTextContent());
             System.out.println("N2: "+
                     nj.getTextContent()); System.exit(0);
                     }*/


        ConfigObj configObj = configGeneral.getConfigStructure();
        double res;
        boolean dup;
        
        BayesNetwork bn = new BayesNetwork(configObj, ni, nj);
        ValueNode ns = bn.getBayesNetwork();// System.out.println(((ObjBayes)redeEst.get(4)).getNext());
        Formula fm = new Formula(configGeneral.getThreshold(), configGeneral.getUniquePF());
        res = fm.getNetworkScore(ns);
        
        // escreve no ficheiro duplicates_total(verdadeiros duplicados presentes
        // na base de dados)
        if (((Element) ni).getAttributes().item(0).getTextContent().equals(
                ((Element) nj).getAttributes().item(0).getTextContent())) {
            dup = true;
        } else {
            dup = false;
        }

        if (res >= configGeneral.getThreshold()) {

            XMLDup._duplicatesFound++;

            // Em memoria
            if (configGeneral.getStorageType() == 1) {

                ObjSort ordn = new ObjSort();
                ordn.setIndexNode1((short)indexni);
                ordn.setIndexNode2((short)indexnj);
                ordn.setSimilaridade((float)res);
                ordn.setDup(dup);
                //int indice = getPairIndex(_dup, res);
                //_dup.add(indice, ordn); nada eficiente. Neste caso usar lista ligada
                _dup.add(ordn); //quando a ordenacao é feita à posteriori
            }

            // BD relacional
            if (configGeneral.getStorageType() == 0) {

                _db.insertDuplicatesFound(indexni, indexnj, res, dup);
            }

        }
         //System.out.println("SIM "+(float)res);
        return res;
    }

    /**
     * Checks where an object pair should b inserted according to the similarity score
     * 
     * @param lst The list of duplicate object pairs found by the system
     * @param sim The similarity score of the object pair to be inserted
     * @return The position where the object pair should be inserted
     */
    public int getPairIndex(List<ObjSort> lst, double sim) {

        int i = 0;
        int size = lst.size();

        if (size == 0)
            return 0;

        while (size > i) {

            if (lst.get(i).getSimilaridade() >= sim)
                i++;
            else
                break;
        }

        return i;
    }

    /**
     * @return The array of duplicate object pairs found by the system
     */
    public List<ObjSort> getDup() {
        return _dup;
    }

}
