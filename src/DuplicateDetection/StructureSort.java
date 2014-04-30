package DuplicateDetection;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import org.xml.sax.SAXException;

import RDB.DataBase;

public class StructureSort {

    private DataBase _db;

    public StructureSort() {
        this._db = DataBase.getSingletonObject(false);
    }

    public ConfigObj sortStructure(ConfigObj configStructure) throws SAXException, IOException {

        sortConfigStructure(configStructure.getChildrenList());

        BufferedWriter out = new BufferedWriter(new FileWriter(XMLDup._resultsPath+ "/sorted_structure.txt"));
        percorreEstruturaXMLTeste(configStructure.getChildrenList(), out);
        out.close();

        return configStructure;
    }

    public List<ConfigObj> sortConfigStructure(List<ConfigObj> structureList) {
        
        //refactorConfigStructureByDepth(structureList);//passar para baixo dos AttributesPerObject
        
        
        //refactorConfigStructureByDistinctiveness(structureList);
        //refactorConfigStructureByAvgStrSize(structureList);
        //refactorConfigStructureByAttributesPerObject(structureList);
        
        

        // refactorConfigStructureByLeafNumber(structureList);
        // refactorConfigStructureByChildren(structureList);

        for (int i = 0; structureList.size() > i; i++) {
            if (structureList.get(i).getChildrenList().size() > 0)
                sortConfigStructure(structureList.get(i).getChildrenList());
        }

        return structureList;
    }

    public void refactorConfigStructureByDepth(List<ConfigObj> structureList) {

        Vector<Integer> vec_aux = new Vector<Integer>();
        int dl;

        for (int i = 0; i < structureList.size(); i++) {

            dl = getDeepestLeaf(structureList.get(i), 1);
            vec_aux.add(dl);
        }

        sortLevel(structureList, vec_aux);

    }

    public void refactorConfigStructureByChildren(List<ConfigObj> structureList) {

        Vector<Integer> vec_aux = new Vector<Integer>();
        int cs;

        for (int i = 0; i < structureList.size(); i++) {

            cs = structureList.get(i).getChildrenList().size();
            // if(cs == 0) cs = 1;
            vec_aux.add(cs);
        }
        sortLevel(structureList, vec_aux);
    }

    public void refactorConfigStructureByLeafNumber(List<ConfigObj> structureList) {

        Vector<Integer> vec_aux = new Vector<Integer>();
        int dl;

        for (int i = 0; i < structureList.size(); i++) {

            dl = getLeafNumber(structureList.get(i));
            vec_aux.add(dl);
        }
        sortLevel(structureList, vec_aux);
    }
    
    public void refactorConfigStructureByAttributesPerObject(List<ConfigObj> structureList) {

        Vector<Double> vec_aux = new Vector<Double>();
        double apo;

        for (int i = 0; i < structureList.size(); i++) {

            apo = getLowestAttributesPerObject(structureList.get(i));
            vec_aux.add(apo);
        }
        sortLevelDoubleAsc(structureList, vec_aux);
    }

    public void refactorConfigStructureByDistinctiveness(List<ConfigObj> structureList) {

        Vector<Double> vec_aux = new Vector<Double>();
        double idf;

        for (int i = 0; i < structureList.size(); i++) {

            idf = getHighestDistinctiveness(structureList.get(i));
            vec_aux.add(idf);
        }
        sortLevelDoubleDesc(structureList, vec_aux);
    }

    public void refactorConfigStructureByAvgStrSize(List<ConfigObj> structureList) {

        Vector<Double> vec_aux = new Vector<Double>();
        double avg_str_size;

        for (int i = 0; i < structureList.size(); i++) {

            avg_str_size = getLowestAvgStringSize(structureList.get(i));
            vec_aux.add(avg_str_size);
        }
        sortLevelDoubleAsc(structureList, vec_aux);
    }

    public void sortLevel(List<ConfigObj> level, Vector<Integer> vec_aux) {

        int j;
        int i;
        int key;
        ConfigObj key_obj;
        for (j = 1; j < level.size(); j++) {
            key = vec_aux.get(j);
            key_obj = level.get(j);
            i = j - 1;
            while (i > -1 && vec_aux.get(i) > key) {
                level.set(i + 1, level.get(i));
                vec_aux.set(i + 1, vec_aux.get(i));
                i = i - 1;

            }
            level.set(i + 1, key_obj);
            vec_aux.set(i + 1, key);
        }
    }

    // usado para o distinctiveness
    public void sortLevelDoubleDesc(List<ConfigObj> level, Vector<Double> vec_aux) {

        int i;
        double key;
        ConfigObj key_obj;
        for (int j = 1; j < level.size(); j++) {
            key = vec_aux.get(j);
            key_obj = level.get(j);
            i = j - 1;
            while (i > -1 && vec_aux.get(i) < key) {
                // trocar o segundo sinal do while
                // para fazer a ordenação
                // inversa.
                // '<' decrescente
                // '>' crescente
                level.set(i + 1, level.get(i));
                vec_aux.set(i + 1, vec_aux.get(i));
                i = i - 1;

            }
            level.set(i + 1, key_obj);
            vec_aux.set(i + 1, key);
        }
    }
    
    public void sortLevelDoubleAsc(List<ConfigObj> level, Vector<Double> vec_aux) {

        int i;
        double key;
        ConfigObj key_obj;
        for (int j = 1; j < level.size(); j++) {
            key = vec_aux.get(j);
            key_obj = level.get(j);
            i = j - 1;
            while (i > -1 && vec_aux.get(i) > key) {
                // trocar o segundo sinal do while
                // para fazer a ordenação
                // inversa.
                // '<' decrescente
                // '>' crescente
                level.set(i + 1, level.get(i));
                vec_aux.set(i + 1, vec_aux.get(i));
                i = i - 1;

            }
            level.set(i + 1, key_obj);
            vec_aux.set(i + 1, key);
        }
    }

    public Vector<Integer> insertDeepestLeaf(Vector<Integer> vec, int dl) {

        for (int i = 0; vec.size() > i; i++) {
            if (dl <= vec.get(i)) {
                vec.add(i, dl);
                return vec;
            }
        }
        vec.add(dl);
        return vec;
    }

    public int getDeepestLeaf(ConfigObj obj, int level) {

        int highestDepth = level;
        int depth;

        int size = obj.getChildrenList().size();
        if (size == 0)
            return highestDepth - 1;

        for (int i = 0; i < size; i++) {

            if (obj.getChildrenList().get(i).getChildrenList().size() > 0) {
                depth = getDeepestLeaf(obj.getChildrenList().get(i), level + 1);
                if (depth > highestDepth)
                    highestDepth = depth;
            }
        }

        return highestDepth;
    }

    public double getLowestIDF(ConfigObj obj) {

        double lowestIDF = Double.MAX_VALUE;
        double idf;

        int size = obj.getChildrenList().size();
        if (size == 0) {
            lowestIDF = _db.getAttributeDistinctiveness(obj.getNodeName());
        }

        for (int i = 0; i < size; i++) {

            if (obj.getChildrenList().get(i).getChildrenList().size() > 0) {
                idf = getLowestIDF(obj.getChildrenList().get(i));
            } else {
                idf = _db.getAttributeDistinctiveness(obj.getChildrenList().get(i).getNodeName());
            }

            if (idf < lowestIDF)
                lowestIDF = idf;
        }

        return lowestIDF;
    }
    
    public double getLowestAttributesPerObject(ConfigObj obj) {

        double lowestAPO = Double.MAX_VALUE;
        double apo;

        int size = obj.getChildrenList().size();
        if (size == 0) {
            lowestAPO = _db.getAttributesPerObject(obj.getNodeName());
        }

        for (int i = 0; i < size; i++) {

            if (obj.getChildrenList().get(i).getChildrenList().size() > 0) {
                apo = getLowestAttributesPerObject(obj.getChildrenList().get(i));
            } else {
                apo = _db.getAttributesPerObject(obj.getChildrenList().get(i).getNodeName());
            }

            if (apo < lowestAPO)
                lowestAPO = apo;
        }

        return lowestAPO;
    }
    
    public double getHighestDistinctiveness(ConfigObj obj) {

        double highestDstnc = Double.MIN_VALUE;
        double idf;

        int size = obj.getChildrenList().size();
        if (size == 0) {
            highestDstnc = _db.getAttributeDistinctiveness(obj.getNodeName());
        }

        for (int i = 0; i < size; i++) {

            if (obj.getChildrenList().get(i).getChildrenList().size() > 0) {
                idf = getHighestDistinctiveness(obj.getChildrenList().get(i));
            } else {
                idf = _db.getAttributeDistinctiveness(obj.getChildrenList().get(i).getNodeName());
            }

            if (idf > highestDstnc)
                highestDstnc = idf;
        }

        return highestDstnc;
    }

    public double getLowestAvgStringSize(ConfigObj obj) {

        double lowestAvgSS = Double.MAX_VALUE;
        double ss;

        int size = obj.getChildrenList().size();
        if (size == 0) {
            lowestAvgSS = _db.getAverageStringSize(obj.getNodeName());
        }

        for (int i = 0; i < size; i++) {

            if (obj.getChildrenList().get(i).getChildrenList().size() > 0) {
                ss = getLowestAvgStringSize(obj.getChildrenList().get(i));
            } else {
                ss = _db.getAverageStringSize(obj.getChildrenList().get(i).getNodeName());
            }

            if (ss < lowestAvgSS)
                lowestAvgSS = ss;
        }

        return lowestAvgSS;
    }

    public int getLeafNumber(ConfigObj obj) {

        int leafNumber = 0;
        int size = obj.getChildrenList().size();
        if (size == 0)
            return 1;

        for (int i = 0; i < size; i++) {

            if (obj.getChildrenList().get(i).getChildrenList().size() > 0) {
                leafNumber = leafNumber + getLeafNumber(obj.getChildrenList().get(i));
            } else
                leafNumber++;
        }

        return leafNumber;
    }

    // teste
    public void percorreEstruturaXMLTeste(List<ConfigObj> lst, BufferedWriter out) {

        FileHandler fh = new FileHandler();
        
        for (int i = 0; lst.size() > i; i++) {

            if (lst.get(i).getChildrenList().size() == 0) {

                fh.writeOpenFile(lst.get(i).getNodeName(), out);
                 //System.out.println("Name: "+lst.get(i).getNodeName());
                 //System.out.println("DProb: "+lst.get(i).getDefaultProb());
                 //System.out.println("Flag: "+lst.get(i).getUseFlag());
            }

            if (lst.get(i).getChildrenList().size() > 0) {

                fh.writeOpenFile(lst.get(i).getNodeName(), out);
                 //System.out.println("Name: "+lst.get(i).getNodeName());
                 //System.out.println("DProb: "+lst.get(i).getDefaultProb());
                 //System.out.println("Flag: "+lst.get(i).getUseFlag());
                // System.out.println("Formula: "+lst.get(i).getFormula());

                percorreEstruturaXMLTeste(lst.get(i).getChildrenList(), out);
            }
        }
    }
}
