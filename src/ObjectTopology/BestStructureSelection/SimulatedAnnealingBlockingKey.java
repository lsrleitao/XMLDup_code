package ObjectTopology.BestStructureSelection;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import CandidateDefinition.Canopy;
import CandidateDefinition.SNM;
import CandidateDefinition.SNMII;
import CandidateDefinition.SuffixArrays;
import Clustering.Entropy;
import Clustering.KMeans;
import DuplicateDetection.ConfigGeneral;
import DuplicateDetection.XMLDup;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class SimulatedAnnealingBlockingKey {

    private static String _featuresOutputFile = "./XMLDup Data/StructureLearning/Features/featuresTrain.dat";
    private static String _bestStatesPath = "./XMLDup Data/StructureLearning/BestScoreTopologies";
    private static String _configFile = "./XMLDup Data/DBConfigFile/config_restaurants_classified.xml";
    private static String _dbPath = "./XMLDup Data/DB/restaurants_transformed.xml";
    private static int _maxNumberSAIterations = 500;
    

    //Percentagens dos varios tipos de pares a figurar na amostra
    private static int _criticalPairspercentageTP = 100;
    private static int _criticalPairspercentageFP = 100;
    private static int _criticalPairspercentageTN = 100;
    private static int _criticalPairspercentageFN = 100;

    private static String _resultsPath = "./XMLDup Data/Results/" + _dbPath.split("/")[_dbPath.split("/").length-1].replace(".xml", "");

    private static String _resultsMeasureRecall = "./XMLDup Data/RecallMeasure/results_pairs.txt";
    
    //private static HashSet<BigInteger> _visitedStructures = new HashSet<BigInteger>();
    private static HashSet<String> _visitedStructures = new HashSet<String>();
    
    private static int generateRandomNumberRange(int min, int max){

        if(max < 0) return 0;
        Random r = new Random();
        return r.nextInt((max-min)+1) + min;
    }


    private static double acceptanceFunction(double p, double p_new, int t){
        if(p_new <= p)
            return 1;
        else{
            return Math.exp( (p - p_new) / t );
        }
    }

    private static void loadAttributeFeaturesAssignementToDB(Map<String,Integer> attributes, DataBase db, String dbName){

        Map<String,List<Integer>> occurrences = db.getOccurrences(attributes);
        Map<String,List<Integer>> occurrences_st1 = db.getOccurrencesSegment(attributes,1,true);
        Map<String,List<Integer>> occurrences_st2 = db.getOccurrencesSegment(attributes,2,true);
        Map<String,List<Integer>> occurrences_st3 = db.getOccurrencesSegment(attributes,3,true);
        Map<String,List<Integer>> occurrences_end1 = db.getOccurrencesSegment(attributes,1,false);
        Map<String,List<Integer>> occurrences_end2 = db.getOccurrencesSegment(attributes,2,false);
        Map<String,List<Integer>> occurrences_end3 = db.getOccurrencesSegment(attributes,3,false);
        
        
        Map<String,List<Integer>> stringSizes = db.getStringSizes(attributes);
        
        Vector<String> v = new Vector<String>(attributes.keySet());
        Collections.sort(v);
        Iterator<String> it = v.iterator();
        Entropy e;
        //KMeans km;
        String key;

        while(it.hasNext()){
            key = it.next();

            //km = new KMeans(db, 1, key, 0.8);
            //km.clusterData();
            e = new Entropy(db, key);
            db.insertFeatureKeys(
                    
                    e.distinctiveness(occurrences.get(key)),
                    e.distinctiveness(occurrences_st1.get(key)),
                    e.distinctiveness(occurrences_st2.get(key)),
                    e.distinctiveness(occurrences_st3.get(key)),
                    e.distinctiveness(occurrences_end1.get(key)),
                    e.distinctiveness(occurrences_end2.get(key)),
                    e.distinctiveness(occurrences_end3.get(key)),

                    e.harmonicMean(occurrences.get(key)),
                    e.harmonicMean(occurrences_st1.get(key)),
                    e.harmonicMean(occurrences_st2.get(key)),
                    e.harmonicMean(occurrences_st3.get(key)),
                    e.harmonicMean(occurrences_end1.get(key)),
                    e.harmonicMean(occurrences_end2.get(key)),
                    e.harmonicMean(occurrences_end3.get(key)),
                    
                    e.stdDeviation(occurrences.get(key)),
                    e.stdDeviation(occurrences_st1.get(key)),
                    e.stdDeviation(occurrences_st2.get(key)),
                    e.stdDeviation(occurrences_st3.get(key)),
                    e.stdDeviation(occurrences_end1.get(key)),
                    e.stdDeviation(occurrences_end2.get(key)),
                    e.stdDeviation(occurrences_end3.get(key)),
                    
                    e.diversityMean(occurrences.get(key)),
                    e.diversityMean(occurrences_st1.get(key)),
                    e.diversityMean(occurrences_st2.get(key)),
                    e.diversityMean(occurrences_st3.get(key)),
                    e.diversityMean(occurrences_end1.get(key)),
                    e.diversityMean(occurrences_end2.get(key)),
                    e.diversityMean(occurrences_end3.get(key)),
                    
                    e.diversityIndex(occurrences.get(key)),
                    e.diversityIndex(occurrences_st1.get(key)),
                    e.diversityIndex(occurrences_st2.get(key)),
                    e.diversityIndex(occurrences_st3.get(key)),
                    e.diversityIndex(occurrences_end1.get(key)),
                    e.diversityIndex(occurrences_end2.get(key)),
                    e.diversityIndex(occurrences_end3.get(key)),
                    
                    db.getAttributesPerObject(key),
                    
                    e.getEntropyKey(occurrences.get(key)),
                    e.getEntropyKey(occurrences_st1.get(key)),
                    e.getEntropyKey(occurrences_st2.get(key)),
                    e.getEntropyKey(occurrences_st3.get(key)),
                    e.getEntropyKey(occurrences_end1.get(key)),
                    e.getEntropyKey(occurrences_end2.get(key)),
                    e.getEntropyKey(occurrences_end3.get(key)),
                    
                    e.average(stringSizes.get(key)),            
                    e.getEntropyKey(stringSizes.get(key)),
                    db.getEmptiness(key),
                    db.getDataTypePercentage(key, 1),
                    db.getDataTypePercentage(key, 2), 
                    db.getDataTypePercentage(key, 3),
                    key, dbName);

        }
    }
    
    private static void loadAttributeFeaturesAssignementToDBSingleKey(Map<String,Integer> attributes, DataBase db, String dbName){

        Map<String,List<Integer>> occurrences = db.getOccurrencesSingleKey(attributes);
        Map<String,List<Integer>> occurrences_st1 = db.getOccurrencesSegmentSingleKey(attributes,1,true);
        Map<String,List<Integer>> occurrences_st2 = db.getOccurrencesSegmentSingleKey(attributes,2,true);
        Map<String,List<Integer>> occurrences_st3 = db.getOccurrencesSegmentSingleKey(attributes,3,true);
        Map<String,List<Integer>> occurrences_end1 = db.getOccurrencesSegmentSingleKey(attributes,1,false);
        Map<String,List<Integer>> occurrences_end2 = db.getOccurrencesSegmentSingleKey(attributes,2,false);
        Map<String,List<Integer>> occurrences_end3 = db.getOccurrencesSegmentSingleKey(attributes,3,false);
        
        
        Map<String,List<Integer>> stringSizes = db.getStringSizesSingleKey(attributes);
        
        Vector<String> v = new Vector<String>(attributes.keySet());
        //Collections.sort(v);
        Iterator<String> it = v.iterator();
        Entropy e;
        //KMeans km;
        String key = "";

        while(it.hasNext()){
            key += it.next() + ",";
        }

        key = key.substring(0, key.length()-1);
        System.out.println("KEY!!! = " + key);
        
            //km = new KMeans(db, 1, key, 0.8);
            //km.clusterData();
            e = new Entropy(db, key);
            db.insertFeatureKeys(
                    
                    e.distinctiveness(occurrences.get(key)),
                    e.distinctiveness(occurrences_st1.get(key)),
                    e.distinctiveness(occurrences_st2.get(key)),
                    e.distinctiveness(occurrences_st3.get(key)),
                    e.distinctiveness(occurrences_end1.get(key)),
                    e.distinctiveness(occurrences_end2.get(key)),
                    e.distinctiveness(occurrences_end3.get(key)),

                    e.harmonicMean(occurrences.get(key)),
                    e.harmonicMean(occurrences_st1.get(key)),
                    e.harmonicMean(occurrences_st2.get(key)),
                    e.harmonicMean(occurrences_st3.get(key)),
                    e.harmonicMean(occurrences_end1.get(key)),
                    e.harmonicMean(occurrences_end2.get(key)),
                    e.harmonicMean(occurrences_end3.get(key)),
                    
                    e.stdDeviation(occurrences.get(key)),
                    e.stdDeviation(occurrences_st1.get(key)),
                    e.stdDeviation(occurrences_st2.get(key)),
                    e.stdDeviation(occurrences_st3.get(key)),
                    e.stdDeviation(occurrences_end1.get(key)),
                    e.stdDeviation(occurrences_end2.get(key)),
                    e.stdDeviation(occurrences_end3.get(key)),
                    
                    e.diversityMean(occurrences.get(key)),
                    e.diversityMean(occurrences_st1.get(key)),
                    e.diversityMean(occurrences_st2.get(key)),
                    e.diversityMean(occurrences_st3.get(key)),
                    e.diversityMean(occurrences_end1.get(key)),
                    e.diversityMean(occurrences_end2.get(key)),
                    e.diversityMean(occurrences_end3.get(key)),
                    
                    e.diversityIndex(occurrences.get(key)),
                    e.diversityIndex(occurrences_st1.get(key)),
                    e.diversityIndex(occurrences_st2.get(key)),
                    e.diversityIndex(occurrences_st3.get(key)),
                    e.diversityIndex(occurrences_end1.get(key)),
                    e.diversityIndex(occurrences_end2.get(key)),
                    e.diversityIndex(occurrences_end3.get(key)),
                    
                    db.getAttributesPerObject(key),
                    
                    e.getEntropyKey(occurrences.get(key)),
                    e.getEntropyKey(occurrences_st1.get(key)),
                    e.getEntropyKey(occurrences_st2.get(key)),
                    e.getEntropyKey(occurrences_st3.get(key)),
                    e.getEntropyKey(occurrences_end1.get(key)),
                    e.getEntropyKey(occurrences_end2.get(key)),
                    e.getEntropyKey(occurrences_end3.get(key)),
                    
                    e.average(stringSizes.get(key)),            
                    e.getEntropyKey(stringSizes.get(key)),
                    db.getEmptinessSingleKey(),
                    db.getDataTypePercentageSingleKey(key, 1),
                    db.getDataTypePercentageSingleKey(key, 2), 
                    db.getDataTypePercentageSingleKey(key, 3),
                    key, dbName);

        
    }

    private static void printBestTopologiesToFile(List<Document> lst, DocumentTransformer dt) throws Exception{
        File f = new File(_bestStatesPath);
        if(f.exists()){
            File[] fls = f.listFiles();
            for(int i = 0; fls.length > i ; i++){
                fls[i].delete();
            }
            f.delete();
        }
            
        f.mkdir();
        
        for(int i = 0; lst.size() > i ; i++){
            dt.createFile(lst.get(i), _bestStatesPath + "/conf" + i + ".xml");
        }
    }

    private static String[] initializeState(List<Integer> attrMaxLength, List<String> attributes){
        
        int paramsSize = attributes.size();
        String[] res = new String[paramsSize];
        
        for(int i = 0; paramsSize > i; i++){
            res[i] = "[0-" + (attrMaxLength.get(i)-1) + "]";
        }

        return res;
    }
    
    private static List<Integer> getAttributesMaxLength(DataBase rdb, List<String> attributes){
        
        List<Integer> res = new ArrayList<Integer>();
        int paramsSize = attributes.size();
        
        for(int i = 0; paramsSize > i; i++){
            res.add(rdb.getAttributeMaxLength(attributes.get(i)));
        }

        return res;
    }

    private static String generateKey(String[] state){

        String key = "";

        for(int i = 0; state.length > i ; i++){
            key = key + state[i] + "#";
        }

        return key;
    }
    
    private static String[] onlineNeighbor(String[] state, List<Integer> attrMaxLength){

        int maxSegmentSize = 5;
        
        Random r = new Random();
        int statePos = r.nextInt(state.length);
        int maxLength = attrMaxLength.get(statePos);

        String segment = state[statePos].substring(1, state[statePos].length()-1);
        String[] splitSeg = segment.split("-");
        int index1 = Integer.parseInt(splitSeg[0]);
        int index2 = Integer.parseInt(splitSeg[1]);
        
        if(r.nextBoolean()){
            index1 = 1;
            index2 = 0;
        }
        else{
        
            if(r.nextBoolean()){
                index1 = index1 + 1;
            }
            else{
                index2 = index2 - 1;
            }
        
        }
        
        if((index1 > 0 && index1 <= maxLength && index2 > 0 && index2 <= maxLength && index2 >= index1) ||
                (index1 == 1 && index2 == 0)){
            state[statePos] = "[" + index1 + "-" + index2 + "]";
        }

        String key = generateKey(state);

        while(_visitedStructures.contains(key)){//System.out.println("HIT!");
            
            statePos = r.nextInt(state.length);
            maxLength = attrMaxLength.get(statePos);
            
            index1 = r.nextInt(maxLength);
            index2 = generateRandomNumberRange(index1,index1+maxSegmentSize);
            
            if(index1 > 0 && index1 <= maxLength && index2 > 0 && index2 <= maxLength && index2 >= index1){
                state[statePos] = "[" + index1 + "-" + index2 + "]";
            }

            key = generateKey(state);
        }         

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());

        return state;     
    }
    

    private static Document loadParamsToConfigFile(Document d, String[] state, List<String> attributes){

        Element n = d.getDocumentElement();
        String params = "";
        for(int i = 0; state.length > i; i++){
            params += attributes.get(i) + "#" + state[i] + ",";
        }
        n.setAttribute("key", params.substring(0, params.length()-1));

        return d;

    }
    
    private static String[] copyState(String[] state){
        int size = state.length;
        String[] res = new String[size];
        for(int i = 0; size > i; i++){
            res[i]=state[i];
        }
        return res;
    }
    
    private static List<String> getTrueDuplicates(String resultsFile){

        List<String> dups = new ArrayList<String>();

        try{

            FileInputStream fin = new FileInputStream(resultsFile);
            BufferedInputStream bis = new BufferedInputStream(fin);
            BufferedReader in = new BufferedReader(new InputStreamReader(bis));
            boolean more = true;
            String aux = null;

            more = true;
            while(more){
                aux = in.readLine();
                if (aux == null) {
                    more = false;
                } else {
                    //System.out.println("Pairs: "+aux);
                    String[] line = aux.split("=");
                    String[] line_aux = line[1].split("and");

                    int obj1 = Integer.parseInt(line_aux[0].trim());
                    int obj2 = Integer.parseInt(line_aux[1].trim());

                    aux = in.readLine();
                    //System.out.println("SIM: "+aux);
                    line = aux.split("=");
                    //double similarity = Double.parseDouble(line[1].trim());

                    aux = in.readLine();
                    //System.out.println("DUP?: "+aux);
                    boolean isDup;
                    if(aux.equals("DUP? = true"))
                        isDup = true;
                    else
                        isDup = false;
                    

                    if(isDup){//System.out.println("entrou");
                        dups.add(obj1+"#"+obj2);
                    }
                   

                    in.readLine();
                }
            }

        }catch(Exception e){e.printStackTrace();}

        return dups;
    }

    private static Map<String,Integer> getAttibuteKeys(Document configFile){
        Map<String,Integer> res = new LinkedHashMap<String,Integer>();
        
        Element n = configFile.getDocumentElement();

        String attributeKeys = n.getAttribute("key");
    
        String[] keys = attributeKeys.split(",");
        
        for(int i = 0; keys.length > i; i++){
            String[] parts = keys[i].split("#");
            res.put(parts[0], Integer.parseInt(parts[1]));
        }

        return res;
    }
    
    private static double getPossibleRecall2(DataBase rdb, double recallPrecentageAllowed, String resultsFile){
        
        int occurringDups=0;
        List<String> dups = getTrueDuplicates(resultsFile);    
        Set<Integer> coOccurringPairs = rdb.getCoOccurringPairs();
        for(int i = 0; dups.size() > i; i++){
            int obj1 = Integer.parseInt(dups.get(i).split("#")[0]);
            int obj2 = Integer.parseInt(dups.get(i).split("#")[1]);
            
            if(coOccurringPairs.contains(obj1) && coOccurringPairs.contains(obj2)){
                occurringDups++;
            }
        }
        
        double existingDuplicates = rdb.getExistingDuplicates(); 
        double recall = (double)occurringDups/(double)existingDuplicates;
        
        return recall * recallPrecentageAllowed;

    }
    
    private static List<String> getAttributeList(NodeList nl, List<String> lst){

        for(int i=0; nl.getLength() > i; i++){


            Node n = nl.item(i);
            NodeList childNodes = n.getChildNodes();
            int cnSize = childNodes.getLength();
            String nodeName = n.getNodeName();
            if(n.getNodeType() == Node.ELEMENT_NODE){
                Element e = (Element)n;
                if(cnSize == 0){                  
                    if(e.getAttribute("useFlag").equals("1")){
                        lst.add(nodeName);
                    }
                }
                else if(cnSize > 0){
                    if(e.getAttribute("useFlag").equals("0")){
                        continue;
                    }
                    else{
                    lst = getAttributeList(childNodes, lst);
                    }
                }
            }
        }
        return lst;

    }
    
    public static int getKeySize(String[] state){
        
        int size = 0;
        
        for(int i = 0; state.length > i; i++){
            String s = state[i].substring(1, state[i].length()-1);
            String[] indexes = s.split("-");
            size = size + (Integer.parseInt(indexes[1])-Integer.parseInt(indexes[0])) + 1;
        }
        
        return size;
    }
    
    private static Document toggleBlockingState(Document d, boolean state){

        Element n = d.getDocumentElement();
        n.setAttribute("blocking", Boolean.toString(state));

        return d;

    }
    
    public static void main(String[] args) {

        try{

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document dbDoc = builder.parse(new File(_dbPath));

            DocumentBuilderFactory factory2 = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder2 = factory2.newDocumentBuilder();
            Document document = builder2.parse(new File(_configFile));
            String objectName = document.getDocumentElement().getNodeName();

            DocumentTransformer dt = new DocumentTransformer();
            document = dt.transformConfigFileUniqueLeaves(document);
            Map<String,List<String>> paths = dt.getPaths();
            String configFileEdited = _configFile.replace(".xml", "_edited.xml");
            dt.createFile(document, configFileEdited);
            String f_aux = _dbPath.replace(".xml", "_transformed.xml");
            String f_aux_sample = _dbPath.replace(".xml", "_transformed_sample.xml");

            ConfigGeneral cg = new ConfigGeneral(document);
            
            System.out.print("Pre-processing strings...");
            dbDoc = dt.preProcessXMLStrings(dbDoc, cg.getDbObjectName());
            System.out.println("FINISH!");
            
            List<String> attributes = getAttributeList(document.getDocumentElement().getChildNodes(), new ArrayList<String>());

            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false); //faz o mesmo que a linha seguinte mas escreve para o ficheiro
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);       

            DataBase rdb = DataBase.getSingletonObject(true);

            XMLToTable xmltt = new XMLToTable(db, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();                   

            Document db_sample;
            XMLDup xmldup;
            
            document = toggleBlockingState(document, false);

            //Extrair amostra da bd
            if(_criticalPairspercentageTP < 100 ||
                    _criticalPairspercentageFP < 100 ||
                    _criticalPairspercentageTN < 100 ||
                    _criticalPairspercentageFN < 100){

                xmldup = new XMLDup(document, _dbPath, cg);
                xmldup.run(db);

                db_sample = dt.extractTestSetCriticalElementsToDoc(_resultsPath+"/results_pairs.txt", db, objectName,
                        _criticalPairspercentageTP, _criticalPairspercentageFP, _criticalPairspercentageTN, _criticalPairspercentageFN);         
                //dt.createFile(db_sample, f_aux_sample);
            }
            else{
                db_sample = db;
            }
            
            
            List<Integer> attrMaxLength = getAttributesMaxLength(rdb,attributes);
            String[] blcKey_params = initializeState(attrMaxLength,attributes);
            
            
            document = loadParamsToConfigFile(document, blcKey_params, attributes);
            
               
            //para recolher o recall referencia
            xmldup = new XMLDup(document, f_aux_sample, cg);
            xmldup.run(db_sample);
            document = toggleBlockingState(document, true); 
            
            //double recall = getPossibleRecall2(rdb, 1, _resultsMeasureRecall);
            double recall = xmldup.getScores().get("Recall")*1;//permite 90% do recall obtido
            rdb.cleanBlockingKeysTable();
            
            //o ficheiro de configuração inicial tem de ter os pfs a 1
            //double p = xmldup.getScores().get("Recall");

            File f = new File(f_aux);
            f.deleteOnExit();
            File f2 = new File(f_aux_sample);
            f2.deleteOnExit();
            File f3 = new File(configFileEdited);
            //f3.deleteOnExit();
          
            int ks = getKeySize(blcKey_params);
            double p = xmldup.getScores().get("Pairs Compared");//Pairs Compared
            //double p = getKeySize(blcKey_params);//tamanho da chave
            System.out.println("Reference Recall: " + recall);

            //SA algorithm
            int chagesCnt = 0;
            String[] s = copyState(blcKey_params);

            int ks_new;
            double p_new;
            int imax  = _maxNumberSAIterations;

            double recall_iter;

            List<Document> bestScoreStates = new ArrayList<Document>();
            Document doc = (Document) document.cloneNode(true);
            //bestScoreStates.add(document);
            //String key = generateKey(s);
            //_visitedStructures.add(key);

            int i = imax;
            double p_best = p;
            String[] s_best = copyState(s);

            
            while(i > 0){
                int t = i/imax;
                System.out.print("Selecting Neighbor...");
                //double[] s_new = copyState(onlineNeighborProgressive(s));
                String[] s_new = copyState(onlineNeighbor(copyState(s),attrMaxLength));  
                System.out.println("FINISHED!");
              
                System.out.println("new state inicio");
                for(int k = 0; s_new.length > k; k++){
                    System.out.println(s_new[k]);
                }
                System.out.println("new state fim");

                //calcula novo score                          
                document = loadParamsToConfigFile(document, s_new, attributes);

                cg = new ConfigGeneral(document);

                xmldup = new XMLDup(document, f_aux_sample, cg);
                xmldup.run(db_sample);
                rdb.cleanBlockingKeysTable();

                ks_new = getKeySize(s_new);
                p_new = xmldup.getScores().get("Pairs Compared");//Pairs Compared
                //p_new = getKeySize(s_new);//tamanho da chave
                recall_iter = xmldup.getScores().get("Recall");
                System.out.println("Recall_iter: " + recall_iter);
                System.out.println("Reference Recall: " + recall);
                               
                System.out.println("KS: " + ks);
                System.out.println("KS_new: "+ ks_new);
                
                System.out.println("comparisons: " + p);
                System.out.println("comparisons_new: "+ p_new);

                //testa aceitacao
                if(acceptanceFunction(p, p_new, t) > Math.random() && recall_iter >= recall/* && ks_new < ks*/){
                    s = copyState(s_new);
                    p = p_new;
                    
                    ks = ks_new;

                    //dt.createFile(document, configFileEdited);

                    System.out.println("State changed. Current Score: " + p);
                    chagesCnt++;
                }

                if(p_new == p_best && recall_iter >= recall){
                    doc = (Document) document.cloneNode(true);
                    bestScoreStates.add(doc);
                }

                if(p_new >= p_best && recall_iter >= recall/* && ks_new <= ks*/){
                    p_best = p_new;
                    s_best = copyState(s_new);
                    bestScoreStates = new ArrayList<Document>();
                    doc = (Document) document.cloneNode(true);
                    bestScoreStates.add(doc);
                }

                i--;
                
                /*System.out.println("(new state)PFs inicio");
                for(int k = 0; s_new.length > k; k++){
                    System.out.println(s_new[k]);
                }
                System.out.println("(new state)PFs fim");
                
                System.out.println("(obtained score)PFs inicio");
                for(int k = 0; s.length > k; k++){
                    System.out.println(s[k]);
                }
                System.out.println("(obtained score)PFs fim");*/
                
                System.out.println("(best score)PFs inicio");
                for(int k = 0; s_best.length > k; k++){
                    System.out.println(s_best[k]);
                }
                System.out.println("(best score)PFs fim");
                
                System.out.println("Remaining States: " + i);
                
                System.out.println("OBTAINED SCORE: " + p);
                System.out.println("BEST SCORE: "+ p_best);

            }

            //SA algorithm END

            System.out.println("GENERATED STATES: " + _maxNumberSAIterations);
            System.out.println("OBTAINED SCORE: " + p);
            System.out.println("BEST SCORE: "+ p_best);
            System.out.println("REFERENCE RECALL: " + recall);
            
            System.out.println("ITERATIONS: " + imax);
            System.out.println("STATE CHANGED: " + chagesCnt + " TIMES");
            System.out.println("STATES WITH THE SAME SCORE SCORE: " + bestScoreStates.size());

            printBestTopologiesToFile(bestScoreStates, dt);

            System.out.println("Processing Complete DB With Best State...");
            document = loadParamsToConfigFile(document, s_best, attributes);
            cg = new ConfigGeneral(document);

            dt.createFile(db, f_aux);
            
            xmldup = new XMLDup(document, f_aux, cg);
            xmldup.run(db);
            

            if(bestScoreStates.size() > 0){
                System.out.println("Storing States Stats...");
                BestStructureStats bss = new BestStructureStats(_bestStatesPath, bestScoreStates.get(0), rdb);
                bss.storeLevelOccurrence();
                bss.storeLevelStats();
            }

            rdb = DataBase.getSingletonObject(false);

            //Map<String,Integer> attributeKeys = getAttibuteKeys(document);
            //System.out.println(attributeKeys);

            System.out.println("Loading Features to DB...");
            //loadAttributeFeaturesAssignementToDB(attributeKeys, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            //loadAttributeFeaturesAssignementToDBSingleKey(attributeKeys, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            System.out.println("Loading Features to DB...FINISHED!");

            System.out.print("Writing Features To File...");
            //rdb.writeTrainingKeyFeaturesToFile(_featuresOutputFile);         
            System.out.println("Writing Features To File...FINISHED!");

            rdb.closeConnection();


        }catch(Exception e){e.printStackTrace();}

    }

}


