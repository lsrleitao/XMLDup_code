package ObjectTopology.BestStructureSelection;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

public class SimulatedAnnealingBlockingKeyNL {

    private static String _bestStatesPath = "./XMLDup Data/StructureLearning/BestScoreTopologies";
    private static String _configFile = "./XMLDup Data/DBConfigFile/config_imdb_fd_classified.xml";
    private static String _dbPath = "./XMLDup Data/DB/movie_restructured_transformed.xml";
    private static int _maxNumberSAIterations = 500;
    
    //private static HashSet<BigInteger> _visitedStructures = new HashSet<BigInteger>();
    private static HashSet<String> _visitedStructures = new HashSet<String>();
    
    private static int generateRandomNumberRange(int min, int max){

        if(max < 0) return 0;
        Random r = new Random();
        return r.nextInt((max-min)+1) + min;
    }


    private static double acceptanceFunction(double p, double p_new, int t){
        if(p_new > p)
            return 1;
        else{
            return Math.exp( (p_new - p) / t );
        }
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

    private static String[] initializeState(List<Integer> attrLength){
        
        int paramsSize = attrLength.size();
        String[] res = new String[paramsSize];
        
        for(int j = 0; paramsSize > j; j++){
            String entry = "";
            for(int i = 0; attrLength.get(j) > i; i++){
                entry = entry + i + ",";
            }
            entry = entry.substring(0, entry.length()-1);
            res[j] = entry;
        }
        
        return res;
    }
    
    private static String[] initializeEmptyState(List<Integer> attrLength){
        
        int paramsSize = attrLength.size();
        String[] res = new String[paramsSize];
        
        for(int j = 0; paramsSize > j; j++){
            res[j] = "#";
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
    
    private static String getCharIndexes(int chars, Map<Integer,Float> charDstnct){
        
        String res = "";
        
        int i = 0;
        for(Map.Entry<Integer, Float> e: charDstnct.entrySet()){
            if(i > chars){
                break;
            }
            res = res + e.getKey() + ",";
            i++;
        }
        
        res = res.substring(0, res.length()-1);
        
        return res;
    }
    
    private static String getCharIndex(int charPos, Map<Integer,Float> charDstnct){

        String res = "";
        
        int i = 1;
        for(Map.Entry<Integer, Float> e: charDstnct.entrySet()){
            if(i == charPos+1){
                res = res + e.getKey();
                break;
            }
            i++;
        }

        return res;
    }
    
    private static String getCharIndex(String state, Map<Integer,Float> charDstnct, float threshold){
        
        Random r = new Random();
        Set<Integer> availableIndexes = new HashSet<Integer>();             

        for(Map.Entry<Integer, Float> e: charDstnct.entrySet()){
            if(e.getValue() <= threshold){
                availableIndexes.add(e.getKey());
            }
        }
        
        if(availableIndexes.size() == 0){
            return null;
        }
        
        if(state.equals("#")){
            List<Integer> indexes = new ArrayList<Integer>(availableIndexes);
            return String.valueOf(indexes.get(r.nextInt(indexes.size())));
        }
        
        String[] stateVec = state.split(",");
        for(int i = 0; stateVec.length > i; i++){
            availableIndexes.remove(Integer.parseInt(stateVec[i]));
        }
        
        List<Integer> indexes = new ArrayList<Integer>(availableIndexes);
        
        if(indexes.size() == 0){
            return null;
        }

        return String.valueOf(indexes.get(r.nextInt(indexes.size())));
    }
    
    public static boolean isEmptyState(String[] state){
        for(int i = 0; state.length > i; i++){
            if(!state[i].equals("#"))
                return false;
        }
        
        return true;
    }
    
    private static int getEntrySize(String entry){
        
        if(entry.equals("#")){
            return 0;
        }
        else{
            return entry.split(",").length;
        }
    }
    
    private static String removeLastIndex(String entry){
        int index = -1;
        for(int i = entry.length()-1; i > 0; i--){
            if(entry.charAt(i) == ','){
                index = i;
            }     
        }
        return entry.substring(0, index);
    }
    
    private static String removeRandomIndex(String entry){
        
        String[] entryVec = entry.split(",");
        Random r = new Random();
        int indexesNum = entryVec.length;
        int index = r.nextInt(indexesNum);
        String res = "";
        
        for(int i = 0; indexesNum > i; i++){
            if(i != index){
                res = res + entryVec[i] + ",";
            }     
        }
        return res.substring(0, res.length()-1);
    }
    
    private static String[] onlineNeighbor3(String[] state, List<String> attributes, Map<String,Map<Integer,Float>> charDstnct){

        int keySize = 20;
        float emptinessThreshold = 0f;
        
        Random r = new Random();
        int attrIndex = r.nextInt(attributes.size()); 
        String charIndex;
        
        int entrySize = getEntrySize(state[attrIndex]);
        
        if(r.nextBoolean()){
            if(state[attrIndex].equals("#")){
                charIndex = getCharIndex(state[attrIndex], charDstnct.get(attributes.get(attrIndex)), emptinessThreshold);
                if(charIndex != null){
                    state[attrIndex] = charIndex;
                }
            }
            else if(entrySize < keySize){
                charIndex = getCharIndex(state[attrIndex], charDstnct.get(attributes.get(attrIndex)), emptinessThreshold);
                if(charIndex != null){
                    state[attrIndex] = state[attrIndex] + "," + charIndex;
                }
            }
        }
        else{
            if(entrySize <= 1){
                state[attrIndex] = "#";
            }
            else{
                state[attrIndex] = removeRandomIndex(state[attrIndex]);
            }
        }

        String key = generateKey(sortKeySegments(state));

        while(_visitedStructures.contains(key) || isEmptyState(state) || getKeySize(state) != keySize){//System.out.println("HIT!");
            
            attrIndex = r.nextInt(attributes.size()); 
            
            entrySize = getEntrySize(state[attrIndex]);
            
            if(r.nextBoolean()){
                if(state[attrIndex].equals("#")){
                    charIndex = getCharIndex(state[attrIndex], charDstnct.get(attributes.get(attrIndex)), emptinessThreshold);
                    if(charIndex != null){
                        state[attrIndex] = charIndex;
                    }
                }
                else if(entrySize < keySize){
                    charIndex = getCharIndex(state[attrIndex], charDstnct.get(attributes.get(attrIndex)), emptinessThreshold);
                    if(charIndex != null){
                        state[attrIndex] = state[attrIndex] + "," + charIndex;
                    }
                }
            }
            else{
                if(entrySize <= 1){
                    state[attrIndex] = "#";
                }
                else{
                    state[attrIndex] = removeRandomIndex(state[attrIndex]);;
                }
            }

            key = generateKey(sortKeySegments(state));
             
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());

        return state;   
    }
    
    private static String[] onlineNeighbor2(String[] state, List<String> attributes, Map<String,Map<Integer,Float>> charDstnct){

        int keySize = 15;
        
        Random r = new Random();
        int attrIndex = r.nextInt(attributes.size()); 
        String charIndex;
        
        int charsSize = charDstnct.get(attributes.get(attrIndex)).size();
        int entrySize = getEntrySize(state[attrIndex]);
        
        if(r.nextBoolean()){
            if(state[attrIndex].equals("#")){
                charIndex = getCharIndex(entrySize, charDstnct.get(attributes.get(attrIndex)));
                state[attrIndex] = charIndex;
            }
            else if(entrySize < keySize && entrySize < charsSize){
                charIndex = getCharIndex(entrySize, charDstnct.get(attributes.get(attrIndex)));
                state[attrIndex] = state[attrIndex] + "," + charIndex;
            }
        }
        else{
            if(entrySize <= 1){
                state[attrIndex] = "#";
            }
            else{
                state[attrIndex] = removeLastIndex(state[attrIndex]);
            }
        }

        String key = generateKey(state);

        while(_visitedStructures.contains(key) || isEmptyState(state) || getKeySize(state) != keySize){//System.out.println("HIT!");
            
            attrIndex = r.nextInt(attributes.size()); 
            
            charsSize = charDstnct.get(attributes.get(attrIndex)).size();
            entrySize = getEntrySize(state[attrIndex]);
            
            if(r.nextBoolean()){
                if(state[attrIndex].equals("#")){
                    charIndex = getCharIndex(entrySize, charDstnct.get(attributes.get(attrIndex)));
                    state[attrIndex] = charIndex;
                }
                else if(entrySize < keySize && entrySize < charsSize){
                    charIndex = getCharIndex(entrySize, charDstnct.get(attributes.get(attrIndex)));
                    state[attrIndex] = state[attrIndex] + "," + charIndex;
                }
            }
            else{
                if(entrySize <= 1){
                    state[attrIndex] = "#";
                }
                else{
                    state[attrIndex] = removeLastIndex(state[attrIndex]);;
                }
            }

            key = generateKey(state);
             
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());

        return state;   
    }
    
    private static String[] onlineNeighbor(String[] state, List<String> attributes, Map<String,Map<Integer,Float>> charDstnct){

        int maxcharSize = 10;
        int mincharSize = 3;
        int keySize = 10;
        
        Random r = new Random();
        int attrIndex = r.nextInt(attributes.size()); 
        int numChars;
        String charIndexes;
        
        if(r.nextBoolean()){                  
            numChars = generateRandomNumberRange(0,maxcharSize);
            charIndexes = getCharIndexes(numChars, charDstnct.get(attributes.get(attrIndex)));            
            state[attrIndex] = charIndexes;
        }
        else{
            state[attrIndex] = "#";
        }

        String key = generateKey(state);

        while(_visitedStructures.contains(key) || isEmptyState(state) || getKeySize(state) > keySize){//System.out.println("HIT!");
            
            attrIndex = r.nextInt(attributes.size());       
            numChars = generateRandomNumberRange(0,keySize - getKeySize(state));
            charIndexes = getCharIndexes(numChars, charDstnct.get(attributes.get(attrIndex)));            
            state[attrIndex] = charIndexes;

            key = generateKey(state);
        }         

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());

        return state;     
    }
    

    private static Document loadParamsToConfigFile(Document d, String[] state, List<String> attributes){

        Element n = d.getDocumentElement();
        String key = "";
        for(int i = 0; state.length > i; i++){
          
            if(state[i].equals("#")){
                continue;
            }
            
            String[] params = state[i].split(",");
            key = key + attributes.get(i) + "#[";
            
            for(int j = 0; params.length > j; j++){
                key = key + params[j] + "-" + params[j] + ";";
            }
            key = key.substring(0,key.length()-1);
            key = key + "],";
            
        }
        key = key.substring(0,key.length()-1);
        n.setAttribute("key", key);

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
            
            if(state[i].equals("#")){
                continue;
            }
            
            size = size + state[i].split(",").length;
        }
        
        return size;
    }
    
    private static List<Integer> getAttributesMaxLength(DataBase rdb, List<String> attributes){
        
        List<Integer> res = new ArrayList<Integer>();
        int paramsSize = attributes.size();
        
        for(int i = 0; paramsSize > i; i++){
            res.add(rdb.getAttributeMaxLength(attributes.get(i)));
        }

        return res;
    }
    
    private static List<Integer> getAttributesMinLength(DataBase rdb, List<String> attributes){
        
        List<Integer> res = new ArrayList<Integer>();
        int paramsSize = attributes.size();
        
        for(int i = 0; paramsSize > i; i++){
            res.add(rdb.getAttributeMinLength(attributes.get(i)));
        }

        return res;
    }
    
    public static String[] sortKeySegments(String[] state){
        String[] res = new String[state.length];
        
        for(int i = 0; state.length > i; i++){
            
            if(state[i].equals("#")){
                res[i] = "#";
                continue;
            }
            
            String[] array = state[i].split(",");
            int[] intArray = new int[array.length];
            for(int k = 0; array.length > k; k++){
                intArray[k] = Integer.parseInt(array[k]);
            }
            
            Arrays.sort(intArray);
            res[i] = "";
            for(int j=0; intArray.length > j; j++){
                res[i] = res[i] + intArray[j] + ",";
            }
            
            res[i] = res[i].substring(0,res[i].length()-1);
            
        }

        return res;
    }
    
    public static float getScoreFromKeyChars(String[] state, List<String> attributes, String measure, DataBase rdb){
        
        return rdb.getMeasureFromKeysChars(state,attributes,measure);
    }
    
    public static List<String> getAttributeListFromKeyParam(Document configDoc){
        
        List<String> res = new ArrayList<String>();
        
        String key = configDoc.getDocumentElement().getAttributes().getNamedItem("key").getTextContent();
        String[] keys = key.split(",");
        
        for(int i = 0; keys.length > i; i++){
            res.add(keys[i].split("#")[0]);
        }
        
        return res;
       
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
            
            List<String> attributes = getAttributeListFromKeyParam(document);
            //List<String> attributes = getAttributeList(document.getDocumentElement().getChildNodes(), new ArrayList<String>());
            System.out.println("Attributes: " + attributes);

            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false); //faz o mesmo que a linha seguinte mas escreve para o ficheiro
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);       

            DataBase rdb = DataBase.getSingletonObject(true);

            XMLToTable xmltt = new XMLToTable(db, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();                                   
            
            List<Integer> attrMaxLength = getAttributesMaxLength(rdb,attributes);
            List<Integer> attrMinLength = getAttributesMinLength(rdb,attributes);

            Map<String,Map<Integer,Float>> charDstnct = rdb.buildCharsHistogram(attrMaxLength, attributes, "emptiness");
            System.out.println("HISTOGRAM: " + charDstnct);
            Map<String,Map<Integer,Float>> charDstnctE = rdb.buildCharsHistogram(attrMaxLength, attributes, "entropy");
            System.out.println("HISTOGRAM_Entropy: " + charDstnctE);
            
            //SA algorithm
            int chagesCnt = 0;
            String[] s = initializeEmptyState(attrMaxLength);
            //String[] s = initializeState(attrMaxLength);
            
            //double p = getKeySize(s);
            double p = 0;//rdb.getDistinctivenessFromKeysChars(s,attributes);
            float emptiness = getScoreFromKeyChars(initializeState(attrMaxLength), attributes, "emptiness", rdb);
            //double distnc = rdb.getDistinctivenessFromKeysChars(s,attributes)*1;//distinctiveness
            //System.out.println("Reference Distinct: " + distnc);
            
            List<Document> bestScoreStates = new ArrayList<Document>();
            Document doc = (Document) document.cloneNode(true);

            //double distnc_iter;
            double emptiness_iter;
            double p_new;
            int imax  = _maxNumberSAIterations;

            int i = imax;
            double p_best = p;
            String[] s_best = copyState(s);

            
            while(i > 0){
                int t = i/imax;
                System.out.print("Selecting Neighbor...");
                String[] s_new = copyState(onlineNeighbor3(copyState(s), attributes, charDstnct));  
                System.out.println("FINISHED!");

                System.out.println("new state inicio");
                for(int k = 0; s_new.length > k; k++){
                    System.out.println(s_new[k]);
                }
                System.out.println("new state fim");

                document = loadParamsToConfigFile(document, sortKeySegments(s_new), attributes);
                
                //calcula novo score
                rdb.cleanBlockingKeysTable();
                p_new = getScoreFromKeyChars(s_new, attributes, "distinctiveness", rdb);
                //distnc_iter = rdb.getDistinctivenessFromKeysChars(s_new,attributes);
                //emptiness_iter = getScoreFromKeyChars(s_new, attributes, "emptiness", rdb);
                //System.out.println("Reference Distinct: " + distnc);
                //System.out.println("Distinct_iter: " + distnc_iter);
                //p_new = getKeySize(s_new);

                //testa aceitacao
                if(acceptanceFunction(p, p_new, t) > Math.random() /*&& emptiness_iter <= emptiness*/){
                    s = copyState(s_new);
                    p = p_new;

                    //dt.createFile(document, configFileEdited);

                    System.out.println("State changed. Current Score: " + p);
                    chagesCnt++;
                }

                if(p_new == p_best){
                    doc = (Document) document.cloneNode(true);
                    bestScoreStates.add(doc);
                }
                
                if(p_new > p_best /*&& emptiness_iter <= emptiness*/){
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
            
            System.out.println("ITERATIONS: " + imax);
            System.out.println("STATE CHANGED: " + chagesCnt + " TIMES");

            System.out.println("Processing Complete DB With Best State...");
            
            printBestTopologiesToFile(bestScoreStates, dt);
            
            document = loadParamsToConfigFile(document, sortKeySegments(s_best), attributes);
            cg = new ConfigGeneral(document);

            dt.createFile(db, f_aux);
            
            rdb.cleanBlockingKeysTable();
            XMLDup xmldup = new XMLDup(document, f_aux, cg);
            xmldup.run(db);
          
            File f = new File(f_aux);
            f.delete();

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


