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

public class SimulatedAnnealingBlocking {

    private static String _featuresOutputFile = "./XMLDup Data/StructureLearning/Features/featuresTrain.dat";
    private static String _bestStatesPath = "./XMLDup Data/StructureLearning/BestScoreTopologies";
    private static String _configFile = "./XMLDup Data/DBConfigFile/config_persons_classified.xml";
    private static String _dbPath = "./XMLDup Data/DB/persons_FLAT_dirty_transformed.xml";
    private static int _maxNumberSAIterations = 1000;
    
    private static int _maxKeySize;
    

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


    private static double acceptanceFunction(double p, double p_new, int t, double recall_iter, double recall){
        if(p_new <= p)
            return 1;
        else{
            return Math.exp( (p - p_new) / t );
        }
    }

    //extrai features do base de dados
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
    
    //extrai features da blocking key
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

    private static float[] initializePF(ConfigGeneral cg, String pf){
        
        float[] params = cg.getBlockingParams();
        int paramsSize = params.length;
        float[] res = new float[paramsSize+1];
        
        for(int i = 0; params.length > i; i++){
            res[i] = params[i];
        }
        res[paramsSize] = Float.parseFloat(pf);//pf

        return res;
    }

    private static String generateKey(float[] state){

        String key = "";

        for(int i = 0; state.length > i ; i++){
            key = key + state[i] + "#";
        }

        return key;
    }
    
    private static float[] onlineNeighborProgressive(float[] state){      
        
        float[] res = generateNewState(state, 1);
        String key = generateKey(res);

        while(_visitedStructures.contains(key)){
            res = generateNewState(state, 3);
            key = generateKey(res);
        }
      
        _visitedStructures.add(key);

        return res;     
    }
    
    private static float[] generateNewState(float[] state, int step){
        
        DecimalFormat twoDForm = new DecimalFormat("#.##");
        
        for(int i = 0; i < step; i++){
        
            Random r = new Random(); 
            int attributePF = generateRandomNumberRange(0,state.length-1);
            
            float changes = ((float)generateRandomNumberRange(0,10))/100f;
            
            if(r.nextBoolean()){
                changes = changes * -1f;
            }
            
            System.out.println("Change value: " + changes);
            
            state[attributePF] = Float.valueOf(twoDForm.format(state[attributePF] + changes));
            
            if(state[attributePF] > 1f ){
                state[attributePF] = 1f;
            }
            
            if(state[attributePF] < 0f ){
                state[attributePF] = 0f;
            }
        }
        
        return state;
    }
    
    private static float[] onlineNeighbor(String blockingAlgo, float[] state){
        if(blockingAlgo.equals("SuffixA")){
            return onlineNeighborSuffixA(state);
        }
        else if(blockingAlgo.substring(0, 3).equals("SNM")){
            return onlineNeighborSNM(state);
        }
        else if(blockingAlgo.equals("Canopy")){
            return onlineNeighborCanopy(state);
        }
        
        return null;
    }
    
    private static float[] onlineNeighborSNM(float[] state){

        //SNM
        //posiçao 1 - tamanho janela
        //posiçao 2 - keyruns
        //posiçao 3 - pf
        int keyRunsLimit = 1;
        int windowSizeMaxStep = 50;
        
        Random r = new Random();  
        DecimalFormat twoDForm = new DecimalFormat("#.#");

        int attributePos = generateRandomNumberRange(0,state.length-1);
        float attributeVal;
        int sig = 1;
        
        if(r.nextBoolean()){
            sig = -1;
        }
        
        if(attributePos != 2){
           attributeVal = 1;
        }
        else{
            //if(r.nextBoolean()){
                attributeVal = Float.valueOf(twoDForm.format(generateRandomNumberRange(1,2)/10f));
                /*}
            else{
                attributeVal = Float.valueOf(twoDForm.format(generateRandomNumberRange(1,2)/100f));
            }*/
        }
        
        float res = state[attributePos] + (attributeVal * sig);
        
        if(((attributePos == 0 && res < windowSizeMaxStep) ||
                (attributePos == 1 && res <= keyRunsLimit) ||
                (attributePos == 2)) && res > 0){
            
            state[attributePos] = attributeVal;
        }
        
        String key = generateKey(state);

        while(_visitedStructures.contains(key)){//System.out.println("HIT!");
            attributePos = generateRandomNumberRange(0,state.length-1);

            if(attributePos == 0){
                attributeVal = generateRandomNumberRange(1,windowSizeMaxStep);
            }
            else if(attributePos == 1){
                attributeVal = generateRandomNumberRange(1,keyRunsLimit);
            }
            else{
                attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
            }
            
            state[attributePos] = attributeVal;
            
            key = generateKey(state);
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());
        
        return state;     
    }
    
    private static float[] onlineNeighborSuffixA(float[] state){

        //SNM
        //posiçao 0 - tamanho sufixo - lms
        //posiçao 1 - tamanho max bloco - lbs
        //posiçao 2 - keyruns
        //posiçao 3 - threshold
        //posiçao 4 - pf
        
        float minThreshold = 0.5f;
        
        int keyRunsLimit = 1;
        int lbsLimit = 100;
        int lmsLimit = _maxKeySize;
        
        Random r = new Random();  
        DecimalFormat twoDForm = new DecimalFormat("#.#");

        int attributePos = generateRandomNumberRange(0,state.length-1);
        float attributeVal;
        int sig = 1;
        
        if(r.nextBoolean()){
            sig = -1;
        }
        
        if(attributePos != 3 && attributePos != 4){
           attributeVal = 1;
        }
        else{
            //if(r.nextBoolean()){
                attributeVal = Float.valueOf(twoDForm.format(generateRandomNumberRange(1,2)/10f));
            /*}
            else{
                attributeVal = Float.valueOf(twoDForm.format(generateRandomNumberRange(1,2)/100f));
            }*/
        }
        
        float res = state[attributePos] + (attributeVal * sig);
        
        if(((attributePos == 0 && res <= lmsLimit) ||
                (attributePos == 1 && res <= lbsLimit) ||
                (attributePos == 2 && res <= keyRunsLimit) ||
                (attributePos == 3 && res >= minThreshold) ||
                (attributePos == 4)) &&
                res > 0){
            
            state[attributePos] = attributeVal;
        }
        
        String key = generateKey(state);

        while(_visitedStructures.contains(key)){//System.out.println("HIT!");
            attributePos = generateRandomNumberRange(0,state.length-1);

            if(attributePos == 0){
                attributeVal = generateRandomNumberRange(1,lmsLimit);
            }
            else if(attributePos == 1){
                attributeVal = generateRandomNumberRange(1,lbsLimit);
            }
            else if(attributePos == 2){
                attributeVal = generateRandomNumberRange(1,keyRunsLimit);
            }
            else if(attributePos == 3){
                attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
                while(attributeVal < minThreshold){
                    attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
                }
            }
            else{
                attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
            }
            
            state[attributePos] = attributeVal;
            
            key = generateKey(state);
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());
        
        return state;     
    }
    
    private static float[] onlineNeighborCanopy(float[] state){

        //SNM
        //posiçao 0 - t1
        //posiçao 1 - t2
        //posiçao 2 - gramSize
        //posiçao 3 - pf
        
        float minThreshold = 0.2f;
        int maxGramSize = 8;
        int minGramSize = 2;
        float minPF = 0.5f;
        
        Random r = new Random();  
        DecimalFormat twoDForm = new DecimalFormat("#.#");

        int attributePos = generateRandomNumberRange(0,state.length-1);
        
        float attributeVal;
        
        if(attributePos == 2){
            attributeVal = 1;
        }
        else{
            //if(r.nextBoolean()){
                attributeVal = (float)generateRandomNumberRange(1,2)/10f;
            /*}
            else{
                attributeVal = (float)generateRandomNumberRange(1,2)/100f;
            }*/
        }
        
        int sig = 1;
        if(r.nextBoolean()){
            sig = -1;
        }   
      
        float res = Float.valueOf(twoDForm.format(state[attributePos] + (attributeVal * sig)));
        
        if(((attributePos == 0 && res > state[1] && res <= 1) ||
            (attributePos == 1 && res < state[0] && res <= 1 && res >= minThreshold) ||
            (attributePos == 3 && res <= 1 && res >= minPF) ||
            (attributePos == 2 && res >= minGramSize && res <= maxGramSize /*&& res <= _maxKeySize*/))){
            
            state[attributePos] = res;
        }
        
        String key = generateKey(state);

        while(_visitedStructures.contains(key)){//System.out.println("HIT!");
            attributePos = generateRandomNumberRange(0,state.length-1);
          
            attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
            
            if(attributePos == 0){
                //while(attributeVal <= state[1]){
                    attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
                //}
            }
            else if(attributePos == 1){
                //while(attributeVal >= state[0]){
                    attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
                //}
            }
            else if(attributePos == 3){
                attributeVal = Float.valueOf(twoDForm.format(r.nextFloat()));
            }
            else{
                attributeVal = generateRandomNumberRange(minGramSize,maxGramSize);//_maxKeySize);
            }
            
            if(((attributePos == 0 && attributeVal > state[1]) ||
                (attributePos == 1 && attributeVal < state[0] && attributeVal >= minThreshold)
                || (attributePos == 2) || (attributePos == 3 && attributeVal >= minPF))) // esgota os estados mais rapidamente (ciclo infinito) mas parece mais rapido a convergir para melhor soluçao
                {
                
                state[attributePos] = attributeVal;
                //System.out.println("key: " + generateKey(state));
            }
           
            key = generateKey(state);
            
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());
        
        return state;     
    }

    private static Document loadParamsToConfigFile(Document d, float[] state){

        Element n = d.getDocumentElement();
        String params = "";
        for(int i = 0; state.length-1 > i; i++){
            params += state[i] + ",";
        }
        n.setAttribute("blockingParams", params.substring(0, params.length()-1));
        n.setAttribute("uniquePF", Float.toString(state[state.length-1]));

        return d;

    }
    
    private static Document setConfigFileParams(Document d, String param, String value){

        Element n = d.getDocumentElement();
        n.setAttribute(param, value);

        return d;

    }
    
    private static String getConfigFileParams(Document d, String param){

        Element n = d.getDocumentElement();
        return n.getAttribute(param);
    }
    
    private static Document toggleBlockingState(Document d, boolean state){

        Element n = d.getDocumentElement();
        n.setAttribute("blocking", Boolean.toString(state));

        return d;

    }
    
    private static float[] copyState(float[] state){
        int size = state.length;
        float[] res = new float[size];
        for(int i = 0; size > i; i++){
            res[i]=state[i];
        }
        return res;
    }
    
    private static int getMaxKeySize(ConfigGeneral cg){
        String[] key = cg.getkey();
        int maxSize = 0;
        for(int i = 0; key.length > i; i++){
            maxSize += Integer.parseInt(key[i].split("#")[1]);
        }
        
        return maxSize;
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
    
    private static double getPossibleRecall(DataBase rdb, double recallPrecentageAllowed, double maxRecallObtained){
        
        double maxPossibleDupObjects = rdb.getRecallNoEmptyKeyObjects();    System.out.println(maxPossibleDupObjects);      
        double existingDuplicates = rdb.getExistingDuplicates();    System.out.println(existingDuplicates);  
        double duplicatesFound = maxRecallObtained * existingDuplicates;    System.out.println(duplicatesFound);
        
        double recall = maxPossibleDupObjects/duplicatesFound;  System.out.println(recall);
        
        if(recall > 1){
            recall = duplicatesFound/existingDuplicates * recallPrecentageAllowed;
        }
        else{
            recall = recall * recallPrecentageAllowed;
        }
        
        return recall;
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

            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false); //faz o mesmo que a linha seguinte mas escreve para o ficheiro
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);       

            DataBase rdb = DataBase.getSingletonObject(true);

            XMLToTable xmltt = new XMLToTable(db, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();                   

            Document db_sample;
            XMLDup xmldup;
            
            //garante que a execucao inicial nao aplica blocking nem pf < 1, portanto nao tem perdas
            //guarda o pf inicial para poder usar como estado de partida do SA
            String initialUniquePF = getConfigFileParams(document,"uniquePF");
            //document = setConfigFileParams(document,"uniquePF", "1");
            //document = toggleBlockingState(document, false);

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

            //para recolher o recall referencia
            xmldup = new XMLDup(document, f_aux_sample, cg);
            xmldup.run(db_sample);
            //valor forcado
            //double rp =  0.870909090909091;//imdb_fd
            
            double rp = getPossibleRecall2(rdb, 0.9, _resultsMeasureRecall);
            //double rp = xmldup.getScores().get("Recall")*0.9;//permite 90% do recall obtido
            System.out.println("Reference Recall: " + rp);
            
            //o ficheiro de configuração inicial tem de ter os pfs a 1
            double p = xmldup.getScores().get("Comparisons");

            File f = new File(f_aux);
            f.deleteOnExit();
            File f2 = new File(f_aux_sample);
            f2.deleteOnExit();
            File f3 = new File(configFileEdited);
            //f3.deleteOnExit();

            
            document = toggleBlockingState(document, true);
            document = setConfigFileParams(document,"uniquePF", initialUniquePF);
            float[] blck_params = initializePF(new ConfigGeneral(document),initialUniquePF);

            //SA algorithm
            int chagesCnt = 0;
            float[] s = copyState(blck_params);

            double p_new;
            int imax  = _maxNumberSAIterations;

            double rp_iter;

            List<Document> bestScoreStates = new ArrayList<Document>();
            Document doc = (Document) document.cloneNode(true);
            //bestScoreStates.add(document);
            //String key = generateKey(s);
            //_visitedStructures.add(key);

            int i = imax;
            double p_best = p;
            float[] s_best = copyState(s);

            String blockingAlgo = cg.getBlockingAlgo();
            _maxKeySize = getMaxKeySize(cg);
            
            while(i > 0){
                int t = i/imax;
                System.out.print("Selecting Neighbor...");
                //double[] s_new = copyState(onlineNeighborProgressive(s));
                float[] s_new = copyState(onlineNeighbor(blockingAlgo,copyState(s)));  
                System.out.println("FINISHED!");
              
                System.out.println("new state inicio");
                for(int k = 0; s_new.length > k; k++){
                    System.out.println(s_new[k]);
                }
                System.out.println("new state fim");

                //calcula novo score                          
                document = loadParamsToConfigFile(document, s_new);

                cg = new ConfigGeneral(document);

                xmldup = new XMLDup(document, f_aux_sample, cg);
                xmldup.run(db_sample);

                p_new = xmldup.getScores().get("Comparisons");
                rp_iter = xmldup.getScores().get("Recall");
                System.out.println("Recall_iter: " + rp_iter);
                System.out.println("Reference Recall: " + rp);

                //testa aceitacao
                if(acceptanceFunction(p, p_new, t, rp_iter, rp) > Math.random() && rp_iter >= rp){
                    s = copyState(s_new);
                    p = p_new;

                    //dt.createFile(document, configFileEdited);

                    System.out.println("State changed. Current Score: " + p);
                    chagesCnt++;
                }

                if(p_new == p_best && rp_iter >= rp){
                    doc = (Document) document.cloneNode(true);
                    bestScoreStates.add(doc);
                }

                if(p_new <= p_best && rp_iter >= rp){
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
            System.out.println("REFERENCE RECALL: " + rp);
            
            System.out.println("ITERATIONS: " + imax);
            System.out.println("STATE CHANGED: " + chagesCnt + " TIMES");
            System.out.println("STATES WITH THE SAME SCORE SCORE: " + bestScoreStates.size());

            printBestTopologiesToFile(bestScoreStates, dt);

            System.out.println("Processing Complete DB With Best State...");
            document = loadParamsToConfigFile(document, s_best);
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

            Map<String,Integer> attributeKeys = getAttibuteKeys(document);
            System.out.println(attributeKeys);

            System.out.println("Loading Features to DB...");
            //loadAttributeFeaturesAssignementToDB(attributeKeys, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            //loadAttributeFeaturesAssignementToDBSingleKey(attributeKeys, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            System.out.println("Loading Features to DB...FINISHED!");

            System.out.print("Writing Features To File...");
            rdb.writeTrainingKeyFeaturesToFile(_featuresOutputFile);         
            System.out.println("Writing Features To File...FINISHED!");

            rdb.closeConnection();


        }catch(Exception e){e.printStackTrace();}

    }

}


