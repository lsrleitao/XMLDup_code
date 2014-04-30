package ObjectTopology.BestStructureSelection;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import Clustering.Entropy;
import Clustering.KMeans;
import DuplicateDetection.ConfigGeneral;
import DuplicateDetection.XMLDup;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class SimulatedAnnealingAttributeSelection {

    private static String _featuresOutputFile = "./XMLDup Data/StructureLearning/Features/featuresTrain.dat";
    private static String _bestStatesPath = "./XMLDup Data/StructureLearning/BestScoreTopologies";
    private static String _configFile = "./XMLDup Data/DBConfigFile/config_cddb.xml";
    private static String _dbPath = "./XMLDup Data/DB/FreeDB_obj_dup_100_el_dup_8_erros_20_del_30.xml";
    private static int _maxNumberSAIterations = 60;

    //Percentagens dos varios tipos de pares a figurar na amostra
    private static int _criticalPairspercentageTP = 100;
    private static int _criticalPairspercentageFP = 100;
    private static int _criticalPairspercentageTN = 100;
    private static int _criticalPairspercentageFN = 100;

    private static String _resultsPath = "./XMLDup Data/Results/" + _dbPath.split("/")[_dbPath.split("/").length-1].replace(".xml", "");

    //private static HashSet<BigInteger> _visitedStructures = new HashSet<BigInteger>();
    private static HashSet<String> _visitedStructures = new HashSet<String>();

    private static int generateRandomNumberRange(int min, int max){

        if(max < 0) return 0;
        Random r = new Random();
        return r.nextInt((max-min)+1) + min;
    }


    private static double acceptanceFunction(double p, double p_new, int t){
        if(p_new > p){
            return 1;
        }
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

    private static int[] initializeAttr(int[] defaultAttr){
        for(int i = 0; defaultAttr.length > i; i++){
            defaultAttr[i] = 1;
        }

        return defaultAttr;
    }

    private static String generateKey(int[] state){

        String key = "";

        for(int i = 0; state.length > i ; i++){
            key = key + state[i] + "#";
        }

        return key;
    }

    private static int[] onlineNeighbor(int[] state){

        Random r = new Random();
        
        int attributeIndex = r.nextInt(state.length);
        int attributeValue = 0;
        
        if(r.nextBoolean()){
            attributeValue = 1;
        }
        
        state[attributeIndex] = attributeValue;
        
        String key = generateKey(state);

        while(_visitedStructures.contains(key)){System.out.println("HIT!");
            attributeIndex = r.nextInt(state.length);
            attributeValue = 0;
        
            if(r.nextBoolean()){
                attributeValue = 1;
            }
        
            state[attributeIndex] = attributeValue;
        
            key = generateKey(state);
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());
        
        return state;     
    }

    private static Document loadAttrsToConfigFile(Document d, int[] state, List<String> attributes){

        for(int i = 0; attributes.size() > i; i++){
            Element n = d.getDocumentElement();
            Element e = (Element)n.getElementsByTagName(attributes.get(i)).item(0);
            e.setAttribute("useFlag", Integer.toString(state[i]));
        }

        return d;

    }
    
    private static Document reconcileEmptyPaths(Document d){
        
        walkEmptyPath(d.getDocumentElement());
        
        return d;
    }
    
    private static int walkEmptyPath(Element e){
        
        NodeList nl = e.getChildNodes();
        
        int cnt = 0;
        
        for(int i = 0; nl.getLength() > i; i++){
            
            if(nl.item(i).getNodeType() == Node.TEXT_NODE && nl.item(i).getNodeValue().trim().length() == 0){
                continue;
            }
            
            if(nl.item(i).getNodeType() == Node.ELEMENT_NODE && nl.item(i).getChildNodes().getLength() == 0){

                if(((Element)nl.item(i)).getAttribute("useFlag").equals("1")){

                    cnt++;
                }
            }
            else{                    
                cnt = cnt + walkEmptyPath((Element)nl.item(i));
            }
        }

        if(cnt == 0){
            e.setAttribute("useFlag", "0");
        }
        else{
            e.setAttribute("useFlag", "1");
        }
        
        return cnt;
    }
    
    private static void loadAttributeFeaturesAssignementToDB(Map<String,Integer> attributes, DataBase db, String dbName){

        Map<String,List<Integer>> occurrences = db.getOccurrencesFullContent(attributes);
        Map<String,List<Integer>> occurrencesTokens = db.getOccurrencesTokens(attributes);
        Map<String,List<Integer>> stringSizes = db.getStringSizesFullContent(attributes);       
        Map<String,List<Integer>> maxTokensSizeOccurrences = db.getMaxTokensSizeOccurrences(attributes);
        /*Map<String,List<Integer>> occurrences_st1 = db.getOccurrencesStringTokens(attributes,1);
        Map<String,List<Integer>> occurrences_st2 = db.getOccurrencesStringTokens(attributes,2);
        Map<String,List<Integer>> occurrences_st3 = db.getOccurrencesStringTokens(attributes,3);
        Map<String,List<Integer>> occurrences_end1 = db.getOccurrencesStringTokens(attributes,4);
        Map<String,List<Integer>> occurrences_end2 = db.getOccurrencesStringTokens(attributes,5);
        Map<String,List<Integer>> occurrences_end3 = db.getOccurrencesStringTokens(attributes,6);*/
        
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
            db.insertFeature(e.distinctiveness(occurrences.get(key)),
                    e.harmonicMean(occurrences.get(key)),
                    e.stdDeviation(occurrences.get(key)),
                    e.diversityMean(occurrences.get(key)),
                    e.diversityIndex(occurrences.get(key)),                   
                    db.getAttributesPerObject(key),
                    e.getEntropyKey(occurrences.get(key)), 
                    e.average(stringSizes.get(key)),                 
                    e.getEntropyKey(stringSizes.get(key)),        
                    db.getEmptiness(key), 
                    db.getDataTypePercentage(key, 1),
                    db.getDataTypePercentage(key, 2), 
                    db.getDataTypePercentage(key, 3),
                    e.median(occurrences.get(key)),
                    e.median(stringSizes.get(key)),
                    e.max(occurrencesTokens.get(key)),
                    e.min(occurrencesTokens.get(key)),
                    e.average(occurrencesTokens.get(key)),
                    e.getEntropyKey(occurrencesTokens.get(key)),
                    e.stdDeviation(occurrencesTokens.get(key)),
                    e.median(occurrencesTokens.get(key)),
                    e.average(maxTokensSizeOccurrences.get(key)),
                    e.getEntropyKey(maxTokensSizeOccurrences.get(key)),
                    e.stdDeviation(maxTokensSizeOccurrences.get(key)),                   
                    e.max(maxTokensSizeOccurrences.get(key)),
                    e.min(maxTokensSizeOccurrences.get(key)),
                    e.median(maxTokensSizeOccurrences.get(key)),
                    key, dbName, -1, -1f);
        }
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
    
    private static int[] copyState(int[] state){
        int size = state.length;
        int[] res = new int[size];
        for(int i = 0; size > i; i++){
            res[i]=state[i];
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

            List<String> attributes = getAttributeList(document.getDocumentElement().getChildNodes(), new ArrayList<String>());
            int[] attrConfig = new int[attributes.size()];

            ConfigGeneral cg = new ConfigGeneral(document);
            
            
            System.out.print("Pre-processing strings...");
            dbDoc = dt.preProcessXMLStrings(dbDoc, cg.getDbObjectName());
            System.out.println("FINISH!");
            

            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false); //faz o mesmo que a linha seguinte mas escreve para o ficheiro
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);       

            DataBase rdb = DataBase.getSingletonObject(true);                  

            Document db_sample;
            XMLDup xmldup;

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
                //dt.createFile(db_sample, f_aux_sample);
            }

            
            XMLToTable xmltt = new XMLToTable(db_sample, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();
            
            //para recolher o recall referencia
            xmldup = new XMLDup(document, f_aux_sample, cg);
            xmldup.run(db_sample);
            
            //o ficheiro de configuração inicial tem de ter os pfs a 1

            double p = xmldup.getScores().get("R-Precision");

            File f = new File(f_aux);
            f.deleteOnExit();
            File f2 = new File(f_aux_sample);
            f2.deleteOnExit();
            File f3 = new File(configFileEdited);
            //f3.deleteOnExit();

            attrConfig = initializeAttr(attrConfig);      

            //SA algorithm
            int chagesCnt = 0;
            int[] s = copyState(attrConfig);

            double p_new;
            int imax  = _maxNumberSAIterations;

            List<Document> bestScoreStates = new ArrayList<Document>();
            Document doc = (Document) document.cloneNode(true);
            bestScoreStates.add(doc);
            String key = generateKey(s);
            _visitedStructures.add(key);

            int i = imax;
            double p_best = p;
            int[] s_best = copyState(s);

            while(i > 0){
                int t = i/imax;
                System.out.print("Selecting Neighbor...");
                int[] s_new = copyState(onlineNeighbor(copyState(s)));  
                System.out.println("FINISHED!");
                
                //calcula novo score                          
                document = loadAttrsToConfigFile(document, s_new, attributes);
                document = reconcileEmptyPaths(document);
                
                cg = new ConfigGeneral(document);

                xmldup = new XMLDup(document, f_aux_sample, cg);
                xmldup.run(db_sample);

                p_new = xmldup.getScores().get("R-Precision");

                //testa aceitacao
                if(acceptanceFunction(p, p_new, t) > Math.random()){
                    s = copyState(s_new);
                    p = p_new;

                    //dt.createFile(document, configFileEdited);

                    System.out.println("State changed. Current Score: " + p);
                    chagesCnt++;
                }

                if(p_new == p_best){
                    doc = (Document) document.cloneNode(true);
                    bestScoreStates.add(doc);
                    System.out.println("Adicionado Maximo");
                }

                if(p_new > p_best){
                    p_best = p_new;
                    s_best = copyState(s_new);
                    bestScoreStates = new ArrayList<Document>();
                    doc = (Document) document.cloneNode(true);
                    bestScoreStates.add(doc);
                    System.out.println("Novo Maximo: " + p_best);
                }

                i--;
                
                System.out.println("(new state)SMs inicio");
                for(int k = 0; s_new.length > k; k++){
                    System.out.println(attributes.get(k) + " = "+s_new[k]);
                }
                System.out.println("(new state)SMs fim");
                
                System.out.println("(obtained score)SMs inicio");
                for(int k = 0; s.length > k; k++){
                    System.out.println(attributes.get(k) + " = "+s[k]);
                }
                System.out.println("(obtained score)SMs fim");
                
                System.out.println("(best score)SMs inicio");
                for(int k = 0; s_best.length > k; k++){
                    System.out.println(attributes.get(k) + " = "+s_best[k]);
                }
                System.out.println("(best score)SMs fim");
                
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
            System.out.println("STATES WITH THE SAME SCORE SCORE: " + bestScoreStates.size());

            printBestTopologiesToFile(bestScoreStates, dt);

            System.out.println("Processing Complete DB With Best State...");
            document = loadAttrsToConfigFile(document, s_best, attributes);
            document = reconcileEmptyPaths(document);
            cg = new ConfigGeneral(document);

            dt.createFile(db, f_aux);
            
            rdb = DataBase.getSingletonObject(true);
            
            xmltt = new XMLToTable(db, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();
            
            xmldup = new XMLDup(document, f_aux, cg);
            xmldup.run(db);
            

            if(bestScoreStates.size() > 0){
                System.out.println("Storing States Stats...");
                BestStructureStats bss = new BestStructureStats(_bestStatesPath, bestScoreStates.get(0), rdb);
                bss.storeLevelOccurrence();
                bss.storeLevelStats();
            }        

            Map<String,Integer> attributesLevel = rdb.getAttributesLevel();

            System.out.println("Loading Features to DB...");
            loadAttributeFeaturesAssignementToDB(attributesLevel, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            System.out.println("Loading Features to DB...FINISHED!");

            System.out.print("Writing Features To File...");
            //rdb.writeTrainingFeaturesToFile(_featuresOutputFile);         
            System.out.println("Writing Features To File...FINISHED!");

            rdb.closeConnection();


        }catch(Exception e){e.printStackTrace();}

    }

}


