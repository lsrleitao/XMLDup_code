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

public class SimulatedAnnealingPruningFactor {

    private static String _featuresOutputFile = "./XMLDup Data/StructureLearning/Features/featuresTrain.dat";
    private static String _bestStatesPath = "./XMLDup Data/StructureLearning/BestScoreTopologies";
    private static String _configFile = "./XMLDup Data/DBConfigFile/config_movie_restructured.xml";
    private static String _dbPath = "./XMLDup Data/DB/movie_restructured.xml";
    private static int _maxNumberSAIterations = 120;

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


    private static double acceptanceFunction(double p, double p_new, int t, double recall_iter, double recall){
        if(p_new < p)
            return 1;
        else{
            return Math.exp( (p - p_new) / t );
        }
    }

    private static void loadAttributeFeaturesAssignementToDB(Map<String,Integer> attributes, DataBase db, String dbName){

        Map<String,List<Integer>> occurrences = db.getOccurrencesFullContent(attributes);
        Map<String,List<Integer>> occurrencesTokens = db.getOccurrencesTokens(attributes);
        Map<String,List<Integer>> maxTokensSizeOccurrences = db.getMaxTokensSizeOccurrences(attributes);
        Map<String,List<Integer>> stringSizes = db.getStringSizesFullContent(attributes);
        
        Vector<String> v = new Vector<String>(attributes.keySet());
        Collections.sort(v);
        Iterator<String> it = v.iterator();

        int maxDepth = attributes.size()-1;
        Entropy e;
        KMeans km;
        String key;
        int level;
        while(it.hasNext()){
            key = it.next();
            level = attributes.get(key);

            km = new KMeans(db, 1, key, 0.8);
            km.clusterData();
            e = new Entropy(db, key);
            db.insertFeature(e.getDistinctiveness(),
                    e.getHarmonicMean(),
                    e.getStdDeviation(),
                    e.getDiversityMean(),
                    e.getDiversityIndex(),
                    db.getAttributesPerObject(key),
                    e.getDistinctivenessEntropy(), 
                    db.getAvgStringSize(key), 
                    e.getStringSizeEntropy(),
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
                    key, dbName, level, (float) (1-(((float)level)/((float)maxDepth))));

        }
    }

    private static void printBestTopologiesToFile(List<Document> lst, DocumentTransformer dt) throws Exception{
        for(int i = 0; lst.size() > i ; i++){
            dt.createFile(lst.get(i), _bestStatesPath + "/conf" + i + ".xml");
        }
    }

    private static double[] initializePF(double[] emptyPF, double value){
        for(int i = 0; emptyPF.length > i; i++){
            emptyPF[i] = value;
        }

        return emptyPF;
    }

    private static String generateKey(double[] state){

        String key = "";

        for(int i = 0; state.length > i ; i++){
            key = key + state[i] + "#";
        }

        return key;
    }
    
    private static double[] onlineNeighborProgressive(double[] state){      
        
        double[] res = generateNewState(state, 1);
        String key = generateKey(res);

        while(_visitedStructures.contains(key)){
            res = generateNewState(state, 3);
            key = generateKey(res);
        }
      
        _visitedStructures.add(key);

        return res;     
    }
    
    private static double[] generateNewState(double[] state, int step){
        
        DecimalFormat twoDForm = new DecimalFormat("#.##");
        
        for(int i = 0; i < step; i++){
        
            Random r = new Random(); 
            int attributePF = generateRandomNumberRange(0,state.length-1);
            
            double changes = ((double)generateRandomNumberRange(0,10))/100d;
            
            if(r.nextBoolean()){
                changes = changes * -1d;
            }
            
            System.out.println("Change value: " + changes);
            
            state[attributePF] = Double.valueOf(twoDForm.format(state[attributePF] + changes));
            
            if(state[attributePF] > 1d ){
                state[attributePF] = 1d;
            }
            
            if(state[attributePF] < 0d ){
                state[attributePF] = 0d;
            }
        }
        
        return state;
    }

    private static double[] onlineNeighbor(double[] state){

        Random r = new Random(); 

        //é indiferente o valor do primeiro pf porque para este, na avaliaçao da rede, 
        //vai ser sempre calculado o valor antes de testar o corte ????????????????
        //é necessário que o primeiro elemento seja o primeiro elemento depois da ordenação
        //Neste momento o vector segue a ordem do ficheiro de configuração
        //Alterar se necessário!!
        int attributePF = generateRandomNumberRange(0,state.length-1);
        
        DecimalFormat twoDForm = new DecimalFormat("#.#");
        state[attributePF] = Double.valueOf(twoDForm.format(r.nextDouble()));
        
        String key = generateKey(state);

        while(_visitedStructures.contains(key)){System.out.println("HIT!");
            attributePF = generateRandomNumberRange(0,state.length-1);
            state[attributePF] = Double.valueOf(twoDForm.format(r.nextDouble()));            
            key = generateKey(state);
        }

        _visitedStructures.add(key);
        System.out.println("Visited Structures: " + _visitedStructures.size());
        
        return state;     
    }

    private static Document loadPFToConfigFile(Document d, double[] state, List<String> attributes){

        for(int i = 0; attributes.size() > i; i++){
            Element n = d.getDocumentElement();
            Element e = (Element)n.getElementsByTagName(attributes.get(i)).item(0);
            e.setAttribute("pf", String.valueOf(state[i]));
        }

        return d;

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
    
    private static double[] copyState(double[] state){
        int size = state.length;
        double[] res = new double[size];
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
            double[] pfs = new double[attributes.size()];

            ConfigGeneral cg = new ConfigGeneral(document);

            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false); //faz o mesmo que a linha seguinte mas escreve para o ficheiro
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);       

            DataBase rdb = DataBase.getSingletonObject(true);

            XMLToTable xmltt = new XMLToTable(db, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();                   

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
            }


            //para recolher o recall referencia
            xmldup = new XMLDup(document, f_aux_sample, cg);
            xmldup.run(db_sample);
            double recall = xmldup.getScores().get("Recall")*0.9;//permite reducao de 10% de recall
            System.out.println("Reference Recall: " + recall);
            
            //o ficheiro de configuração inicial tem de ter os pfs a 1
            double p = xmldup.getScores().get("Comparisons");

            File f = new File(f_aux);
            f.deleteOnExit();
            File f2 = new File(f_aux_sample);
            f2.deleteOnExit();
            File f3 = new File(configFileEdited);
            //f3.deleteOnExit();

            pfs = initializePF(pfs,0.5d);      

            //SA algorithm
            int chagesCnt = 0;
            double[] s = copyState(pfs);

            double p_new;
            int imax  = _maxNumberSAIterations;

            double recall_iter;

            List<Document> bestScoreStates = new ArrayList<Document>();
            bestScoreStates.add(document);
            String key = generateKey(s);
            _visitedStructures.add(key);

            int i = imax;
            double p_best = p;
            double[] s_best = copyState(s);

            while(i > 0){
                int t = i/imax;
                System.out.print("Selecting Neighbor...");
                //double[] s_new = copyState(onlineNeighborProgressive(s));
                double[] s_new = copyState(onlineNeighbor(copyState(s)));  
                System.out.println("FINISHED!");

                //calcula novo score                          
                document = loadPFToConfigFile(document, s_new, attributes);

                cg = new ConfigGeneral(document);

                xmldup = new XMLDup(document, f_aux_sample, cg);
                xmldup.run(db_sample);

                p_new = xmldup.getScores().get("Comparisons");
                recall_iter = xmldup.getScores().get("Recall");
                System.out.println("Recall_iter: " + recall_iter);
                System.out.println("Reference Recall: " + recall);

                //testa aceitacao
                if(acceptanceFunction(p, p_new, t, recall_iter, recall) > Math.random() && recall_iter >= recall){
                    s = copyState(s_new);
                    p = p_new;

                    //dt.createFile(document, configFileEdited);

                    System.out.println("State changed. Current Score: " + p);
                    chagesCnt++;
                }

                if(p_new == p_best && recall_iter >= recall){
                    bestScoreStates.add(document);
                }

                if(p_new < p_best && recall_iter >= recall){
                    p_best = p_new;
                    s_best = copyState(s_new);
                    bestScoreStates = new ArrayList<Document>();
                    bestScoreStates.add(document);
                }

                i--;
                
                System.out.println("(new state)PFs inicio");
                for(int k = 0; s_new.length > k; k++){
                    System.out.println(attributes.get(k) + " = "+s_new[k]);
                }
                System.out.println("(new state)PFs fim");
                
                System.out.println("(obtained score)PFs inicio");
                for(int k = 0; s.length > k; k++){
                    System.out.println(attributes.get(k) + " = "+s[k]);
                }
                System.out.println("(obtained score)PFs fim");
                
                System.out.println("(best score)PFs inicio");
                for(int k = 0; s_best.length > k; k++){
                    System.out.println(attributes.get(k) + " = "+s_best[k]);
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
            document = loadPFToConfigFile(document, s_best, attributes);
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

            Map<String,Integer> attributesLevel = rdb.getAttributesLevel();

            System.out.println("Loading Features to DB...");
            loadAttributeFeaturesAssignementToDB(attributesLevel, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            System.out.println("Loading Features to DB...FINISHED!");

            System.out.print("Writing Features To File...");
            rdb.writeTrainingFeaturesToFile(_featuresOutputFile);         
            System.out.println("Writing Features To File...FINISHED!");

            rdb.closeConnection();


        }catch(Exception e){e.printStackTrace();}

    }

}


