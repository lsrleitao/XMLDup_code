package ObjectTopology.BestStructureSelection;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import Clustering.Entropy;
import Clustering.KMeans;
import DuplicateDetection.ConfigGeneral;
import DuplicateDetection.LoadStringSimilarityScores;
import DuplicateDetection.XMLDup;
import ObjectTopology.SructureGenerator.EditDistance;
import ObjectTopology.SructureGenerator.Generator;
import ObjectTopology.SructureGenerator.MatrixToConf;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class SimulatedAnnealing {

    private static String _featuresOutputFile = "./XMLDup Data/StructureLearning/Features/featuresTrain.dat";
    private static String _bestTopologiesPath = "./XMLDup Data/StructureLearning/BestScoreTopologies";
    private static String _configFile = "./XMLDup Data/DBConfigFile/config_cddb.xml";
    private static String _dbPath = "./XMLDup Data/DB/cddb_ID_nested_10000_FIXED.xml";
    private static int _maxNumberTopology = 10000;
    private static int _maxNumberSAIterations = 1000;
    private static int _sampleSizeDuplicates = 40;//percentagem de duplicados da bd usada no treino.
    private static int _sampleSizeNonDuplicates = 40;//percentagem de nao duplicados da bd usada no treino.
    
    //Percentagens dos varios tipos de pares a figurar na amostra
    private static int _criticalPairspercentageTP = 40;
    private static int _criticalPairspercentageFP = 40;
    private static int _criticalPairspercentageTN = 40;
    private static int _criticalPairspercentageFN = 40;
    
    private static List<int[][]> _possibleTopologies;
    private static String _resultsPath = "./XMLDup Data/Results/" + _dbPath.split("/")[_dbPath.split("/").length-1].replace(".xml", "");
    
    //private static HashSet<BigInteger> _visitedStructures = new HashSet<BigInteger>();
    private static HashSet<String> _visitedStructures = new HashSet<String>();
    
    //calcula no maximo para 29 atributos. Para um valor > 29 substituir long por BigInteger.
    private static long countStructures(int attributes){
        long comb;
        long res= 0;
        for(int i = 1; i <= attributes-2; i++){

            comb = factorial(attributes) / (factorial(i) * factorial(attributes - i));
            if(attributes-i == 1 || attributes-i == 2){
                res = res + (comb * 1);
            }
            else{
                res = res + (comb * countStructures(attributes - i));
            }
        }
        
        return res + 1;
    }
    
    private static long factorial(int n){
        long res = 1;
        while(n > 0){
            res = res * n;
            n--;
        }
        
        return res;
    }
    
    private static int[][] onlineNeighbor(int[][] structure, Generator gn){
        System.out.println("Visited Topologies: "+_visitedStructures.size());
        //System.out.println("Visited Topologies: "+_visitedStructures);
        int[][] res = selectNeighbor(structure, 1, gn);
        //BigInteger key = gn.generateKey(res);
        String key = gn.generateStringKey("", structure.length-1, res, 0);
        
        System.out.print("Generating new Topology...");
        while(_visitedStructures.contains(key)){System.out.println("HIT!");
            res = selectNeighbor(structure, 3, gn);
            //key = gn.generateKey(res);
            key = gn.generateStringKey("", structure.length-1, res, 0);
        }
        System.out.println("FINISHED!");
        gn.printMatrix(res);
        
        _visitedStructures.add(key);
        return res;
    }
    
    private static int[][] selectNeighbor(int[][] structure, int neighborStep, Generator gn){
        int[][] res = gn.generateNeighborState(structure);
        Random r = new Random();
        int step = r.nextInt(neighborStep);
        
        while(step > 0){
            res = gn.generateNeighborState(res);
            step--;
        }
        
        return res;
    }
    
    private static int[][] neighbor(int[][] structure, int neighborsDistanceDegree){
        
        int[][] res = null;
        
        try{
            
            Hashtable<Double,List<Integer>> topologiesDistance = new Hashtable<Double,List<Integer>>();
        
            List<Integer> lstIndex;
            for(int i = 0; _possibleTopologies.size() > i; i++){
                
               int[][] candidateTopology = _possibleTopologies.get(i);
               EditDistance ed = new EditDistance();
               double dst = ed.getDistance(structure, candidateTopology);
               
               if(topologiesDistance.containsKey(dst)){
                   lstIndex = topologiesDistance.get(dst);
                   lstIndex.add(i);
                   topologiesDistance.put(dst, lstIndex);
               }
               else{
                   lstIndex = new ArrayList<Integer>();
                   lstIndex.add(i);
                   topologiesDistance.put(dst, lstIndex);
               }
        }
            
            //System.out.println(topologiesDistance.size() + " Different Distances");
            
            List<Integer> lstSelectedIndexes = getLowestDistanceIndexes(topologiesDistance, neighborsDistanceDegree);

            int r = generateRandomNumberRange(0, lstSelectedIndexes.size() - 1);
            
            int index = lstSelectedIndexes.get(r).intValue();
            res = _possibleTopologies.get(index);
            _possibleTopologies.remove(index);
        
        }catch(Exception e){e.printStackTrace();}    

        return res;
    }
    
    private static List<Integer> getLowestDistanceIndexes(Hashtable<Double,List<Integer>> topologiesDistance, int neighborsDistanceDegree){
        
        int r;
        int tpSize = topologiesDistance.size();
        
        if(neighborsDistanceDegree <= tpSize)
            r = generateRandomNumberRange(0,neighborsDistanceDegree-1);
        else
            r = generateRandomNumberRange(0,tpSize-1);
        
        List<Integer> res = new ArrayList<Integer>();
        
        Vector<Double> v = new Vector<Double>(topologiesDistance.keySet());
        Collections.sort(v);
        Iterator<Double> it = v.iterator();
        
        double key = -1;
        for(int i = 0; i <= r; i++){
            key = it.next();
        }
        res.addAll(topologiesDistance.get(key));

        return res;
    }
    
    
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
    
    private static Map<String,Integer> getAttributesLevel(NodeList nl, Map<String,Integer> attrLevel, int level){
        
        String nodeName;
        Node node;
        NodeList cn;
        int cnSize;   
        for(int i = 0; nl.getLength() > i; i++){
  
            node = nl.item(i);
            
            if(node.getNodeType() == Node.ELEMENT_NODE){
                
                cn = node.getChildNodes();
                cnSize = cn.getLength();
                nodeName = node.getNodeName();
                if(cnSize <= 1){
                    attrLevel.put(nodeName, level);
                }
                else{
                    attrLevel = getAttributesLevel(node.getChildNodes(), attrLevel, level+1);
                }
                    
            }
        }
        return attrLevel;
    }
    
    private static void printBestTopologiesToFile(List<Document> lst, DocumentTransformer dt) throws Exception{
        for(int i = 0; lst.size() > i ; i++){
            dt.createFile(lst.get(i), _bestTopologiesPath + "/conf" + i + ".xml");
        }
    }
    
    private static void areRepeatedTopologies(List<int[][]> lst){
        Generator gn = new Generator();
        for(int i = 0; lst.size() > i; i++){
            for(int j = i+1; lst.size() > j; j++){
                if(lst.get(i).equals(lst.get(j))){
                    System.out.println("ERRO: Topologias repetidas!");
                    System.out.println("matriz: "+ i);
                    gn.printMatrix(lst.get(i));
                    System.out.println("matriz: "+ j);
                    gn.printMatrix(lst.get(j));
                    System.exit(0);
                }   
            }
        }
    }
    
    private static double scoringFunction(double[] scores){
        double res = 0;
        for(int i = 0; scores.length > i; i++){
            res = res + scores[i];
        }
        
        return res/scores.length;
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
            
            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false); //faz o mesmo que a linha seguinte mas escreve para o ficheiro
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);       

            DataBase rdb = DataBase.getSingletonObject(true);
            
            XMLToTable xmltt = new XMLToTable(db, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();
                      
            //dt.createFile(db,f_aux);
            //new LoadStringSimilarityScores(f_aux, rdb, document, objectName);
            
            
            XMLDup xmldup = new XMLDup(document, _dbPath, cg);
            xmldup.run(db);

            Document db_sample = dt.extractTestSetCriticalElementsToDoc(_resultsPath+"/results_pairs.txt", db, objectName,
                   _criticalPairspercentageTP, _criticalPairspercentageFP, _criticalPairspercentageTN, _criticalPairspercentageFN);         
            dt.createFile(db_sample, f_aux);
            
            //dt.extractTestSetBalanced(f_aux_sample, f_aux, objectName, _sampleSizeDuplicates, _sampleSizeNonDuplicates);//faz o mesmo que a linha seguinte mas escreve para o ficheiro
            //Document db_sample = dt.extractTestSetBalancedToDoc(db, objectName, _sampleSizeDuplicates, _sampleSizeNonDuplicates);          
            
            
            File f = new File(f_aux);
            f.deleteOnExit();
            File f2 = new File(f_aux_sample);
            f2.deleteOnExit();
            File f3 = new File(configFileEdited);
            //f3.deleteOnExit();
            
            MatrixToConf mtfe = new MatrixToConf();
            //carrega o hash com os nos e a sua lista de atributos. Basta fazer uma vez, neste caso, 
            //desde que as tags ja se encontrem correctamente transformadas em tags unicas.
            LinkedHashMap<String, NamedNodeMap> ci = mtfe.loadConfigFile(configFileEdited);
            
            Generator gn = new Generator(ci);
            
            /*System.out.print("Generating Structure Topologies...");
            gn.generatePossibleStates(_maxNumberTopology);
            System.out.println("FINISHED!"); 
            _possibleTopologies = gn.getStatesList();
            System.out.println("Topologies Generated: " + _possibleTopologies.size()); 
            areRepeatedTopologies(_possibleTopologies);
            int r = generateRandomNumberRange(0, _possibleTopologies.size()-1);
            int[][] initialTopology = _possibleTopologies.get(r);*/
            
            int[][] initialTopology = gn.generateOnlyLeafNodesMatrix();
            MatrixToConf mtf = new MatrixToConf(initialTopology, ci);
            document = mtf.buildConfiguration();
            cg = new ConfigGeneral(document);
  
            System.out.println("Attributes: " + (ci.size()-1));
            System.out.println("Possible Structures: " + countStructures(ci.size()-1));
            
            //ultimo parametro passado a true, pois como os elementos ja se encontram com as
            //tags correctas apenas é necessario transferir os nos segundo a configuracao
            //sem necessidade de considerar os caminhos
            
            //dt.transformDBToConfigStructure(document, f_aux_sample, f_aux_sample, paths, objectName, true);
            db_sample = dt.transformDBToConfigStructureToDoc(document, db_sample, paths, objectName, true);
            dt.createFile(db_sample, f_aux);
            
            xmldup = new XMLDup(document, f_aux, cg);
            xmldup.run(db_sample);
            
            //SA algorithm
            int chagesCnt = 0;
            int[][] s = initialTopology;/*_possibleTopologies.get(r);*/
            //_possibleTopologies.remove(r);
            
          //recebe um vector com varias medidas calculadas         
            double p = xmldup.getScores().get("R-Precision");
            //double p = scoringFunction(scores); if(p < 0) System.exit(0);
            
            double p_new;
            int imax  = _maxNumberSAIterations;
            
            /*if(_possibleTopologies.size() < _maxNumberSAIterations){
                imax = _possibleTopologies.size();
            }
            else{
                imax = _maxNumberSAIterations;
            }*/
            
            List<Document> bestScoreTopologies = new ArrayList<Document>();
            bestScoreTopologies.add(document);
            int i = imax;
            double p_best = p;
            int[][] s_best = s;
            //int neighborStep = _maxNumberTopology/100;
            //if(neighborStep < 1) neighborStep = 1;
            while(i > 0 /*&& p < 1 && _possibleTopologies.size() > 0*/){
                int t = i/imax;
                System.out.print("Selecting Neighbor...");
                //int[][] s_new = neighbor(s, neighborStep);
                int[][] s_new = onlineNeighbor(s, gn);
                System.out.println("FINISHED!");
                System.out.println("Remaining Topologies: " + i);
                System.out.println("Best Score: " + p_best);
                
                //calcula novo score                          
                mtf = new MatrixToConf(s_new, ci);
                document = mtf.buildConfiguration();
                cg = new ConfigGeneral(document);
                
                //dt.createFile(document, f_aux_sample);
                //dt.createFile(db_sample, f_aux);
                //dt.transformDBToConfigStructure(document, f_aux_sample, f_aux_sample, paths, objectName, true);
                db_sample = dt.transformDBToConfigStructureToDoc(document, db_sample, paths, objectName, true);
                
                //dt.createFile(document, f_aux_sample);
                xmldup = new XMLDup(document, f_aux_sample, cg);
                xmldup.run(db_sample);
                
                //p_new = scoringFunction(scores); if(p_new  < 0) System.exit(0);
                p_new = xmldup.getScores().get("R-Precision");
                
                //dt.createFile(document, configFileEdited);
                //testa aceitacao
                if(acceptanceFunction(p, p_new, t) > Math.random()){
                    s = s_new;
                    p = p_new;
                    
                    dt.createFile(document, configFileEdited);
                    
                    System.out.println("Topology changed. Current Score: " + p);
                    chagesCnt++;
                }
                
                if(p_new == p_best){
                    bestScoreTopologies.add(document);
                }
                
                if(p_new > p_best){
                    p_best = p_new;
                    s_best = s_new;
                    bestScoreTopologies = new ArrayList<Document>();
                    bestScoreTopologies.add(document);
                }

                
                System.out.print("Actual: ");
                gn.printMatrix(s);
                
                i--;
            }
            
          //SA algorithm END
            
            System.out.println("GENERATED TOPOLOGIES: " + _maxNumberSAIterations);
            System.out.println("OBTAINED SCORE: " + p);
            System.out.println("BEST SCORE: "+ p_best);
            System.out.println("ITERATIONS: " + imax);
            System.out.println("TOPOLOGY CHANGED: " + chagesCnt + " TIMES");
            System.out.println("TOPOLOGIES WITH THE SAME SCORE SCORE: " + bestScoreTopologies.size());
            
            printBestTopologiesToFile(bestScoreTopologies, dt);
     
            System.out.println("Processing Complete DB With Best Topology...");
            mtf = new MatrixToConf(s_best, ci);
            document = mtf.buildConfiguration();
            cg = new ConfigGeneral(document);
            
            //dt.transformDBToConfigStructure(document, f_aux, f_aux, paths, objectName, true);          
            db = dt.transformDBToConfigStructureToDoc(document, db, paths, objectName, true);
            
            xmldup = new XMLDup(document, f_aux, cg);
            xmldup.run(db);
            
            if(bestScoreTopologies.size() > 0){
                System.out.println("Storing Topologies Stats...");
                BestStructureStats bss = new BestStructureStats(_bestTopologiesPath, bestScoreTopologies.get(0), rdb);
                bss.storeLevelOccurrence();
                bss.storeLevelStats();
            }
            
            rdb = DataBase.getSingletonObject(false);
            
            //Hashtable<String,Integer> attributesLevel = getAttributesLevel(document.getDocumentElement().getChildNodes(), new Hashtable<String,Integer>(), 1);
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
