package ObjectTopology.BestStructureSelection;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
import DuplicateDetection.XMLDup;
import ObjectTopology.SructureGenerator.EditDistance;
import ObjectTopology.SructureGenerator.Generator;
import ObjectTopology.SructureGenerator.MatrixToConf;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class RunSelectedStructure {

    private static String _featuresOutputFile = "./XMLDup Data/StructureLearning/Features/featuresTrain.dat";
    private static String _configFile_OriginalStructure = "./XMLDup Data/DBConfigFile/config_cddb.xml";
    private static String _configFile_SelectedStructure = "./XMLDup Data/DBConfigFile/config_freeDB_real_classified.xml";
    private static String _dbPath = "./XMLDup Data/DB/cddb_ID_nested_10000_FIXED.xml";
    
    private static void loadAttributeFeaturesAssignementToDB(Hashtable<String,Integer> attributes, DataBase db, String dbName){
        
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
            
            km = new KMeans(db, 5, key, 0.8);
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
    
    private static Hashtable<String,Integer> getAttributesLevel(NodeList nl, Hashtable<String,Integer> attrLevel, int level){
        
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

    public static void main(String[] args) {

        try{
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document dbDoc = builder.parse(new File(_dbPath));
            
            DocumentBuilderFactory factory2 = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder2 = factory2.newDocumentBuilder();
            Document document = builder2.parse(new File(_configFile_OriginalStructure));
            String objectName = document.getDocumentElement().getNodeName();
            
            DocumentTransformer dt = new DocumentTransformer();
            document = dt.transformConfigFileUniqueLeaves(document);
            Map<String,List<String>> paths = dt.getPaths();System.out.println(paths);
            String configFileEdited = _configFile_OriginalStructure.replace(".xml", "_edited.xml");
            //dt.createFile(document, configFileEdited);
            String f_aux = _dbPath.replace(".xml", "_transformed.xml");
            
            //dt.transformDBToConfigStructure(document, _dbPath, f_aux, paths, objectName, false);
            //Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);
            
            document = builder2.parse(new File(_configFile_SelectedStructure));
            Document db = dt.transformDBToConfigStructureToDoc(document, dbDoc, paths, objectName, false);
            dt.createFile(db, f_aux);
            //System.exit(0);
            
            File f = new File(f_aux);
            f.deleteOnExit();
            File f2 = new File(configFileEdited);
            f2.deleteOnExit();
          
            ConfigGeneral cg = new ConfigGeneral(document);
            
            DataBase rdb = DataBase.getSingletonObject(true);
            
            XMLToTable xmltt = new XMLToTable(document, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
            xmltt.loadXMLDocToTable();
            
            XMLDup xmldup = new XMLDup(document, f_aux, cg);
            xmldup.run(db);
            
            /*Hashtable<String,Integer> attributesLevel = getAttributesLevel(document.getDocumentElement().getChildNodes(), new Hashtable<String,Integer>(), 1);
            
            System.out.println("Loading Features to DB...");
            loadAttributeFeaturesAssignementToDB(attributesLevel, rdb, _dbPath.split("/")[_dbPath.split("/").length-1]);
            System.out.println("Loading Features to DB...FINISHED!");
            
            System.out.print("Writing Features To File...");
            rdb.writeTrainingFeaturesToFile(_featuresOutputFile);         
            System.out.println("Writing Features To File...FINISHED!");*/
    
        }catch(Exception e){e.printStackTrace();}

    }

}

