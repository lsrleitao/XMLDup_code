package RDB;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import CandidateDefinition.Candidates;
import CandidateDefinition.Canopy;
import Clustering.Entropy;
import DuplicateDetection.ConfigGeneral;
import ObjectTopology.XMLTransformer.DocumentTransformer;
import RDB.DataBase;
import RDB.XMLToTable;

public class FeatureExtractor {

    private static String _configFile = "./XMLDup Data/DBConfigFile/config_persons_classified.xml";
    private static String _dbFolder = "./XMLDup Data/DB";

    public static void main(String[] args){
        
        try{
            
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document configDocument = builder.parse(new File(_configFile));
                  
            ConfigGeneral cg = new ConfigGeneral(configDocument);
            
            File f = new File(_dbFolder);
            File[] f2 = f.listFiles();
            
            for(int i = 0; f2.length > i ; i++){
                String fPath = f2[i].getPath();
                
                DocumentTransformer dt = new DocumentTransformer();
                Document dbDoc = builder.parse(new File(fPath));
                
                System.out.print("Pre-processing strings...");
                Document transformedDb = dt.preProcessXMLStrings(dbDoc, cg.getDbObjectName());
                System.out.println("FINISH!");
                
                configDocument = dt.transformConfigFileUniqueLeaves(configDocument);
                Map<String,List<String>> paths = dt.getPaths();

                DataBase rdb = DataBase.getSingletonObject(true); 
                
                XMLToTable xmltt = new XMLToTable(transformedDb, cg.getDbObjectName(), rdb, dt.getUniqueAttributes(paths));
                xmltt.loadXMLDocToTable();
                
                Candidates cnd = new Canopy(cg.getBlockingParams()[0], cg.getBlockingParams()[1], (int)cg.getBlockingParams()[2]);
                cnd.buildKeys(cg.getkey());
                
                Map<String,Integer> attributeKeys = getAttibuteKeys(configDocument);
                System.out.println(attributeKeys);
                loadAttributeFeaturesAssignementToDBSingleKey(attributeKeys, rdb, fPath.split("/")[fPath.split("/").length-1]);
            }
         
        }catch(Exception e){e.printStackTrace();}
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

}
