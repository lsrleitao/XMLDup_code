package ObjectTopology.BestStructureSelection;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import RDB.DataBase;

public class BestStructureStats {
    
    private List<String> _attributes;
    private Hashtable<String,Hashtable<Integer,Integer>> _levelHistogram = new Hashtable<String,Hashtable<Integer,Integer>>();
    private DataBase _rdb;
    
    public BestStructureStats(String bestStructuresPath, Document selectedStructure, DataBase rdb){
        
        _attributes = getAttributes(selectedStructure.getDocumentElement().getChildNodes(), new ArrayList<String>());
        this._rdb = rdb;
        
        
        try{
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc;
        
            File f = new File(bestStructuresPath);
            File[] files = f.listFiles();
            for(int i = 0; files.length > i; i++){
                doc = builder.parse(files[i]);
                storeAttributesLevel(doc.getDocumentElement().getChildNodes(), 1);
            }
            System.out.println(_levelHistogram);
        
        }catch(Exception e){e.printStackTrace();}
    }
    
    private List<String> getAttributes(NodeList nl, List<String> attrList){
        
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
                    attrList.add(nodeName);
                }
                else{
                    attrList = getAttributes(node.getChildNodes(), attrList);
                }
                    
            }
        }
        return attrList;
    }
    
    private void storeAttributesLevel(NodeList nl, int level){
        
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
                   if(_levelHistogram.containsKey(nodeName)){
                       Hashtable<Integer,Integer> ht = _levelHistogram.get(nodeName);
                       if(ht.containsKey(level)){
                           ht.put(level,ht.get(level)+1);
                           _levelHistogram.put(nodeName, ht);
                       }
                       else{
                           ht.put(level, 1);
                           _levelHistogram.put(nodeName, ht);
                       }
                   }
                   else{
                       Hashtable<Integer,Integer> ht = new Hashtable<Integer,Integer>();
                       ht.put(level, 1);
                       _levelHistogram.put(nodeName, ht);
                   }
                }
                else{
                    storeAttributesLevel(node.getChildNodes(), level+1);
                }                    
            }
        }
    }
    
    public void storeLevelOccurrence(){
        
        for(int i = 0; _attributes.size() > i; i++){
            setLevelOccurrence(_levelHistogram.get(_attributes.get(i)), _attributes.get(i));    
        }
        
    }
    
    private void setLevelOccurrence(Hashtable<Integer,Integer> ht, String attributeName){
        
        List<Integer> lst = new ArrayList<Integer>();
        
        Enumeration<Integer> e = ht.keys();
        while(e.hasMoreElements()){
            int key = e.nextElement();
            
            lst.add(key);
            lst.add(ht.get(key));
        }
        
        _rdb.insertAttrNodeLevelOccurences(attributeName, lst);

    }
    
    public void storeLevelStats(){
        String attribute;
        for(int i = 0; _attributes.size() > i; i++){
            attribute = _attributes.get(i);
            
            _rdb.insertLevelStats(attribute,
                                  _levelHistogram.get(attribute).size(),
                                  getMaxOccurrenceLevel(attribute),
                                  getMaxOccurPercentage(attribute),
                                  getOccurEntropy(attribute),
                                  getOccurStdDeviation(attribute),
                                  getOccurDiversity(attribute));
        }
    }
    
    private int getMaxOccurrenceLevel(String attribute){
        Hashtable<Integer,Integer> ht = _levelHistogram.get(attribute);
        Enumeration<Integer> e = ht.keys();
        int max = -1;
        int occurrences;
        int key;
        int level = -1;
        while(e.hasMoreElements()){
            key = e.nextElement();
            occurrences = ht.get(key);
            if(occurrences > max){
                max = occurrences;
                level = key; 
            }
        }
        
        return level;
    }
    
    private float getMaxOccurPercentage(String attribute){
        Hashtable<Integer,Integer> ht = _levelHistogram.get(attribute);
        Enumeration<Integer> e = ht.keys();
        int totalOccurrences = 0;
        int max = -1;
        int occurrences;
        int key;
        while(e.hasMoreElements()){
            key = e.nextElement();
            occurrences = ht.get(key);
            totalOccurrences += occurrences;
            if(occurrences > max){
                max = occurrences;
            }
        }
        
        return ((float) max)/((float)totalOccurrences);
    }
    
    private float getOccurEntropy(String attribute){
        Hashtable<Integer,Integer> ht = _levelHistogram.get(attribute);
        Enumeration<Integer> e = ht.keys();
        int totalOccurrences = 0;
        int occurrences;
        int key;
        List<Integer> occur = new ArrayList<Integer>();
        float entropy = 0;
        while(e.hasMoreElements()){
            key = e.nextElement();
            occurrences = ht.get(key);
            totalOccurrences += occurrences;
            occur.add(occurrences);
        }
        
        float po;
        for(int i = 0; occur.size() > i; i++){
            po = (float)occur.get(i)/(float)totalOccurrences;
            entropy += po*Math.log(po);
        }
        
        entropy *= -1;
        
        return entropy;
    }
    
    private double getOccurStdDeviation(String attribute){
        Hashtable<Integer,Integer> ht = _levelHistogram.get(attribute);
        float elements = ht.size();       
        Enumeration<Integer> e = ht.keys();
        int occurrences;
        int key;
        float numeratorSum = 0;
        int totalOccurrences = 0;
        List<Integer> occur = new ArrayList<Integer>();
        
        while(e.hasMoreElements()){
            key = e.nextElement();
            occurrences = ht.get(key);
            totalOccurrences += occurrences;
            occur.add(occurrences);
        }
        
        float average = totalOccurrences/elements;
        
        for(int i = 0; occur.size() > i; i++){
            occurrences = occur.get(i);
            totalOccurrences += occurrences;
            numeratorSum += Math.pow(((float)occurrences-average),2);
        }
        
        double std = Math.sqrt(numeratorSum/elements);
              
        return std;
    }
    
    private float getOccurDiversity(String attribute){
        Hashtable<Integer,Integer> ht = _levelHistogram.get(attribute);
        Enumeration<Integer> e = ht.keys();
        float totalOccurrences = 0;
        int occurrences;
        int key;
        float numerator = 0;
        while(e.hasMoreElements()){
            key = e.nextElement();
            occurrences = ht.get(key);
            totalOccurrences += occurrences;
            numerator+=(occurrences*(occurrences-1));
        }
        if(numerator == 0)
            return 0;
        else
            return numerator/(totalOccurrences*(totalOccurrences-1));
    }
    
    public void closeDBConnection(){     
        _rdb.closeConnection();
    }

}
