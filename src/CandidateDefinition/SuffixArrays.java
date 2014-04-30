package CandidateDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import DuplicateDetection.StringMatching;
import RDB.DataBase;

public class SuffixArrays implements Candidates{
    
    DataBase _db = DataBase.getSingletonObject(false);
    private final int _lms;
    private final int _lbs;
    private final int _keys;
    private float _simThreshold;
    Set <Integer> _emptyKeys;
    
    public SuffixArrays(int lms, int lbs, int keys, float simThreshold){
        _keys = keys;
        _lms = lms;
        _lbs = lbs;
        _simThreshold = simThreshold;
    }
    
    public void buildKeys(String[] attributes){
        _db.buildKeys(attributes, false);           
    }
    
 public Map<Integer,Set<Integer>> getCandidates(){
        
        Map<Integer,Set<Integer>> pairsII = new HashMap<Integer,Set<Integer>>();
        
        try{
        
            List<ResultSet> keys_rs = _db.getKeys(_keys);
            TreeMap<String,Set<Integer>> suffixII = new TreeMap<String,Set<Integer>>();
            Set<Integer> emptyKeys = new HashSet<Integer>();
            
            System.out.print("Building Candidates...");
            int cntKeys = 0;
            for(int k = 0; keys_rs.size() > k; k++){
                
                suffixII = new TreeMap<String,Set<Integer>>();
                ResultSet rs = keys_rs.get(k);
                cntKeys = 0;
                while(rs.next()){
                    cntKeys++;
                    int id = rs.getInt(1)-1;  
                    String key = rs.getString(2);
                    
                    if(key.isEmpty()){
                        emptyKeys.add(id);
                        continue;
                    }
                    
                    suffixII = addSuffixes(id,key,_lms,suffixII);
                }
                
                //System.out.println(suffixII);
                //ordena suffixII
                //TreeMap<String,Set<Integer>> sorted_suffixII = new TreeMap<String,Set<Integer>>(suffixII);
                suffixII = mergeBlocks(suffixII);
                pairsII = updatePairs(suffixII,pairsII);
                System.out.println("FINISH!");
                
            }
            
            //Adiciona chaves vazias
            _emptyKeys = emptyKeys;
            /*pairsII = addEmptyKeys(emptyKeys, pairsII);
            pairsII = updateEmptyKeyPairs(new ArrayList<Integer>(emptyKeys),pairsII);
            pairsII = addLastIndexKeys(emptyKeys, pairsII, cntKeys);*/
            
            for(int i = 0; keys_rs.size() > i; i++){
                keys_rs.get(i).close();
            }
            
            
        }catch(SQLException sqle){sqle.printStackTrace();}

        return pairsII;       
    }
 
     private Map<Integer,Set<Integer>> addLastIndexKeys(Set<Integer> emptyKeys, Map<Integer,Set<Integer>> candidates, int cntKeys){
         
         if(emptyKeys.size() == 0){
             return candidates;
         }
         
         for(int i = 0; cntKeys > i; i++){
             if(!emptyKeys.contains(i) && !candidates.containsKey(i)){
                 candidates.put(i, emptyKeys);
             }
         }
         
         return candidates;
     }
 
    private Map<Integer,Set<Integer>> updateEmptyKeyPairs(List<Integer> indexes, Map<Integer,Set<Integer>> pairsII){

     int iSize = indexes.size();
     for(int i = 0; iSize > i; i++){
         for(int j = i+1; iSize > j; j++){
             int index1 = indexes.get(i);
             int index2 = indexes.get(j);
             int containsIndex = containsIndexes(pairsII, index1, index2);
             if(containsIndex == 0){
                 pairsII.get(index1).add(index2);
             }
             else if(containsIndex == 1){
                 pairsII.get(index2).add(index1);
             }
             else if(containsIndex == 2){
                 Set<Integer> iSet = new HashSet<Integer>();
                 iSet.add(index2);
                 pairsII.put(index1, iSet);
             }
         }
     }
     
     return pairsII;
 }
 
     private Map<Integer,Set<Integer>> addEmptyKeys(Set<Integer> emptyKeys, Map<Integer,Set<Integer>> pairsII){
         Iterator<Integer> it = emptyKeys.iterator();
         while(it.hasNext()){
             int index = it.next();
             for(Map.Entry<Integer, Set<Integer>> e : pairsII.entrySet()){
                 e.getValue().add(index);
             }
         }        
     return pairsII;
 }
 
    private TreeMap<String,Set<Integer>> addSuffixes(int id, String key, int suffixSize, TreeMap<String,Set<Integer>> suffixII){
        
        List<String> lst = generateSuffixes(key, suffixSize);
        lst.add(key);
        
        for(int i = 0; lst.size() > i; i++){

            String suffix = lst.get(i);//System.out.println(suffix);
            
            if(suffixII.containsKey(suffix)){//System.out.println("entrou");
                suffixII.get(suffix).add(id);
            }
            else{
                Set<Integer> indexes = new HashSet<Integer>();
                indexes.add(id);
                suffixII.put(suffix,indexes);
            }
        }
        
        return suffixII;
    }
    
    private List<String> generateSuffixes(String key, int suffixSize){
        List<String> suffixes = new ArrayList<String>();
        int keySize = key.length();
        //sufixos
        for(int i = keySize-suffixSize; i > 0; i--){
            suffixes.add(key.substring(i,keySize));
        }
        //prefixos
        for(int i = suffixSize; i < keySize-1; i++){
            suffixes.add(key.substring(0,i));
        }
        return suffixes;
    }
 
    private TreeMap<String,Set<Integer>> mergeBlocks(TreeMap<String,Set<Integer>> suffixII){
        
        TreeMap<String,Set<Integer>> suffixII_aux = new TreeMap<String,Set<Integer>>(suffixII);
        
        StringMatching sm = new StringMatching();
        String key_prev = "";
        Set<Integer> lst_prev = new HashSet<Integer>();
        for(Map.Entry<String, Set<Integer>> e : suffixII_aux.entrySet()){
            String key = e.getKey();
            Set<Integer> lst = e.getValue();
            if(sm.jaro(key, key_prev) >= _simThreshold){
                suffixII.remove(key_prev);
                Enumeration<Integer> it = Collections.enumeration(lst_prev);
                while(it.hasMoreElements()){
                    suffixII.get(key).add(it.nextElement());
                }
            }
            key_prev = key;
            lst_prev = new HashSet<Integer>(lst);
            
        }
        
        return suffixII;
    }
    
    private Map<Integer,Set<Integer>> updatePairs(Map<String,Set<Integer>> suffixII, Map<Integer,Set<Integer>> pairsII){

        for(Map.Entry<String, Set<Integer>> e : suffixII.entrySet()){
        
            List<Integer> indexes = new ArrayList<Integer>(e.getValue());
            int iSize = indexes.size();
            
            //remove blocos acima de uma determinada dimensao
            if(iSize > _lbs){
                continue;
            }
            
            for(int i = 0; iSize > i; i++){
                for(int j = i+1; iSize > j; j++){
                    int index1 = indexes.get(i);
                    int index2 = indexes.get(j);
                    int containsIndex = containsIndexes(pairsII, index1, index2);
                    Set<Integer> iSet;
                    if(containsIndex == 0){
                        iSet = pairsII.get(index1);
                        pairsII.get(index1).add(index2);
                    }
                    else if(containsIndex == 1){
                        iSet = pairsII.get(index2);
                        pairsII.get(index2).add(index1);
                    }
                    else if(containsIndex == 2){
                        iSet = new HashSet<Integer>();
                        iSet.add(index2);
                        pairsII.put(index1, iSet);
                    }
                }
            }
        }
        
        return pairsII;
    }
    
    private int containsIndexes(Map<Integer,Set<Integer>> pairsII, int i1, int i2){
        
        Set<Integer> i1_cnt = pairsII.get(i1);
        Set<Integer> i2_cnt = pairsII.get(i2);
        
        if((i1_cnt != null && i1_cnt.contains(i2)) ||
           (i2_cnt != null && i2_cnt.contains(i1))){
            return -1;
        }
        else if(i1_cnt != null){
            return 0;
        }
        else if(i2_cnt != null){
            return 1;
        }
        else{
            return 2;
        }
    }
    
    public Set<Integer> getEmptyKeys(){
        return _emptyKeys;
    }

}
