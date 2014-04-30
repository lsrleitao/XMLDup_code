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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import antlr.collections.Enumerator;

import RDB.DataBase;

public class SNMII implements Candidates{
    
    final int _windowSize;
    final int _keys;
    DataBase _db = DataBase.getSingletonObject(false);
    Set <Integer> _emptyKeys;
    
    public SNMII(int windowSize, int keys){
        _windowSize = windowSize;
        _keys = keys;
    }
    
    public void buildKeys(String[] attributes){
        _db.buildKeys(attributes, true);//Pairs Compared: 4562657, String Comparisons: 21605449, 96
        //_db.buildKeysDBMS(attributes, true, _keys);  //Pairs Compared: 2450080, String Comparisons: 15448587, 92
    }
    
    public Map<Integer,Set<Integer>> getCandidates(){
        
        Map<Integer,Set<Integer>> pairsII = new HashMap<Integer,Set<Integer>>();
        
        try{
        
            List<ResultSet> keys_rs = _db.getKeys(_keys);
            Set<Integer> emptyKeys = new HashSet<Integer>();
            
            int cntKeys = 0;
            for(int k = 0; keys_rs.size() > k; k++){
                TreeMap<String,List<Integer>> indexesII = new TreeMap<String,List<Integer>>();
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
                    
                    if(indexesII.containsKey(key)){
                        indexesII.get(key).add(id);
                    }
                    else{//System.out.println("key: " + key);
                        List<Integer> ilst = new ArrayList<Integer>();
                        ilst.add(id);
                        indexesII.put(key, ilst);
                    }
                }
                //System.out.println(indexesII.size());
                //TreeMap<String,List<Integer>> sorted_indexesII = new TreeMap<String,List<Integer>>(indexesII);
                
                pairsII = snm(indexesII,pairsII);        
     
            }
            
            //adiciona os objectos com chave vazia para comparaçao entre si e com os restantes objectos com chave nao vazia
            _emptyKeys = emptyKeys;
            /*pairsII = addEmptyKeys(emptyKeys,pairsII);
            pairsII = updateEmptyKeyPairs(new ArrayList<Integer>(emptyKeys),pairsII);
            pairsII = addLastIndexKeys(emptyKeys,pairsII,cntKeys);*/
            
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
    
    private Map<Integer,Set<Integer>> snm(TreeMap<String,List<Integer>> indexesII, Map<Integer,Set<Integer>> pairsII){
        
        List<List<Integer>> lst_index = new ArrayList<List<Integer>>();
        int cnt = 0;
        int i = 0;

        Enumeration<Entry<String, List<Integer>>> en = Collections.enumeration(indexesII.entrySet());
        while(en.hasMoreElements()){
         
            if(cnt == _windowSize){//System.out.println("Constroi bloco!"+ lst_index);
                 i++;
                 pairsII = updatePairs(lst_index, pairsII);
                 lst_index = new ArrayList<List<Integer>>();
                 cnt = 0;
                 en = Collections.enumeration(indexesII.entrySet());
                 en = forwardEntry(en,i);
            }
            Entry<String, List<Integer>> e = en.nextElement();
            lst_index.add(e.getValue());//System.out.println(e.getKey());
            cnt++;
        }
        
        pairsII = updatePairs(lst_index, pairsII);
        
        return pairsII;
    }
    
    private Enumeration<Entry<String, List<Integer>>> forwardEntry(Enumeration<Entry<String, List<Integer>>> entry, int i){
        for(int j = 0; j < i; j++){
            entry.nextElement();
        }
        return entry;
    }
    
    private Map<Integer,Set<Integer>> updatePairs(List<List<Integer>> indexes, Map<Integer,Set<Integer>> pairsII){

        int iSize = indexes.size();
        for(int i = 0; iSize > i; i++){
            
            if(iSize == 1){
                List<Integer> lst = indexes.get(0);
                int size = lst.size();
                for(int k = 0; size > k; k++){
                    if(size == 1){
                        break;
                    }
                    for(int l = k+1; size > l; l++){
                        int index1 = lst.get(k);
                        int index2 = lst.get(l);
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
                break;
            }
            
            for(int j = i+1; iSize > j; j++){
                List<Integer> index1_lst = indexes.get(i);
                List<Integer> index2_lst = indexes.get(j);
                List<Integer> mergeLst = new ArrayList<Integer>();
                mergeLst.addAll(index1_lst);
                mergeLst.addAll(index2_lst);
                int mergeLst_size = mergeLst.size();
                for(int k = 0; mergeLst_size > k; k++){
                    for(int l = k+1; mergeLst_size > l; l++){
                        int index1 = mergeLst.get(k);
                        int index2 = mergeLst.get(l);
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
