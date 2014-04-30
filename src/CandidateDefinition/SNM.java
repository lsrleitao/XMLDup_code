package CandidateDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import RDB.DataBase;

public class SNM implements Candidates{
    
    final int _windowSize;
    final int _keys;
    DataBase _db = DataBase.getSingletonObject(false);
    Set <Integer> _emptyKeys;
    
    public SNM(int windowSize, int keys){
        _windowSize = windowSize;
        _keys =keys;
    }
    
    public void buildKeys(String[] attributes){
        _db.buildKeys(attributes, true);
            
    }
    
    public Map<Integer,Set<Integer>> getCandidates(){
        
        
        Map<Integer,Set<Integer>> pairsII = new HashMap<Integer,Set<Integer>>();
        
        try{
        
            List<ResultSet> keys_rs = _db.getKeys(_keys);
            
            Set<Integer> emptyKeys = new HashSet<Integer>();
            
            int cntKeys = 0;
            for(int k = 0; keys_rs.size() > k; k++){
            
                List<Integer> indexes = new ArrayList<Integer>();
                int cnt = 0;
                int i = 1;
                ResultSet rs = keys_rs.get(k);
                
                cntKeys = 0;
                while(rs.next()){
                    cntKeys++;
                    if(rs.getString(2).isEmpty()){
                        emptyKeys.add(rs.getInt(1)-1);
                        i++;
                        continue;
                    }
                    
                    if(cnt == _windowSize){//System.out.println(indexes);
                        pairsII = updatePairs(indexes,pairsII); 
                        i++;
                        //verificar o custo computacional do metodo absolute()
                        rs.absolute(i);
                        cnt = 0;
                        indexes = new ArrayList<Integer>();
                    }
                    
                    //get ID column
                    indexes.add(rs.getInt(1)-1);
                    cnt++;
                }
                
                pairsII = updatePairs(indexes,pairsII);
               
     
            }
            
            //Adiciona chaves vazias
            _emptyKeys = emptyKeys;
            //System.out.println("EK: " + emptyKeys);
            /*pairsII = addEmptyKeys(emptyKeys, pairsII);
            pairsII = updatePairs(new ArrayList<Integer>(emptyKeys),pairsII);
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
    
    private Map<Integer,Set<Integer>> updatePairs(List<Integer> indexes, Map<Integer,Set<Integer>> pairsII){

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
