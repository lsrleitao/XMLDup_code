package CandidateDefinition;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NoBlocking implements Candidates{

    private long _dbSize;
    Set <Integer> _emptyKeys;
    
    public NoBlocking(long dbSize){
        _dbSize = dbSize;
    }
    
    public void buildKeys(String[] attributes){
    }
    
    public Map<Integer,Set<Integer>> getCandidates(){
        Map<Integer,Set<Integer>> cnd = new HashMap<Integer,Set<Integer>>();
        
        Set<Integer> iSet = new HashSet<Integer>();
        for(int i = 0; _dbSize > i; i++){          
            iSet = new HashSet<Integer>();
            for(int j = i+1; _dbSize > j; j++){              
                iSet.add(j);              
            }
            cnd.put(i, iSet);
        }
        
        return cnd;
    }
    
    public Set<Integer> getEmptyKeys(){
        return _emptyKeys;
    }
}
