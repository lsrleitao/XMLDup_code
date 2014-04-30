package CandidateDefinition;

import java.util.Map;
import java.util.Set;

public interface Candidates {    
    
    //controi a tabela com as chaves criadas
    void buildKeys(String[] attributes);
    Set<Integer> getEmptyKeys();
    
    Map<Integer,Set<Integer>> getCandidates();
}
