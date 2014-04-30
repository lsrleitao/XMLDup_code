package CandidateDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import RDB.DataBase;

public class Canopy implements Candidates{

    private DataBase _db = DataBase.getSingletonObject(false);
    private String _simMeasure = "Jaccard";//alternativa "TFIDF"
    private boolean _compareAllAttributesInKey = false; //por a true para considerar todos os atributos, mesmo quando nao existem em ambos os objectos do par
    private float _t1;
    private float _t2;
    private int _gramSize;
    private Map<String,Integer> _idf = new LinkedHashMap<String,Integer>();
    private Map<Integer,Map<String,Integer>> _tf = new LinkedHashMap<Integer,Map<String,Integer>>();
    LinkedList<Integer> _indexList = new LinkedList<Integer>();
    Set<Integer> _emptyKeys;
    
    private Map<Integer,Map<Integer,Set<String>>> _terms = new LinkedHashMap<Integer, Map<Integer,Set<String>>>();
    
    public Canopy(float t1, float t2, int gramSize){
        _t1 = t1;
        _t2 = t2;
        _gramSize = gramSize;
    }
    
    public void buildKeys(String[] attributes){
        _db.buildKeys2(attributes, false);
        //_db.buildKeysDBMS(attributes, false, 1);
    }
    
    private boolean keyIsEmpty(String key){
        char[] str_array = key.toCharArray();
        for(int i = 0; str_array.length > i; i++){
            if(str_array[i] != '#'){
                return false;
            }
        }
        return true;
    }
    
    public Map<Integer,Set<Integer>> getCandidates(){
            
        Map<Integer,Set<Integer>> cnd = new HashMap<Integer,Set<Integer>>();
        Set<Integer> emptyKeys = new HashSet<Integer>();
        
        try{
        
            List<ResultSet> keys_rs = _db.getKeys(1);
            ResultSet rs = keys_rs.get(0);
            int cntKeys = 0;
            while(rs.next()){
                cntKeys++;
                int index = rs.getInt(1)-1;
                String key = rs.getString(2);
                
                //if(key.isEmpty()){
                if(keyIsEmpty(key)){
                    emptyKeys.add(index);
                    continue;
                }
                
                //String[] keyGrams = getNGrams(key,_gramSize);
                //System.out.println("Key: " + key);
                //printArray(keyGrams);
                /*if(this._simMeasure.equals("TFIDF")){
                    updateTFIDF(keyGrams, index);
                }
                else{*/
                    updateTerms(key,_gramSize, index);
                //}
                _indexList.add(index);
            }
            
            System.out.print("Building Canopies...");
            
            //if(this._simMeasure.equals("Jaccard")){
                cnd = buildCanopiesJaccard(_terms);
            /*}
            else{
                Map<Integer,Map<String,Float>> TFIDF_matrix = buildTFIDFMatrix();
                cnd = buildCanopiesTFIDF(TFIDF_matrix, _tf);
            }*/
            System.out.println("FINISH!");
            
            //Adiciona chaves vazias
            _emptyKeys = emptyKeys;
            /*cnd = addEmptyKeys(emptyKeys, cnd);
            cnd = updateEmptyKeyPairs(new ArrayList<Integer>(emptyKeys),cnd);
            cnd = addLastIndexKeys(emptyKeys, cnd, cntKeys);*/
            
            rs.close();
            
        }catch(SQLException sqle){sqle.printStackTrace();}
        
        return cnd;
    }
    
    private void updateTerms(String key, int gramSize, int index){
        
        String[] attributeKey = key.split("#",-1);
        String concatAttributes = key.replace("#", "");
        
        Map<Integer,Set<String>> attributeGrams = new LinkedHashMap<Integer,Set<String>>();
        int keyGramsSize;
        int attributeKeySize = attributeKey.length;
        
        if(!_compareAllAttributesInKey){
        
            for(int i = 0; attributeKeySize > i; i++){
            
                String[] keyGrams = getNGrams(attributeKey[i],gramSize);
                            
                if(keyGrams == null){
                    keyGramsSize = 0;
                }
                else{
                    keyGramsSize = keyGrams.length;
                }
    
                Set<String> strt = new LinkedHashSet<String>();
                
                for(int j = 0; keyGramsSize > j; j++){
                    strt.add(keyGrams[j]);
                }
                
                attributeGrams.put(i, strt);          
            }
        
        }
        else{  
          //armazena q-grams da concatenaçao
            String[] keyGrams = getNGrams(concatAttributes,gramSize);
            keyGramsSize = keyGrams.length;
            Set<String> strt = new LinkedHashSet<String>();
            for(int j = 0; keyGramsSize > j; j++){
                strt.add(keyGrams[j]);
            }       
            attributeGrams.put(-1, strt);
          //fim armazena q-grams da concatenaçao
        }
        
        _terms.put(index, attributeGrams);
        
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
    
    //parte a string em segmentos de tamanho gramSize sem sobreposiçao
//    private String[] getNGrams(String s, int gramSize){
//        int size = s.length();
//        int keyGramSize = (int)Math.ceil(((float)s.length()/(float)gramSize));
//        String[] res = new String[keyGramSize];
//        int i = 0;
//        int j = 0;
//        while(i < size){
//            if((i + gramSize) > size){
//                res[j] = s.substring(i, size);
//                
//                int charLeft = gramSize - res[j].length();
//                for(int k = 0; k < charLeft; k++){
//                    res[j]+="*";
//                }
//                
//                break;
//            }
//            else{
//                res[j] = s.substring(i, i + gramSize);
//            }
//            i = i + gramSize;
//            j++;
//        }
//        
//        return res;
//    }
    
    //parte a string em segmentos de tamanho gramSize com sobreposiçao.
    //compoe blocos iniciais apenas com o primeiro caracter.
    //estrategia como usada no paper "Approximate String Joins in a Database (Almost) for Free"
    private String[] getNGrams(String s, int gramSize){
       
        if(s.isEmpty()){
            return null;
        }
        
        s = addQgramCharacters(s,gramSize);

        int size = s.length();
        int keyGramSize = s.length()-gramSize+1;
        
        if(keyGramSize <= 0){
            return new String[0];
        }
        
        String[] res = new String[keyGramSize];
        int i = 0;
        while(i < (size-gramSize+1)){
            res[i] = s.substring(i, i + gramSize);
            i++;
        }
        
        return res;
    }
    
    private String addQgramCharacters(String s, int q){
        
        String startCharacter = "#";
        String endCharacter = "$";
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder(s);
        
        for(int i = 0; q-1 > i; i++){
            sb.append(startCharacter);
            sb2.append(endCharacter);
        }
        
        sb.append(sb2);
        
        return sb.toString();
    }
    
    private void updateTFIDF(String[] keyGrams, int objIndex){
        
        Map<String,Integer> objFreq = new HashMap<String,Integer>();
        HashSet<String> addedTokens = new HashSet<String>();
        String token;
        
        for(int i = 0; keyGrams.length > i; i++){
            
            token = keyGrams[i];
            Integer freq;
            
            //update idf
            if(!addedTokens.contains(token)){
                freq = _idf.get(token);
                if(freq != null){
                    _idf.put(token, freq+1);
                }
                else{
                    _idf.put(token, 1);
                }
                addedTokens.add(token);
            }
            
            //update tf
            freq = objFreq.get(token);
            if(freq != null){
                objFreq.put(token, freq+1);
            }
            else{
                objFreq.put(token, 1);
            }
        }
        _tf.put(objIndex, objFreq);
    }
    
    private void printArray(String[] a){
        System.out.print("Array: ");
        for(int i = 0; a.length >i; i++){
            System.out.print(a[i]+",");
        }
        System.out.println();
    }
    
    private Map<Integer,Set<Integer>> buildCanopiesJaccard(Map<Integer,Map<Integer,Set<String>>> terms){
        Map<Integer,Set<Integer>> res = new HashMap<Integer,Set<Integer>>();
        
        //Random r = new Random();
        
        Iterator<Integer> it = _indexList.iterator();
        
        int size1 = _indexList.size();
        for(int i = 0; size1 > i; i++){
        //while(it.hasNext()){
            
            //int randPos = r.nextInt(_indexList.size());
            //int objIndex1 = _indexList.get(randPos);
      
            int objIndex1 = it.next();
            //_indexList.remove(randPos);
            it.remove();
            
            List<Integer> lst_aux = new ArrayList<Integer>();
            lst_aux.add(objIndex1);
            int objIndex2;
            float sim;
            
            int size2 = _indexList.size();
            for(int j = 0; size2 > j; j++){
            //while(it.hasNext()){
                objIndex2 = it.next();

                sim = jaccard(terms.get(objIndex1), terms.get(objIndex2));
                /*if((objIndex1 == 3691 & objIndex2 == 9691) || (objIndex2 == 3691 & objIndex1 == 9691)){
                    System.out.println("PAIR sim : " + sim);
                    System.out.println("objIndex1: " + objIndex1);
                    System.out.println("objIndex2: " + objIndex2);
                }*/
                
                if(sim < _t2){
                    continue;
                }
                else if(_t1 <= sim){
                    lst_aux.add(objIndex2);
                    it.remove();
                    
                    size2 = size2-1;
                    size1 = size1-1;
                }
                else if(_t2 <= sim){
                    lst_aux.add(objIndex2);
                }
    
            }
            //System.out.println("cand: " + lst_aux);
            res = updateState(res, lst_aux);
            it = _indexList.iterator();
        }
        
        /*if((res.containsKey(9178) & res.get(9178).contains(3178)) || (res.containsKey(3178) & res.get(3178).contains(9178))){
            System.out.println("TEM!!");

        }*/
     
        return res;
    }
    
    private Map<Integer,Set<Integer>> buildCanopiesTFIDF(Map<Integer,Map<String,Float>> tfidfMatrix, Map<Integer,Map<String,Integer>> tfMatrix){
        Map<Integer,Set<Integer>> res = new HashMap<Integer,Set<Integer>>();
        
        //Random r = new Random();
        
        Iterator<Integer> it = _indexList.iterator();
        
        while(it.hasNext()){
            //int randPos = r.nextInt(_indexList.size());
            //int objIndex1 = _indexList.get(randPos);
   
            int objIndex1 = it.next();
            Map<String,Float> v1 = tfidfMatrix.get(objIndex1);
            //_indexList.remove(randPos);
            it.remove();
            
            List<Integer> lst_aux = new ArrayList<Integer>();
            lst_aux.add(objIndex1);
            int objIndex2;
            Map<String,Float> v2;
            float sim;
            while(it.hasNext()){
                objIndex2 = it.next();
                v2 = tfidfMatrix.get(objIndex2);
                
                sim = cosine(v1,v2);
                
                if(sim < _t2){
                    continue;
                }
                else if(_t1 <= sim){
                    lst_aux.add(objIndex2);
                    it.remove();
                }
                else if(_t2 <= sim){
                    lst_aux.add(objIndex2);
                }
    
            }
            //System.out.println("cand: " + lst_aux);
            res = updateState(res, lst_aux);
            it = _indexList.iterator();
        }
     
        return res;
    }
    
    private float cosine(Map<String,Float> v1, Map<String,Float> v2){
        
        int num=0;
        int denum_a=0;
        int denum_b=0;
        Float v1_pos=0f;
        Float v2_pos=0f;
        
        String token;
        
        for(Map.Entry<String,Float> e1 : v1.entrySet()){
            token = e1.getKey();
            v1_pos = e1.getValue();
            v2_pos = v2.get(token);
            
            if(v2_pos==null){
                v2_pos=0f;
            }
            num+=v1_pos*v2_pos;
            denum_a+=Math.pow(v1_pos,2);
            denum_b+=Math.pow(v2_pos,2);
        }
        
        for(Map.Entry<String,Float> e2 : v2.entrySet()){
            
            if(!v1.containsKey(e2.getKey())){

                denum_b+=Math.pow(e2.getValue(),2);  
            }
          
        }
        
        float res = (float)num/(float)(Math.sqrt(denum_a)*Math.sqrt(denum_b));
        return res;
    }
    
    private Set<String> mergeQgrams(Set<String> src, Set<String> dst){
        
        int dstSize = dst.size();
        
        if(dstSize == 0){
            return src;
        }
              
        //System.out.println("dst: " + dst);
        //System.out.println("src: " + src);
        
        Iterator<String> dst_iter = dst.iterator();
        List<String> gramsToMerge = new ArrayList<String>();
        int i = 1;
        
        
        //dst_iter.next();
        while(i <= dstSize-_gramSize+1/*-1*/){
            dst_iter.next();
            i++;
        }
        
        Set<String> dstModified = new LinkedHashSet<String>(dst);
        i = 1;
        //dst_iter.next();
        while(i <= _gramSize-1){
            String g = dst_iter.next();
            gramsToMerge.add(g);
            dstModified.remove(g);
            i++;
        }
        
        Iterator<String> src_iter = src.iterator();
        Set<String> srcModified = new LinkedHashSet<String>(src);
        i = 1;
        while(i <= _gramSize-1){
            String src_g = src_iter.next();
            String dst_g = gramsToMerge.get(i-1);
            srcModified.remove(src_g);
            dst_g = dst_g.substring(0, _gramSize-i);
            src_g = src_g.substring(_gramSize-i, _gramSize);
            dstModified.add(dst_g.concat(src_g));
            i++;
        }
        
        dstModified.addAll(srcModified);//System.out.println(dstModified);
        
        return dstModified;
    }
    
 private float jaccard(Map<Integer,Set<String>> k1, Map<Integer,Set<String>> k2){
     
     //System.out.println(k1);
     //System.out.println(k2);
     
        Set<String> v1 = new LinkedHashSet<String>();
        Set<String> v2 = new LinkedHashSet<String>();

        if(_compareAllAttributesInKey){
            v1 = k1.get(-1);
            v2 = k2.get(-1);
        }
        else{
            for(Map.Entry<Integer,Set<String>> e: k1.entrySet()){
                int attrIndex = e.getKey();
                
                if(k1.get(attrIndex).size() > 0 & k2.get(attrIndex).size() > 0){
                    //apenas adiciona os q-grams extraidos de cada atributo dado que o mesmo atributo esta presente em ambas os objectos
                    
                    //System.out.println(attrIndex);                    
                    //System.out.println(k1.get(attrIndex));
                    //System.out.println(k2.get(attrIndex));
                    
                    v1 = mergeQgrams(k1.get(attrIndex), v1);
                    v2 = mergeQgrams(k2.get(attrIndex), v2);
                }
            }
        
        }
        
        
        //System.out.println(v1);
        //System.out.println(v2);
        
  
        float num=0; 
        String token;
        Set<String> union = new HashSet<String>();
        
        Iterator<String> v1_iter = v1.iterator();
              
        int v1Size = v1.size();
        for(int i = 0; v1Size > i; i++){
        //while(v1_iter.hasNext()){
            token = v1_iter.next();
            union.add(token);
            
            if(v2.contains(token)){
                num++;
            }
        }
        
        Iterator<String> v2_iter = v2.iterator();
        
        int v2Size = v2.size();
        for(int i = 0; v2Size > i; i++){
        //while(v2_iter.hasNext()){
            union.add(v2_iter.next());
        }
        
        //System.out.println("sim: " + (float)num/(float)union.size());
        
        float unionSize = union.size();
        
        if(unionSize == 0){
            //System.out.println("Pair");
            //System.out.println(k1);
            //System.out.println(k2);
            return this._t2;
        }
        
        return num/unionSize;
    }
    
    private Map<Integer,Map<String,Float>> buildTFIDFMatrix(){
        
        int tfSize = _tf.size();
        
        Map<Integer,Map<String,Float>> res = new HashMap<Integer,Map<String,Float>>();

        for(Map.Entry<Integer, Map<String,Integer>> e : _tf.entrySet()){
            
            Map<String,Integer> tfEntry = e.getValue();
            
            Map<String,Float> tfidfW = new HashMap<String,Float>();
         
            for(Map.Entry<String, Integer> e2 : _idf.entrySet()){             
                String token = e2.getKey();       
                Integer tokenTF = tfEntry.get(token);               
                
                if(tokenTF != null){
                    float tokenIDF = (float)tfSize/(float)e2.getValue();
                    tfidfW.put(token, (float)Math.log(1+tokenTF) * (float)Math.log(tokenIDF));
                }               
                
            }
            
            res.put(e.getKey(), tfidfW);
        }
        
        return res;
    }
    
    private Map<Integer,Set<Integer>> updateState(Map<Integer,Set<Integer>> state, List<Integer> canopyIndexes){
        
        int size = canopyIndexes.size();
        for(int i = 0; size > i; i++){
            int objIndex1 = canopyIndexes.get(i);
            for(int j = i + 1; size > j; j++){
                int objIndex2 = canopyIndexes.get(j);
                
                Set<Integer> objIndex1cnt = state.get(objIndex1);
                Set<Integer> objIndex2cnt = state.get(objIndex2);
                
                if(objIndex1cnt == null && objIndex2cnt == null){
                    HashSet<Integer> iSet = new HashSet<Integer>();
                    iSet.add(objIndex2);
                    state.put(objIndex1, iSet);                   
                }
                else if(objIndex1cnt == null /*&& !objIndex2cnt.contains(objIndex1)*/){
                    state.get(objIndex2).add(objIndex1);
                }
                else if(objIndex2cnt == null /*&& !objIndex1cnt.contains(objIndex2)*/){
                    state.get(objIndex1).add(objIndex2);
                }
                else if(/*objIndex1cnt != null && objIndex2cnt != null &&*/ !objIndex1cnt.contains(objIndex2) && !objIndex2cnt.contains(objIndex1)){
                    state.get(objIndex1).add(objIndex2);

                }
                    
            }
        }
        
        return state;
    }
    
private Map<Integer,Set<Integer>> updateStateUnoptimized(Map<Integer,Set<Integer>> state, List<Integer> canopyIndexes){
        
        int size = canopyIndexes.size();
        for(int i = 0; size > i; i++){
            int objIndex1 = canopyIndexes.get(i);
            for(int j = i + 1; size > j; j++){
                int objIndex2 = canopyIndexes.get(j);
                
                Set<Integer> objIndex1cnt = state.get(objIndex1);
                Set<Integer> objIndex2cnt = state.get(objIndex2);
                
                if(objIndex1cnt != null){
                    if(!objIndex1cnt.contains(objIndex2)){
                        if(objIndex2cnt != null){
                            if(!objIndex2cnt.contains(objIndex1)){
                                state.get(objIndex2).add(objIndex1);
                            }
                        }
                        else{
                            state.get(objIndex1).add(objIndex2);
                        }
                    }
                }
                else if(objIndex2cnt != null){
                    if(!objIndex2cnt.contains(objIndex1)){
                        state.get(objIndex2).add(objIndex1);
                    }
                }
                else{
                    HashSet<Integer> iSet = new HashSet<Integer>();
                    iSet.add(objIndex2);
                    state.put(objIndex1, iSet); 
                }
            }
        }
            
        return state;
    }
    
            
    public Set<Integer> getEmptyKeys(){
        return _emptyKeys;
    }
    
}
