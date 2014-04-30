package DuplicateDetection;

// SecondString
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import RDB.DataBase;

import com.wcohen.ss.Jaro;
import com.wcohen.ss.JaroWinkler; 
// import com.wcohen.ss.Level2Levenstein;
import com.wcohen.ss.MongeElkan;
import com.wcohen.ss.Jaccard; 
// import com.wcohen.ss.NeedlemanWunsch;
// import com.wcohen.ss.SmithWaterman;
import com.wcohen.ss.SoftTFIDF;
import com.wcohen.ss.api.StringDistance;

// Simmetrics
import uk.ac.shef.wit.simmetrics.similaritymetrics.Soundex;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import uk.ac.shef.wit.simmetrics.similaritymetrics.QGramsDistance;
import uk.ac.shef.wit.simmetrics.similaritymetrics.CosineSimilarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.EuclideanDistance;

/**
 * @author Lu�s Leit�o
 */
public class StringMatching {

    boolean _similarityScoresPreviouslyLoaded = false;
    DataBase _rdb = DataBase.getSingletonObject(false);
    
    public StringMatching() {

    }
    
    public int checkDataType(String str){
        
        int res = -1;
        char[] c = new char[]{'0','1','2','3','4','5','6','7','8','9'};
        char[] str_array = str.toCharArray();
        char symbol;
        boolean numberOcurrence = false;
        boolean letterOcurrence = false;

        int size = str_array.length;
        for (int i = 0; i < size; i ++) {
            symbol = str_array[i];
            
            if(numberOcurrence && (!isSymbol(c,symbol)) || (letterOcurrence && isSymbol(c,symbol))){
                res = 3;
                break;
            }
            if(isSymbol(c,symbol)){
                numberOcurrence = true;
                if(i == size-1){
                    res = 2;
                    break;
                }
            }
            if(!isSymbol(c,symbol)){
                    letterOcurrence = true;
                    if(i == size-1){
                        res = 1;
                        break;
                    }
            }
        }
        return res;
    }

    public String removeSpaces(String s) {

        StringBuilder str_aux = new StringBuilder();
        char[] str_array = s.toCharArray();
        int size = str_array.length;
        char c;
        for (int i = 0; size > i; i++) {
            c = str_array[i];
            if (c != ' ' && c != '\t' && c !='\n')
                str_aux.append(c);
        }
        return str_aux.toString();
    }

    public boolean isEmptyString(String s) {
        if (s.isEmpty() || removeSpaces(s).isEmpty())
            return true;
        return false;
    }
    
    public char unAccent(char symbol) {

        String s = String.valueOf(symbol);
        
        String temp = Normalizer.normalize(s, Normalizer.Form.NFD);
        Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
        return pattern.matcher(temp).replaceAll("").toCharArray()[0];
    }

    //Faz o pre-processamento das Strings. Remove espaços, passa letras para lower case, etc
    /*public String preProcessString(String str) {
        
        boolean removeSpace = true;
        
        char[] c = new char[]{' ','\t','\n','\r','.',':',';',',','-','_','\\',')','(','#',
                '\'','"','!','?','&','[',']','{','}','/','*', '`','^','~','$',
                '%','@','>','<','»','«'};
        
        if(!removeSpace){
            c[0] = '.';
        }
        
        char[] str_array = str.toCharArray();
        char symbol;
        StringBuilder str_aux = new StringBuilder();

        int size = str_array.length;
        for (int i = 0; i < size; i++) {
            symbol = str_array[i];
            if (!isSymbol(c, symbol))
                str_aux.append(unAccent(Character.toLowerCase(symbol)));
        }

        //Para pre-processar devolver str_aux, caso contrario devolver str
        //return str;
        return str_aux.toString();
    }*/
    
    public String preProcessString(String input) { 
        
        boolean appendWhiteSpace = true;
        boolean ignorePunctuation = true;
        
        StringBuilder str_aux = new StringBuilder();
        int cursor = 0;
        while (cursor<input.length()) {
        char ch = input.charAt(cursor);
        StringBuilder buf = new StringBuilder();
        if (Character.isWhitespace(ch)) {
                cursor++;
        } else if (Character.isLetter(ch)) {
                while (cursor<input.length() && Character.isLetter(input.charAt(cursor))) {
                    buf.append(unAccent(Character.toLowerCase(input.charAt(cursor))));
                    cursor++;
                }
                
                str_aux.append(buf);
                if(appendWhiteSpace){
                    str_aux.append(" ");
                }
                
        } else if (Character.isDigit(ch)) {
                while (cursor<input.length() && Character.isDigit(input.charAt(cursor))) {
                    buf.append(unAccent(input.charAt(cursor)));
                    cursor++;
                }
                
                str_aux.append(buf);
                if(appendWhiteSpace){
                    str_aux.append(" ");
                }
                
        } else {
                if (!ignorePunctuation) {
                    buf.append(ch);
                }
                cursor++;
        }
        
        }
        
        return /*removeTokens(*/str_aux.toString().trim()/*)*/;
    }
    
    private String removeTokens(String str){
        String res = "";
        String[] s = str.split(" ");
        for(int i = 0; s.length > i; i++){
            if(s[i].equals("dos") || s[i].equals("de") || s[i].equals("das")
                    || s[i].equals("os") || s[i].equals("as")){
                continue;
            }     
            res = res + s[i] + " ";
        }
        
        return res.substring(0,res.length()-1);
    }

    public boolean isSymbol(char[] c, char symbol) {
        int size = c.length;
        for (int j = 0; j < size; j++) {
            if (symbol == c[j]) {
                return true;
            }
        }
        return false;
    }
    
    private double getCustomScore(String token1, String token2, int minNumTokens){
        
        try{  
           int t1 = Integer.parseInt(token1);
           int t2 = Integer.parseInt(token2);
           
           if(t1 != t2){
               return 2;
           }
           else{
               return 1;
           }
        }  
        catch(Exception e){  
            if(((token1.length() == 1 && token2.charAt(0) == token1.charAt(0)) || 
                    (token2.length() == 1 && token1.charAt(0) == token2.charAt(0))) &&
                    minNumTokens > 2){
                     return 1;
                 }
                 else if(token1.length() == token2.length() && soundex(token1, token2) == 1){
                     return 1;
                 }
                 else{
                     return levenstein(token1, token2);
                 }
        }  
    }
    
    private boolean isAbbreviation(String s1, String s2){
        if((s1.length() == 1 && s2.charAt(0) == s1.charAt(0)) || 
           (s2.length() == 1 && s1.charAt(0) == s2.charAt(0))){
                 return true;
        }
        else{
            return false;
        }
    }
    
    private double custom2(String str1, String str2){
        if((str1.equals("lu") && str2.equals("si")) || 
           (str1.equals("si") && str2.equals("lu")) ||
           
           (str1.equals("br") && str2.equals("uz")) || 
           (str1.equals("uz") && str2.equals("br")) ||
           
           (str1.equals("lm") && str2.equals("uz")) || 
           (str1.equals("uz") && str2.equals("lm")) ||
           
           (str1.equals("lm") && str2.equals("br")) || 
           (str1.equals("br") && str2.equals("lm")) ||
                     
           (str1.equals(str2))){
            return 1;
        }
        else{
            return 0;
        }
    }
    
    private double custom(String str1, String str2){
        
        List<Double> res = new ArrayList<Double>();
        String[] s1 = str1.split(" ");
        String[] s2 = str2.split(" ");
        String[] max;
        String[] min;
        
        if(s1.length > s2.length){
            max = s1;
            min = s2;
        }
        else{
            max = s2;
            min = s1;
        }
        
        Set<Integer> excludedIndexes = new HashSet<Integer>();
        int abbrv = 0;
        double ld;
        double maxSim = Double.MIN_VALUE;
        for(int i = 0; min.length > i; i++){
            for(int j = 0; max.length > j; j++){
                
                if(excludedIndexes.contains(j)){
                    continue;
                }
                
                ld = getCustomScore(min[i],max[j], min.length);
                if(ld == 1){
                    
                    if(isAbbreviation(min[i],max[j])){
                        abbrv++;
                    }
                    
                    excludedIndexes.add(j);
                    maxSim = ld;
                    break;
                }
                
                if(ld > maxSim){
                   maxSim = ld; 
                }
            }
            if(maxSim == 2){
                maxSim = 0;
            }
            res.add(maxSim);
            maxSim = Double.MIN_VALUE;
        }
        
        double avg = 0;
        for(int i = 0; res.size() > i; i++){
           avg = avg + res.get(i); 
        }
        
        if(min.length < 3 || min.length-abbrv < 2){
            return avg/(double)max.length;
        }
        else{
            return avg/(double)min.length;
        }
       
    }

    public double stringMatching(String simMeasure, String str1, String str2) {

        Singleton sg = Singleton.getSingletonObject();
        sg.increaseComparisons();

        double res = -1;
        //String str1_aux = preProcessString(str1);
        //String str2_aux = preProcessString(str2);
        
        String str1_aux = str1;
        String str2_aux = str2;
        
        if(_similarityScoresPreviouslyLoaded){         
            return _rdb.getSimilarityScores(str1_aux, str2_aux);
        }

        if (simMeasure.equals("levenstein"))
            res = levenstein(str1_aux, str2_aux);
        else if (simMeasure.equals("softTFIDF"))
            res = softTFIDF(str1, str2);
        else if (simMeasure.equals("jaccard"))
            res = jaccard(str1, str2);
        else if (simMeasure.equals("mongeElkan"))
            res = mongeElkan(str1, str2);
        else if (simMeasure.equals("jaroWinkler"))
            res = jaroWinkler(str1_aux, str2_aux);
        else if (simMeasure.equals("jaro"))
            res = jaro(str1_aux, str2_aux);
        else if (simMeasure.equals("soundex"))
            res = soundex(str1_aux, str2_aux);
        else if (simMeasure.equals("qGrams"))
            res = qGrams(str1_aux, str2_aux);
        else if (simMeasure.equals("cosine"))
            res = cosine(str1, str2);
        else if (simMeasure.equals("euclidean"))
            res = euclidean(str1, str2);
        else if (simMeasure.equals("geoDistance"))
            res = geoDistance(str1, str2);
        else if(simMeasure.equals("custom"))
            res = custom(str1_aux,str2_aux);
        else if(simMeasure.equals("custom2"))
            res = custom2(str1_aux,str2_aux);

        if (res < 0)
            System.err.println("StringMatching() returned a negative value!");

        //System.out.println(str1);
        //System.out.println(str2);
        //System.out.println(res);
        return res;
    }

    public double softTFIDF(String str1, String str2) {

        StringDistance sd = new JaroWinkler();
        /*
         * StringDistance sd2 = new Jaccard(); StringDistance sd3 = new
         * SmithWaterman(); StringDistance sd4 = new Jaro(); StringDistance sd5
         * = new Level2Levenstein(); StringDistance sd6 = new NeedlemanWunsch();
         */

        SoftTFIDF stfidf = new SoftTFIDF(sd, 0.9);

        return stfidf.score(str1, str2);
    }

    public double jaccard(String str1, String str2) {

        Jaccard j = new Jaccard();
        return j.score(str1, str2);
    }

    public double jaroWinkler(String str1, String str2) {

        JaroWinkler jw = new JaroWinkler();
        return jw.score(str1, str2);
    }

    public double jaro(String str1, String str2) {

        Jaro j = new Jaro();
        return j.score(str1, str2);
    }

    public double mongeElkan(String str1, String str2) {

        MongeElkan me = new MongeElkan();
        return me.score(str1, str2);
    }

    public double soundex(String str1, String str2) {

        Soundex sndx = new Soundex();
        return sndx.getSimilarity(str1, str2);
    }

    public double levenstein(String str1, String str2) {

        Levenshtein lvn = new Levenshtein();
        return lvn.getSimilarity(str1, str2);
    }

    public double qGrams(String str1, String str2) {

        QGramsDistance qgrm = new QGramsDistance();
        return qgrm.getSimilarity(str1, str2);
    }

    public double cosine(String str1, String str2) {

        CosineSimilarity cos = new CosineSimilarity();
        return cos.getSimilarity(str1, str2);
    }

    public double euclidean(String str1, String str2) {

        EuclideanDistance eucd = new EuclideanDistance();
        return eucd.getSimilarity(str1, str2);
    }

    public double geoDistance(String str1, String str2) {

        String[] p1 = str1.split(",");
        String[] p2 = str2.split(",");
        double lat1 = Double.parseDouble(p1[0]);
        double lon1 = Double.parseDouble(p1[1]);
        double lat2 = Double.parseDouble(p2[0]);
        double lon2 = Double.parseDouble(p2[1]);

        int R = 6371;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double d = R * c;

        if (d > 20000) {
            System.out.println("lat1: " + lat1);
            System.out.println("lat2: " + lat2);
            System.out.println("lon1: " + lon1);
            System.out.println("lon2: " + lon2);
            System.exit(0);
        }
        if (d > 20000)
            System.exit(0);
        // System.out.println(d);
        d = 1 - (d / 20000);// normalize. 40000 is the approx earth perimeter

        return d;
    }
}
