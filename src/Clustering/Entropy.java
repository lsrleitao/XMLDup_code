package Clustering;

import java.util.Collections;
import java.util.List;

import RDB.DataBase;

public class Entropy {

	private int _objects;
	List<Integer> _clusterOccurrences;
	List<Integer> _StringSizeOccurrences;
	
	public Entropy(DataBase db, String attribute){
		this._objects = db.getObjects(attribute);
		this._clusterOccurrences = db.getClusterOccurrences(attribute);
		this._StringSizeOccurrences = db.getStringSizeOccurrences(attribute);
	}
	
	private double getEntropy(List<Integer> lst){
		
		if(_objects == 0)
			return 0;
		
		double tot = 0;
		double pi;
		
		for(int i = 0 ; lst.size() > i ; i++){
			pi = ((double)lst.get(i))/(double)_objects;
			tot+=pi*Math.log(pi);
		}
		
		tot = tot*(-1d);
		
		return tot;
	}
	
	   public double getEntropyKey(List<Integer> lst){
	        
	       double objects = 0;
	       
	       for(int i = 0; lst.size() > i; i++){
	           objects+= lst.get(i);
	       }
	       
	        if(objects == 0)
	            return 0;
	        
	        double tot = 0;
	        double pi;
	        
	        for(int i = 0 ; lst.size() > i ; i++){
	            pi = ((double)lst.get(i))/objects;
	            tot+=pi*Math.log(pi);
	        }
	        
	        tot = tot*(-1d);
	        
	        return tot;
	    }
	
	public double stdDeviation(List<Integer> lst){
	    
	    double average = 0;
	    
	    int size = lst.size();
	    for(int i = 0; size > i; i++){
	        average+=lst.get(i);
	    }
	    
	    average /= size;
	    
	    double numeratorSum = 0;
	    
	    for(int i = 0; size > i; i++){
            numeratorSum += Math.pow(lst.get(i)-average,2);
        }
        
        double std = Math.sqrt(numeratorSum/size);
              
        return std;
	}
	
	public double diversityIndex(List<Integer> lst){
	    
	    int occurrences;
	    double numerator = 0;
	    double totalOccurrences = 0;
	    
	    int size = lst.size();
        for(int i = 0; size > i; i++){
            occurrences = lst.get(i);
            numerator+=(occurrences*(occurrences-1));
            totalOccurrences += occurrences;
        }
        if(numerator == 0)
            return 0;
        else
            return numerator/(totalOccurrences*(totalOccurrences-1));
	}
	
	public double diversityMean(List<Integer> lst){
        
	    double arithmeticMean = 0;
	    double geometricMean = 1;
	    int occurrences;
	    
        int size = lst.size();
        for(int i = 0; size > i; i++){
            occurrences = lst.get(i);
            arithmeticMean+=occurrences;
            geometricMean*=occurrences;
        }
        
        arithmeticMean /= size;
        geometricMean = Math.pow(geometricMean, 1d/(double)size);
	    
	    return arithmeticMean/geometricMean;
	}
	
	public double harmonicMean(List<Integer> lst){
        double denominator = 0;
        
        int size = lst.size();
        for(int i = 0; size > i; i++){
            denominator+=1d/(double)lst.get(i);
        }
        
        return ((double)size)/denominator;
	}
	
	public double distinctiveness(List<Integer> lst){
        double sum = 0;
        
        int size = lst.size();
        for(int i = 0; size > i; i++){
            sum+=lst.get(i);
        }
        
        return ((double)size)/sum;
	}
	
	public double average(List<Integer> lst){
	    
	    double sum = 0;
	    
        for(int i = 0; lst.size() > i; i++){
            sum+=lst.get(i);
        }
        
        return sum/(double)lst.size();
	}
	
	public int max(List<Integer> lst){
	        
	        int max = -1;
	        
	        for(int i = 0; lst.size() > i; i++){
	            if(lst.get(i) > max){
	                max = lst.get(i);
	            }
	        }
	        
	        return max;
	}
	
	public int min(List<Integer> lst){
           
           int min = Integer.MAX_VALUE;
           
           for(int i = 0; lst.size() > i; i++){
               if(lst.get(i) < min){
                   min = lst.get(i);
               }
           }
           
           return min;
   }
	
   public int median(List<Integer> lst){
           
       Collections.sort(lst);
       
       return lst.get((lst.size()+1)/2);
   }
	
	public double getDistinctivenessEntropy(){
		return getEntropy(_clusterOccurrences);
	}
	
	public double getStringSizeEntropy(){
		return getEntropy(_StringSizeOccurrences);
	}
	
	public double getDiversityIndex(){
	    return diversityIndex(_clusterOccurrences);
	}
	
    public double getDiversityMean(){
        return diversityMean(_clusterOccurrences);
    }
    
    public double getStdDeviation(){
        return stdDeviation(_clusterOccurrences);
    }
    
    public double getHarmonicMean(){
        return harmonicMean(_clusterOccurrences);
    }
    
    public double getDistinctiveness(){
        return distinctiveness(_clusterOccurrences);
    }
	
}