package DuplicateDetection;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

import RDB.DataBase;

public class FileHandler {

    /**
     * Writes the sorted object pairs to a file which shows their similarity score,
     * position in the database(index) and a field that indicates if they are in fact
     * duplicates.
     * 
     * @param l A list of objects. Each one contains the information about the two objects
     *            compared. Sorted by similarity.
     * @param out A buffer to write the data
     */
    public void writeSortedFile(List<ObjSort> l, BufferedWriter out, int storageType) {

        // Em memoria
        if (storageType == 1) {

            for (int i = 0; l.size() > i; i++) {

                writeOpenFile("PAIR = " + l.get(i).getIndexNode1() + " and "
                        + l.get(i).getIndexNode2(), out);
                writeOpenFile("SIMILARITY = " + (l.get(i)).getSimilaridade(), out);
                writeOpenFile("DUP? = " + (l.get(i)).getDupState(), out);
                writeOpenFile("#", out);

            }
        }

        // BD relacional
        if (storageType == 0)
            DataBase.getSingletonObject(false).writeSortedFile(out);

    }

    /**
     * Writes a line to the specified buffer
     * 
     * @param str The string to be written
     * @param out The buffer where to write
     */
    public void writeOpenFile(String str, BufferedWriter out) {

        try {
            out.write(str);
            out.newLine();
        } catch (IOException e) {
        }
    }

    /**
     * Prints the values of precision and recall to be plotted
     * 
     * @param simCut The similarity threshold
     */
    public Map<String,Double> printPrecisionRecall(float simCut, String resultsPath, long dt, long df, long pairsTotal, long candidates) {

        double dcg = 0;
        double dcg_p = 0;
        int dcg_index = -1;
        boolean dcg_index_fixed = false;
        double ndcg;
        double rr = 1f - ((double)candidates/(double)pairsTotal);
        double pc = -1;
        double pq = -1;
        double f_score_blocking = -1;
        
        int duplicates_10 = (int) Math.round(dt*0.1);
        int duplicates_20 = (int) Math.round(dt*0.2);
        int duplicates_30 = (int) Math.round(dt*0.3);
        int duplicates_40 = (int) Math.round(dt*0.4);
        int duplicates_50 = (int) Math.round(dt*0.5);
        double precision_10 = -1;
        double precision_20 = -1;
        double precision_30 = -1;
        double precision_40 = -1;
        double precision_50 = -1;
        
        Map<String,Double> res= new HashMap<String,Double>() ;
        
        String aux = null;
        String aux2 = null;
        String aux3 = null;

        boolean more = true;

        double rPrecision = -1;
        double avgPrecision = 0;
        double maxFMeasure = -1;
        
        double similarity = 0.0;
        int i = 1;
        double dup = 0.0;
        double precision = 0.0;
        double recall = 0.0;
        double numDup = dt;
        double numDupFound = df;
        
        double breakEven = -1;

        eraseFile(resultsPath + "/precision_recall_plot_cut_" + simCut + ".txt");

        try {

            FileInputStream fin = new FileInputStream(resultsPath + "/results_pairs.txt");
            BufferedInputStream bis = new BufferedInputStream(fin);
            BufferedReader in = new BufferedReader(new InputStreamReader(bis));

            BufferedWriter out = new BufferedWriter(new FileWriter(resultsPath
                    + "/precision_recall_plot_cut_" + simCut + ".txt", true));
            out.write("#PRECISION	RECALL	SIMILARITY	Nr PAIRS	% PAIRS");

            out.newLine();

            while (more) {

                aux = in.readLine();
                if (aux == null) {
                    more = false;
                } else {

                    aux2 = in.readLine();
                    aux2 = aux2.replace("SIMILARITY = ", "");
                    similarity = new Double(aux2).doubleValue();
                    if (similarity < simCut)
                        break;

                    aux3 = in.readLine();
                    aux3 = aux3.replace("DUP? = ", "");
                    if (aux3.equals("true")) {
                        dup++;
                        avgPrecision += (dup / (double) i);
                        
                        dcg += ((Math.pow(2, 1) - 1)/(Math.log(i+1)/Math.log(2)));
                    }
                    else{
                        dcg += ((Math.pow(2, 0) - 1)/(Math.log(i+1)/Math.log(2)));
                    }

                    precision = dup / (double) i;
                    
                    if (numDup == 0){
                        recall = 0;
                    }
                    else{
                        recall = dup / numDup;
                    }
                    
                    if(precision == recall){
                       breakEven = precision;
                    }
                    
                    if((2*precision*recall)/(precision+recall) > maxFMeasure){
                        maxFMeasure = (2*precision*recall)/(precision+recall);
                    }
                    
                    if(numDup == (double) i){
                        rPrecision = precision;                  
                    }
                    
                    if(numDup == dup && !dcg_index_fixed){
                        dcg_p = dcg;
                        dcg_index = i;
                        dcg_index_fixed = true;
                    }
                    
                    if((double) i == duplicates_10){
                        precision_10 = precision;
                    }else if((double) i == duplicates_20){
                        precision_20 = precision;
                    }else if((double) i == duplicates_30){
                        precision_30 = precision;
                    }else if((double) i == duplicates_40){
                        precision_40 = precision;
                    }else if((double) i == duplicates_50){
                        precision_50 = precision;
                    }
                        

                    out.write(precision + "	" + recall + "	" + similarity +
                            "	" + i + "	" + ((i / numDupFound) * 100));
                    out.newLine();

                    in.readLine();
                    i++;
                }

            }

            i--;
            
            if(i < numDup){
               rPrecision = dup / numDup;
            }
            
            System.out.println("Duplicates Found: " +dup);
            System.out.println("Duplicates Total: " +numDup);
            if(numDup > dup){//System.out.println("numDup > dup");
                dcg_p = dcg;
                dcg_index = i;
            }
            
            ndcg = dcg_p/getIDCG(numDup, dcg_index);       
            avgPrecision = avgPrecision/dup;
            pc = dup/numDup;
            pq = dup/(float)candidates;
            f_score_blocking = 2*((pq*pc)/(pq+pc));

            System.out.println("Correct = " + (int) dup);
            System.out.println("Found = " + i);
            
            //System.out.println("DCG_p = " + dcg_p);
            //System.out.println("DCG_index = " + dcg_index);
            //System.out.println("IDCG = " + getIDCG(numDup, dcg_index));
            //System.out.println("NDCG = " + ndcg);
            
            System.out.println("precision_10 = " + Math.round(precision_10 * 100) + "%");
            System.out.println("precision_20 = " + Math.round(precision_20 * 100) + "%");
            System.out.println("precision_30 = " + Math.round(precision_30 * 100) + "%");
            System.out.println("precision_40 = " + Math.round(precision_40 * 100) + "%");
            System.out.println("precision_50 = " + Math.round(precision_50 * 100) + "%");
              
            
            
            System.out.println("Reduction Ratio = " + Math.round(rr * 100) + "%"); 
            System.out.println("Pairs Completeness = " + Math.round(pc * 100) + "%");
            System.out.println("Pairs Quality = " + Math.round(pq * 100) + "%"); 
            System.out.println("F-score blocking = " + Math.round(f_score_blocking * 100) + "%"); 
            System.out.println("Break Even = " + Math.round(breakEven * 100) + "%");           
            System.out.println("R-Precision = " + Math.round(rPrecision * 100) + "%");
            System.out.println("Avg-Precision = " + Math.round(avgPrecision * 100) + "%");
            System.out.println("Max F-Measure = " + Math.round(maxFMeasure * 100) + "%");
            System.out.println("Precision = " + Math.round(precision * 100) + "%");
            System.out.println("Recall = " + Math.round(recall * 100) + "%");
            System.out.println("Total = " + (int) numDup);

            fin.close();
            out.close();

        }

        catch (IOException e) {
            System.err.println("Unable to read from file");
            System.exit(-1);
        }
        
        res.put("R-Precision",rPrecision);
        res.put("Avg Precision",avgPrecision);
        res.put("Max F-Measure",maxFMeasure);
        res.put("Break Even",breakEven);
        res.put("Recall",recall);
        res.put("Pairs Completeness",pc);
        res.put("Pairs Quality",pq);
        res.put("Reduction Ratio",rr);
        res.put("F-Score Blocking", f_score_blocking);
        res.put("Pairs Compared", (double)candidates);
        
        writeMeasuresToFile(resultsPath, res);
        
        return res;

    }
    
    private void writeMeasuresToFile(String resultsPath, Map<String,Double> scores){
        
        try{
        
            BufferedWriter out = new BufferedWriter(new FileWriter(resultsPath
                    + "/AccuracyMeasuresScores.txt", true));
            
            for(Map.Entry<String, Double> e : scores.entrySet()){
                out.write(e.getKey() + " - " + e.getValue());
                out.newLine();
            }
            
            out.close();
            
        }catch (IOException e) {
            System.err.println("Unable to read from file");
            System.exit(-1);
        }
    }
    
    private double getIDCG(double numDup, int p){
        //System.out.println(p);
        double idcg = 0;
        int rel;
        
        for(int i = 1; i <= p; i++){
            if(i<=numDup){
                rel = 1;
            }else{
                rel = 0;
            }

            idcg += (Math.pow(2, 1) - rel)/(Math.log(i+1)/Math.log(2));
        }    
        
        return idcg;
    }

    /**
     * Erases the specified file if it exists
     * 
     * @param filePath The file path
     */
    public void eraseFile(String filePath) {

        File file = new File(filePath);

        if (file.exists()) {
            file.delete();
            System.out.println("A apagar ficheiro existente em " + filePath);
        }
    }

    public void deleteFolderContent(File folder) {

        File[] files = folder.listFiles();

        for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }

    }

    public void writeComparisonsToFile(long cmp, String resultsPath) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter(resultsPath
                + "/string_comparisons.txt", true));
        out.write(Long.toString(cmp));
        out.close();
    }

    public void writeTimeToFile(String timeType, long t, String resultsPath) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter(resultsPath + "/time.txt", true));

        long timeMillis = t;
        long time = timeMillis / 1000;
        String seconds = Integer.toString((int) (time % 60));
        String minutes = Integer.toString((int) ((time % 3600) / 60));
        String hours = Integer.toString((int) (time / 3600) % 24);
        String days = Integer.toString((int) (time / 3600 / 24));

        for (int i = 0; i < 2; i++) {
            if (seconds.length() < 2) {
                seconds = "0" + seconds;
            }
            if (minutes.length() < 2) {
                minutes = "0" + minutes;
            }
            if (hours.length() < 2) {
                hours = "0" + hours;
            }
            if (days.length() < 2) {
                days = "0" + days;
            }
        }

        System.out.println(timeType + " - " + days + "D:" + hours + "H:" + minutes + "M:" + seconds
                + "S");
        out.write(timeType + " - " + days + "D:" + hours + "H:" + minutes + "M:" + seconds + "S");
        out.newLine();
        out.close();

    }

    public void copyFile(String src, String dst) throws Exception {

        File _src = new File(src);
        File _dst = new File(dst);

        InputStream in = new FileInputStream(_src);
        OutputStream out = new FileOutputStream(_dst);

        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
        out.close();
        // System.out.println("File '" + src + "' copied.");
    }
    
    public void docToFile(Document doc, String outputFile) throws Exception{
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.transform(new DOMSource(doc), new StreamResult(new FileOutputStream(outputFile)));
    }
}
