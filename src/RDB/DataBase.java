package RDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.IOException;

import Clustering.Entropy;
import DuplicateDetection.StringMatching;
import DuplicateDetection.FileHandler;

public final class DataBase {

    private static Connection conn = null;
    private static String dbName = "XMLDup";
    
    private static String driverClassName = "com.mysql.jdbc.Driver"; //mysql
    private static String databaseURL = "jdbc:mysql://localhost/" + dbName; //mysql
    private static String createTables = "./XMLDup Data/SqlScripts/createTables.sql"; //mysql
    private static String duplicatesTable = "./XMLDup Data/SqlScripts/duplicatesTable.sql"; //mysql
    private static String blockingKeysTable = "./XMLDup Data/SqlScripts/blockingKeysTable.sql"; //mysql
    private static String dbLoadTable = "./XMLDup Data/SqlScripts/dbLoadTable.sql"; //mysql
    /*private static String driverClassName = "org.sqlite.JDBC"; //sqlite
    private static String databaseURL = "jdbc:sqlite:XMLDup.db"; //sqlite
    private static String createTables = "./XMLDup Data/SqlScripts/createTables_sqlite.sql"; //sqlite
    private static String duplicatesTable = "./XMLDup Data/SqlScripts/duplicatesTable_sqlite.sql"; //sqlite
    private static String blockingKeysTable = "./XMLDup Data/SqlScripts/blockingKeysTable_sqlite.sql"; //sqlite*/

    private static String tableName = "DBLOAD";
    private static String keysTable = "OBJ_KEYS";
    private static String stringSimScoresTableName = "STRING_SIM_SCORES";
    private static String DupFoundTable = "DUP_FOUND";
    
    //usadas no clustering de strings
    private static String clusterTableName = "CLUSTERS";
    private static String distanceTableName = "DISTANCE";
    
    //usadas no na estruturação dos objectos
    private static String featureTableName = "FEATURES_ASSGN";
    private static String featureTableNameKeys = "FEATURES_ASSGN_KEYS";
    private static String attrStructLevelsTableName = "ATTR_STRUCT_LEVELS";
    private static String attrLevelStatsTableName = "ATTR_LEVEL_STATS";
      
    private static String user = "root";
    private static String password = "internet";
    private static PreparedStatement ps;
    private static int TableFetchSize = 10000;
    private static DataBase ref;
    
    private Map<String,Float> _similarityScores = new HashMap<String,Float>();
    private int _lastObjectLoadedIntoSimScores = 0;
    private static int _objectsLoadedIntoMemory = 1000;

    public DataBase() {

        try {

            Class.forName(driverClassName);

            // Open a connection to the database
            conn = DriverManager.getConnection(databaseURL, user, password);

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }

    }
    
    public static DataBase getSingletonObject(boolean initDB) {
            
           if (ref == null){System.out.println("Create new DB");
               ref = new DataBase();
           }
            
           if(initDB){
               runSqlScript(createTables);
               runSqlScript(duplicatesTable);
           }
        
        
        return ref;
    }

    public double getDistance(String str1, String str2) {

        double res = -1;

        try {
            ps = conn.prepareStatement("SELECT distance " + "FROM " + distanceTableName + " "
                    + "WHERE string1 = ? AND string2 = ?");
            
            ps.setString(1, str1);
            ps.setString(2, str2);

            ResultSet rs = ps.executeQuery();

            if (rs.next())
                res = rs.getDouble(1);

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    public int getObjects(String attribute) {

        int res = 0;

        try {
            ps = conn.prepareStatement("SELECT sum(occurrences) " + "FROM " + clusterTableName + " "
                    + "WHERE attribute = ?");
            
            ps.setString(1, attribute);

            ResultSet rs = ps.executeQuery();

            if (rs.next())
                res = rs.getInt(1);

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    public List<Integer> getClusterOccurrences(String attribute) {

        List<Integer> res = new ArrayList<Integer>();

        try {
            ps = conn.prepareStatement("SELECT sum(occurrences) " + "FROM " + clusterTableName + " "
                    + "WHERE attribute = ? GROUP BY cluster");
            
            ps.setString(1, attribute);

            ResultSet rs = ps.executeQuery();

            while (rs.next())
                res.add(rs.getInt(1));

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    public List<List<Object>> getStringsIntoMemory(String attribute) {

        List<List<Object>> lst = new ArrayList<List<Object>>();

        try {
            ps = conn.prepareStatement("SELECT content, COUNT(attribute) cnt FROM "
                    + tableName + " WHERE attribute = ? AND content != \"\" " + "GROUP BY content ORDER BY cnt DESC");

            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();

            List<Object> lst_aux;

            while (rs.next()) {
                lst_aux = new ArrayList<Object>();
                String attrContent = rs.getString(1).toLowerCase();
                int occurrences = rs.getInt(2);
                lst_aux.add(attrContent);
                lst_aux.add(occurrences);
                lst.add(lst_aux);
            }

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return lst;
    }

    public void closeConnection() {
        try {
            conn.close();
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    public void insertDistance(String str1, String str2, double distance) {

        try {
            ps = conn.prepareStatement("INSERT INTO " + distanceTableName
                    + "(string1, string2, distance) VALUES (?,?,?)");

            ps.setString(1, str1);
            ps.setString(2, str2);
            ps.setDouble(3, distance);
            ps.addBatch();

            ps.executeBatch();
            ps.clearBatch();

            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

    }

    public void insertObject(Map<String,List<String>> row, String id, int obj_index){      
        
        StringMatching sm = new StringMatching();
        
        try{
            
        ps = conn.prepareStatement("INSERT INTO " + tableName + "(id, obj_index, attribute, content, occurrence, data_type) VALUES (?,?,?,?,?,?)");
        
        String attribute;
        for(Map.Entry<String,List<String>> e : row.entrySet()){
        
            attribute = e.getKey();           
            int size = e.getValue().size();
            for(int i = 0; size > i; i++){
                
                String str = e.getValue().get(i);
                
                ps.setInt(1, Integer.parseInt(id));
                ps.setInt(2, obj_index + 1);
                ps.setString(3, attribute);//System.out.println(attribute);
                ps.setString(4,str);//System.out.println(row.get(attribute).get(i));
                ps.setInt(5, i);
                ps.setInt(6, sm.checkDataType(str));
                ps.addBatch();
            }
        }
        
        ps.executeBatch();
        ps.clearBatch();

        ps.close();
        
        }catch(SQLException sqle){sqle.printStackTrace();
        System.exit(0);}

    }

    public void insertClusterData(List<List<Object>> lst) {

        try {
            ps = conn.prepareStatement("INSERT INTO " + clusterTableName
                    + "(attribute, content, distance, occurrences, cluster) VALUES (?,?,?,?,?)");

            List<Object> l;

            int size = lst.size();
            for (int i = 0; size > i; i++) {

                l = lst.get(i);

                ps.setString(1, (String) l.get(0));
                ps.setString(2, (String) l.get(1));
                ps.setDouble(3, 1 - (Double) l.get(2));// System.out.println(attribute);
                ps.setInt(4, (Integer) l.get(3));// System.out.println(row.get(attribute).get(i));
                ps.setInt(5, (Integer) l.get(4));
                ps.addBatch();
            }

            ps.executeBatch();
            ps.clearBatch();

            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

    }

    public double getAppearences(String attribute) {

        double res = 0;

        try {
            ps = conn.prepareStatement("SELECT MAX(obj_index) " + "FROM " + tableName);

            ResultSet rs = ps.executeQuery();

            if (rs.next())
                res = rs.getDouble(1);

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    // calcula a probabilidade de, para um dado atributo, o conteudo das strings
    // que sao calculadas na rede representar a mesma informaçao.
    public double getSameObjectProbability(String attribute, double appearences) {

        double res = 0;
        double nComb = factorialRatio(appearences);

        try {
            ps = conn.prepareStatement("SELECT SUM(occurrences) " + "FROM " + clusterTableName + " "
                    + "WHERE attribute = ? GROUP BY cluster");
            
            ps.setString(1, attribute);

            ResultSet rs = ps.executeQuery();

            while (rs.next())
                res = res + factorialRatio(rs.getDouble(1));

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res / nComb;
    }

    // calcula a probabilidade de, para um dado atributo, o conteudo de uma ou
    // de ambas as strings que sao calculadas na rede ser vazio.
    public double getEmptyObjectProbability(String attribute, double appearences) {

        double res = 0;

        try {
            ps = conn.prepareStatement("SELECT " + appearences + " - COUNT(distinct obj_index) " + "FROM "
                    + tableName + " WHERE attribute = ? AND content != \"\"");

            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();

            if (rs.next())
                res = cobinatorialCalculator(appearences, rs.getDouble(1));

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    public double getAttributeDistinctiveness(String attribute) {

        double res = 0;

        try {
            //ps = conn.prepareStatement("SELECT LN(COUNT(*)/COUNT(DISTINCT(CONTENT))) " + "FROM "
              ps = conn.prepareStatement("SELECT COUNT(DISTINCT(CONTENT))/COUNT(*) " + "FROM "
                    + tableName + " " + "WHERE ATTRIBUTE = ? AND CONTENT != \"\"");

            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();

            if (rs.next())
                res = rs.getDouble(1);

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    public double getAverageStringSize(String attribute) {

        double res = 0;

        try {
            ps = conn.prepareStatement("SELECT SUM(LENGTH(CONTENT))/COUNT(*) " + "FROM "
                    + tableName + " " + "WHERE ATTRIBUTE = ? AND CONTENT != \"\"");

            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();

            if (rs.next())
                res = rs.getDouble(1);

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

        return res;
    }

    private double factorialRatio(double n) {

        return (n * (n - 1)) / 2;
    }

    private double cobinatorialCalculator(double n, double ne) {

        return ((n - 1) * ne - factorialRatio(ne)) / factorialRatio(n);
    }

    // usado no armazenamento dos duplicados encontrados....Compare.java
    public void insertDuplicatesFound(int indexni, int indexnj, double res, boolean dup) {

        try {

            String query = "INSERT INTO " + DupFoundTable + " VALUES (?,?,?,?)";

            ps = conn.prepareStatement(query);

            ps.setInt(1, indexni);
            ps.setInt(2, indexnj);
            ps.setDouble(3, res);
            ps.setString(4, Boolean.toString(dup));

            ps.execute();

            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }

    public void writeSortedFile(BufferedWriter out) {

        FileHandler fh = new FileHandler();
        
        try {

            String query = "SELECT NODE1_INDEX, NODE2_INDEX, SIMILARITY, DUP FROM " + DupFoundTable
                    + " ORDER BY SIMILARITY DESC";

            ps = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    java.sql.ResultSet.CONCUR_READ_ONLY);

            ps.setFetchSize(TableFetchSize);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                fh.writeOpenFile("PAIR = " + rs.getInt(1) + " and " + rs.getInt(2), out);
                fh.writeOpenFile("SIMILARITY = " + rs.getDouble(3), out);
                fh.writeOpenFile("DUP? = " + rs.getString(4), out);
                fh.writeOpenFile("#", out);
            }

            out.close();
            rs.close();
            ps.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cleanBlockingKeysTable() {

        runSqlScript(blockingKeysTable);
    }
    
    public void cleanDuplicatesTable() {

        runSqlScript(duplicatesTable);
        
        /*try {

            /*Statement stmt = conn.createStatement();
     
            stmt.execute("DROP TABLE IF EXISTS " + DupFoundTable);

            stmt.execute("CREATE TABLE DUP_FOUND(NODE1_INDEX INTEGER, NODE2_INDEX INTEGER, SIMILARITY DOUBLE, DUP VARCHAR(3))");

            stmt.close();

            stmt.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }*/
    }

    private static void runSqlScript(String filePath) {

        try {

            FileInputStream fin = new FileInputStream(filePath);
            BufferedInputStream bis = new BufferedInputStream(fin);
            BufferedReader in = new BufferedReader(new InputStreamReader(bis));

            String aux = null;
            boolean more = true;
            String query_aux = "";
            String[] querys;
            PreparedStatement ps = null;

            while (more) {

                aux = in.readLine();

                if (aux == null)
                    more = false;
                else
                    query_aux = query_aux.concat(aux);

            }

            querys = query_aux.split(";");

            for (int x = 0; x < querys.length; x++) {

                ps = conn.prepareStatement(querys[x]);

                try {
                    ps.execute();
                } catch (SQLException sqle) {
                    continue;
                }
            }

            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    
    public void insertFeature(double distinctiveness, double harmonicMean,
                              double stdDeviation, double diversityMean,
                              double diversityIndex, float attributesPerObject,
                              double distinctivenessEntropy, double AVGStringSize, 
                              double stringSizeEntropy, double emptiness,
                              double nonNumericContentRate,
                              double numericContentRate, double mixedContentRate,
                              int median, int stringSizeMedian, int maxTokenNum,
                              int minTokenNum, double AVGTokenNum,
                              double tokenNumEntropy, double STDTokenNum,
                              int medianTokenNum, double AVGTokenSize,
                              double tokenSizeEntropy, double STDTokenSize,
                              int maxTokenSize, int minTokenSize,
                              int medianTokenSize, String attribute,
                              String dbPath, int nodeLevel, float relativeDepth){
                          
        try{
            ps = conn.prepareStatement("INSERT INTO " + featureTableName + "(distinctiveness, harmonicMean, stdDeviation, diversityMean, diversityIndex, attributesPerObject, distinctivenessEntropy," +
                    "AVGStringSize, stringSizeEntropy, emptiness, nonNumericContentRate, " +
            "numericContentRate, mixedContentRate, Median, StringSizeMedian, MaxTokensNum, MinTokensNum, AVGTokensNum, TokensNumEntropy, STDTokensNum, MedianTokensNum, AVGTokensSize, TokensSizeEntropy, STDTokensSize, MaxTokensSize, MinTokensSize,MedianTokensSize," +
            "attribute, db, nodeLevel, relativeDepth) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            
            ps.setDouble(1, distinctiveness);
            ps.setDouble(2, harmonicMean);           
            ps.setDouble(3, stdDeviation);
            ps.setDouble(4, diversityMean);
            ps.setDouble(5, diversityIndex);            
            ps.setFloat(6, attributesPerObject);
            ps.setDouble(7, distinctivenessEntropy);
            ps.setDouble(8, AVGStringSize);
            ps.setDouble(9, stringSizeEntropy);
            ps.setDouble(10, emptiness);
            ps.setDouble(11, nonNumericContentRate);
            ps.setDouble(12, numericContentRate);
            ps.setDouble(13, mixedContentRate);           
            ps.setInt(14, median);
            ps.setInt(15, stringSizeMedian);           
            ps.setInt(16, maxTokenNum);
            ps.setInt(17, minTokenNum);
            ps.setDouble(18, AVGTokenNum);
            ps.setDouble(19, tokenNumEntropy);
            ps.setDouble(20, STDTokenNum);
            ps.setInt(21, medianTokenNum);            
            ps.setDouble(22, AVGTokenSize);
            ps.setDouble(23, tokenSizeEntropy);
            ps.setDouble(24, STDTokenSize);            
            ps.setInt(25, maxTokenSize);
            ps.setInt(26, minTokenSize);
            ps.setInt(27, medianTokenSize);            
            ps.setString(28, attribute);
            ps.setString(29, dbPath);
            ps.setInt(30, nodeLevel);
            ps.setFloat(31, relativeDepth);
            ps.addBatch();

            ps.executeBatch();
            ps.clearBatch();

            ps.close();

        }catch(SQLException sqle){sqle.printStackTrace();}

    }
    
    public void insertFeatureKeys(
                                  Double distinctiveness,
                                  Double distinctivenessST1,
                                  Double distinctivenessST2,
                                  Double distinctivenessST3,
                                  Double distinctivenessEND1,
                                  Double distinctivenessEND2,
                                  Double distinctivenessEND3,
                                  Double harmonicMean,
                                  Double harmonicMeanST1,
                                  Double harmonicMeanST2,
                                  Double harmonicMeanST3,
                                  Double harmonicMeanEND1,
                                  Double harmonicMeanEND2,
                                  Double harmonicMeanEND3,
                                  Double stdDeviation,
                                  Double stdDeviationST1,
                                  Double stdDeviationST2,
                                  Double stdDeviationST3,
                                  Double stdDeviationEND1,
                                  Double stdDeviationEND2,
                                  Double stdDeviationEND3,
                                  Double diversityMean,
                                  Double diversityMeanST1,
                                  Double diversityMeanST2,
                                  Double diversityMeanST3,
                                  Double diversityMeanEND1,
                                  Double diversityMeanEND2,
                                  Double diversityMeanEND3,
                                  Double diversityIndex,
                                  Double diversityIndexST1,
                                  Double diversityIndexST2,
                                  Double diversityIndexST3,
                                  Double diversityIndexEND1,
                                  Double diversityIndexEND2,
                                  Double diversityIndexEND3,
                                  Float attributesPerObject,
                                  Double entropy,
                                  Double entropyST1,
                                  Double entropyST2,
                                  Double entropyST3,
                                  Double entropyEND1,
                                  Double entropyEND2,
                                  Double entropyEND3,
                                  Double AVGStringSize, 
                                  Double stringSizeEntropy,
                                  Double emptiness,
                                  Double nonNumericContentRate,
                                  Double numericContentRate,
                                  Double mixedContentRate,
                                  String attribute,
                                  String dbPath){
                          
        try{
            ps = conn.prepareStatement("INSERT INTO " + featureTableNameKeys + "(" +
            		"distinctiveness," +
            		"distinctiveness_ST1," +
            		"distinctiveness_ST2," +
            		"distinctiveness_ST3," +
            		"distinctiveness_END1," +
                    "distinctiveness_END2," +
                    "distinctiveness_END3," +
                    "harmonicMean," +
                    "harmonicMean_ST1," +
                    "harmonicMean_ST2," +
                    "harmonicMean_ST3," +
                    "harmonicMean_END1," +
                    "harmonicMean_END2," +
                    "harmonicMean_END3," +
                    "stdDeviation," +
                    "stdDeviation_ST1," +
                    "stdDeviation_ST2," +
                    "stdDeviation_ST3," +
                    "stdDeviation_END1," +
                    "stdDeviation_END2," +
                    "stdDeviation_END3," +
                    "diversityMean," +
                    "diversityMean_ST1," +
                    "diversityMean_ST2," +
                    "diversityMean_ST3," +
                    "diversityMean_END1," +
                    "diversityMean_END2," +
                    "diversityMean_END3," +
                    "diversityIndex," +
                    "diversityIndex_ST1," +
                    "diversityIndex_ST2," +
                    "diversityIndex_ST3," +
                    "diversityIndex_END1," +
                    "diversityIndex_END2," +
                    "diversityIndex_END3," +
                    "attributesPerObject," +
                    "entropy," +
                    "entropy_ST1," +
                    "entropy_ST2," +
                    "entropy_ST3," +
                    "entropy_END1," +
                    "entropy_END2," +
                    "entropy_END3," +
                    "AVGStringSize," +
                    "stringSizeEntropy," +
                    "emptiness," +
                    "nonNumericContentRate," +
                    "numericContentRate," +
                    "mixedContentRate," +
                    "attribute," +
                    "db) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            
            ps.setDouble(1, distinctiveness);            
            ps.setDouble(2, distinctivenessST1);
            ps.setDouble(3, distinctivenessST2);
            ps.setDouble(4, distinctivenessST3);           
            ps.setDouble(5, distinctivenessEND1);
            ps.setDouble(6, distinctivenessEND2);
            ps.setDouble(7, distinctivenessEND3);
            
            ps.setDouble(8, harmonicMean);
            ps.setDouble(9, harmonicMeanST1); 
            ps.setDouble(10, harmonicMeanST2);
            ps.setDouble(11, harmonicMeanST3); 
            ps.setDouble(12, harmonicMeanEND1); 
            ps.setDouble(13, harmonicMeanEND2); 
            ps.setDouble(14, harmonicMeanEND3); 
            
            ps.setDouble(15, stdDeviation);
            ps.setDouble(16, stdDeviationST1);
            ps.setDouble(17, stdDeviationST2);
            ps.setDouble(18, stdDeviationST3);
            ps.setDouble(19, stdDeviationEND1);
            ps.setDouble(20, stdDeviationEND2);
            ps.setDouble(21, stdDeviationEND3);
            
            ps.setDouble(22, diversityMean);
            ps.setDouble(23, diversityMeanST1);
            ps.setDouble(24, diversityMeanST2);
            ps.setDouble(25, diversityMeanST3);
            ps.setDouble(26, diversityMeanEND1);
            ps.setDouble(27, diversityMeanEND2);
            ps.setDouble(28, diversityMeanEND3);
            
            ps.setDouble(29, diversityIndex);
            ps.setDouble(30, diversityIndexST1); 
            ps.setDouble(31, diversityIndexST2); 
            ps.setDouble(32, diversityIndexST3); 
            ps.setDouble(33, diversityIndexEND1); 
            ps.setDouble(34, diversityIndexEND2); 
            ps.setDouble(35, diversityIndexEND3); 
            
            ps.setFloat(36, attributesPerObject);
            
            ps.setDouble(37, entropy);
            ps.setDouble(38, entropyST1);
            ps.setDouble(39, entropyST2);
            ps.setDouble(40, entropyST3);
            ps.setDouble(41, entropyEND1);
            ps.setDouble(42, entropyEND2);
            ps.setDouble(43, entropyEND3);
            
            ps.setDouble(44, AVGStringSize);
            ps.setDouble(45, stringSizeEntropy);
            ps.setDouble(46, emptiness);
            ps.setDouble(47, nonNumericContentRate);
            ps.setDouble(48, numericContentRate);
            ps.setDouble(49, mixedContentRate);
            ps.setString(50, attribute);
            ps.setString(51, dbPath);
            ps.addBatch();

            ps.executeBatch();
            ps.clearBatch();

            ps.close();

        }catch(SQLException sqle){sqle.printStackTrace();}

    }
    
    public void writeTrainingFeaturesToFile(String outputFile){
        
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFile, true));
            
            ps = conn.prepareStatement("SELECT * FROM " + featureTableName);

            ResultSet rs = ps.executeQuery();

            while(rs.next()){
                out.write(rs.getFloat("RelativeDepth")+" 1:"+Float.toString(rs.getFloat("AttributesPerObject"))
                                       +" 2:"+Double.toString(rs.getDouble("DistinctivenessEntropy"))
                                       +" 3:"+Double.toString(rs.getDouble("AVGStringSize"))
                                       +" 4:"+Double.toString(rs.getDouble("StringSizeEntropy"))
                                       +" 5:"+Double.toString(rs.getDouble("Emptiness"))
                                       +" 6:"+Double.toString(rs.getDouble("NonNumericContentRate"))
                                       +" 7:"+Double.toString(rs.getDouble("NumericContentRate"))
                                       +" 8:"+Double.toString(rs.getDouble("MixedContentRate")));
                
                out.newLine();

            }
            ps.close();
            out.close();

        }catch(Exception e){e.printStackTrace();}
        
    }
    
public void writeTrainingKeyFeaturesToFile(String outputFile){
        
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFile, true));
            
            ps = conn.prepareStatement("SELECT * FROM " + featureTableNameKeys+ " ORDER BY distinctiveness DESC");

            ResultSet rs = ps.executeQuery();

            ResultSetMetaData rm = rs.getMetaData();
            int nrColumns = rm.getColumnCount();
            String line = "";
            int featureCnt = 0;
            while(rs.next()){
                
                if(featureCnt == 0){
                    out.write("#" + rs.getString(nrColumns));
                    out.newLine();
                }
                
                for(int i = 0; nrColumns > i; i++){
                    
                    if(rm.getColumnType(i+1) == java.sql.Types.DOUBLE){
                        line += (i + 1 + featureCnt) + ":" + Double.toString(rs.getDouble(i+1)) + " ";
                    }
                    else if(rm.getColumnType(i+1) == java.sql.Types.FLOAT){
                        line += (i + 1 + featureCnt) + ":" + Float.toString(rs.getFloat(i+1)) + " ";
                    }
                }
                
                featureCnt += (nrColumns-2);
            }
            
            out.write(line);
            out.newLine();
            
            ps.close();
            out.close();

        }catch(Exception e){e.printStackTrace();}
        
    }
    
    //1 - apenas letras; 2 - apenas numeros, 3 - misto
    public double getDataTypePercentage(String attribute, int dataType){

        double res = 0;

        try{
            ps = conn.prepareStatement("SELECT COUNT(*)/(SELECT COUNT(*) FROM "+
                    tableName +" WHERE attribute = ? AND content != \"\") " +
                    "FROM " + tableName + " " +
                    "WHERE attribute = ? AND content != \"\" " +
                    "AND data_type = ?");
            
            ps.setString(1, attribute);
            ps.setString(2, attribute);
            ps.setInt(3, dataType);

            ResultSet rs = ps.executeQuery();

            if(rs.next())
                res = rs.getDouble(1);
            
            rs.close();
            ps.close();

        }catch(SQLException sqle){sqle.printStackTrace();}

        return res;

    }
    
    public double getDataTypePercentageSingleKey(String key, int dataType){

        double res = 0;

        String[] attributes = key.split(",");
        String selectClause = "SELECT ";
        String caseClause = "CASE ";
        String when1 = "";
        String when2 = "";
        String when3 = "";
        String fromClause= "FROM ";
        String whereClause = "WHERE ";

        /*for(int i = 0; attributes.length > i; i++){
            selectClause += "at" + (i+1) + ".content a" + (i+1) + ", at" + (i+1) + ".data_type dt" + (i+1) + ",";
        }*/

        if(attributes.length == 1){
            selectClause += "at1.data_type dt_final ";
            caseClause = "";
            fromClause += "(SELECT * FROM DBLOAD D where attribute='" + attributes[0] + "' AND data_type!=-1) as at1 ";
            whereClause += "at1.obj_index=at1.obj_index";

        }
        else{
            for(int i = 0; attributes.length > i; i++){

                if((i+1)==1){
                    when1 +="WHEN at" + (i+1) + ".data_type=3 ";
                    when2 +="WHEN at" + (i+1) + ".data_type=2 ";
                    when3 +="WHEN at" + (i+1) + ".data_type=1 ";
                }
                else{
                    if((i+1)==2){
                        when1 +=" OR at" + (i+1) + ".data_type=3 ";
                        when2 +="THEN CASE WHEN at" + (i+1) + ".data_type=1 ";
                        when3 +="THEN CASE WHEN at" + (i+1) + ".data_type=2 ";
                    }
                    else{
                        when1 +=" OR at" + (i+1) + ".data_type=3 ";
                        when2 +="OR at" + (i+1) + ".data_type=1 ";
                        when3 +="OR at" + (i+1) + ".data_type=2 ";
                    }
                }

            }
            when1+="THEN 3 ";
            when2+="THEN 3 ELSE 2 END ";
            when3+="THEN 3 ELSE 1 END ";
            caseClause += when1 + when2 + when3 + "END dt_final ";

            for(int i = 0; attributes.length > i; i++){
                fromClause += "(SELECT * FROM DBLOAD D where attribute='" + attributes[i] + "' AND data_type!=-1) as at" + (i+1) + " ,";
            }
            fromClause = fromClause.substring(0,fromClause.length()-1);


            for(int i = 1; attributes.length+1 > i; i++){
                for(int j = i+1; attributes.length+1 > j; j++){
                    whereClause += "at" + i + ".obj_index=at" + j + ".obj_index AND ";
                }      
            }
            whereClause = whereClause.substring(0,whereClause.length()-5);

        }

        try{

            String query = "CREATE TABLE KeyDataTypeAux " + selectClause + caseClause + fromClause + whereClause;

            ps = conn.prepareStatement(query);
            System.out.println(query);

            ps.execute();

            query = "SELECT (SELECT COUNT(*) FROM KeyDataTypeAux WHERE dt_final=" + dataType + ")/(SELECT COUNT(*) FROM KeyDataTypeAux)";
            System.out.println(query);

            ps = conn.prepareStatement(query);

            ResultSet rs = ps.executeQuery();

            if(rs.next()){
                res = rs.getDouble(1);
            }

            query = "DROP TABLE KeyDataTypeAux";

            ps = conn.prepareStatement(query);

            ps.execute();

            rs.close();
            ps.close();

        }catch(SQLException sqle){sqle.printStackTrace();}

        return res;

    }
    
    public double getEmptiness(String attribute){
        
        double res = 0;
        
        try{
            ps = conn.prepareStatement("SELECT (MAX(obj_index)-COUNT(DISTINCT(obj_index)))/(MAX(obj_index)) " +
                                        "FROM " + tableName + " " +
                                        "WHERE attribute = ? AND content != \"\"");
            
            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();
            
            if(rs.next()){
                if(rs.getString(1) == null)
                    return 1;
                res = rs.getDouble(1);
            }
            
            rs.close();
            ps.close();
            
        }catch(SQLException sqle){sqle.printStackTrace();}
        
        return res;
        
    }
    
public double getEmptinessSingleKey(){
        
        double res = 0;
        
        try{
            ps = conn.prepareStatement("SELECT (MAX(obj_index)-COUNT(DISTINCT(obj_index)))/(MAX(obj_index)) " +
                                        "FROM " + keysTable + " " +
                                        "WHERE obj_key1 != \"\"");
            
            
            ResultSet rs = ps.executeQuery();
            
            if(rs.next()){
                if(rs.getString(1) == null)
                    return 1;
                res = rs.getDouble(1);
            }
            
            rs.close();
            ps.close();
            
        }catch(SQLException sqle){sqle.printStackTrace();}
        
        return res;
        
    }
    
public double getAvgStringSize(String attribute){
        
        double res = 0;
        double cnt = 0;
        
        try{
            ps = conn.prepareStatement("SELECT length(content) " +
                                        "FROM " + tableName + " " +
                                        "WHERE attribute = ? AND content != \"\"");
            
            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();
            
            while(rs.next()){
                res = res + rs.getInt(1);
                cnt++;
            }
            
            rs.close();
            ps.close();
            
        }catch(SQLException sqle){sqle.printStackTrace();}
        
        if(cnt == 0)
            return 0;
        else
            return res/cnt;
        
    }

    public List<Integer> getStringSizeOccurrences(String attribute){
    
        List<Integer> res = new ArrayList<Integer>();
        
        try{
            ps = conn.prepareStatement("SELECT COUNT(content) " +
                                        "FROM " + tableName + " " +
                                        "WHERE attribute = ? " +
                                        "GROUP BY LENGTH(content)");
            
            ps.setString(1, attribute);
            
            ResultSet rs = ps.executeQuery();
            
            while(rs.next())
                res.add(rs.getInt(1));
            
            rs.close();
            ps.close();
            
        }catch(SQLException sqle){sqle.printStackTrace();}
    
        return res;
    }
    
    public void insertAttrNodeLevelOccurences(String attributeName, List<Integer> lst) {

        try {
            ps = conn.prepareStatement("INSERT INTO " + attrStructLevelsTableName
                    + "(attribute, NodeLevel, Occurrences) VALUES (?,?,?)");

            int size = lst.size();
            for (int i = 0; size > i; i=i+2) {

                ps.setString(1, attributeName);
                ps.setInt(2, lst.get(i));
                ps.setInt(3, lst.get(i+1));
                ps.addBatch();
            }

            ps.executeBatch();
            ps.clearBatch();

            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

    }
    
    public void insertLevelStats(String attributeName, int selectedNodeLevels, int monl, float mop, float entropy, double stdDeviation, float diversity) {

        try {
            ps = conn.prepareStatement("INSERT INTO " + attrLevelStatsTableName
                    + "(attribute, SelectedNodeLevels, MaxOccurNodeLevel, MaxOccurPercentage," +
                      " OccurrencesEntropy, OccurrencesStdDeviation, OccurrencesDiversity) VALUES (?,?,?,?,?,?,?)");


            ps.setString(1, attributeName);
            ps.setInt(2, selectedNodeLevels);
            ps.setInt(3, monl);
            ps.setFloat(4, mop);
            ps.setFloat(5, entropy);
            ps.setDouble(6, stdDeviation);
            ps.setFloat(7, diversity);
            ps.addBatch();

            ps.executeBatch();
            ps.clearBatch();

            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }

    }
    
    public float getAttributesPerObject(String attributeName){
       
        float res = -1;
        
        try {
            ps = conn.prepareStatement("SELECT COUNT(attribute)/COUNT(DISTINCT(obj_index)) " +
            		                   "FROM " + tableName + " WHERE  content != '' AND attribute = \"" + attributeName + "\"");

            ResultSet rs = ps.executeQuery();

            if (rs.next())
               res = rs.getFloat(1);

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return res;
    }
    
    public Map<String,Integer> getAttributesLevel(){
        
        Map<String,Integer> res = new HashMap<String,Integer>();
        
        try {
            ps = conn.prepareStatement("SELECT Attribute, MaxOccurNodeLevel " +
                                       "FROM " + attrLevelStatsTableName);

            ResultSet rs = ps.executeQuery();

            while(rs.next()){
               res.put(rs.getString(1), rs.getInt(2));
            }

            rs.close();
            ps.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return res;
    }
    
public void loadStringSimilarityScoresToMemory(List<String> attributes){
        
        System.out.println("Loading Similarity Scores To Memory...");
    
        Map<String,Float> ss = new HashMap<String,Float>();
    
        try {
            
            StringMatching sm = new StringMatching();
            
            PreparedStatement ps = conn.prepareStatement("SELECT DISTINCT(content), obj_index " +
                    "FROM " + tableName + " " +
                    "WHERE content != \"\" AND " +
                    "attribute = ?");

            PreparedStatement ps2 = conn.prepareStatement("SELECT DISTINCT(content), obj_index " +
             "FROM " + tableName + " " +
             "WHERE content != \"\" AND " +
             "attribute = ?");
               
            for(int i = 0; attributes.size() > i; i++){System.out.println("Attribute: " + attributes.get(i));

                String attribute = attributes.get(i);
                ps.setString(1, attribute);                
                ps2.setString(1, attribute);
    
                ResultSet rs = ps.executeQuery();
                ResultSet rs2 = ps2.executeQuery();
    
                int j = 1;
                while(rs.next()){
                   //System.out.println("Loading String Similarity of Element(" + attribute + "): " + j);
                   rs2.absolute(j);
                   String str1 = rs.getString(1);
                   int id1 = rs.getInt(2);
                   while(rs2.next()){
      
                       int id2 = rs2.getInt(2);
                       
                       if(id1==id2)
                           continue;
                       
                       String str2 = rs2.getString(1);
                       
                       String str1_org = str1;
                       
                       int strcmp = str1.compareTo(str2);
                       if(strcmp > 0){
                           String aux = str1;
                           str1 = str2;
                           str2 = aux;
                       }
                       
                       String pair = str1.concat("#").concat(str2);
                       
                       if(ss.containsKey(pair)){
                           str1 = str1_org;
                           continue;
                       }
                       else{
                           ss.put(pair, (float) sm.levenstein(str1, str2));
                       }
                       
                       str1 = str1_org;

                   }
                   
                   String pair = str1.concat("#").concat(str1);;
                   ss.put(pair, 1f);
                   j++;
                }
    
                rs.close();
                rs2.close();
                ps.clearParameters();
                ps2.clearParameters();
                
            }
            

            ps.close();
            ps2.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        _similarityScores = ss;
        
        System.out.println("FINISHED!");
    }

    public float getSimilarityScores(String str1, String str2){
        
        if(str1.equals(str2)){
            return 1;
        }
        
        int strcmp = str1.compareTo(str2);
        if(strcmp > 0){
            String aux = str1;
            str1 = str2;
            str2 = aux;
        }
        
        String res = str1.concat("#").concat(str2);
        
        if(_similarityScores.containsKey(res)){
            return _similarityScores.get(res);
        }
        else{
            loadObjectsToMemory();
        }
        
        return _similarityScores.get(res);
    }
    
    public void loadStringSimilarityScoresToDB(List<String> attributes){
        
        try {
            System.out.print("Loading Similarity Scores To DB...");
            
            StringMatching sm = new StringMatching();
        
            ps = conn.prepareStatement("DROP TABLE IF EXISTS " + stringSimScoresTableName);
            ps.execute();
            ps.close();
            
            ps = conn.prepareStatement("CREATE TABLE " + stringSimScoresTableName + "(" +
            		"ID INTEGER, String1 VARCHAR(500), String2 VARCHAR(500), Similarity DOUBLE)");
            
            ps.execute();
            ps.close();
            
            /*ps = conn.prepareStatement("CREATE INDEX ScoresIndex ON " + stringSimScoresTableName + "(" +
            "String1,String2) USING HASH");
            ps.execute();
            ps.close();*/
            
            PreparedStatement ps3 = conn.prepareStatement("INSERT INTO " + stringSimScoresTableName
                    + "(ID, String1, String2, Similarity) VALUES (?,?,?,?)");
            
            ps = conn.prepareStatement("SELECT DISTINCT(content), obj_index " +
                    "FROM " + tableName + " " +
                    "WHERE content != \"\" AND " +
                    "attribute = ? GROUP BY content ORDER BY obj_index ASC");

            PreparedStatement ps2 = conn.prepareStatement("SELECT DISTINCT content, obj_index " +
             "FROM " + tableName + " " +
             "WHERE content != \"\" AND " +
             "attribute = ? GROUP BY content ORDER BY obj_index ASC");
               
            int batchSize = 0;
            
            for(int i = 0; attributes.size() > i; i++){System.out.println("Attribute: " + attributes.get(i));

                String attribute = attributes.get(i);
                ps.setString(1, attribute);                
                ps2.setString(1, attribute);
    
                ResultSet rs = ps.executeQuery();
                ResultSet rs2 = ps2.executeQuery();
                rs.setFetchSize(10000);
                rs2.setFetchSize(10000);
    
                int j = 1;
                while(rs.next()){
                   System.out.println("Loading String Similarity of Element(" + attribute + "): " + j);
                   rs2.absolute(j);
                   System.out.println("ponteiro posicionado");
                   String str1 = rs.getString(1);
                   int id1 = rs.getInt(2);
                   
                   while(rs2.next()){
      
                       int id2 = rs2.getInt(2);//System.out.println(id2);
                       
                       if(id1==id2)
                           continue;
                       
                       String str2 = rs2.getString(1);
                       
                       String str1_org = str1;
                       
                       int strcmp = str1.compareTo(str2);
                       if(strcmp > 0){
                           String aux = str1;
                           str1 = str2;
                           str2 = aux;
                       }
                       
                       //criar uma chave primaria e apanhar a excepçao que vai dar quando tentar inserir pares iguais
                       //atençao para que a chave primaria nao crie um indice na tabela
                       /*if(stringPairExists(str1,str2)){
                           str1 = str1_org;
                           continue;
                       }*/
                       
                       ps3.setInt(1, id1);
                       ps3.setString(2, str1);
                       ps3.setString(3, str2);
                       ps3.setDouble(4, sm.levenstein(str1, str2));
                       
                       ps3.addBatch();
                       ps3.clearParameters();
                       batchSize++;
                       
                       //ps3.execute();                     
                       
                       str1 = str1_org;

                   }
                   
                   ps3.setInt(1, id1);
                   ps3.setString(2, str1);
                   ps3.setString(3, str1);
                   ps3.setDouble(4, 1d);                 
                   ps3.addBatch();
                   ps3.clearParameters();
                   batchSize++;
                   
                   if(batchSize > 1000000){
                       System.out.println("Carrega batch para BD...");
                       conn.setAutoCommit(false);
                       int [] updateCounts = ps3.executeBatch();
                       conn.commit();
                       conn.setAutoCommit(true);
                       System.out.println(updateCounts.length + " updates");
                       ps3.clearBatch();
                       batchSize = 0;
                   }
                   
                   //ps3.execute();                                                
                   
                   j++;
                }
                
                System.out.println("Carrega batch para BD...");
                conn.setAutoCommit(false);
                int [] updateCounts = ps3.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);
                System.out.println(updateCounts.length + " updates");
                ps3.clearBatch();
                batchSize = 0;
    
                rs.close();
                rs2.close();
                ps.clearParameters();
                ps2.clearParameters();
                ps3.clearParameters();
                
            }
            

            ps.close();
            ps2.close();
            //ps3.close();
            
            System.out.println("DB to HashTable...");
            loadObjectsToMemory();
            
            System.out.println("FINISHED!");

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }
    
    private void loadObjectsToMemory(){
        
        try{
            
            _similarityScores.clear();
            
            ps = conn.prepareStatement("SELECT ID, String1, String2, Similarity " +
                    "FROM " + stringSimScoresTableName + " " +
                    "WHERE ID > ? AND ID < ?");
            
            ps.setInt(1, _lastObjectLoadedIntoSimScores);
            ps.setInt(2, _lastObjectLoadedIntoSimScores + _objectsLoadedIntoMemory);
            
            ResultSet rs = ps.executeQuery();
            
            int id = -1;
            
            while(rs.next()){
                id = rs.getInt(1);
                _similarityScores.put(rs.getString(2).concat("#").concat(rs.getString(3)), rs.getFloat(4));
            }
            
            _lastObjectLoadedIntoSimScores = id;
        
            rs.close();
            ps.close();
            
        }catch(SQLException sqle){
            sqle.printStackTrace();
        }
    }
    
    private boolean stringPairExists(String str1, String str2) throws SQLException{
        
        PreparedStatement ps = conn.prepareStatement("SELECT * " +
                "FROM " + stringSimScoresTableName + " " +
                "WHERE String1 = ? AND String2 = ?");
        
        ps.setString(1, str1);
        ps.setString(2, str2);
        
        ResultSet rs = ps.executeQuery();
        
        if(rs.next()){            
            rs.close();
            ps.close();
            return true;
        }
        else{
            rs.close();
            ps.close();
            return false;
        }
        
        
    }
    
    public double getSimilarityScore(String str1, String str2){
     
        double res = -1;
        
        try{
            
            PreparedStatement ps = conn.prepareStatement("SELECT Similarity " +
                    "FROM " + stringSimScoresTableName + " " +
                    "WHERE String1 = ? AND String2 = ?");
            
            int strcmp = str1.compareTo(str2);
            if(strcmp > 0){
                String aux = str1;
                str1 = str2;
                str2 = aux;
            }
            
            ps.setString(1, str1);
            ps.setString(2, str2);
            
            ResultSet rs = ps.executeQuery();
            rs.next();
            res = rs.getDouble(1);
            
            rs.close();
            ps.close();
            
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return res;
    }
    
    private boolean tableExists(String tableName){
        try{
            
            ps = conn.prepareStatement("SELECT table_name" +
            		" FROM information_schema.tables" +
            		" WHERE table_schema = '" + dbName + "' AND table_name = '" + tableName + "'");
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                return true;
            }
            
        }catch(SQLException sqle){sqle.printStackTrace();}
        
        return false;
    }
    
    private boolean tableIsEmpty(String tableName){
        try{
            
            ps = conn.prepareStatement("SELECT * FROM " + tableName);
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                return false;
            }
            
        }catch(SQLException sqle){sqle.printStackTrace();}
        
        return true;
    }
    
    public void buildKeys(String[] attributes_size, boolean fillAttributes){  
        
        if(tableExists(keysTable) && !tableIsEmpty(keysTable)) return;
        
        int attrSize = attributes_size.length;
        int[] attrDim = new int[attrSize];
        String[] attributes = new String[attrSize];
        String[] sp;
        for(int i = 0; attrSize > i; i++){
            sp = attributes_size[i].split("#");
            attrDim[i] = Integer.parseInt(sp[1]);
            attributes[i] = sp[0];
        }
        
        String attr = "";
        for(int i = 0; attrSize > i; i++){
            attr += " attribute = ? OR";
        }
        
        attr = attr.substring(0, attr.length() - 2);
        
        try {
            
            ps = conn.prepareStatement("SELECT COUNT(obj_index) FROM "
                    + tableName + " WHERE (" + attr + ") AND OCCURRENCE = 0");

            for(int i = 0; attrSize > i; i++){
                ps.setString(i+1, attributes[i]);
            }
            
            ResultSet rs = ps.executeQuery();
            
            rs.next();
            int numKeys = rs.getInt(1);
         
            rs.close();
            ps.close();
            
            ps = conn.prepareStatement("SELECT obj_index, attribute, content FROM "
                    + tableName + " WHERE (" + attr + ") AND OCCURRENCE = 0");

            for(int i = 0; attrSize > i; i++){
                ps.setString(i+1, attributes[i]);
            }
                       
            rs = ps.executeQuery();
            
            String key_aux = "";
            String[] mgKey = new String[attrSize];
            String attrName;
            String content;
            int id = -1;
            int id_aux = 0;
            Map<String,String> key = new HashMap<String,String>();
            boolean first = true;
            
            String query = "INSERT INTO " + keysTable + " VALUES (?,?,?,?)";
            
            PreparedStatement ps2 = conn.prepareStatement(query);
            StringMatching sm = new StringMatching();
            int cnt = 1;
            boolean last = false;
            while (rs.next()) {
                attrName = rs.getString(2);
                id_aux = rs.getInt(1);
                content = rs.getString(3);
                
                if(first){
                    id = id_aux;
                }
                
                if(id != id_aux || last){
                    
                    boolean emptyKey = true;
                    for(int i = 0; attrSize > i; i++){
                        String attributes_aux = attributes[i];
                        String selectedKey = key.get(attributes_aux);
                        
                        if(selectedKey != null){
                            key_aux = sm.removeSpaces(selectedKey);

                        }
                        
                        
                        int attrDim_aux = attrDim[i];
                        int key_aux_size = key_aux.length();
                        int wildCharSize = attrDim_aux - key_aux_size;
                        if(key_aux_size > attrDim_aux){
                            key_aux = key_aux.substring(0, attrDim_aux);
                        }
                        for(int j = 0; fillAttributes && (wildCharSize > j); j++){
                            key_aux += "*";
                        }
                        
                        if(key_aux_size > 0){
                            emptyKey = false;
                        }
                        
                        mgKey[i] = key_aux;
                        key_aux = "";
                    }
                    
                    if(emptyKey){
                        for(int i = 0; attrSize > i; i++){
                            mgKey[i] = "";
                        }
                    }

                    ps2.setInt(1, id);
                    ps2.setString(2,swapKeyAttributes(mgKey,1));
                    ps2.setString(3,swapKeyAttributes(mgKey,2));
                    ps2.setString(4,swapKeyAttributes(mgKey,3));
                    
                    ps2.execute();

                    mgKey = new String[attrSize];
                    id = rs.getInt(1);
             
                }

                key.put(attrName, content);
                first = false;
                
                
                
                if(cnt == numKeys){
                    rs.previous();
                    last = true;
                }
                
                
                cnt++; 

            }

            rs.close();
            ps.close();
            ps2.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }
    
    private int getAttributesDim(String attrSegments){
        String[] seg = attrSegments.split(";");
        int dim = 0;
        for(int i = 0; seg.length > i; i++){
            String[] indexes = seg[i].split("-");
            int i1 = Integer.parseInt(indexes[0]);
            int i2 = Integer.parseInt(indexes[1]);
            
            dim+= (i2-i1+1);
        }
        
        return dim;
    }
    
    private String buildKeyFromIndexes(String key, String indexSegments){
        String[] segIndex = indexSegments.split(";");
        String key_aux = "";
        int keyLength = key.length();
        for(int i = 0; segIndex.length > i; i++){
            String[] indexes = segIndex[i].split("-");
            int i1 = Integer.parseInt(indexes[0]);
            int i2 = Integer.parseInt(indexes[1]);
            if(i2 > keyLength-1){
                if(i1 > keyLength-1){
                    break;
                }
                key_aux = key_aux.concat(key.substring(i1,keyLength));
                break;
            }
            else{
                key_aux = key_aux.concat(key.substring(i1,i2+1));
            }
        }       
        
        return key_aux;
    }
    
    //considera os segmentos da chave indicados depois do simbolo '#' Ex: title#[2-8],author#[1-3;5-7]
    public void buildKeys2(String[] attributes_size, boolean fillAttributes){  
        
        if(tableExists(keysTable) && !tableIsEmpty(keysTable)) return;
        
        int attrSize = attributes_size.length;
        int[] attrDim = new int[attrSize];
        String[] attributes = new String[attrSize];
        String[] attrSegIndexes = new String[attrSize];
        String[] sp;
        for(int i = 0; attrSize > i; i++){
            sp = attributes_size[i].split("#"); 
            attrSegIndexes[i] = sp[1].substring(1, sp[1].length()-1);
            attrDim[i] = getAttributesDim(attrSegIndexes[i]);
            attributes[i] = sp[0];
        }
        
        String attr = "";
        for(int i = 0; attrSize > i; i++){
            attr += " attribute = ? OR";
        }
        
        attr = attr.substring(0, attr.length() - 2);
        
        try {
            
            ps = conn.prepareStatement("SELECT COUNT(obj_index)_index FROM "
                    + tableName + " WHERE (" + attr + ") AND OCCURRENCE = 0");

            for(int i = 0; attrSize > i; i++){
                ps.setString(i+1, attributes[i]);
            }
            
            ResultSet rs = ps.executeQuery();
            
            rs.next();
            int numKeys = rs.getInt(1);
         
            rs.close();
            ps.close();
            
            ps = conn.prepareStatement("SELECT obj_index, attribute, content FROM "
                    + tableName + " WHERE (" + attr + ") AND OCCURRENCE = 0");

            for(int i = 0; attrSize > i; i++){
                ps.setString(i+1, attributes[i]);
            }
                       
            rs = ps.executeQuery();
            
            String key_aux = "";
            String[] mgKey = new String[attrSize];
            String attrName;
            String content;
            int id = -1;
            int id_aux = 0;
            Map<String,String> key = new HashMap<String,String>();
            boolean first = true;
            
            String query = "INSERT INTO " + keysTable + " VALUES (?,?,?,?)";
            
            PreparedStatement ps2 = conn.prepareStatement(query);
            StringMatching sm = new StringMatching();
            int cnt = 1;
            boolean last = false;
            while (rs.next()) {
                attrName = rs.getString(2);
                id_aux = rs.getInt(1);
                content = rs.getString(3);
                
                if(first){
                    id = id_aux;
                }
                
                if(id != id_aux || last){
                    
                    boolean emptyKey = true;
                    for(int i = 0; attrSize > i; i++){
                        String attributes_aux = attributes[i];
                        String selectedKey = key.get(attributes_aux);

                        key_aux = buildKeyFromIndexes(selectedKey,attrSegIndexes[i]);
                        
                        if(selectedKey != null){
                            key_aux = sm.removeSpaces(key_aux);
                        }
                        
                        int attrDim_aux = attrDim[i];
                        int key_aux_size = key_aux.length();
                        int wildCharSize = attrDim_aux - key_aux_size;
                       
                        
                        for(int j = 0; fillAttributes && (wildCharSize > j); j++){
                            key_aux += "*";
                        }
                        
                        if(key_aux_size > 0){
                            emptyKey = false;
                        }
                        
                        mgKey[i] = key_aux;
                        key_aux = "";
                    }
                    
                    if(emptyKey){
                        for(int i = 0; attrSize > i; i++){
                            mgKey[i] = "";
                        }
                    }

                    ps2.setInt(1, id);
                    ps2.setString(2,swapKeyAttributes(mgKey,1));
                    ps2.setString(3,swapKeyAttributes(mgKey,2));
                    ps2.setString(4,swapKeyAttributes(mgKey,3));
                    
                    ps2.execute();

                    mgKey = new String[attrSize];
                    id = rs.getInt(1);
             
                }

                key.put(attrName, content);
                first = false;
                
                
                
                if(cnt == numKeys){
                    rs.previous();
                    last = true;
                }
                
                
                cnt++; 

            }

            rs.close();
            ps.close();
            ps2.close();

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }
    
    private void dropTable(String tableName){
        
        try{
        
            ps = conn.prepareStatement("DROP TABLE IF EXISTS " + keysTable);
            ps.execute();
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
    }
    
    public void buildKeysDBMS(String[] attributes, boolean fillAttributes, int runs){
        
        if(tableExists(keysTable) && !tableIsEmpty(keysTable))
            return;
        else
            dropTable(keysTable);
        
        try{
        
            int attrsSize = attributes.length;
            String fillChar = "";
            if(fillAttributes){
                fillChar = "*";
            }
            
            List<String> concats = new ArrayList<String>();
            String fromSegment = "";
            String whereSegment = "WHERE ";
            
            for(int i = 0; attrsSize > i; i++){
                int size = Integer.parseInt(attributes[i].split("#")[1]);
                String attr = attributes[i].split("#")[0];
                concats.add("concat(D" + (i+1) + ".content," +
                		"CASE length(substring(D" + (i+1) + ".content,1," + size + ")) " +
                		"WHEN 0 THEN '' " +
                		"ELSE repeat('" + fillChar + "'," + size + " - length(substring(D" + (i+1) + ".content,1," + size + "))) END )");
                fromSegment += "(select content, id, obj_index from DBLOAD where attribute = '" + attr + "' and OCCURRENCE = 0) as D" + (i+1) + " ,";
            }
            
            
            List<List<String>> objKeys = new ArrayList<List<String>>();
            String elem = concats.get(0);
            
            if(runs > attrsSize){
                runs = attrsSize;
            }
            
            for(int i = 0; runs > i; i++){
                List<String> originalOrder = new ArrayList<String>(concats);
                originalOrder.set(0, originalOrder.get(i));
                originalOrder.set(i, elem);
                objKeys.add(originalOrder);                
            }
            
            if(attrsSize == 1){
                fromSegment = fromSegment.substring(0,fromSegment.length()-1);
                whereSegment = "";
            }
            else{
                fromSegment = fromSegment.substring(0,fromSegment.length()-1);
                
                for(int i = 1; attrsSize+1 > i; i++){
                    for(int j = i+1; attrsSize+1 > j; j++){
                        whereSegment += "D" + i + ".obj_index=D" + j + ".obj_index AND ";
                    }
                }
                whereSegment = whereSegment.substring(0,whereSegment.length()-5);
            }
                   
            String query = "SELECT D1.obj_index, " + buildObjKeysConcatString(objKeys) +
            		        " FROM " + fromSegment + whereSegment;   		          
            
            ps = conn.prepareStatement("CREATE TABLE " + keysTable + " " + query);
            System.out.println("CREATE TABLE: " + "CREATE TABLE " + keysTable + " " + query);
            System.out.println("QUERY: " + query);
            ps.execute();
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
                
    }
    
    private String buildObjKeysConcatString(List<List<String>> objKeys){
        
        String res = "";
        
        for(int i = 0; objKeys.size() > i; i++){
            List<String> lst = objKeys.get(i);
            res+="concat(";
            for(int j = 0; lst.size() > j; j++){
                res+=lst.get(j) + ",'#',";
            }
            res = res.substring(0,res.length()-5);
            res+=") obj_key" + (i+1) + ",";
        }
        res = res.substring(0,res.length()-1);
        
        return res;
    }
    
    private String keyToString(String[] key){
        String res="";
        for(int i = 0; key.length > i; i++){
            res = res.concat(key[i]).concat("#");
        }

        res = res.substring(0, res.length()-1);
        
        return res;
    }
    
    private String swapKeyAttributes(String[] key, int keys){
        
        String[] res = new String[key.length];
        
        for(int i = 0; key.length > i; i++){
            res[i] = key[i];
        }
        
        if(key.length == 1){
            return keyToString(res);
        }

        if(keys == 2 && key.length >= 2){
            res[0] = key[1];
            res[1] = key[0];
            return keyToString(res);
        }
        
        if(keys == 3 && key.length >= 3){
            res[0] = key[2];
            //res[1] = key[0];
            //res[2] = key[1];
            res[2] = key[0];
            return keyToString(res);
        }
        
        return keyToString(res);
    }
    
    public List<ResultSet> getKeys(int keyNum) throws SQLException{

        List<ResultSet> res = new ArrayList<ResultSet>();
        
        for(int i = 0; keyNum > i; i++){
            ps = conn.prepareStatement("SELECT obj_index,OBJ_KEY" + (i+1) + " FROM "+ keysTable + " ORDER by OBJ_KEY" + (i+1));
            res.add(ps.executeQuery());
        }
        
        /*ps = conn.prepareStatement("SELECT ID,OBJ_KEY1 FROM "+ keys + " ORDER by OBJ_KEY1");
        ResultSet rs1 = ps.executeQuery();
        ps = conn.prepareStatement("SELECT ID,OBJ_KEY2 FROM "+ keys + " ORDER by OBJ_KEY2");
        ResultSet rs2 = ps.executeQuery();
        ps = conn.prepareStatement("SELECT ID,OBJ_KEY3 FROM "+ keys + " ORDER by OBJ_KEY3");
        ResultSet rs3 = ps.executeQuery();
        
        
        res.add(rs1);
        if(keyNum > 1){
            res.add(rs2);
        }
        if(keyNum > 2){
            res.add(rs3);
        }*/

        //ps.close();
        
        return res;
    }
    
    public long getExistingDuplicates(){
        
        try{
        
            ps = conn.prepareStatement("SELECT SUM(dup_entry) FROM " +
            		                   "(SELECT COUNT(DISTINCT(obj_index))" +
            		                   "*(COUNT(DISTINCT(obj_index))-1)/2 AS dup_entry " +
            		                   "FROM "+ tableName +" GROUP BY id) AS dupTotal");
            
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                return rs.getLong(1);
            }
            
            rs.close();
            ps.close();
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
       
        
        return -1;
    }
    
    public Set<String> getDuplicatePairs(String attribute){
        
        Set<String> res = new HashSet<String>();
        
        try{
            String query = "SELECT gtb.id,stb.obj_index,stb.content " +
                    "FROM (SELECT id,count(*) cnt FROM DBLOAD where attribute = '"+attribute+"' and content != '' group by id) as gtb, " +
                    "(SELECT * FROM DBLOAD WHERE attribute = '"+attribute+"' and content!='') as stb " +
                    "WHERE gtb.id=stb.id and gtb.cnt > 1 order by stb.id";
            ps = conn.prepareStatement(query);
            
            ResultSet rs = ps.executeQuery();
            
            
            Map<Integer,Set<Integer>> aux = new HashMap<Integer,Set<Integer>>();
            while(rs.next()){
                int id = rs.getInt(1);
                if(aux.containsKey(id)){
                    aux.get(id).add(rs.getInt(2)-1);
                }
                else{
                    Set<Integer> s = new HashSet<Integer>();
                    s.add(rs.getInt(2)-1);
                    aux.put(id,s);
                }
               
            }
            
            rs.close();
            ps.close();
            
            
            for(Map.Entry<Integer, Set<Integer>> e : aux.entrySet()){
                Set<Integer> dups = e.getValue();
                List<Integer> lst = new ArrayList<Integer>(new TreeSet<Integer>(dups));
                for(int i = 0; lst.size() > i; i++){
                    for(int j = i+1; lst.size() > j; j++){
                        String pair = lst.get(i) + "#" + lst.get(j);
                        res.add(pair);
                    }
                }
            }
            
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
       
        
        return res;
    }
    
    public Map<String,List<Integer>> getOccurrencesFullContent(Map<String,Integer> keys){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT COUNT(content)" +
                		"FROM DBLOAD " +
                		"WHERE attribute = '" + attribute + "' and content != '' " +
                		"GROUP BY content");
                
                ResultSet rs = ps.executeQuery();
                
                while(rs.next()){
                    occurrences.get(attribute).add(rs.getInt(1));
                }
                
                rs.close();
                ps.close();
            
            }
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    public Map<String,List<Integer>> getMaxTokensSizeOccurrences(Map<String,Integer> keys){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT content " +
                        "FROM DBLOAD " +
                        "WHERE attribute = '" + attribute + "' and content != ''");
                
                ResultSet rs = ps.executeQuery();
                
                String[] tokens;
                while(rs.next()){
                    tokens = rs.getString(1).split(" ");
                    int maxSize = -1;
                    for(int i = 0; tokens.length > i; i++){
                        if(maxSize < tokens[i].length()){
                            maxSize = tokens[i].length();
                        }
                    }
                    occurrences.get(attribute).add(maxSize);
                }
                
                rs.close();
                ps.close();
            
            }
        
        }catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    public Map<String,List<Integer>> getOccurrencesTokens(Map<String,Integer> keys){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT content " +
                        "FROM DBLOAD " +
                        "WHERE attribute = '" + attribute + "' and content != ''");
                
                ResultSet rs = ps.executeQuery();
                
                int tokens;
                while(rs.next()){
                    tokens = rs.getString(1).split(" ").length;
                    if(tokens == 2 && rs.getString(1).split(" ")[1].equals("")){
                        tokens = 1;
                    }
                    occurrences.get(attribute).add(tokens);
                }
                
                rs.close();
                ps.close();
            
            }
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    public Map<String,List<Integer>> getOccurrences(Map<String,Integer> keys){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT COUNT(SUBSTR(content,1," + e.getValue() + ")) " +
                		                    "FROM DBLOAD WHERE content!='' " +
                		                    "AND attribute='" + attribute + "' GROUP BY SUBSTR(content,1," + e.getValue() + ")");
                
                ResultSet rs = ps.executeQuery();
                
                while(rs.next()){
                    occurrences.get(attribute).add(rs.getInt(1));
                }
                
                rs.close();
                ps.close();
            
            }
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    public Map<String,List<Integer>> getOccurrencesSingleKey(Map<String,Integer> keys){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            String key = "";
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                           
                key += e.getKey() + ",";
                
            }
            
            key = key.substring(0, key.length()-1);
            
            System.out.println("KEY!!! = " + key);
            
            occurrences.put(key, new ArrayList<Integer>());

            ps = conn.prepareStatement("SELECT COUNT(obj_key1) " +
                    "FROM " + keysTable + " WHERE obj_key1!='' " +
                    "GROUP BY obj_key1");

            ResultSet rs = ps.executeQuery();

            while(rs.next()){
                occurrences.get(key).add(rs.getInt(1));
            }System.out.println(occurrences);

            rs.close();
            ps.close();
            
            
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    
public Map<String,List<Integer>> getOccurrencesStringTokens(Map<String,Integer> keys, int tokenPos){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT COUNT(tb.token) " +
                		"FROM (SELECT REPLACE(SUBSTRING_INDEX(content, ' ', " + tokenPos + "),SUBSTRING_INDEX(content, ' ', " + tokenPos + "-1),'') as token " +
                		"FROM DBLOAD WHERE attribute='" + attribute + "' and content != '') as tb " +
                		"WHERE tb.token != '' " +
                		"GROUP BY tb.token");
                
                ResultSet rs = ps.executeQuery();
                
                while(rs.next()){
                    occurrences.get(attribute).add(rs.getInt(1));
                }
                
                rs.close();
                ps.close();
            
            }
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    
public Map<String,List<Integer>> getOccurrencesSegment(Map<String,Integer> keys, int chars, boolean startFromLeft){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        String substring = "";
        
        if(startFromLeft){
            substring = "1," + chars;
        }
        else{
            substring = "LENGTH(content)-" + (chars-1) + ",LENGTH(content)";
        }
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT COUNT(SUBSTR(content," + substring + ")) " +
                                            "FROM DBLOAD WHERE content!='' " +
                                            "AND attribute='" + attribute + "' GROUP BY SUBSTR(content," + substring + ")");
                
                ResultSet rs = ps.executeQuery();
                
                while(rs.next()){
                    occurrences.get(attribute).add(rs.getInt(1));
                }
                
                rs.close();
                ps.close();
            
            }
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }

public Map<String,List<Integer>> getOccurrencesSegmentSingleKey(Map<String,Integer> keys, int chars, boolean startFromLeft){
    
    Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
    
    String substring = "";
    
    if(startFromLeft){
        substring = "1," + chars;
    }
    else{
        substring = "LENGTH(obj_key1)-" + (chars-1) + ",LENGTH(obj_key1)";
    }
    
    try{
        
        String key = "";
        
        for(Map.Entry<String, Integer> e: keys.entrySet()){
            
            key += e.getKey() + ",";
        }
        
        key = key.substring(0, key.length()-1);
        
        System.out.println("KEY!!! = " + key);
        
        occurrences.put(key, new ArrayList<Integer>());

        ps = conn.prepareStatement("SELECT COUNT(SUBSTR(obj_key1," + substring + ")) " +
                "FROM " + keysTable + " WHERE obj_key1 !='' " +
                "GROUP BY SUBSTR(obj_key1," + substring + ")");

        ResultSet rs = ps.executeQuery();

        while(rs.next()){
            occurrences.get(key).add(rs.getInt(1));
        }

        rs.close();
        ps.close();
        
        
    
    } catch (SQLException sqle) {
        sqle.printStackTrace();
    }
    
    return occurrences;
}
    
public Map<String,List<Integer>> getStringSizesFullContent(Map<String,Integer> keys){
    
    Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
    
    try{
        
        for(Map.Entry<String, Integer> e: keys.entrySet()){
            
            String attribute = e.getKey();
            occurrences.put(attribute, new ArrayList<Integer>());
        
            ps = conn.prepareStatement("SELECT LENGTH(content) lc " +
                                        "FROM DBLOAD WHERE content!='' AND attribute='" + attribute + "' " +
                                        "ORDER BY lc");
            
            ResultSet rs = ps.executeQuery();
            
            while(rs.next()){
                occurrences.get(attribute).add(rs.getInt(1));
            }
            
            rs.close();
            ps.close();
        
        }
    
    } catch (SQLException sqle) {
        sqle.printStackTrace();
    }
    
    return occurrences;
}

    public Map<String,List<Integer>> getStringSizes(Map<String,Integer> keys){
        
        Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                
                String attribute = e.getKey();
                occurrences.put(attribute, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT LENGTH(SUBSTR(content,1," + e.getValue() + ")) " +
                		                    "FROM DBLOAD WHERE content!='' AND attribute='" + attribute + "'");
                
                ResultSet rs = ps.executeQuery();
                
                while(rs.next()){
                    occurrences.get(attribute).add(rs.getInt(1));
                }
                
                rs.close();
                ps.close();
            
            }
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return occurrences;
    }
    
    public Map<String,List<Integer>> getStringSizesSingleKey(Map<String,Integer> keys){
        
Map<String,List<Integer>> occurrences = new HashMap<String,List<Integer>>();
        
        try{
            
            String key = "";
            
            for(Map.Entry<String, Integer> e: keys.entrySet()){
                           
                key += e.getKey() + ",";
                
            }
            
            key = key.substring(0, key.length()-1);
            
            System.out.println("KEY!!! = " + key);
            
            occurrences.put(key, new ArrayList<Integer>());
            
                ps = conn.prepareStatement("SELECT LENGTH(obj_key1) " +
                                            "FROM " + keysTable + " WHERE obj_key1!=''");
                
                ResultSet rs = ps.executeQuery();

                while(rs.next()){
                    occurrences.get(key).add(rs.getInt(1));
                }System.out.println(occurrences);

                rs.close();
                ps.close();
                
                
            
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            }
            
            return occurrences;
    }
    
    public double getDistinctivenessKey(String key, int chars, boolean startFromLeft){
        
       double res = -1;
       String substring = "";
       
       if(startFromLeft){
           substring = "1," + chars;
       }
       else{
           substring = "LENGTH(content)-" + (chars-1) + ",LENGTH(content)";
       }
       
        try{
            
            ps = conn.prepareStatement("SELECT COUNT(DISTINCT(SUBSTR(content," + substring + ")))/COUNT(content) " +
                    "FROM DBLOAD WHERE content!='' " +
                    "AND attribute='" + key + "'");

            ResultSet rs = ps.executeQuery();

            if(rs.next()){
                res = rs.getDouble(1);
            }

            rs.close();
            ps.close();
            
        
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        
        return res;
        
    }
    
    public double getRecallNoEmptyKeyObjects(){
        
        double res=-1;
        
        try{
            ps = conn.prepareStatement("SELECT SUM(C.cnt) " +
            		"FROM (SELECT count(T.obj_key1)*(count(T.obj_key1)-1)/2 cnt " +
            		    "FROM (SELECT D.id,D.obj_index,O.obj_key1 " +
            		        "FROM (SELECT id,obj_index FROM DBLOAD group by obj_index) as D, OBJ_KEYS O " +
            		        "WHERE D.obj_index = O.obj_index and O.obj_key1 NOT LIKE '%#%' and O.obj_key1 != '' " +
            		        "ORDER BY O.obj_key1) as T " +
            		    "GROUP BY T.id) as C");
            
            ResultSet rs = ps.executeQuery();

            if(rs.next()){
                res = rs.getDouble(1);
            }

            rs.close();
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
        return res;
    }
    
 public Set<Integer> getCoOccurringPairs(){
        
        Set<Integer> res = new HashSet<Integer>();
        
        try{
            ps = conn.prepareStatement("SELECT obj_index FROM OBJ_KEYS WHERE obj_key1 !='' AND obj_key1 != '#'");
            
            ResultSet rs = ps.executeQuery();

            while(rs.next()){
                res.add(rs.getInt(1)-1);
            }

            rs.close();
            ps.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
        return res;
    }
 
 public int getAttributeMaxLength(String attribute){
     
     int res = -1;
     
     try{
         ps = conn.prepareStatement("SELECT MAX(LENGTH(content)) FROM " + tableName + " WHERE content != '' AND attribute = '" + attribute + "'");
         
         ResultSet rs = ps.executeQuery();

         if(rs.next()){
             res = rs.getInt(1);
         }

         rs.close();
         ps.close();
     }
     catch(Exception e){
         e.printStackTrace();
     }
     
     return res;
 }
 
public int getAttributeMinLength(String attribute){
     
     int res = -1;
     
     try{
         ps = conn.prepareStatement("SELECT MIN(LENGTH(content)) FROM " + tableName + " WHERE content != '' AND attribute = '" + attribute + "'");
         
         ResultSet rs = ps.executeQuery();

         if(rs.next()){
             res = rs.getInt(1);
         }

         rs.close();
         ps.close();
     }
     catch(Exception e){
         e.printStackTrace();
     }
     
     return res;
 }

public Map<String,Map<Integer,Float>> buildCharsHistogram(List<Integer> attrMaxLength, List<String> attributes, String measure){
    
    Map<String,Map<Integer,Float>> res = new HashMap<String,Map<Integer,Float>>();
    
    try{
        
        String query;
        
        List<Integer> occurrences = null;
        Entropy e;
        float measureScore = -1;
        
        ResultSet rs = null;
        
        for(int i = 0; attributes.size() > i; i++){
            
            for(int j = 0; attrMaxLength.get(i) > j; j++){
                
                if(measure.equals("emptiness")){
                    query = "SELECT COUNT(SUBSTR(content," + (j+1) + ",1))/(SELECT COUNT(SUBSTR(content," + (j+1) + ",1)) FROM " + tableName + " WHERE attribute = '" + attributes.get(i) + "') FROM " + tableName + " WHERE attribute = '" + attributes.get(i) + "' AND SUBSTR(content," + (j+1) + ",1) = ''";
                }
                else{
                    query = "SELECT COUNT(SUBSTR(content," + (j+1) + ",1)) FROM " + tableName + " WHERE attribute = '" + attributes.get(i) + "' GROUP BY SUBSTR(content," + (j+1) + ",1)";
                }
                
                ps = conn.prepareStatement(query);           
                
                rs = ps.executeQuery();

                occurrences = new ArrayList<Integer>();
                while(rs.next()){
                    measureScore = rs.getFloat(1);
                    occurrences.add(rs.getInt(1));
                }
                
                e = new Entropy(this,attributes.get(i));
                
                if(measure.equals("entropy")){
                    measureScore = (float)e.getEntropyKey(occurrences); //Entropy
                }
                
                if(measure.equals("distinctiveness")){
                    measureScore = (float)e.distinctiveness(occurrences); //Distinctiveness
                }
                
                
                if(res.containsKey(attributes.get(i))){
                    res.get(attributes.get(i)).put(j, measureScore);
                }
                else{
                    Map<Integer,Float> entry = new HashMap<Integer,Float>();
                    entry.put(j,measureScore);
                    res.put(attributes.get(i), entry);
                }
                
                rs.close();
            }
            //ordena indice de caracters
            res.put(attributes.get(i), sortHashMap(res.get(attributes.get(i))));
            
        }

        ps.close();
    }
    catch(Exception e){
        e.printStackTrace();
    }
    
    return res;
}
 
 public Map<String,Map<Integer,Float>> buildCharsDistinctivenessHistogram(List<Integer> attrMaxLength, List<String> attributes){
     
     Map<String,Map<Integer,Float>> res = new HashMap<String,Map<Integer,Float>>();
     
     try{
         
         String query;
         String query2;
         
         ResultSet rs = null;
         
         for(int i = 0; attributes.size() > i; i++){
             
             for(int j = 0; attrMaxLength.get(i) > j; j++){
                 
                 query = "SELECT COUNT(DISTINCT(SUBSTR(content," + (j+1) + ",1)))/(SELECT COUNT(SUBSTR(content," + (j+1) + ",1)) FROM " + tableName + " WHERE SUBSTR(content," + (j+1) + ",1) != '' AND attribute = '" + attributes.get(i) + "') FROM " + tableName + " WHERE SUBSTR(content," + (j+1) + ",1) != '' AND attribute = '" + attributes.get(i) + "'";
                 
                 query2 = "SELECT COUNT(DISTINCT(SUBSTR(content," + (j+1) + ",1)))/(SELECT COUNT(SUBSTR(content," + (j+1) + ",1)) FROM " + tableName + " WHERE /*SUBSTR(content," + (j+1) + ",1) != '' AND*/ attribute = '" + attributes.get(i) + "') FROM " + tableName + " WHERE /*SUBSTR(content," + (j+1) + ",1) != '' AND*/ attribute = '" + attributes.get(i) + "'";
                 
                 ps = conn.prepareStatement(query);
                 
                 
                 rs = ps.executeQuery();
             
                 if(rs.next()){
                     
                     if(res.containsKey(attributes.get(i))){
                         res.get(attributes.get(i)).put(j, rs.getFloat(1));
                     }
                     else{
                         Map<Integer,Float> entry = new HashMap<Integer,Float>();
                         entry.put(j,rs.getFloat(1));
                         res.put(attributes.get(i), entry);
                     }
                 }  
                 
                 rs.close();
             }
             //ordena indice de caracters
             res.put(attributes.get(i), sortHashMap(res.get(attributes.get(i))));
             
         }

         ps.close();
     }
     catch(Exception e){
         e.printStackTrace();
     }
     
     return res;
 }
 
 private Map<Integer, Float> sortHashMap(Map<Integer, Float> unsortMap) {

     List <Map.Entry<Integer, Float>> list = new LinkedList<Map.Entry<Integer, Float>>(unsortMap.entrySet());

     //sort list based on comparator
     Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>() {
         public int compare(Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2) {
             //o2-o1(decrescente); o1-o2(crescente)
             return ((Comparable<Float>) ((Map.Entry<Integer, Float>) (o1)).getValue())
                     .compareTo(((Map.Entry<Integer, Float>) (o2)).getValue());
         }
     });

     //put sorted list into map again
     Map<Integer, Float> sortedMap = new LinkedHashMap<Integer, Float>();
     for (Iterator<Map.Entry<Integer, Float>> it = list.iterator(); it.hasNext();) {
         Map.Entry<Integer, Float> entry = (Map.Entry<Integer, Float>)it.next();
         sortedMap.put(entry.getKey(), entry.getValue());
     }
     return sortedMap;
 }    
 
 
 public float getMeasureFromKeysChars(String[] state, List<String> attributes, String measure){
     
     float res = -1;
     
     String keys = "";
     
     for(int i = 0; attributes.size() > i; i++){
         
         if(state[i].equals("#")){
             continue;
         }
         
         keys = keys + attributes.get(i) + "#[";
         String[] indexes = state[i].split(",");
         for(int j = 0; indexes.length > j; j++){            
             keys = keys + indexes[j] + "-" + indexes[j] + ";";
         }
         keys = keys.substring(0, keys.length()-1);
         keys = keys + "],";
     }
     if(keys.length()>0){
         keys = keys.substring(0, keys.length()-1);
     }
     
     //considera os segmentos da chave indicados depois do simbolo '#' Ex: title#[2-8],author#[1-3;5-7]
     buildKeys2(keys.split(","), false);
     
     try{
             
         String query;
         List<Integer> occurrences = null;
         
         if(measure.equals("emptiness")){
             query = "SELECT count(obj_key1)/(SELECT count(*) FROM OBJ_KEYS) FROM OBJ_KEYS where replace(obj_key1,'#','') = ''";
         }
         else{
             //Tem em conta apenas os registos em que o conteudo nao esta vazio
             //query = "SELECT COUNT(obj_key1) FROM " + keysTable + " WHERE obj_key1 != ''GROUP BY obj_key1";
    
             //Tem em conta os registos em que o conteudo esta vazio
             query = "SELECT COUNT(obj_key1) FROM " + keysTable + " GROUP BY obj_key1";
         }
         
         ps = conn.prepareStatement(query);
        
         ResultSet rs = ps.executeQuery();

         occurrences = new ArrayList<Integer>();
         while(rs.next()){
             res = rs.getFloat(1);
             occurrences.add(rs.getInt(1));
         }
         
         Entropy e = new Entropy(this,"");
         if(measure.equals("entropy")){
             res = (float)e.getEntropyKey(occurrences);
         }
         
         if(measure.equals("distinctiveness")){
             res = (float) e.distinctiveness(occurrences);
         }

         rs.close();
         ps.close();
     }
     catch(Exception e){
         e.printStackTrace();
     }
     
     return res;
 }
 
 
 public float getDistinctivenessFromKeysChars(String[] state, List<String> attributes){
     
     float res = -1;
     
     String keys = "";
     
     for(int i = 0; attributes.size() > i; i++){
         
         if(state[i].equals("#")){
             continue;
         }
         
         keys = keys + attributes.get(i) + "#[";
         String[] indexes = state[i].split(",");
         for(int j = 0; indexes.length > j; j++){            
             keys = keys + indexes[j] + "-" + indexes[j] + ";";
         }
         keys = keys.substring(0, keys.length()-1);
         keys = keys + "],";
     }
     if(keys.length()>0){
         keys = keys.substring(0, keys.length()-1);
     }
     
     //considera os segmentos da chave indicados depois do simbolo '#' Ex: title#[2-8],author#[1-3;5-7]
     buildKeys2(keys.split(","), false);
     
     try{
         
         //Tem em conta apenas os registos em que o conteudo nao esta vazio
         String query = "SELECT COUNT(DISTINCT(obj_key1))/(SELECT COUNT(obj_key1) FROM " + keysTable + /*" WHERE obj_key1 != ''*/") FROM " + keysTable /*+ " WHERE obj_key1 != ''"*/;
         
         //Tem em conta os registos com conteudo vazio
         String query2 = "SELECT COUNT(DISTINCT(obj_key1))/(SELECT COUNT(obj_key1) FROM " + keysTable + " WHERE obj_key1 != '') FROM " + keysTable + " WHERE obj_key1 != ''";
         
         ps = conn.prepareStatement(query2);
        
         ResultSet rs = ps.executeQuery();

         if(rs.next()){
             res = rs.getFloat(1);
         }

         rs.close();
         ps.close();
     }
     catch(Exception e){
         e.printStackTrace();
     }
     
     return res;
 }

}
