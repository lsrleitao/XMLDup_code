package DuplicateDetection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Lu�s Leit�o
 */

public class Gnuplot {

    private String _gnuplotCommand;
    private String _configFilePath;
    private String _graphType;
    private String _bdFileName;
    private float _threshold;
    private String _resultsPath;
    private long _dbSize;
    private GnuplotVariables _gnuplotVariables;
    private long _pairsCompared;

    private String _bashFileName = "generatePlot.sh";

    public Gnuplot(String gt, String bdFN, float t, String rsPath, long dbS, long pairsComp) {

        _graphType = gt;
        _bdFileName = bdFN;
        _threshold = t;
        _resultsPath = rsPath;
        _dbSize = dbS;
        _pairsCompared = adjustPairsCompared(pairsComp);

        _gnuplotVariables = new GnuplotVariables();

        if (System.getProperty("os.name").equalsIgnoreCase("Linux")) {
            _gnuplotCommand = "gnuplot";
        } else
            _gnuplotCommand = "pgnuplot";
    }

    /**
     * Builds the configuration file used by Gnuplot
     * 
     * @throws IOException
     */
    public void buildGraphConfigurationFile() throws IOException {

        writeFile();
    }

    /**
     * Draws the plot
     * 
     * @throws IOException
     */
    public void writeGraph() {

        try {

            if (System.getProperty("os.name").equalsIgnoreCase("Linux"))
                writeBashFile();
            else
                Runtime.getRuntime().exec(_gnuplotCommand + " \"" + _configFilePath + "\"");
        } catch (IOException ioe) {
            System.out.println("Cannot execute Gnuplot on " + _configFilePath);
        }
    }

    /**
     * Writes the gnuplot invocation that builds the plots to a bash file. Only when
     * running under a Linux OS.
     */
    public void writeBashFile() {

        try {

            File f = new File(_resultsPath);
            String resultsPath_aux = f.getCanonicalPath();

            BufferedWriter out = new BufferedWriter(new FileWriter(resultsPath_aux + File.separator
                    + _bashFileName, true));
            out.write(_gnuplotCommand + " \"" + _bdFileName + "_" + _graphType + ".plt" + "\"");

            // out.write(_gnuplotCommand + " \"" + _configFilePath + "\"");

            out.newLine();
            out.close();

        } catch (IOException ioe) {
            System.out.println("Cannot create bash file");
        }
    }

    /**
     * Runs the bash file under a Linux OS.
     */
    public void runBashFile() {

        try {
            Runtime.getRuntime().exec(". " + _bashFileName);
        } catch (IOException ioe) {
            System.out.println("Cannot execute bash file");
        }
    }

    /**
     * Adds to the configuration file the values defined according to the plot type
     * 
     * @throws IOException
     */
    private void writeFile() throws IOException {

        String pltPath;

        File f = new File(_resultsPath);
        String resultsPath_aux = f.getCanonicalPath();

        BufferedWriter out = new BufferedWriter(new FileWriter(resultsPath_aux + File.separator
                + _bdFileName + "_" + _graphType + ".plt", true));
        // _configFilePath = resultsPath_aux + File.separator + _bdFileName +
        // "_" +_graphType + ".plt";
        _configFilePath = _bdFileName + "_" + _graphType + ".plt";
        out.write("set terminal png");
        out.newLine();
        // pltPath = "set output " + "\"" + resultsPath_aux + File.separator +
        // _bdFileName + "_" + _graphType + ".png" + "\"";
        pltPath = "set output " + "\"" + _bdFileName + "_" + _graphType + ".png" + "\"";
        pltPath = pltPath.replaceAll(File.separator + File.separator, File.separator
                + File.separator + File.separator + File.separator);
        out.write(pltPath);
        out.newLine();
        out.write("set key bottom right");
        out.newLine();
        out.write(_gnuplotVariables.getXlabel());// varia
        out.newLine();
        out.write(_gnuplotVariables.getYlabel());// varia
        out.newLine();
        out.write("set title \"" + _dbSize + " Objects in DB\"");
        out.newLine();
        out.write(_gnuplotVariables.getXrange());// varia
        out.newLine();
        out.write("set yrange [ 0 : 1.1 ]");
        out.newLine();
        out.write("set mxtics 10");
        out.newLine();
        out.write("set mytics 10");
        out.newLine();
        out.write(_gnuplotVariables.getXtics());// varia
        out.newLine();
        out.write("set ytics 0.1");
        out.newLine();
        // pltPath = "f = " + "\""+ resultsPath_aux + File.separator +
        // "precision_recall_plot_cut_" + _threshold + ".txt" + "\"";
        pltPath = "f = " + "\"" + "precision_recall_plot_cut_" + _threshold + ".txt" + "\"";
        pltPath = pltPath.replaceAll(File.separator + File.separator, File.separator
                + File.separator + File.separator + File.separator);
        out.write(pltPath);
        out.newLine();
        out.write(_gnuplotVariables.getPlot());// varia

        out.close();
    }

    /**
     * Adjusted the number of pairs compared to the closest number that ends with zero
     * 
     * @param pComp The number of pairs compared
     * @return The adjusted value
     */
    public long adjustPairsCompared(long pComp) {

        long pc = pComp;
        String sSize = Long.toString(pComp);
        char c = sSize.charAt(sSize.length() - 1);

        while (c != '0') {
            pc++;
            sSize = Long.toString(pc);
            c = sSize.charAt(sSize.length() - 1);
        }

        return pc;
    }

    public class GnuplotVariables {

        private String _xlabel;
        private String _ylabel;
        private String _xrange;
        private String _xtics;
        private String _plot;

        public GnuplotVariables() {
            initVariables();
        }

        /**
         * Initializes the variables according to the plot type
         */
        public void initVariables() {

            if (_graphType.equals("PRSvsNP")) {
                _xlabel = "set xlabel \"Nr Pairs\"";
                _ylabel = "set ylabel \"Precision + Recall + Similarity\"";
                _xrange = "set xrange [ 0 : " + _pairsCompared + " ]";
                _xtics = "set xtics " + Long.toString(_pairsCompared / 10);
                _plot = "plot f using ($4):($1) title \"precision\" w l 1, "
                        + "f using ($4):($2) title \"recall\" w l 5, "
                        + "f using ($4):($3) title \"similarity\" w l 8";
            }

            if (_graphType.equals("PvsR")) {
                _xlabel = "set xlabel \"Recall\"";
                _ylabel = "set ylabel \"Precision\"";
                _xrange = "set xrange [ 0 : 1 ]";
                _xtics = "set xtics 0.1";
                _plot = "plot f using ($2):($1) notitle w l 1";
            }

            if (_graphType.equals("PRvsS")) {
                _xlabel = "set xlabel \"Similarity\"";
                _ylabel = "set ylabel \"Precision + Recall\"";
                _xrange = "set xrange [ " + _threshold + " : 1 ]";
                _xtics = "set xtics 0.1";
                _plot = "plot f using ($3):($1) title \"precision\" w l 1, "
                        + "f using ($3):($2) title \"recall\" w l 5";
            }

            if (_graphType.equals("PRSvsPP")) {
                _xlabel = "set xlabel \"% Pairs\"";
                _ylabel = "set ylabel \"Precision + Recall + Similarity\"";
                _xrange = "set xrange [ 0 : 100 ]";
                _xtics = "set xtics 10";
                _plot = "plot f using ($5):($1) title \"precision\" w l 1, "
                        + "f using ($5):($2) title \"recall\" w l 5, "
                        + "f using ($5):($3) title \"similarity\" w l 8";
            }
        }

        public String getXlabel() {
            return _xlabel;
        }

        public String getYlabel() {
            return _ylabel;
        }

        public String getXrange() {
            return _xrange;
        }

        public String getXtics() {
            return _xtics;
        }

        public String getPlot() {
            return _plot;
        }

    }

}
