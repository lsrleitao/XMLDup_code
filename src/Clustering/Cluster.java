package Clustering;

import java.util.ArrayList;
import java.util.List;

import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;

public class Cluster {

    private int _clusterID;
    private ClusterObject _centroid;
    private List<ClusterObject> _clusterObjects = new ArrayList<ClusterObject>();
    private double _centroidSOD = Double.MAX_VALUE;
    private boolean _centroidIsStable = false;

    public Cluster(String initCentroid, int ocurrences, int clusterID) {
        this._centroid = new ClusterObject(initCentroid, 0, ocurrences);
        this._clusterID = clusterID;
    }

    public void addObject(ClusterObject co) {
        _clusterObjects.add(co);
    }

    public void resetCluster() {
        this._clusterObjects = new ArrayList<ClusterObject>();
        this._centroidSOD = 0;
    }

    public void updateInitialCentroidSOD(double distance) {
        _centroidSOD = +distance;
    }

    public ClusterObject getCentroid() {
        return _centroid;
    }

    public List<ClusterObject> getClusterObjects() {
        return _clusterObjects;
    }

    public void recalculateCentroid() {

        double sod = 0;
        double newCentroidSOD = _centroidSOD;
        String obj1;
        String obj2;
        int swapIndex = -1;

        int size = _clusterObjects.size();
        for (int i = 0; size > i; i++) {

            obj1 = _clusterObjects.get(i).getObject();

            for (int j = 0; size > j; j++) {

                if (i == j)
                    continue;

                obj2 = _clusterObjects.get(j).getObject();

                sod = +editDistance(obj1, obj2);
            }

            sod = +editDistance(obj1, _centroid.getObject());

            if (sod < newCentroidSOD) {
                newCentroidSOD = sod;
                swapIndex = i;
            }

        }

        if (swapIndex == -1) {
            _centroidIsStable = true;
            // System.out.println("centroid stable");
        } else {// System.out.println("new centroid: " +
            // _clusterObjects.get(swapIndex).getObject());
            _centroidSOD = newCentroidSOD;
            swapCentroid(swapIndex);
        }
        // System.out.println("centroid size: " + _clusterObjects.size());
    }

    private void swapCentroid(int index) {

        double distance = editDistance(_centroid.getObject(), _clusterObjects.get(index)
                .getObject());
        ClusterObject tmp = _centroid;

        _centroid = _clusterObjects.get(index);
        _centroid.setToCentroid();
        _clusterObjects.remove(index);
        tmp.setDistanceToCentroid(distance);
        _clusterObjects.add(tmp);

    }

    public boolean isCentroidStable() {
        return _centroidIsStable;
    }

    private double editDistanceFromDB(String str1, String str2) {

        String s1;
        String s2;
        double res = -1;

        if (str1.compareTo(str2) > 0) {
            s1 = str1;
            s2 = str2;
        } else {
            s1 = str2;
            s2 = str1;
        }

        res = KMeans._db.getDistance(s1, s2);

        if (res == -1) {

            Levenshtein lvn = new Levenshtein();
            res = lvn.getSimilarity(str1, str2);
            KMeans._db.insertDistance(str1, str2, res);
        }

        return res;

    }

    private double editDistance(String str1, String str2) {
        Levenshtein lvn = new Levenshtein();

        int s1 = str1.length();
        int s2 = str2.length();

        if (s1 > s2)
            return lvn.getUnNormalisedSimilarity(str1, str2) / s1;
        else
            return lvn.getUnNormalisedSimilarity(str1, str2) / s2;
    }

    public double getDistanceToCentroid(String str) {

        return editDistance(str, _centroid.getObject());

    }

    public int getClusterID() {
        return this._clusterID;
    }

}
