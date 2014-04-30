package Clustering;

import java.util.ArrayList;
import java.util.List;

import RDB.DataBase;

public class KMeans {

    private int _clusterID = 1;
    private int _K;
    public static DataBase _db;
    private String _attribute;
    private List<Cluster> _clusters = new ArrayList<Cluster>();
    private List<List<String>> _states = new ArrayList<List<String>>();
    private List<ClusterObject> _clusterObjectsUnassigned = new ArrayList<ClusterObject>();
    private double _clusteringThreshold;
    private boolean stop = false;
    private List<ClusterObject> _stringSet;
    private double _dbSize;

    public KMeans(DataBase db, int k, String attribute, double clusteringThreshold) {
        _db = db;
        this._K = k;
        this._attribute = attribute;
        this._stringSet = getClusterObjects();
        this._clusteringThreshold = 1 - clusteringThreshold;
        this._dbSize = db.getAppearences(attribute);
    }

    private List<ClusterObject> getClusterObjects() {
        List<List<Object>> lst = _db.getStringsIntoMemory(_attribute);
        List<ClusterObject> res = new ArrayList<ClusterObject>();

        int size = lst.size();
        for (int i = 0; size > i; i++) {
            res.add(new ClusterObject((String) lst.get(i).get(0), 0, (Integer) lst.get(i).get(1)));
        }

        return res;
    }

    public void clusterData() {

        int segment = 10;
        List<ClusterObject> objs = _stringSet;

        int size = objs.size();
        while (size > 0) {

            objs = runAlgorithm(objs);
            size = objs.size();
            System.out.println("Remaining Objects: " + size);
            if (size <= segment)
                setK(1);
            else
                setK(size / segment);

            resetKMeans();
        }
    }

    private List<ClusterObject> runAlgorithm(List<ClusterObject> objs) {

        initClusters(objs);
        List<ClusterObject> stringSet = extractCentroids(objs);
        System.out.println("Initial Object distribution...");
        initialObjectDistribution(stringSet);
        System.out.println("Initial Centroid Definition...");
        defineNewCentroids();

        System.out.println("Iterative Phase");
        int iteration = 0;
        while (!stop) {
            System.out.println("Iteration: " + iteration);
            System.out.println("Object Redistribution...");
            objectRedistribution();
            System.out.println("Defining New Centroids...");
            defineNewCentroids();
            iteration++;
        }

        System.out.println("Transfering Data To DB...");
        loadClustersToDB();

        return this._clusterObjectsUnassigned;
    }

    public void resetKMeans() {
        _clusters = new ArrayList<Cluster>();
        _states = new ArrayList<List<String>>();
        _clusterObjectsUnassigned = new ArrayList<ClusterObject>();
        stop = false;
    }

    private List<ClusterObject> extractCentroids(List<ClusterObject> stringSet) {

        for (int i = 0; i < _K; i++) {
            stringSet.remove(0);
        }

        return stringSet;
    }

    private void loadClustersToDB() {

        List<List<Object>> lst = new ArrayList<List<Object>>();
        List<Object> v;

        int size = _clusters.size();
        for (int i = 0; size > i; i++) {

            Cluster cl = _clusters.get(i);

            v = new ArrayList<Object>();
            v.add(_attribute);
            v.add(cl.getCentroid().getObject());
            v.add(cl.getCentroid().getDistanceToCentroid());
            v.add(cl.getCentroid().getOcurrences());
            v.add(cl.getClusterID());
            lst.add(v);

            int size2 = cl.getClusterObjects().size();
            for (int j = 0; size2 > j; j++) {

                ClusterObject co = cl.getClusterObjects().get(j);

                v = new ArrayList<Object>();
                v.add(_attribute);
                v.add(co.getObject());
                v.add(co.getDistanceToCentroid());
                v.add(co.getOcurrences());
                v.add(cl.getClusterID());
                lst.add(v);
            }
        }

        _db.insertClusterData(lst);
    }

    private void objectRedistribution() {

        List<ClusterObject> lst = getClusterObjectUnion();
        resetUnassignedObjects();
        resetClusters();

        int size = lst.size();
        for (int i = 0; size > i; i++) {
            assignObjectToCluster(lst.get(i));
        }
    }

    private void resetClusters() {
        int size = _clusters.size();
        for (int i = 0; size > i; i++)
            _clusters.get(i).resetCluster();
    }

    private void resetUnassignedObjects() {
        this._clusterObjectsUnassigned = new ArrayList<ClusterObject>();
    }

    private List<ClusterObject> getClusterObjectUnion() {

        List<ClusterObject> lst = new ArrayList<ClusterObject>();
        lst = merge(lst, this._clusterObjectsUnassigned);

        int size = _clusters.size();
        for (int i = 0; size > i; i++) {
            lst = merge(lst, _clusters.get(i).getClusterObjects());
        }

        return lst;
    }

    private List<ClusterObject> merge(List<ClusterObject> lst1, List<ClusterObject> lst2) {

        int size = lst2.size();
        for (int i = 0; size > i; i++) {
            lst1.add(lst2.get(i));
        }

        return lst1;
    }

    private void initialObjectDistribution(List<ClusterObject> stringSet) {

        int size = stringSet.size();
        for (int i = 0; size > i; i++)
            assignObjectToCluster(stringSet.get(i));
    }

    private void defineNewCentroids() {

        int size = _clusters.size();
        for (int k = 0; size > k; k++)
            _clusters.get(k).recalculateCentroid();

        List<String> lst = new ArrayList<String>();

        for (int m = 0; size > m; m++)
            lst.add(_clusters.get(m).getCentroid().getObject());

        if (clusteringIsStable(lst))
            stop = true;
        else
            _states.add(lst);
    }

    private boolean clusteringIsStable(List<String> lst) {

        boolean equals;

        int size = _states.size();
        for (int m = 0; size > m; m++) {
            equals = true;

            List<String> l = _states.get(m);

            int size2 = l.size();
            for (int k = 0; size2 > k; k++) {
                if (!lst.get(k).equals(l.get(k))) {
                    equals = false;
                    break;
                }
            }
            if (equals)
                return true;
        }
        return false;
    }

    private void assignObjectToCluster(ClusterObject co) {

        double updateDistance = Double.MAX_VALUE;
        double distance;
        int index = -1;

        int size = _clusters.size();
        for (int i = 0; size > i; i++) {

            distance = _clusters.get(i).getDistanceToCentroid(co.getObject());

            if (distance < updateDistance) {
                index = i;
                updateDistance = distance;
            }
        }

        if (updateDistance <= _clusteringThreshold) {
            co.setDistanceToCentroid(updateDistance);
            _clusters.get(index).addObject(co);
            _clusters.get(index).updateInitialCentroidSOD(updateDistance);
        } else
            addUnassignedClusterObjects(co);
        // System.out.println(co.getObject() + " colocado no cluster: " +
        // index);
    }

    private void addUnassignedClusterObjects(ClusterObject co) {
        int size = _clusterObjectsUnassigned.size();
        for (int i = 0; size > i; i++) {
            if (co.getOcurrences() >= _clusterObjectsUnassigned.get(i).getOcurrences()) {
                _clusterObjectsUnassigned.add(i, co);
                return;
            }
        }
        _clusterObjectsUnassigned.add(co);
    }

    private void initClusters(List<ClusterObject> objs) {

        for (int i = 0; i < _K; i++) {
            _clusters.add(new Cluster(objs.get(i).getObject(), objs.get(i).getOcurrences(),
                    _clusterID));
            _clusterID++;
        }
    }

    public void setK(int k) {
        this._K = k;
    }

    public double getEmptyProbability(String attribute) {
        return _db.getEmptyObjectProbability(attribute, _dbSize);
    }

    public double getEqualProbability(String attribute) {
        return _db.getSameObjectProbability(attribute, _dbSize);
    }
}
