package Clustering;

public class ClusterObject {

    private String _object;
    private int _ocurrences;
    private double _distanceToCentroid;

    public ClusterObject(String object, double distanceToCentroid, int ocurrences) {
        this._object = object;
        this._distanceToCentroid = distanceToCentroid;
        this._ocurrences = ocurrences;
    }

    public String getObject() {
        return this._object;
    }

    public int getOcurrences() {
        return this._ocurrences;
    }

    public double getDistanceToCentroid() {
        return this._distanceToCentroid;
    }

    public void increaseOcurrences() {
        this._ocurrences++;
    }

    public void setToCentroid() {
        this._distanceToCentroid = 0;
    }

    public void setDistanceToCentroid(double d) {
        this._distanceToCentroid = d;
    }
}
