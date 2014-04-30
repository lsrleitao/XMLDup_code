// Classe usada na contagem de comparacoes efectuadas pela medida de similaridade de
// strings

package DuplicateDetection;

public final class Singleton {

    private static Singleton ref;
    private long comparisons = 0L;

    private Singleton() {

    }

    public static Singleton getSingletonObject() {
        if (ref == null)
            ref = new Singleton();
        return ref;
    }

    public void increaseComparisons() {
        comparisons++;
    }

    public long getComparisons() {
        return comparisons;
    }
    
    public void resetComparisons(){
        comparisons = 0;
    }
}
