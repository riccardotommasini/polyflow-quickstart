package graph.neo.stream.data;

import org.javatuples.Tuple;
import tech.tablesaw.api.Table;

public interface PGraph  {


    Node[] nodes();

    Edge[] edges();

    default long timestamp() {
        return System.currentTimeMillis();
    }

    void union(PGraph r2);

    interface Node {

        long id();

        String[] labels();

        String[] properties();

        Object property(String p);

    }

    interface Edge extends Node {

        String to();

        String from();

    }
}
