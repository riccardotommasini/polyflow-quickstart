package graph.jena.stream;

import graph.jena.operatorsimpl.r2r.jena.FullQueryUnaryJena;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.jena.graph.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.mem.DatasetGraphInMemory;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import relational.stream.RowStreamGenerator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public class JenaStreamGenerator {
    private static final String PREFIX = "http://test/";
    private static final Long TIMEOUT = 1000l;
    private final String[] colors = new String[]{"Blue", "Green", "Red", "Yellow", "Black", "Grey", "White"};
    private final Map<String, DataStream<Graph>> activeStreams;
    private final AtomicBoolean isStreaming;
    private final Random randomGenerator;
    private AtomicLong streamIndexCounter;



    public JenaStreamGenerator() {
        this.streamIndexCounter = new AtomicLong(0);
        this.activeStreams = new HashMap<String, DataStream<Graph>>();
        this.isStreaming = new AtomicBoolean(false);
        randomGenerator = new Random(1336);
    }

    public DataStream<Graph> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            JenaRDFStream stream = new JenaRDFStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming.get()) {
                    long finalTs = ts;
                    activeStreams.entrySet().forEach(e -> generateDataAndAddToStream(e.getValue(), finalTs));
                    ts += 400;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            };


            Thread thread = new Thread(task);
            thread.start();
        }
    }

    private void generateDataAndAddToStream(DataStream<Graph> stream, long ts) {
        Graph graph = GraphMemFactory.createGraphMem();

        Node p = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        if(stream.getName().equals("http://test/stream1")) {
            graph.add(NodeFactory.createURI(PREFIX + "S" + streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI(PREFIX + selectRandomColor()));
            graph.add(NodeFactory.createURI(PREFIX + "S" + streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI(PREFIX + "Black"));
            stream.put(graph, ts);
        }
        else if(stream.getName().equals("http://test/stream2")){
            graph.add(NodeFactory.createURI(PREFIX + "S" + streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI(PREFIX + randomGenerator.nextInt(10)));
            graph.add(NodeFactory.createURI(PREFIX + "S" + streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI(PREFIX + "0"));
            stream.put(graph, ts);
        }


    }

    private String selectRandomColor() {
        int randomIndex = randomGenerator.nextInt((colors.length));
        return colors[randomIndex];
    }

    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}
