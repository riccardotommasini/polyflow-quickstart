package examples;

import customoperators.CustomFilterContentFactory;
import customoperators.CustomTumblingWindow;
import graph.jena.datatypes.JenaGraphOrBindings;
import graph.jena.operatorsimpl.r2r.jena.FullQueryUnaryJena;
import graph.jena.operatorsimpl.r2s.RelationToStreamOpImpl;
import graph.jena.sds.SDSJena;
import graph.jena.stream.JenaBindingStream;
import graph.jena.stream.JenaStreamGenerator;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.compose.Union;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import shared.coordinators.ContinuousProgramImpl;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.Task;
import shared.querying.TaskImpl;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import shared.operatorsimpl.r2r.DAG.DAGImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomContent {

    /*
     * This example has the same structure as the CustomS2ROperator example, but here we change type of Content to show
     * how a different Content logic can significantly modify the application behaviour.
     * The Content is the component responsible for storing the elements that enter a window, and can be customized
     * through the use of a Factory object.
     *
     * More informations about the content are available in the CustomFilterContent file, inside the customoperators directory
     *
     * Thanks to the system being decoupled, in order to change the S2R example to fit our needs, we only had to change the Content Factory object.
     */
    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input graphs
        JenaStreamGenerator generator = new JenaStreamGenerator();
        // Define a single input stream
        DataStream<Graph> inputStreamColors = generator.getStream("http://test/stream1");
        // define an output stream
        JenaBindingStream outStream = new JenaBindingStream("out");


        /*------------Window Properties------------*/

        // Window properties (report, tick)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------Window Content------------*/

        //Entity that represents the window content. In particular, we create an instance that represents an empty content
        JenaGraphOrBindings emptyContent = new JenaGraphOrBindings(GraphFactory.createGraphMem());

        /*
         * Customization of the content object through the use of a Factory. Details are explained in the CustomFilterContent file
         * inside the customoperators directory.
         */
        ContentFactory<Graph, Graph, JenaGraphOrBindings> customFilterContentFactory = new CustomFilterContentFactory<>(
                //We store Graphs in the window, so no need to transform type I in a new type W
                (g) -> g,

                //Transform a Graph in a custom type R to perform the R2R computations
                (g) -> new JenaGraphOrBindings(g),

                //Define how to go from two R type to a new resulting R
                (r1, r2) -> new JenaGraphOrBindings(new Union(r1.getContent(), r2.getContent())),

                //Empty content (neutral element of the previous operator)
                emptyContent,

                //Predicate to filter elements of type I before they enter the window, here we just want to add a specific graph to our window
                (g)->g.contains(
                        NodeFactory.createURI("http://test/" + "S" + "4"),
                        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                        NodeFactory.createURI("http://test/" + "Black")
                )
        );


        /*------------S2R, R2R and R2S Operators------------*/

        /*
         * Use a custom implementation of the Stream To Relation Operator.
         * The source file can be found in the 'customoperators' directory, along with the details of each parameter.
         */

        StreamToRelationOperator<Graph, Graph, JenaGraphOrBindings> s2rOp_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow",
                        customFilterContentFactory,
                        report,
                        1000);

        //Define a simple Relation To Relation operator to extract every graph in the window
        RelationToRelationOperator<JenaGraphOrBindings> r2rOp1 = new FullQueryUnaryJena("SELECT * WHERE {GRAPH ?g {?s ?p ?o }}", Collections.singletonList(s2rOp_one.getName()), "partial_1");

        //Relation to Stream operator, used to transform the result of a query (type R) to a stream of output objects (type O)
        RelationToStreamOperator<JenaGraphOrBindings, Binding> r2sOp = new RelationToStreamOpImpl();


        /*------------Task definition------------*/

        //Define the Tasks, each of which represent a query
        Task<Graph, Graph, JenaGraphOrBindings, Binding> task = new TaskImpl<>();
        task = task.addS2ROperator(s2rOp_one, inputStreamColors)
                .addR2ROperator(r2rOp1)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSJena())
                .addTime(instance);
        task.initialize();


        List<DataStream<Graph>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStreamColors);

        List<DataStream<Binding>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        /*------------Continuous Program definition------------*/

        //Define the Continuous Program, which acts as the coordinator of the whole system
        ContinuousProgramImpl<Graph, Graph, JenaGraphOrBindings, Binding> cp = new ContinuousProgramImpl<>();
        cp.buildTask(task, inputStreams, outputStreams);


        /*------------Output Stream consumer------------*/

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }
}
