# RSP4J: step by step guide

In order to maximize your understanding of the system, we recommend to read our [Documentation](../rsp4j-documentation.md) before
diving in this step-by-step guide, as we will assume that you partially know the high level responsibility of each component described.

Even though the underlying architecture is fairly simple in its essence, we understand that it might take a while to get 
used to it and all of its components.\
This step by step guide will hopefully help you during your first approach with the library, by giving you a mental map
of the steps needed in order to properly create a functional application.\
To show as many capabilities as possible, and to be as generic as possible, we decided to not use one of the 
famous data models (Relational, Document, Graph..), but to use some custom data model built upon fruit.

The data model is fairly simple: 
- The atomic elements are fruits (Apple, Banana, Pear, Peach etc..)
- A set of fruit is represented by a FruitBasket (a container of fruit)
- Operations are defined on the FruitBasket type (filter out fruit, merge two baskets etc..)

If you prefer, you can see the single fruits as tuples and the FruitBasket as a Table, thus returning to the classic Realtional data model.

Let's begin!

### Main program
First, we define a [Main](./src/main/java/examples/FruitStepByStepGuide.java) class where we will instantiate and connect
all the components of the system.
```java
public class FruitStepByStepGuide {
    
    public static void main(String[] args) throws InterruptedException {

    }
}
```
As we define more components, we will update it step by step.

### Define the data types
As mentioned before, we decided to use our custom data types, thus the first step is to define them.\
First, we create an Abstract Class [Fruit](./src/main/java/customdatatypes/Fruit.java) to give a common ancestor to the various
types of fruit we will define later.
```java
public abstract class Fruit {

    private float weight;
    private String status;

    public Fruit(float weight, String status){
        this.weight = weight;
        this.status = status;
    }
    
    public float getWeight() {
        return weight;
    }

    public String getName() {
        return "Fruit";
    }

    public String getStatus() {
        return status;
    }
}
```
Each fruit will have a name, a weight and a status.\
For the sake of simplicity, we will show here only one type of fruit, but the structure of the class is
the same throughout all the fruits. Here's the [Apple](./src/main/java/customdatatypes/Apple.java):
```java
public class Apple extends Fruit {
    
    public Apple(float weight, String status) {
        super(weight, status);
    }

    @Override
    public String getName() {
        return "Apple";
    }

    @Override
    public String toString(){
        return "name: Apple, status: "+ getStatus() +", weight: "+  getWeight();
    }
}
```
\
Last but not least, we need to define our [FruitBasket](./src/main/java/customdatatypes/FruitBasket.java) type on which our
operators are defined (As mentioned before, you can see it as the table that holds the tuples in the relational model).
```java
public class FruitBasket implements Iterable<Fruit>{

    private List<Fruit> fruits = new ArrayList<>();

    public void addFruit(Fruit f){
        this.fruits.add(f);
    }

    public void addAll(FruitBasket basket){
        basket.forEach(f->fruits.add(f));
    }

    public int getSize(){
        return fruits.size();
    }

    @Override
    public Iterator<Fruit> iterator() {
        return fruits.iterator();
    }
}
```
As we will see later, the data type on which the operations are performed (the generic 'R') must implement the `Iterable` interface, 
this is because we will need to iterate on it in order to transform it back to an output stream of elements.

### Input and Output Streams
Now that our data types are defined, we can begin working on the source and sink of our system.\
The first element we need is a [FruitDataStream](./src/main/java/customdatatypes/FruitDataStream.java) object, which implements
the `DataStream` interface:
```java
public class FruitDataStream implements DataStream<Fruit> {

    List<Consumer<Fruit>> consumerList = new ArrayList<>();
    String name;

    public FruitDataStream(String name){
        this.name = name;
    }

    @Override
    public void addConsumer(Consumer<Fruit> windowAssigner) {
        this.consumerList.add(windowAssigner);
    }

    @Override
    public void put(Fruit fruit, long ts) {
        consumerList.forEach(c->c.notify(this, fruit, ts));
    }

    @Override
    public String getName() {
        return name;
    }
}
```
This class represents an input or output Data Stream: it has a name and a list of `Consumer` interested in it that will
be notified when a new element enters (or exits) the stream. For an input stream, the `Consumer` will be the `ContinuousProgram`.

The questions now is, how do we insert elements in the previously defined `DataStream`?\
In our examples, we use a custom [StreamGenerator](./src/main/java/stream/FruitStreamGenerator.java) class to create
input streams when requested, and add elements to each of them with a given time interval.\
Feel free to look at the generator and create your own custom implementation: for the Fruit example we generate
Fruits randomly, but usually it is better to read data from a file to avoid randomness.

We have enough components to start updating our main class:
```java
public class FruitStepByStepGuide {
    
    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        FruitStreamGenerator generator = new FruitStreamGenerator();

        // Define an input stream
        DataStream<Fruit> inputStreamFruit = generator.getStream("fruit_market_one");

        // define an output stream
        DataStream<Fruit> outStream = new FruitDataStream("fruit_consumer");
    }
}
```
Fancy, isn't it? We asked our Generator to create an Input Stream (so that the generator will know of its existence
and automatically insert elements in it), and we manually defined an Output Stream.

### Window Content
Again, we assume you already read our [Documentation](../rsp4j-documentation.md), but a small refresh never hurts.\
The `Content` is responsible for adding, storing and merging elements of a window. It is highly customizable and 
the inner logic can be as complex as we want, but for this example we decided to keep it simple and use the 
[Accumulator Content Factory](https://github.com/riccardotommasini/polyflow/blob/master/polyflow/src/main/java/shared/contentimpl/factories/AccumulatorContentFactory.java) defined in the [Yasper](https://github.com/riccardotommasini/polyflow/tree/master/polyflow) module.\
We will use some 'default components' that are provided by Yasper throughout this step-by-step guide, mainly because their logic
rarely changes and they fit all the use cases we have. As a reminder, RSP4J provides interfaces that aim at 
standardizing the components used when creating a stream processor engine, but does not provide any implementations. Yasper, instead, 
is a reference implementation of a stream processor created with RSP4J that shows how the various components can be instantiated starting from the interfaces.\
Back to our Window Content, this is how our main class looks like after creating the `AccumulatorContentFactory`:
```java
public class FruitStepByStepGuide {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        FruitStreamGenerator generator = new FruitStreamGenerator();

        // Define an input stream
        DataStream<Fruit> inputStreamFruit = generator.getStream("fruit_market_one");

        // define an output stream
        DataStream<Fruit> outStream = new FruitDataStream("fruit_consumer");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        FruitBasket emptyBasket = new FruitBasket();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<Fruit, Fruit, FruitBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if (basket_1.getSize() > basket_2.getSize()) {
                        basket_1.addAll(basket_2);
                        return basket_1;
                    } else {
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket
        );
    }
}
```
We defined an emptyBasket object to represent the identity element of our 'R' type (FruitBasket), which is returned in case the 
Content of our Window is empty (You can see it as an Empty Table in the Relational world).\
Then we created an `AccumulatorContentFactory` object that will be used to create `Content` objects for every window that opens.\
The notation is a bit heavy, so let's go step by step:
- First parameter of the constructor is a `Function<I, W>`, in this example I = W, so the function does essentially nothing.
- Second parameter is a `Function<W, R>`, which transforms an element in the window (a Fruit in this case) in the 'R' type (the FruitBasket). As you can see, it just creates a new FruitBasket and adds the Fruit to it, nothing fancy. Again, you can see it as a Table with only one Tuple inside.
- Third parameter is a `BiFunction<R, R, R>`, which takes as input two elements of type 'R' and returns a new element of the same type. This is the logic used to merge together all the FruitBaskets created by the previous function, to obtain a single FruitBasket that contains all the Fruits of the window.
- Last parameter is the empty basket defined above. Be careful not to modify this element in any way inside your `Content` class, since it is shared among every `Content`.
The logic of our `AccumulatorContent` (created by the Factory we defined) is not particularly complex, it just accumulates the events that enter the window.
### Stream To Relation Operator
Alright alright, we defined the Content of a Window, but what about the Window itself?\
The `StreamToRelationOperator` is what we need to define next:\
first, we define the window properties such as `Report`, then we create a `Time` instance to integrate the concept of time in our system.
```java
/*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);
```
We are now ready to define the S2R operator: we will use the [CustomTumblingWindow](./src/main/java/customoperators/CustomTumblingWindow.java)
implementation defined in this quickstart guide. The implementation is pretty straight-forward, there is only a single active window at the time 
since we do not support late arrivals yet.
```java
//Define the Stream to Relation operator (blueprint of the windows)
        StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow",
                        accumulatorContentFactory,
                        report,
                        1000);
```
The first parameter is the `Time` object we defined before, followed by the name of the window (you can choose the name you like). We then 
have the `ContentFactory` previously defined, along with the `Report` strategy and the width of the window.\
A this point, your main class should look like this: 
```java
public class FruitStepByStepGuide {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        FruitStreamGenerator generator = new FruitStreamGenerator();

        // Define an input stream
        DataStream<Fruit> inputStreamFruit = generator.getStream("fruit_market_one");

        // define an output stream
        DataStream<Fruit> outStream = new FruitDataStream("fruit_consumer");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        FruitBasket emptyBasket = new FruitBasket();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<Fruit, Fruit, FruitBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if (basket_1.getSize() > basket_2.getSize()) {
                        basket_1.addAll(basket_2);
                        return basket_1;
                    } else {
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket
        );


        /*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operator (blueprint of the windows)
        StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow",
                        accumulatorContentFactory,
                        report,
                        1000);
    }
}
```
Let's summarize a bit what we've done up until this point:
- We created an input stream that receives elements from an external generator and forwards them to its consumers (not defined yet).
- We created an output stream that will send our elements to the interested consumers when a result is present.
- We created a `ContentFactory` object which is used to create a `Content` for each window that we have. The `Content` will hold the elements inside the window (Fruits) and aggregate them in a FruitBasket type when a computation is needed.
- We then defined a `StreamToRelationOperator` along with its properties, which will be responsible for opening and closing windows as needed, as well as to check if a computation needs to be performed (thanks to the `Report` object).

### Relation To Relation Operator
Time to define the component that will perform the 'query' in our system: `RelationToRelationOperator`.\
We will start by defining a simple one, more complexity will be added at the end of this guide.\
Our first operator is the [FilterFruitByRipeOp](./src/main/java/customoperators/FilterFruitByRipeOp.java) which, as the name suggests, 
we will use to filter out fruits based on their maturity level.\
As mentioned multiple times already, the operator will act on a single 'R' type (FruitBasket) and will return a new object with the same type.
```java
public class FilterFruitByRipeOp implements RelationToRelationOperator<FruitBasket> {

    // Name of the operands (one operand in this case)
    List<String> tvgNames;
    //Name of the result
    String resName;
    //Attribute to filter out
    String query;

    public FilterFruitByRipeOp(String query, List<String> tvgNames, String resName){
        this.query = query;
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public FruitBasket eval(List<FruitBasket> datasets) {
        FruitBasket op = datasets.get(0);
        FruitBasket res = new FruitBasket();
        //Add only the fruits with a status different from the one passed to the query
        for(Fruit fruit : op){
            if(!fruit.getStatus().equals(query))
                res.addFruit(fruit);
        }
        return res;
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}
```
Starting by the attributes:
- `List<String> tvgNames` contains the names of the operands on which the operator will be applied. Since the operator is unary, it will just contain a single name.
- `String resName` contains the name of the result of this operator.
- `String query` contains the maturity level we want to filter out ("underripe", "ripe", "overripe").
The `tvgNames` and `resName` are used to chain multiple operators with each other, using the names to identify which operator should be linked with which. For example, if we want to attach a projection operator or another filter
after the current one, we could do so by adding the `resName` of this operator to the `tvgNames` of the next one, effectively linking them together.

The only method worth discussing is `eval`: it takes as input a List of operands (the system only supports unary and binary operators right now), 
and applies the defined operation to them, returning a new 'R' type as the result of the computation. In this case, we remove the fruit with the 
maturity level defined in the `query` attribute, and add the rest of the fruit to a new basket, returning it. We effectively created a Filter Operator.

### Relation To Stream Operator
This operator does not provide any interesting logic in this example, it just loops through a type 'R' (the reason why FruitBasket implements Iterable)
and outputs a stream of elements of type 'O' (still Fruit in this example). You can find the logic in the [RelationToStreamOp](https://github.com/riccardotommasini/polyflow/blob/master/api/src/main/java/org/streamreasoning/rsp4j/api/operators/r2s/RelationToStreamOperator.java) interface.

The main class should look like this at this point:
```java
public class FruitStepByStepGuide {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        FruitStreamGenerator generator = new FruitStreamGenerator();

        // Define an input stream
        DataStream<Fruit> inputStreamFruit = generator.getStream("fruit_market_one");

        // define an output stream
        DataStream<Fruit> outStream = new FruitDataStream("fruit_consumer");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        FruitBasket emptyBasket = new FruitBasket();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<Fruit, Fruit, FruitBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if (basket_1.getSize() > basket_2.getSize()) {
                        basket_1.addAll(basket_2);
                        return basket_1;
                    } else {
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket
        );


        /*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operator (blueprint of the windows)
        StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow",
                        accumulatorContentFactory,
                        report,
                        1000);

        //Define Relation to Relation operators and chain them together. Here we filter out fruits that are underripe
        RelationToRelationOperator<FruitBasket> r2r_filter_underripe = new FilterFruitByRipeOp("underripe", Collections.singletonList(fruit_s2r_one.getName()), "filtered_fruit");

        //Relation to Stream operator, take the final fruit basket and send out each fruit
        RelationToStreamOperator<FruitBasket, Fruit> r2sOp = new RelationToStreamFruitOp();
    }
}
```
### Task
Friendly reminder: a `Task` represents a query in our system, it has a set of S2R operators (representing windows on multiple streams), a set of R2R operators connected
together by a `DAG` (directed acyclic graph), and a R2S operator to output the result of the computation.\
Again, we use the [Task implementation](https://github.com/riccardotommasini/polyflow/blob/master/polyflow/src/main/java/shared/querying/TaskImpl.java) already provided in the [api](https://github.com/riccardotommasini/polyflow/tree/master/api) package
since it is generic enough and satisfies all our needs.\
Same goes for the [DAG](https://github.com/riccardotommasini/polyflow/blob/master/polyflow/src/main/java/shared/operatorsimpl/r2r/DAG/DAGImpl.java) and [SDS](https://github.com/riccardotommasini/polyflow/blob/master/polyflow/src/main/java/shared/sds/SDSDefault.java).
```java
Task<Fruit, Fruit, FruitBasket, Fruit> task = new TaskImpl<>();
task = task.addS2ROperator(fruit_s2r_one, inputStreamFruit)
            .addR2ROperator(r2r_filter_underripe)
            .addR2SOperator(r2sOp)
            .addDAG(new DAGImpl<>())
            .addSDS(new SDSDefault<>())
            .addTime(instance);
task.initialize();
```
As you can see, we are just composing the `Task` by adding the required components one by one, nothing too special:
- First we add all the S2R operators with the input stream they are interested in (S2R operators are windows over a stream)
- Then all the R2R and R2S operators
- Finally, the DAG, SDS and Time 

After composing the `task`, we call the `initialize` method to prepare it internally.
### Continuous Program
Final step, compose the [Continuous Program](https://github.com/riccardotommasini/polyflow/blob/master/polyflow/src/main/java/shared/coordinators/ContinuousProgramImpl.java) object, which is the coordinator of the whole system (forwards events to the interested tasks and manages them).\
First, we create the object:
```java
ContinuousProgram<Fruit, Fruit, FruitBasket, Fruit> cp = new ContinuousProgram<>();
```
Then, we need to create a list of input streams and a list of output streams for each Task we have. Since we have only one Task, and the Task 
is only interested in a single input stream and a single output stream, we will have two lists, each of which with a single object.
```java
List<DataStream<Fruit>> inputStreams = new ArrayList<>();
inputStreams.add(inputStreamFruit);

List<DataStream<Fruit>> outputStreams = new ArrayList<>();
outputStreams.add(outStream);
```
Finally, we tell the Continuous Program which Task is interested to which DataStreams (input and output):
```java
cp.buildTask(task, inputStreams, outputStreams);
```

We are officially done, we just need to add a `Consumer` for the output stream and start the generator:
```java
outStream.addConsumer((out, el, ts) -> System.out.println("Output Element: ["+el+ "]" + " @ " + ts));

generator.startStreaming();
Thread.sleep(20_000);
generator.stopStreaming();
```
The output stream consumer is just a functional interface whose method `notify` will print the received element to the console.

And this is our final [main class](./src/main/java/examples/FruitStepByStepGuide.java):
```java
public class FruitStepByStepGuide {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        FruitStreamGenerator generator = new FruitStreamGenerator();

        // Define an input stream
        DataStream<Fruit> inputStreamFruit = generator.getStream("fruit_market_one");

        // define an output stream
        DataStream<Fruit> outStream = new FruitDataStream("fruit_consumer");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        FruitBasket emptyBasket = new FruitBasket();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<Fruit, Fruit, FruitBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if(basket_1.getSize()>basket_2.getSize()){
                        basket_1.addAll(basket_2);
                        return basket_1;
                    }
                    else{
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket
        );


        /*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operator (blueprint of the windows)
        StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow",
                        accumulatorContentFactory,
                        report,
                        1000);

        //Define Relation to Relation operators and chain them together. Here we filter out fruits that are underripe
        RelationToRelationOperator<FruitBasket> r2r_filter_underripe = new FilterFruitByRipeOp("underripe", Collections.singletonList(fruit_s2r_one.getName()), "filtered_fruit");

        //Relation to Stream operator, take the final fruit basket and send out each fruit
        RelationToStreamOperator<FruitBasket, Fruit> r2sOp = new RelationToStreamFruitOp();


        /*------------Task definition------------*/

        //Define the Tasks, each of which represent a query
        Task<Fruit, Fruit, FruitBasket, Fruit> task = new TaskImpl<>();
        task = task.addS2ROperator(fruit_s2r_one, inputStreamFruit)
                .addR2ROperator(r2r_filter_underripe)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSDefault<>())
                .addTime(instance);
        task.initialize();


        

        /*------------Continuous Program definition------------*/

        //Define the Continuous Program, which acts as the coordinator of the whole system
        ContinuousProgram<Fruit, Fruit, FruitBasket, Fruit> cp = new ContinuousProgram<>();

        List<DataStream<Fruit>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStreamFruit);

        List<DataStream<Fruit>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);
        
        
        cp.buildTask(task, inputStreams, outputStreams);


        /*------------Output Stream consumer------------*/

        outStream.addConsumer((out, el, ts) -> System.out.println("Output Element: ["+el+ "]" + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }



}
```
Our step-by-step guide is done, but we have one last section to show some more advanced features:
- Multiple input streams
- Multiple S2R operators
- A binary R2R operator
- A custom content that filters element before they are inserted in the window

### Advanced features

The code can be found in the [Fruit Advanced Guide](./src/main/java/examples/FruitAdvancedGuide.java).\
First, we create another input stream object:
```java
 // Define the two input streams
DataStream<Fruit> inputStreamFruit_one = generator.getStream("fruit_market_one");
DataStream<Fruit> inputStreamFruit_two = generator.getStream("fruit_market_two");
```
Then, we create a new `ContentFactory` object, this time using the [Filter Content Factory](./src/main/java/customoperators/CustomFilterContentFactory.java).
This type of `Content` allows us to filter elements before they are added to the window instead of using a Filter in the R2R computation, thus saving memory.
```java
 ContentFactory<Fruit, Fruit, FruitBasket> filterContentFactory = new CustomFilterContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if(basket_1.getSize()>basket_2.getSize()){
                        basket_1.addAll(basket_2);
                        return basket_1;
                    }
                    else{
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket,
                (fruit)->fruit.getWeight()>2
        );
```
The only thing that changed with respect to the Accumulator Content is the last parameter of the constructor, which is the predicate
that will be checked before adding an element to the internal data structure. In this case, we keep only the fruit that has a weight bigger than 2.

Next, we need to define a second S2R operator that acts as a window over the second input stream we defined, and that uses the ContentFactory we just defined:
```java
StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_two =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_two",
                        filterContentFactory,
                        report,
                        1000);
```
Remember to give different names to the different S2R you define.

We can now define a new R2R operator that will merge two FruitBasket objects together (a sort of join):
```java
RelationToRelationOperator<FruitBasket> r2r_filter_underripe = new FilterFruitByRipeOp("underripe", Collections.singletonList(fruit_s2r_one.getName()), "filtered_fruit");
RelationToRelationOperator<FruitBasket> r2r_join = new JoinFruitBasketOp(List.of("filtered_fruit", fruit_s2r_two.getName()), "joined_fruit");
```
Notice how the name of the parameters in the constructor match between the operators: the name of the result of the first operator (`"filtered_fruit"`)
appears as a parameter in the operands of the second R2R operator, alongside the name of the second S2R operator. Indeed,
our join operator will consume from the result of the filter R2R operator (that acts on the windows of the first stream) and 
on the windows of the second stream. Basically, we "join" the windows of second stream with the result of the filter on the windows of the first stream.
```
(FIRST_STREAM)->[R2R_FILTER]
                             \
                              [JOIN] --> (RESULT)
                             /  
             (SECOND_STREAM)
```
At this point, we need to add the new components to the `Task`:
```java
Task<Fruit, Fruit, FruitBasket, Fruit> task = new TaskImpl<>();
task = task.addS2ROperator(fruit_s2r_one, inputStreamFruit_one)
            .addS2ROperator(fruit_s2r_two, inputStreamFruit_two)
            .addR2ROperator(r2r_filter_underripe)
            .addR2ROperator(r2r_join)
            .addR2SOperator(r2sOp)
            .addDAG(new DAGImpl<>())
            .addSDS(new SDSDefault<>())
            .addTime(instance);
task.initialize();
```
It's *necessary* that the R2R operators are added in order based on their dependency (topological order).

The last step is to tell the Continuous Program the streams from which the Task will consume:
```java
 List<DataStream<Fruit>> inputStreams = new ArrayList<>();
inputStreams.add(inputStreamFruit_one);
inputStreams.add(inputStreamFruit_two);
```
And finally, the rest of the code:
```java
List<DataStream<Fruit>> outputStreams = new ArrayList<>();
outputStreams.add(outStream);

cp.buildTask(task, inputStreams, outputStreams);

/*------------Output Stream consumer------------*/

outStream.addConsumer((out, el, ts) -> System.out.println("Output Element: ["+el+ "]" + " @ " + ts));

generator.startStreaming();
Thread.sleep(20_000);
generator.stopStreaming();
```

The final code will look like this: 
```java
public class FruitAdvancedGuide {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        FruitStreamGenerator generator = new FruitStreamGenerator();

        // Define the two input streams
        DataStream<Fruit> inputStreamFruit_one = generator.getStream("fruit_market_one");
        DataStream<Fruit> inputStreamFruit_two = generator.getStream("fruit_market_two");

        // define an output stream
        DataStream<Fruit> outStream = new FruitDataStream("fruit_consumer");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        FruitBasket emptyBasket = new FruitBasket();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<Fruit, Fruit, FruitBasket> filterContentFactory = new CustomFilterContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if(basket_1.getSize()>basket_2.getSize()){
                        basket_1.addAll(basket_2);
                        return basket_1;
                    }
                    else{
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket,
                (fruit)->fruit.getWeight()>2
        );

        ContentFactory<Fruit, Fruit, FruitBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (fruit) -> fruit,
                (fruit) -> {
                    FruitBasket fb = new FruitBasket();
                    fb.addFruit(fruit);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if(basket_1.getSize()>basket_2.getSize()){
                        basket_1.addAll(basket_2);
                        return basket_1;
                    }
                    else{
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket
        );


        /*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operators (blueprint of the windows)
        StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_one",
                        accumulatorContentFactory,
                        report,
                        1000);

        StreamToRelationOperator<Fruit, Fruit, FruitBasket> fruit_s2r_two =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_two",
                        filterContentFactory,
                        report,
                        1000);

        //Define Relation to Relation operators and chain them together
        RelationToRelationOperator<FruitBasket> r2r_filter_underripe = new FilterFruitByRipeOp("underripe", Collections.singletonList(fruit_s2r_one.getName()), "filtered_fruit");
        RelationToRelationOperator<FruitBasket> r2r_join = new JoinFruitBasketOp(List.of("filtered_fruit", fruit_s2r_two.getName()), "joined_fruit");

        //Relation to Stream operator, take the final fruit basket and send out each fruit
        RelationToStreamOperator<FruitBasket, Fruit> r2sOp = new RelationToStreamFruitOp();


        /*------------Task definition------------*/

        //Define the Tasks, each of which represent a query
        Task<Fruit, Fruit, FruitBasket, Fruit> task = new TaskImpl<>();
        task = task.addS2ROperator(fruit_s2r_one, inputStreamFruit_one)
                .addS2ROperator(fruit_s2r_two, inputStreamFruit_two)
                .addR2ROperator(r2r_filter_underripe)
                .addR2ROperator(r2r_join)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSDefault<>())
                .addTime(instance);
        task.initialize();




        /*------------Continuous Program definition------------*/

        //Define the Continuous Program, which acts as the coordinator of the whole system
        ContinuousProgram<Fruit, Fruit, FruitBasket, Fruit> cp = new ContinuousProgram<>();

        List<DataStream<Fruit>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStreamFruit_one);
        inputStreams.add(inputStreamFruit_two);

        List<DataStream<Fruit>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);


        cp.buildTask(task, inputStreams, outputStreams);


        /*------------Output Stream consumer------------*/

        outStream.addConsumer((out, el, ts) -> System.out.println("Output Element: ["+el+ "]" + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }

}
```