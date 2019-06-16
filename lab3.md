# 1.实验目的

掌握大图数据计算平台的原理、架构和工作机制，理解大图计算平台的各项功能，包括数据载入、大图数据划分和大图计算。能够编写大图数据计算平台中的常用图分析法（PageRank，SSSP）程序。

# 2.实验环境

Windows操作系统，JavaSE-1.8

# 3.整体架构

## **3.1 Vertex<V, E>**

V，E是泛型标识，分别表示节点保存数据的类型和边保存数据的类型。Vertex类包含三个属性以及几个主要方法，如下：

~~~java
    private V value;
    private int id;
    private boolean active;

    public abstract void compute(Queue<Message> msg);
    public abstract Map<Integer, Message> getMessage(List<Edge<E>> edges);
    public void voteToHalt();
    public void setActive();

~~~

其中，value表示顶点的值，id表示顶点的编号，active表示当前节点是否活跃。上面的代码中展示了除了get和set方法外的主要方法。

compute方法是一个抽象方法，传入的参数是当前节点的接受消息队列，对于不同的需求对其要有不同的实现，表示一个active的节点在接收到消息在一轮BSP中要执行的动作；getMessage也是一个抽象方法，用于获得当前节点要传给其他节点的信息。传入参数的是当前节点对应的所有出边（一般情况下信息都是沿着有向边传输的），返回值是一个Map，其中key值表示要发送至的节点的id，value为要发送给这个节点的信息；voteToHalt方法和setActive方法都是对active值进行改变，分别将avtive值设置为false和ture，用于控制BSP模型执行过程中所有节点的状态。

## **3.2 Worker<V,E>**

V，E是泛型标识，分别表示节点保存数据的类型和边保存数据的类型。Worker类的结构如下所示：

~~~java
    int id;
    private boolean use_combiner = false;
    private Combiner combiner;
    List<Vertex<V,E>> vertexes;
    Map<Integer, List<Edge<E>>> edges;
    Map<Integer, Queue<Message>> last_msg;
    Map<Integer, Queue<Message>> current_msg;
    Map<Integer, Queue<Message>> send_queue;
    List<Long> use_time;
    List<Integer> message_send_num;

    public abstract void init();
    public abstract void load();
    public void run(Master<V,E> master);
    public void finishABSP();
    public boolean isDone();
    public void useCombiner(Combiner combiner);
~~~

其中，id用于表示这个worker的标号；use_combiner变量用于标识是否使用combiner方法，默认为关闭；vertexex变量是一个链表，表示这个worker包含的所有节点；edges变量是一个Map，表示这个worker包含的所有边，其中key为点标号，value是一个Edge的链表，Edge没在这里显示，它是一个内部类，包含两个元素，分别是边上的值和边指向的顶点；last_msg是一个Map，用于存储上一轮BSP得到的这个worker所有节点的消息队列，key为节点标号，value为这个节点接收到的消息队列；current_msg表示这轮BSP结束后这个worker所有节点的消息队列；send_queue表示发送队列；use_time是一个链表，存储的是每一轮BSP所使用的时间；message_send_num是一个链表，存储每轮BSP这个worker信息交换的次数。

init方法是一个抽象方法，对于不同的应用，要有不同的初始化方式（如：初始化哪些节点为活跃节点，初始化消息队列等）；load方法也是一个抽象方法，用与将每个worker对应存储的数据加载到worker中；run方法是一轮BSP每个worker要执行的动作，遍历worker中所有的顶点，如果是顶点在接收队列中有消息或者顶点是活跃的，就执行点的compute方法，并且调用getMessage方法获得需要发送的消息，如果要发送信息的目标就在当前worker中，就直接存入本地的消息队列中即可，如果不再当前worker中，就需要调用master的findWorker方法来获得保存该顶点的worker并将信息存入消息队列中，并将通信数量加一。这是不使用combiner的一次执行，如果使用combiner具体实现也有些不同，具体在combiner中介绍；finishABSP方法是一轮BSP结束每个worker执行的动作，即将current_msg的值赋给last_msg并重新初始化current_msg；isDone方法用于判断当前worker的所有节点是否都不为active，是判断BSP结束的方法；useCombiner方法是使用combiner的调用，传入一个Combiner的实例，具体实现在Combiner中介绍。

## **3.3 Master<V,E>**

V，E是泛型标识，分别表示节点保存数据的类型和边保存数据的类型。Master类的结构如下所示：

~~~java
    List<Worker<V,E>> workers;
    boolean use_combiner = false;
    Combiner combiner;

    public abstract void run(int k);
    public boolean stop();
    public Worker<V,E> findWorker(int id);
    public void partition(String pathname, int k);
    public void useCombiner(Combiner combiner);
~~~

变量workers用链表来存储，表示master所包含的所有workers；use_combiner表示是否使用combiner，默认为false；combiner是使用的Combiner的具体实例，如何使用在后面介绍。

run方法是一个抽象方法，表示对于特定的应用master应该执行的动作；stop方法是用于判定是否需要停止BSP的执行，具体实现是调用每一个Worker的isDone方法，如果都返回true，这个方法也返回true，否则就返回false；findWorker方法在前面提到过，是用来通过点的id来得到它所在的Worker的，如果找不到就返回空指针NULL；partition是实现图划分并存入到磁盘的方法，因为给到的文件集合是边集，部分数据格式如下所示：

    11342   867923
    11342	891835
    824020	0
    824020	91807
    824020	322178
    824020	387543

第一个数据和第二个数据分别表示边的起点和终点，因此要保存点就需要去重。使用链表的查找效率很低，所以首先必须用HashSet来存储所有的点来加快查找效率。然后对于每一条新数据，首先判断其包含的顶点id是否已经存储过，如果存储过就跳过，只需存储该边，如果未存储过就生成一个0-k-1的随机数，将这个点分配到这个随机数对应的worker上。关于边的存储，对于一个边，将其存到存储它起点顶点的worker上即可；useCombiner方法是使用Combiner时要调用的方法，具体在后面介绍。

## **3.4 Combiner**

Combiner是一个接口类，其中只有一个combine方法，定义如下：

~~~java
    public Map<Integer, Message> combine(Map<Integer, Queue<Message>> input);
~~~

combine方法用于将对一个顶点传递的多个消息转变为一个消息，即合并。不同的应用需要合并的方式也就不相同。Combiner是在Worker层要实例化的对象，传入的是一个发送消息队列，即顶点id和顶点消息队列的Map，经过combine，将消息队列合并为一个消息，减少消息传递的大小。

Combiner的使用需要用户实例化这个接口类，并且通过Master和Worker的useCombiner方法将实例化的Combiner传递给Worker，并设置use_combiner为true。在Worker的run方法中，如果use_combiner为false，就按照上面的介绍执行；如果use_combiner为true，就需要做一定的修改，此时只需要将返回的一个Message给current_msg即可。

## **3.5 Aggregator<V,E,R>**

Aggregator是一个接口类，V,E,R是泛型表示，表示节点的数据类型，边的数据类型以及要得到的数据的返回类型。其中包含两个抽象方法，定义如下：

~~~java
    public void report(Vertex<V,E> v);
    public R aggregate();
~~~

Aggregator方法主要是用来获取所有节点结合的一个信息，如SUM, AVG, MIN, MAX等。其中report方法是从每个节点获取一个消息，传入的是一个节点，将其保存在具体实现的类中的Collection中；aggregate方法通过得到的Collection来获得一个聚集值。它的使用主要是是在master中的，获得一个全局的信息。

## **3.6 Statistics**

Statistics类实现的是统计功能。它的结构如下：

~~~java
    Master<Integer,Integer> master;

    public void getEdgeNum();
    public void getVertexNum();
    public void getBSPMessage();
~~~

master是一个Master<Integer,Integer>的实例，因此这个统计器只适用于SSSP。其中包含的getEdgeNum，getVertexNum，getBSPMessage方法都是通过调用master及其包含的Worker的属性来得到的。getEdgeNum方法是通过调用每个Worker对应的edges的size得到的，getVertexNum方法也能通过这个方法实现，但是值得一提的是这里我是使用Aggragator来实现的。VertexCountAggregator类就是实现的这个功能，它是Aggregator<Integer,Integer,Integer>接口类的一个实现，重写了report和aggragate方法，并且包含一个存储report结果的data：

~~~java
    List<Integer> data;
    @Override
    public void report(Vertex<Integer, Integer> v);
    @Override
    public Integer aggregate();
~~~

因为是统计顶点个数，report对于传入的顶点无需获取其中的信息，返回整数1即可，存入data中，然后aggragate方法对data里面所有的数据求和，就得到了顶点个数；对于getBSPMessage方法，需要的信息存储在Worker类中的use_time和message_send_num变量中，每轮BSP存储其使用的时间和交换信息的数量，然后getBSPMessage方法调用这些信息即可。

# **4.具体应用实现**

## **4.1 SSSP实现**

### **4.1.1 SSSPVertex**

SSSPVertex类是对Vertex<Integer,Integer>类的一个实现。所以要重写compute方法和getMessage方法：

~~~ java
    @Override
    public void compute(Queue<Message> msg);
    @Override
    public Map<Integer, Message> getMessage(List<Edge<Integer>> edges);
~~~

对于单源最短路径，compute函数的实现是：对于输入消息队列的所有值，找到其中的最小值，如果最小值小于自己原有的值，就用那个最小值替换；getMessage的实现时：对于输入参数中的所有自己的邻接边，将自己的值和边上的值求和，作为消息发送给边的目的顶点，保存在Map中返回。

### **4.1.2 SSSPWorker**

SSSPWorker类是对Worker<Integer,Integer>类的一个实现。所以要重写init和load方法。

~~~java
    @Override
    public void init();
    @Override
    public void load();
~~~

对单源最短路径而言，init方法除了初始化两个输入消息队列last_msg和current_msg外，还需要在last_msg中添加一个元素，即向id为0（最小编号的节点）发送一个值为0的消息，表示0节点设置为active并初始化其值为0。load方法是对每个worker对应数据的读入，对于单源最短路径来说，读入的每个顶点都要将其值设置为INF（无穷大），因为实验给的图数据不包含边的值，所以将边的值设置为1。

### **4.1.3 SSSPCombiner**

SSSPCombiner是对Combiner类的一个实现，所以要重写combine方法：

~~~java
    @Override
    public Map<Integer, Message> combine(Map<Integer, Queue<Message>> input);
~~~

单源最短路径的Combiner比较简单，对于输入的消息队列，遍历并找到最小值，然后将最小值重新构造一个Map并返回即可。

### **4.1.4 SSSP**

SSSP类是对Master<Integer,Integer>类的一个继承，所以要重写run函数，同时里面还有一个writeResult函数用于将结果写入磁盘，如下：

~~~java
    @Override
    public void run(int k);
    public void writeResult(String filepath);
~~~

对于单源最短路径，run方法首先要对所有的Worker进行初始化，从0-k-1初始化Worker并设置其id，然后调用load方法和init方法，并判断是否使用Combiner如果使用，还需要调用Worker的useCombiner方法。然后进入一个while(true)的循环，首先对于每个worker调用run方法，然后对于每个worker调用finishABSP方法，模拟一轮BSP的执行，并且最后调用stop函数，如果返回为true就停止BSP表示已经得到结果，除此，还在最后实例化了一个Statistics类，将**this**作为参数传递，并调用getEdgeNum()、getVertexNum()、st.getBSPMessage()方法，实现统计功能；对于writeResult方法，每一行输出一个点的单源最短路径，通过tap分割即可。

## **4.2 PageRank**

### **4.2.1 PageRankVertex**

PageRankVertex类是对Vertex<Double,Integer>类的一个继承，因此要重写compute和getMessage两个方法：

~~~java
    @Override
    public void compute(Queue<Message> msg);
    @Override
    public Map<Integer, Message> getMessage(List<Edge<Integer>> edges);
~~~

对于PageRank而言，compute方法需要对得到的消息的所有值求和，然后将其作为自己的值。考虑到第一轮无消息传入，所以要加特判如果消息队列为0不能就认为求和为0进行赋值；getMessage方法需要按下面公式来计算：

$$NewValue=0.85*CurrentVaule/EdgeNum+0.15$$

然后将得到的值发给所有的出边即可。

### **4.2.2 PageRankWorker**

PageRankVer是对Worker<Double,Integer>的继承，因此要重写init和load方法：

~~~java
    @Override
    public void init();
    @Override
    public void load();
~~~

与SSSP不同，init方法无需进行其他操作，只需要初始化两个消息队列即可；load方法初始化所有顶点的值为1.0（可以定义为其他值），而且对于PageRank而言无需知道边的值，这里为了赋值仍给它值为1。

### **4.2.3 PageRankCombiner**

PageRankCombiner是对Combiner类的一个实现，要重写combine方法：

~~~java
    @Override
    public Map<Integer, Message> combine(Map<Integer, Queue<Message>> input)
~~~

对PageRank来说，combine方法需要将所有的消息队列的值求和，然后对于每个顶点得到的求和重新构造一个Map返回即可。

### **4.2.4 PageRank**

PageRank类是对Master类的一个继承，和SSSP类一样，它不仅重写了run方法，而且还定义了一个writeResult方法：

~~~java
    @Override
    public void run(int k);
    public void writeResult(String filepath);
~~~

PageRank的writeResult方法和SSSP一样，都是将顶点的id和顶点的值输出到文件即可。对于PageRank的run方法，首先仍然需要对所有的worker进行初始化，执行load和init方法，并判断是否使用Combiner。然后定义了迭代次数，执行迭代次数BSP轮，得到迭代若干次后的PageRank结果。

# 5.结果分析

单源最短路径的结果存储在SSSP_Resule文件中。结果是按照每行存储点的id和点对应的单源最短路径实现的，截取部分结果如下所示：

    786446	10
    262146	11
    8	INF
    262158	10
    14	10
    786435	11
    786462	INF

结果中，对于单源最短路径顶点能够到达的顶点，结果为距离，否则则为INF表示起点到该点之前没有连通的边。PageRank的结果是按照每行存储点的id和其对应的PageRank的值来实现的，保存在PageRankResult文件中，对于初始化顶点值全为1的情况，截取部分结果如下所示：

    786446	3.0869
    262146	1.2339
    8	.2928
    262158	1.3327
    14	.4123
    786435	3.1071
    786462	.1947
    786456	2.3726  

然后对于统计功能实现的结果，以单源最短路径为例，结果保存在message文件里，以id为0的worker为例，截取的部分结果如下所示：

    worker id:0	edge number:637023
    worker id:0	vertex number:108886
    worker id:0	superstep1	use time:4ms
    worker id:0	superstep1	message exchange times:0
    worker id:0	superstep2	use time:5ms
    worker id:0	superstep2	message exchange times:45
    worker id:0	superstep3	use time:5ms
    worker id:0	superstep3	message exchange times:178

# 6.实验心得

首先，因为这次实验要实现一个架构并且在这个架构中实现具体的应用，这就涉及到抽象类，接口以及泛型等的设计，回顾了软件构造课程的相关知识。然后，通过对Pregel的模拟，加深了对BSP模型的理解，对大图计算模型各个部分的功能有了更深刻的理解，并通过自己实现具体算法了解了如何在大图数据计算平台的基础下编写应用。

在读图数据的过程中遇到了一些问题。最开始我是用List来存储顶点的，但是因为给的图数据是边集，所以通过边获得顶点的过程在存储前首先要判断是否已经存储过这个顶点。所以需要遍历所有的List来进行这样的判断，这是效率很低的。所以在最开始运行过程中需要的时间很久，后来意识到这个问题后就选择使用HashSet来存储顶点，这样在使用contain方法就要比遍历List要快的多，在10s左右就能结束这个算法。

# 7.参考文献

**[1]** Pregel: A System for Large-Scale Graph Processing, ACM SIGMOD 2010.

**[2]**《大数据分析》课程讲稿（4.2节）.


<script type="text/javascript" src="http://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML"></script>
<script type="text/x-mathjax-config">
        MathJax.Hub.Config({ tex2jax: {inlineMath: [['$', '$']]}, messageStyle: "none" });
</script>
