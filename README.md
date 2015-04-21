# Nephele Streaming

Nephele Streaming is a December 2013 fork of the Stratosphere project (now Apache Flink). Nephele Streaming is
strictly a research prototype that explores massively parallel stream processing with latency constraints.

Nephele Streaming shares some goals with Apache Storm: It is a software framework for massively parallel real-time
computation on large clusters or clouds. Nephele Streaming jobs are continuous data flows where tasks communicate
via sending records along predefined channels.

The main feature of Nephele Streaming is that users can annotate their applications (jobs) with latency constraints.
A latency constraint as declaration of a non-functional application requirement. It specifies a desired upper latency
bound in milliseconds for a portion of the job's dataflow graph. At runtime the engine attempts to enforce the
constraints by three techniques:

* **Adaptive Output Batching**: Emitted records are by default sent immediately to the receiver for low latency.
  Adaptive output batching flushes those buffers in a time driven fashion to enforce the constraint. The lower the
  constraint, the more often buffers are flushed (and vice versa).
* **Dynamic Task Chaining**: The mapping of pipeline parallel tasks on the same worker process (task manager) is
  changed ad-hoc at runtime by the framework, depending on CPU utilization. This eliminates queues between pipeline
  parallel tasks.
* **Elastic Scaling**: Queues are a major source of latency in stream processing. In order to fulfill latency constraints
  despite variations in stream rates and computational load, Nephele Streaming autoscales data parallel tasks.
  This technique uses a predictive latency model based on queueing theory.


## Status

Nephele Streaming is strictly alpha software and intended to be a flexible platform for research. It currently does not
offer any fault tolerance, nor is it particularly well documented.


## Documentation

Due to being a research prototype, there is very little documentation for Nephele Streaming. There are some example stream processing
jobs here: https://github.com/citlab/nephele-streaming-jobs

The techniques for stream processing with latency constraints have been published in the following technical papers:

* Lohrmann, B. and Janacik, P. and Kao, O. "Elastic Stream Processing with Latency Guarantees", ICDCS'15 (accepted 
for publication, [preprint](http://www.cit.tu-berlin.de/fileadmin/a34331500/misc/icdcs15_preprint.pdf))
* Lohrmann, B. and Warneke, D. and Kao, O. "Nephele streaming: stream processing under QoS constraints at scale", 
Cluster Computing, Springer, 2013 ( [paper](http://arxiv.org/pdf/1308.1031v1) / [slides] (http://www.slideshare.net/bjoernlohrmann/hpdc-presentation))
* Lohrmann, B. and Warneke, D. and Kao, O. "Massively-parallel Stream Processing under QoS Constraints with Nephele", 
HPDC'12, ACM, 2012


## Difference to Stratosphere and Apache Flink

Stratosphere was primarily an engine for batch processing, written by researchers at TU-Berlin. Its execution engine
Nephele offered a pipelined execution model based on continuous dataflows. Nephele Streaming adopts this as the basis
for stream processing, but everything specific to batch processing has been removed (HDFS support, higher-level
programming models, e.g. PACTs).

In 2014, Stratosphere became an Apache project and was renamed to "Flink". Flink has since evolved heavily and is
now a data processing framework that unifies stream and batch processing. Due to the fast evolution of Flink, at the
time of this writing (2015), there is very little common code between Nephele Streaming and Flink.

##  Build From Source

This tutorial shows how to build Stratosphere on your own system. Please open a bug report if you have any troubles!

### Requirements
* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6 or 7

```
git clone https://github.com/bjoernlohrmann/nephele-streaming.git
cd nephele-streaming
mvn clean package
```

Stratosphere-Streaming is now installed in `stratosphere-dist/target`
If you’re a Debian/Ubuntu user, you’ll find a .deb package. We will continue with the generic case.

	cd stratosphere-dist/target/stratosphere-dist-streaming-git-bin/stratosphere-streaming-git/

The directory structure here looks like the contents of the official release distribution.

## Authors

Nephele Streaming is developed by:

* Björn Lohrmann (Lead Developer, Research Associate, bjoern.lohrmann\<at\>tu-berlin.de)
* Sascha Wolke (MSc student at TU-Berlin)
* Ilya Verbitskiy (BSc student at TU-Berlin)
