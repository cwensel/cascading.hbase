
Welcome

 This is the Cascading.HBase module.

 It provides support for reading/writing Cascading data
 to/from an HBase cluster.

 Cascading is a feature rich API for defining and executing
 complex, scale-free, and fault tolerant data processing
 workflows on a Hadoop cluster. It can be found at the
 following location:

   http://www.cascading.org/

 This module is is licensed under a Creative Commons
 Attribution-Share Alike 3.0 United States License.
 http://creativecommons.org/licenses/by-sa/3.0/us/


Building

 To build, call

 > ant -Dhadoop.home=... -Dcascading.home=... -Dhbase.home=... jar

 To test, call

 > ant -Dhadoop.home=... -Dcascading.home=... -Dhbase.home=... test

where "..." is the install path of each of the dependencies.


Using

  The cascading-hbase.jar file should be added to the "lib"
  directory of your Hadoop application jar file.

  This is the same requirement of all Hadoop app third-party libraries.

  See the HBaseTest unit test for sample code on using the HBase taps and
  schemes in your Cascading application.