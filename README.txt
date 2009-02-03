
Welcome

 This is the Cascading.HBase module.

 It provides support for reading/writing data to/from an HBase
 cluster when bound to a Cascading data processing flow.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/

 HBase is the Hadoop database. Its an open-source, distributed,
 column-oriented store modeled after the Google paper on Bigtable.

   http://hadoop.apache.org/hbase/

 This module is licensed under the
 Creative Commons Attribution-Share Alike 3.0 United States License.

  http://creativecommons.org/licenses/by-sa/3.0/us/


Building

 To build a jar,

 > ant -Dcascading.home=... -Dhadoop.home=... -Dhbase.home=... jar

 To test,

 > ant -Dcascading.home=... -Dhadoop.home=... -Dhbase.home=... test

where "..." is the install path of each of the dependencies.


Using

  The cascading-hbase.jar file should be added to the "lib"
  directory of your Hadoop application jar file along with all
  Cascading dependencies.

  See the HBaseTest unit test for sample code on using the HBase taps and
  schemes in your Cascading application.