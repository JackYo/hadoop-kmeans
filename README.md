#build commands
=================
hadoop com.sun.tools.javac.Main newKMeans.java
jar cf output.jar newKMeans*.class
hadoop jar output.jar newKMeans {input_file} {output_file} {cluster_number} {data_dimension} {convergence_threshold}

##for example
------------
hadoop com.sun.tools.javac.Main newKMeans.java
jar cf output.jar newKMeans*.class
hadoop jar output.jar newKMeans hd-iris.txt output 3 4 0.01

#data formate requirement
========================
{vector values separated by comma}, {category}

##for example
------------
1.0,2.0,3.0,Father
2.0,3.0,4.0,Mother
3.0,4.0,5.0,Sister
5.0,2.0,1.0,Brother

in this example, {data_dimension} is 3

#Algorithm
================
```
main function
   read data
   randomly choose data points for initial centroids
   set previous_centroids as initial centroids
   iterate KMeans MapReduce until convergence reached
       start Mapper 
           send (key:centroid, value:data point)
       start Reducer 
           receive
           send (key:new_centroid, value:categories in the centroid)
       read current_centroids
	   calculate the average distant(aka. movement) between current_centroids and previous_centroids
       
	   set previous_centroids as current_centroids

       judge to continue or not
```
