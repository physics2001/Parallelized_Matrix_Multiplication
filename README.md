# Parallelized_Matrix_Multiplication

This repo is a spark implementation of the worst case optimal algorithm proposed by Xiao Hu and Ke Yi in section 3.1 of 
their paper "Parallel Algorithms for Sparse Matrix Multiplication and Join-Aggregate Queries"[[1]](#1).

## Running the algorithms locally

1. Go to the current [release](https://github.com/physics2001/Parallelized_Matrix_Multiplication/releases/tag/1.0.0) and download the jar file. 
2. Download the sample and generated data from this repo's [data](https://github.com/physics2001/Parallelized_Matrix_Multiplication/tree/main/data) folder. 
3. Make sure you have Spark, Hadoop, Java installed. I used Spark 3.6.1, Hadoop 3.3.6, and Java 17.0.11 
4. Copy the jar and data under the same directory. 
5. Run via spark-submit in terminal with sample data. You may need to fix the paths for the different options.
   - `---r1-path` is the path of input relation 1
   - `---r2-path` is the path of input relation 2
   - `--num-reducers` is the number of partitions for the data. This is better when it's equal to `--num-executors` \times `--executor-cores` 
   - `--verbose` is whether you want to see spark debug `INFO` messages, add it to options list if you want to see it, otherwise remove it from the options list. 
   - `--num-executors`, `--executor-cores`, and `--executor-memory` are spark related settings, feel free to change them too. 

### Default Join
```
spark-submit --num-executors 10 --executor-cores 1 --executor-memory 1G --class uwaterloo.mpcmm.join.DefaultJoin algorithm-1.0.0.jar ---r1-path data\R1MoreHeavy ---r2-path data\R2MoreHeavy --num-reducers 10 --result-folder result\MoreHeavy --verbose
```
### Worst Case Optimal Join
```
spark-submit --num-executors 10 --executor-cores 1 --executor-memory 1G --class uwaterloo.mpcmm.join.WorstCaseOptimalJoin algorithm-1.0.0.jar ---r1-path data\R1MoreHeavy ---r2-path data\R2MoreHeavy --num-reducers 10 --result-folder result\MoreHeavy --verbose
```
### Default Matrix Multiplication
```
spark-submit --num-executors 10 --executor-cores 1 --executor-memory 1G --class uwaterloo.mpcmm.matrix.DefaultMatrixMult algorithm-1.0.0.jar ---r1-path data\matrix\InSize10000R1 ---r2-path data\matrix\InSize10000R2 --num-reducers 10 --result-folder result\Default10000 --verbose
```
### Worst Case Optimal Matrix Multiplication
```
spark-submit --num-executors 10 --executor-cores 1 --executor-memory 1G --class uwaterloo.mpcmm.matrix.WorstCaseOptimalMatrixMult algorithm-1.0.0.jar ---r1-path data\matrix\InSize10000R1 ---r2-path data\matrix\InSize10000R2 --num-reducers 10 --result-folder result\Default10000 --verbose
```

## Developing locally

1. Clone this repo.
2. Make sure you have Spark, Hadoop, Java installed and the correct versions just like above. 
3. I used Intellij. Open the project in Intellij select `File -> New -> Project From Existing Sources -> select the repo folder -> import project from existing model -> SBT`
(require scala plugins and sbt installed first).
4. On the right hand side of Intellij, open `sbt` with list of options and there is a reload button. Reload project to install dependencies. 
5. After reloading the project, in `sbt shell`, run the command `assembly` to build a jar in `target\scala-2.13\algorithm-{version}.jar`. 
6. Run via spark-submit in terminal with sample data with the same above command, but need to replace `algorithm-1.0.0.jar` with `target\scala-2.13\algorithm-{version}.jar`. 
7. To create more sample data, that can be done by running the main method `CreateRDD`, `CreateRDDMoreHeavy`, `CreateRDDWithWeights`, `CreateRDDWithWeightsMoreHeavy`, and `RandomSparseMatrixGenerator`. 
Before running each, **PLEASE** `Edit Configurations` for each main on the top right dropdown. 
For each, please click `Modify options` first and check `Add VM options` in the Java section. 
Then, add the following options to VM options: `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`. 
Now the main classes can be run without errors.

## Known Issues

The timer and recorded join time are incorrect. After starting the algorithm, please check the local endpoint for more details on the time each step takes in Spark. 
In the `INFO` logs, Spark would say the address at which it opens a `Spark web UI`. Running on linux may allow checking history for each run. 

## References
<an id="1">[1]</an>
Xiao Hu and Ke Yi. 2020. Parallel Algorithms for Sparse Matrix Multiplication and Join-Aggregate Queries. 
In Proceedings of the 39th ACM SIGMODSIGACT-SIGAI Symposium on Principles of Database Systems (PODS’20), 
June 14–19, 2020, Portland, OR, USA. ACM, New York, NY, USA, 15 pages. https://doi.org/10.1145/3375395.3387657
