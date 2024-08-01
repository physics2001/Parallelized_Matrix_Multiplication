# Parallelized_Matrix_Multiplication

This repo is a spark implementation of the worst case optimal algorithm proposed by Xiao Hu and Ke Yi in section 3.1 of 
their paper "Parallel Algorithms for Sparse Matrix Multiplication and Join-Aggregate Queries"[[1]](#1).

## Running the algorithms
Run via spark-submit in terminal with sample data:
### Default Join
```
spark-submit --num-executors 3 --executor-cores 3 --executor-memory 1G --class uwaterloo.mpcmm.DefaultJoin target\scala-2.13\algorithm.jar ---r1-path data\R1Simple ---r2-path data\R2Simple --num-reducers 9
```
### Worst Case Optimal Algorithm
```
spark-submit --num-executors 3 --executor-cores 3 --executor-memory 1G --class uwaterloo.mpcmm.WorstCaseOptimalAlgorithm target\scala-2.13\algorithm.jar ---r1-path data\R1Simple ---r2-path data\R2Simple --num-reducers 9
```

## References
<a id="1">[1]</a>
Xiao Hu and Ke Yi. 2020. Parallel Algorithms for Sparse Matrix Multiplication and Join-Aggregate Queries. 
In Proceedings of the 39th ACM SIGMODSIGACT-SIGAI Symposium on Principles of Database Systems (PODS’20), 
June 14–19, 2020, Portland, OR, USA. ACM, New York, NY, USA, 15 pages. https://doi.org/10.1145/3375395.3387657
