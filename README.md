# MatrixMultiplicationHadoopMapReduce

This project aims to perform 2 problems both in Single Node & pseudo-cluster modes. The first one is the word-count problem and the second one is the matrix-vector multiplication problem.
The input takes the form of : "<M> <row num> <col num> <element>" for the matrix and "<V> <row num> <col num> <element>" for the vector.
The matrix and Vector has a dimension of 1100*1100 & 1100*1 respectively. The matrix and the vector are read from the same file : input-sharmin.txt
A serial algorthm for matrix-vector-multiplication has also been implemented to verify the output and time comparison.
Both output file reveals the same outcome which verifies the correctness of the algorithm. After comparing execution time it's found that, for serial algorthm it took approx. 1886 ms whereas for the hadoop map-reduce framework it took longer than this time.
