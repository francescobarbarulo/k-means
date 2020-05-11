# k-means
The k-means Clustering Algorithm in MapReduce

https://www.overleaf.com/9194241676nqmpmykgkkpz

## random sampling algorithm
before starting the java part, I have modified the point generator to include indexes to identify points uniquely. 

```
section{Bruk's proposal for initial random sampling}
    
\class{Driver}
    \Function{main}
        \State\text set a seed, K, and number of values for random generation
        \State\text ... go on to the configuration
    \EndFunction
\EndClass
\Class{Mapper}
    \Function{map}{valueId vids, value vs}
        \State\text declare an int array of K size
        \State\text declare a random generator with seed from the configuration 
        \State\ForAll{elements in the array}
            \State\text generate random number and assign them the value with in the range of total number of points
            \EndFor
        \stat\texte split the value v in to array of strings vals
        \State\ForAll{element in the array}
            \State\If{element == vals[0]}
                \State {\hspace{0.2cm} \textsc{Emit}(element index, val1:2)}
            \EndFor
    \EndFunction
\EndClass

\Class{Reducer}
    \Function{reduce}{valueId vids, value vs}
        \State\text I did something hear but it is useless
    \EndFunction
\EndClass

```
