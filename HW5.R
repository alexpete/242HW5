###########################################################
# Get sample of flights and perform classification in 
# parallel using random forests and bagging
###########################################################
library(parallel)
library(randomForest)
Sys.setlocale(locale="C") # for strange characters in 2001.csv and 2002.csv

########################## Get Sample ###########################

# Yearly csv files should be in a directory called Data
setwd('Data')

files = list.files(pattern = 'csv')

# Create fork cluster and sample in parallel

cl = makeCluster(4, type = 'FORK')
clusterSetRNGStream(cl, 2938401)

flts.smp = clusterApply(cl, files, function(f){
  
  n = as.integer(substr(system(sprintf('wc -l %s', f), intern = T), 
                          1,7)) - 1
  smp = sort(sample(n, 100000))
  val = rep(NA, length(smp))
  smp.diff = diff(c(0, smp)) # how many lines to read at a time
  con = file(f, open = 'r')
  nms = strsplit(readLines(con, n = 1), split = ',')[[1]]
  for(i in 1:length(smp)) val[i] = readLines(con, n = smp.diff[i])[smp.diff[i]]
  close(con)
  tbl = read.table(textConnection(val), sep = ',')
  names(tbl) = nms
  
  # remove fields with over 1/3 NA'a, and also origin and destination (too many levels)
  
  tbl[-c(11, 14, 17, 18, 20:29)]

})

stopCluster(cl)

flts.smp = do.call(rbind, flts.smp)

#remove Cancelled and Diverted flights, as well as flights without distance available
flts.smp = flts.smp[!is.na(flts.smp$ArrDelay) & !is.na(flts.smp$Distance), ] 

########################## Get Random Forests ###########################

cl = makeCluster(4, type = 'FORK') #make another fork to avoid transfer of data
clusterSetRNGStream(cl, 392757)

flts.rf = clusterEvalQ(cl, randomForest(x = flts.smp[-13)], 
                                        y = factor(flts.smp$ArrDelay>10), 
                                        ntree = 100, maxnodes = 100))

flts.rf = do.call(combine, flts.rf)
stopCluster(cl)

########################## Perform Bagging ###########################
# bagging just means set mtry = 14

cl = makeCluster(4, type = 'FORK')
clusterSetRNGStream(cl, 29380)

flts.bag = clusterEvalQ(cl, randomForest(x = flts.smp[-13], 
                                         y = factor(flts.smp$ArrDelay>10), 
                                         ntree = 100, maxnodes = 100,
                                         mtry = 14))

stopCluster(cl)

###################### Check Predictions #############################






