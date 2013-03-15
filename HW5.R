###########################################################
# Get sample of flights and perform classification in 
# parallel using random forests and bagging
###########################################################
library(parallel)
Sys.setlocale(locale="C") # for strange characters in 2001.csv and 2002.csv

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
  tbl[-c(11, 14, 20, 21, 23, 25:29)] # remove fields with almost all NAs
})

flts.smp = do.call(rbind, flts.smp)
stopCluster(cl)

save.image(file = '../get_sample.rda')

