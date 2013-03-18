###################################################################
# Predict method for randomForest object.  Predicts one tree at a
# time using a user-provided cluster object.
#
# Inputs:
#   
#   rf - random forest object
#   x - data frame of predictors
#   cl - a cluster object for parallelization
#
# Output:
#
#   y - predictions corresponding to x
#
###################################################################

predict.randomForest = function(rf, x, cl){
  
  # Set up binarys function for later use
  binarys<-function(i){
    a<-2^(31:0)
    b<-2*a
    sapply(i,function(x) paste(as.integer((x %% b)>=a),collapse=""))
  }
  clusterExport(cl, 'binarys')
  
  
  y = clusterApply(cl, 1:rf$ntree, function(i){
    
    tree = getTree(rf, k = i)
    
    votes = sapply(1:nrow(x), function(j){
      
      cur.nd = 1
      while(tree[cur.nd, 5] == 1){
        
        svar = tree[cur.nd, 3]; spt = tree[cur.nd, 4]
        
        if(class(x[[svar]]) == 'factor'){
          
          n = length(levels(x[[svar]]))
          spt.bn = binarys(spt)
          flag = strsplit(substr(spt.bn, 33 - n, 32), split = '')[[1]][x[j, svar]] == '1'
          cur.nd = ifelse(flag, tree[cur.nd, 1], tree[cur.nd, 2])
          
        }else{
          
          cur.nd = ifelse(x[j, svar] <= spt, tree[cur.nd, 1], tree[cur.nd, 2])
          
        }
        
      }
      
      tree[cur.nd, 6]
      
    })
    
  })
  
  1*(colMeans(do.call(rbind, y)) >= .5) + 1
}
  
  
  
