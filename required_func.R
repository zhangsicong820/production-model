### The function will return a data.table containing basin DI.
### Author: Xiao Wang
### Date: 2016-03-24

partition_load <-function(tbl_name, part_num){
  ### tbl_name: the large table to extract basin data.
  ### tbl_name should be like: dev.zsz_permian_dec
  ### part_num: how many parts need to be partitioned.

  ## load required library first.
  library(pipeR) # if loaded before, this line can be commented.
  library(data.table)
  ###

  first_prod_year <- sprintf("select distinct(first_prod_year) from %s
                             order by first_prod_year", tbl_name) %>>%
                             {dbGetQuery(base, .)}
  n = nrow(first_prod_year)
  PARTNUM <- part_num
  ind <- seq(from = 1, to = n,  by = (n - 1)/PARTNUM) # partition the table into 5 parts
  thres <- first_prod_year[ind[2:PARTNUM],] # threshold for making partitions.

  # create an empty data table to hold all values.
  result_table <- sprintf("select * from %s limit 0", tbl_name) %>>%
                          {dbGetQuery(base, .)} %>>%
                          {as.data.table(.)}

  for(i in 1:(PARTNUM)){
    if(i == 1){
      query <- sprintf("select * from %s
                       where first_prod_year < %s", tbl_name, thres[i])
    } else if(i == (PARTNUM)){
      query <- sprintf("select * from %s
                       where first_prod_year >= %s", tbl_name, thres[i - 1])
    } else{
      query <- sprintf("select * from %s where first_prod_year >= %s
                       and first_prod_year < %s", tbl_name,
                       thres[i - 1], thres[i])
    }

    ## using data.table and rbindlist for fast table binding.
    partition <- dbGetQuery(base, query) %>>% as.data.table()
    result_table <- rbindlist(list(result_table, partition))
  }
  return(result_table)
}
