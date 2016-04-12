library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(doParallel)
library(data.table)
library(foreach)
library(pipeR)

pgsql <- JDBC("org.postgresql.Driver", "C:/postgresql-9.2-1003.jdbc4.jar", "`")

base<-dbConnect(pgsql, "jdbc:postgresql://ec2-54-204-4-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",
                user="u9dhckqe2ga9v1",password="pa49dck9aopgfrahuuggva497mh")


dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-23-21-136-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",
                      user="uc52v8enn6qvod",
                      password="p36h60963tc5mbf1td3ta08ufa0")

source("C:/Users/clipper/Desktop/clipper/R/SourceCodeBackUp/function_source.R")

options("scipen"=100)
#################################################################################################

ark <- dbGetQuery(base, "select * from dev.zsz_ark_out;")

ark$last_prod_date <- "2014-12-01"

#ark <- dbGetQuery(base, "select * from dev.zsz_ark_out")

#------------#
cat('Production data was loaded successfully...\n')
#------------#

## Change data struture into data.table
ark <- as.data.table(ark)
## Set keys for faster searching.
setkey(ark, entity_id, basin, first_prod_year)

ark[, comment := ""]
ark[, last_prod_date := as.character(last_prod_date)]
ark[, prod_date:= as.character(prod_date)]



## change the liq into daily level.
## This could be done directly in the database.
# ark[, liq := liq/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = 'days')]


## Load decline rate data for all basins here.
#dcl_all <- dbGetQuery(base, "select * from dev.zxw_nd_adj_log_dcl")
# dcl_all <- fread('C:/Users/Xiao Wang/Desktop/Programs/Projects/Prod_CO_WY/dcl_all_simple.csv')
dcl_all <- as.data.table(dcl_all)
setkey(dcl_all, basin, first_prod_year)
## Find all the basin names for current state.
basin_name <- unique(ark[, basin])
## Subset the decline rate table for faster matching.
dcl <- dcl_all[basin %in% basin_name, ]


### Load basin maximum production month table.
#basin_max_mth_tbl <- dbGetQuery(base, "select * from dev.zxw_basin_max_mth")
basin_max_mth_tbl <- as.data.table(basin_max_mth_table)
setkey(basin_max_mth_tbl, basin, first_prod_year)

#---------------------------------------------#
# Part Three -- 15 month forward projection   #
#---------------------------------------------#

## Parallel computing setting.
numberOfWorkers = 3
cl_forward <- makeCluster(numberOfWorkers) # create the clusters.
registerDoParallel(cl_forward) # register the cluster setting to use multicores.


#------------#
cat(sprintf('Start making forward projection at %s...\n',as.character(Sys.time())))
#------------#

## Main Routine.
tic_forward = proc.time()
for (i in 1:15) {
  forward <- sqldf("with t0 as (
                   select entity_id, max(n_mth) as max
                   from ark
                   group by entity_id),
                   
                   t1 as (
                   select a.entity_id, avg(liq) as avg
                   from ark a join t0 b on a.entity_id = b. entity_id
                   where n_mth >= max - 6 
                   group by a.entity_id)
                   
                   select entity_id
                   from t1
                   where avg >= 15")
  
  forward = as.data.table(forward)
  ark_last = ark[last_prod_date == prod_date, ]
  forward_dt = ark_last[entity_id %in% forward[, entity_id], ]
  
  # entities whose liq would remain constant.
  forward_const <- sqldf("with t0 as (
                         select entity_id, max(n_mth) as max
                         from ark
                         --where comment != 'All Zeros'
                         group by entity_id),
                         
                         t1 as (
                         select a.entity_id, avg(liq) as avg
                         from ark a join t0 b on a.entity_id = b. entity_id
                         where n_mth >= max - 6 --and comment != 'All Zeros'
                         group by a.entity_id)
                         
                         select entity_id
                         from t1
                         where avg < 15 and avg > 0.667")
  forward_const <- as.data.table(forward_const)
  const_forward_dt = ark_last[entity_id %in% forward_const[, entity_id], ]
  
  # Making forward projection parallelly.
  # Cluster need to be set before.
  forward_liq_proj = foreach(j = 1:nrow(forward_dt), .combine = c, .packages = 'data.table') %dopar% 
    forward_liq_func(j)
  
  ### Except liq and last_prod_date, changing the other columns outside the long iterations.
  ## entity_id, basin, first_prod_year remain the same.
  ## Only update values for last_prod_date, n_mth, prod_date, comment
  temp_ark_forward = forward_dt
  temp_ark_forward[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_ark_forward[, n_mth:= (n_mth + 1)]
  temp_ark_forward[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_ark_forward[, comment:= "Inserted"]
  temp_ark_forward[, liq:= forward_liq_proj]
  
  
  ### data table to store all the entities with constant forward production.
  temp_ark_const = const_forward_dt
  const_liq = const_forward_dt[, liq] # use the last availale data as the production.
  
  temp_ark_const[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_ark_const[, n_mth:= (n_mth + 1)]
  temp_ark_const[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_ark_const[, comment:= "Inserted"]
  temp_ark_const[, liq:= const_liq]
  
  # Update the last_prod_date for all the entities.
  ark[entity_id %in% forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  ark[entity_id %in% const_forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  
  # Append the forward prediction in original data set.
  ark = rbindlist(list(ark, temp_ark_forward,temp_ark_const))
  setkey(ark, entity_id, basin, first_prod_year)
  cat(sprintf('Congratulations! Iteration %i runs successfully...\n', i))
  cat(sprintf('Interation finished at %s...\n', as.character(Sys.time())))
}

toc_forward <- proc.time()
time_usage_forward <- toc_forward - tic_forward
time_usage_forward

stopCluster(cl_forward)

cat(sprintf('Forward projection was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')

plyr::ddply(ark, 'prod_date', summarise, prod = sum(liq)/1000)