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

dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-54-225-92-149.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",
                      user="uagv4d92665mm0",
                      password="pevkvfmdcjuddh8tc2sq81rnm0r")

source("C:/Users/clipper/Desktop/clipper/R/SourceCodeBackUp/function_source.R")

options("scipen"=100)
#################################################################################################

gom <- dbGetQuery(base, "select * from dev.zsz_gom_out;")

gom$last_prod_date <- "2014-12-01"

#gom <- dbGetQuery(base, "select * from dev.zsz_gom_out")

#------------#
cat('Production data was loaded successfully...\n')
#------------#

## Change data struture into data.table
gom <- as.data.table(gom)
## Set keys for faster searching.
setkey(gom, entity_id, basin, first_prod_year)

gom[, comment := ""]
gom[, last_prod_date := as.character(last_prod_date)]
gom[, prod_date:= as.character(prod_date)]



## change the liq into daily level.
## This could be done directly in the database.
# gom[, liq := liq/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = 'days')]


## Load decline rate data for all basins here.
#dcl_all <- dbGetQuery(base, "select * from dev.zxw_nd_adj_log_dcl")
# dcl_all <- fread('C:/Users/Xiao Wang/Desktop/Programs/Projects/Prod_CO_WY/dcl_all_simple.csv')
dcl_all <- as.data.table(dcl_all)
setkey(dcl_all, basin, first_prod_year)
## Find all the basin names for current state.
basin_name <- unique(gom[, basin])
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
                   from gom
                   group by entity_id),
                   
                   t1 as (
                   select a.entity_id, avg(liq) as avg
                   from gom a join t0 b on a.entity_id = b. entity_id
                   where n_mth >= max - 6 
                   group by a.entity_id)
                   
                   select entity_id
                   from t1
                   where avg >= 15")
  
  forward = as.data.table(forward)
  gom_last = gom[last_prod_date == prod_date, ]
  forward_dt = gom_last[entity_id %in% forward[, entity_id], ]
  
  # entities whose liq would remain constant.
  forward_const <- sqldf("with t0 as (
                         select entity_id, max(n_mth) as max
                         from gom
                         --where comment != 'All Zeros'
                         group by entity_id),
                         
                         t1 as (
                         select a.entity_id, avg(liq) as avg
                         from gom a join t0 b on a.entity_id = b. entity_id
                         where n_mth >= max - 6 --and comment != 'All Zeros'
                         group by a.entity_id)
                         
                         select entity_id
                         from t1
                         where avg < 15 and avg > 0.667")
  forward_const <- as.data.table(forward_const)
  const_forward_dt = gom_last[entity_id %in% forward_const[, entity_id], ]
  
  # Making forward projection parallelly.
  # Cluster need to be set before.
  forward_liq_proj = foreach(j = 1:nrow(forward_dt), .combine = c, .packages = 'data.table') %dopar% 
    forward_liq_func(j)
  
  ### Except liq and last_prod_date, changing the other columns outside the long iterations.
  ## entity_id, basin, first_prod_year remain the same.
  ## Only update values for last_prod_date, n_mth, prod_date, comment
  temp_gom_forward = forward_dt
  temp_gom_forward[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_gom_forward[, n_mth:= (n_mth + 1)]
  temp_gom_forward[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_gom_forward[, comment:= "Inserted"]
  temp_gom_forward[, liq:= forward_liq_proj]
  
  
  ### data table to store all the entities with constant forward production.
  temp_gom_const = const_forward_dt
  const_liq = const_forward_dt[, liq] # use the last availale data as the production.
  
  temp_gom_const[, last_prod_date:= as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  temp_gom_const[, n_mth:= (n_mth + 1)]
  temp_gom_const[, prod_date:= as.character(format(as.Date(prod_date)+32,'%Y-%m-01'))]
  temp_gom_const[, comment:= "Inserted"]
  temp_gom_const[, liq:= const_liq]
  
  # Update the last_prod_date for all the entities.
  gom[entity_id %in% forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  gom[entity_id %in% const_forward_dt[,entity_id], last_prod_date:=as.character(format(as.Date(last_prod_date)+32,'%Y-%m-01'))]
  
  # Append the forward prediction in original data set.
  gom = rbindlist(list(gom, temp_gom_forward,temp_gom_const))
  setkey(gom, entity_id, basin, first_prod_year)
  cat(sprintf('Congratulations! Iteration %i runs successfully...\n', i))
  cat(sprintf('Interation finished at %s...\n', as.character(Sys.time())))
}

toc_forward <- proc.time()
time_usage_forward <- toc_forward - tic_forward
time_usage_forward

stopCluster(cl_forward)

cat(sprintf('Forward projection was executed successfully at %s...\n', as.character(Sys.time())))
cat('#-------------------------------------------#\n')
#-------------------------------------------------------------
# Part Four -- New production prediction
#-------------------------------------------------------------

## forward prediction for new production #########################################################################################################

# sales price
hist_price <- dbGetQuery(base, "with t0 as (
                         select (substr(contract,4,2)::INT + 2000) as year, 
                         case when substr(contract,3,1) = 'F' then 1
                         when substr(contract,3,1) = 'G' then 2
                         when substr(contract,3,1) = 'H' then 3
                         when substr(contract,3,1) = 'J' then 4
                         when substr(contract,3,1) = 'K' then 5
                         when substr(contract,3,1) = 'M' then 6
                         when substr(contract,3,1) = 'N' then 7
                         when substr(contract,3,1) = 'Q' then 8
                         when substr(contract,3,1) = 'U' then 9
                         when substr(contract,3,1) = 'V' then 10
                         when substr(contract,3,1) = 'X' then 11
                         when substr(contract,3,1) = 'Z' then 12 end as month, avg(month1) as avg	     	  
                         from nymex_nearby 
                         where tradedate >= current_date - interval '4 months' and product_symbol = 'CL'
                         group by year, month
                         order by 1, 2),
                         
                         t1 as (
                         select extract('year' from sale_date) as year, extract('month' from sale_date) as month, avg(price) as avg
                         from di.pden_sale
                         where prod_type = 'OIL' and sale_date >= '2012-10-01'
                         group by 1, 2
                         order by 1, 2)
                         
                         select * from t1
                         union
                         select * from t0
                         order by 1, 2                ")

f_price <- dbGetQuery(base, "select (substr(contract,4,2)::INT + 2000) as year, 
                      case when substr(contract,3,1) = 'F' then 1
                      when substr(contract,3,1) = 'G' then 2
                      when substr(contract,3,1) = 'H' then 3
                      when substr(contract,3,1) = 'J' then 4
                      when substr(contract,3,1) = 'K' then 5
                      when substr(contract,3,1) = 'M' then 6
                      when substr(contract,3,1) = 'N' then 7
                      when substr(contract,3,1) = 'Q' then 8
                      when substr(contract,3,1) = 'U' then 9
                      when substr(contract,3,1) = 'V' then 10
                      when substr(contract,3,1) = 'X' then 11
                      when substr(contract,3,1) = 'Z' then 12 end as month, *	     	  
                      from nymex_nearby 
                      where contract = (select contract from nymex_nearby where tradedate = (select max(tradedate) from nymex_nearby where product_symbol = 'CL') and product_symbol = 'CL')")

future_price <- as.data.frame(matrix(nrow = 11, ncol = 3));
colnames(future_price)<-c('year', 'month', 'avg');


##transform future price table
for (i in 1:11){
  
  if(unique(f_price$month) + i <= 12){
    future_price$year[i] <- unique(f_price$year)
    future_price$month[i] <- unique(f_price$month) + i
    future_price$avg[i] <- mean(f_price[, (6+i)])
  } else {
    future_price$year[i] <- unique(f_price$year)
    future_price$month[i] <- unique(f_price$month) + i - 12
    future_price$avg[i] <- mean(f_price[, (6+i)])
  }
}

price <- as.data.frame(rbind(hist_price, future_price))

## calculate the moving average
Moving_Avg <- function(data, interval){
  n = length(data)
  m = interval
  SMA = data.frame('SMA' = rep(0, (n - m + 1)))
  
  for(i in n:1){
    if((i - m + 1) <= 0){
      break
    } else{
      avg <- mean(data[i: (i - m + 1)])
      j = (n - i + 1)
      SMA[j,1] = avg
    }
  }
  SMA = SMA[c((n - m + 1):1),]
  
  return(as.data.frame(SMA))
}



# prod of new wells
new_prod <- dbGetQuery(base, "select first_prod_date, extract('year' from first_prod_date) first_prod_year, 
                       extract('month' from first_prod_date) first_prod_month, 
                       round(sum(liq)/1000/(extract(days from (first_prod_date + interval '1 month' - first_prod_date))),0) as new_prod
                       from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                       where liq_cum >0 and prod_date >= '2012-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 and basin in ('GOM - SHELF', 'GOM - DEEPWATER') 
                       and first_prod_date < '2015-01-01' and first_prod_date = prod_date
                       group by 1,2,3
                       order by 1,2,3")

new_prod$first_prod_date <- as.Date(new_prod$first_prod_date)

## prod

hist_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                        from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                        where liq_cum >0 and prod_date >= '2012-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 
                        and basin in ('GOM - SHELF', 'GOM - DEEPWATER') and prod_date < '2015-01-01'
                        group by prod_date
                        order by 1")

hist_prod$prod_date <- as.Date(hist_prod$prod_date)

## forward prod from hist wells

prod <-  plyr::ddply(gom, 'prod_date', summarise, prod = sum(liq)/1000)

#prod <- mutate(prod, prod = sum/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = c("days")))

updated_prod <- prod[prod$prod_date > max(hist_prod$prod_date),]


new_first_prod <- as.data.frame(matrix(nrow = 15, ncol = 2));
colnames(new_first_prod)<-c('prod_date', 'prod');



#----------------------------------------------------------------------------------------#
### Calculate the state average decline rate.
#monthly_prod = plyr::ddply(gom, .(prod_date, basin), summarise, basin_prod = sum(liq)/1000) %>>% as.data.table()
# Using the last five months production to calulate their weights.
#prod_subset = monthly_prod[(prod_date <= '2014-12-01' & prod_date >= '2014-07-01'), ]
#prod_subset[, basin_prod := basin_prod/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = c("days"))]
# Calculate the state total production
#state_total = plyr::ddply(prod_subset, .(prod_date), summarise, state_prod = sum(basin_prod))
#weight = sqldf("select a.*, round(basin_prod/state_prod,6) weight from prod_subset a, state_total b
#               where a.prod_date = b.prod_date")
# Using the last five months' weights to calculate the average weight.
#avg_weight = plyr::ddply(weight, .(basin), summarise, avg_weight = mean(weight)) %>>%
#  as.data.table()

#dcl_weight_avg = dcl
#basin_name_ = avg_weight$basin
#for(i in 1:length(basin_name_)){
#  dcl_weight_avg[basin == basin_name_[i], weighted_avg:= avg*avg_weight[basin == basin_name_[i], avg_weight]]
#}

# calculate the weighted average decline rate for each first_prod_year and n_mth combination.
#dcl_state_avg <- plyr::ddply(dcl_weight_avg, .(first_prod_year, n_mth), summarise, avg = sum(weighted_avg))
#dcl_state_avg <- as.data.table(dcl_state_avg)
#setkey(dcl_state_avg, first_prod_year, n_mth)

## prod from new wells and 15 month forward
test <- updated_prod

for (i in 1:20) {
  if(as.Date(format(as.Date(max(hist_prod$prod_date))+32,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
  {
    break
  }
  if(as.Date(format(as.Date(max(hist_prod$prod_date))+32,'%Y-%m-01')) <= as.Date(max(prod$prod_date)))
  {
    n = nrow(hist_prod)
    
    fit <- as.data.frame(cbind(new_prod$new_prod[2:n], new_prod$new_prod[1:(n-1)], hist_prod$prod[1:(n-1)], Moving_Avg(price$avg, 3)[1:(n-1),]))
    
    colnames(fit) <- c("new_prod","new_prod_lag", "prod", "avg")
    
    lm <- lm(new_prod ~  new_prod_lag + prod + avg, data = fit)
    
    #summary(lm)
    
    data <- as.data.frame(cbind(new_prod$new_prod[n], hist_prod$prod[n],Moving_Avg(price$avg, 3)[n,]))
    
    colnames(data) <- c("new_prod_lag", "prod", "avg")
    
    #prod of new wells
    new_prod[n+1,1] <- as.character(format(as.Date(new_prod$first_prod_date[n]+32),'%Y-%m-01')) 
    new_prod[n+1,2] <- new_prod$first_prod_year[n] 
    new_prod[n+1,3] <- new_prod$first_prod_month[n] + 1
    new_prod[n+1,4] <- round(predict(lm, data),0)
    
    # new first month production
    new_first_prod[i,1] <- as.character(format(as.Date(hist_prod$prod_date[n]+32),'%Y-%m-01'))
    new_first_prod[i,2] <- round(predict(lm, data),0)
    
    
    # update the updated production
    temp <- new_first_prod[i,]
    
    
    for (j in 1:20) {
      if(as.Date(format(as.Date(min(temp$prod_date))+32*j,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
      {
        break
      }
      if(as.Date(format(as.Date(min(temp$prod_date))+32*j,'%Y-%m-01'))<= as.Date(max(prod$prod_date)))
      {
        m = nrow(temp)
        temp[m+1, 1] <- as.character(format(as.Date(temp$prod_date[j])+32,'%Y-%m-01'))
        
        if(j < max(dcl$n_mth[dcl$first_prod_year == max(dcl$first_prod_year)])) {
          dcl_factor <- 10^(dcl[first_prod_year == max(first_prod_year) & n_mth == (j + 1), avg]/100)
          temp[m+1, 2] <- round((1 + temp$prod[m]) * dcl_factor - 1,0)
        } else {
          dcl_factor <- 10^(dcl[first_prod_year == max(first_prod_year) & n_mth == max(dcl_state_avg[first_prod_year == max(first_prod_year), n_mth]), avg]/100)
          temp[m+1, 2] <- round((1 + temp$prod[m]) * dcl_factor - 1,0)
        }
      }
    }
    
    
    
    #temp$prod_date <- as.Date(temp$prod_date)
    sql_query <- sprintf("select a.*, coalesce(b.prod, 0) as prod%s
                         from test a left join temp b on a.prod_date = b.prod_date", i)
    
    test <- sqldf(sql_query)
    
    updated_prod <- sqldf("select a.prod_date, a.prod + coalesce(b.prod, 0) as prod
                          from updated_prod a left join temp b on a.prod_date = b.prod_date")
    
    #sql_query <- sprintf("select * from tbl where col = %s and col2 = %g", var1, var2)
    
    ## new total production
    hist_prod[n+1,1] <- as.character(format(as.Date(hist_prod$prod_date[n]+32),'%Y-%m-01')) 
    hist_prod[n+1,2] <- round(updated_prod$prod[updated_prod$prod_date == hist_prod$prod_date[(n+1)]], 0)
  }
}



