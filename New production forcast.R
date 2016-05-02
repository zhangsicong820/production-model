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

dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-54-221-234-57.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",
                      user="u7gr3vd1oki1uj",
                      password="p19kcqldeb6aok877dkhnuvf52i")

source("C:/Users/clipper/Desktop/clipper/R/SourceCodeBackUp/function_source.R")

options("scipen"=100)

## forward prediction for new production #########################################################################################################

#entity <- dbGetQuery(base, "select distinct entity_id, county 
 #                    from di.pden_desc
 #                    where basin = 'POWDER RIVER' and last_prod_date >= '2014-01-01' and liq_cum >0 and ALLOC_PLUS IN ('Y','X') and county = 'MIDLAND (TX)'")


dcl_all <- as.data.table(dcl_all)
setkey(dcl_all, basin, first_prod_year)
## Find all the basin names for current state.
basin_name <- "POWDER RIVER"
## Subset the decline rate table for faster matching.
dcl <- dcl_all[basin %in% basin_name, ]


### Load basin maximum production month table.
#basin_max_mth_tbl <- dbGetQuery(base, "select * from dev.zxw_basin_max_mth")
basin_max_mth_tbl <- as.data.table(basin_max_mth_table)
setkey(basin_max_mth_tbl, basin, first_prod_year)


cutoff_date <- as.Date("2014-12-01")

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
                         where tradedate <= '2014-12-31' and product_symbol = 'CL'
                         group by year, month
                         order by 1, 2),
                         
                         t1 as (
                         select extract('year' from sale_date) as year, extract('month' from sale_date) as month, avg(price) as avg
                         from di.pden_sale
                         where prod_type = 'OIL' and sale_date >= '2012-11-01' and sale_date <= '2013-02-01'
                         group by 1, 2
                         order by 1, 2)
                         
                         select * from t1
                         union
                         select * from t0 
                         order by 1, 2                  ")

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
                      where contract = (select contract from nymex_nearby where tradedate = '2014-12-31' and product_symbol = 'CL')")

future_price <- as.data.frame(matrix(nrow = 11, ncol = 3));
colnames(future_price)<-c('year', 'month', 'avg');


##transform future price table
for (i in 1:11){
  
  if(unique(f_price$month) + i <= 12){
    future_price$year[i] <- unique(f_price$year)
    future_price$month[i] <- unique(f_price$month) + i
    future_price$avg[i] <- mean(f_price[, (6+i)])
  } else {
    future_price$year[i] <- unique(f_price$year) + 1
    future_price$month[i] <- unique(f_price$month) + i - 12
    future_price$avg[i] <- mean(f_price[, (6+i)])
  }
}

price <- as.data.frame(rbind(hist_price, future_price))

first_price <- price[1,3]
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

mvg_price <- Moving_Avg(price$avg/first_price, 3)

# prod of new wells

first_new_prod <- dbGetQuery(dev_base, 
                             sprintf("select first_prod_date, sum(new_prod) as new_prod
                             from zsz.crd_new_prod
                             where basin in %s  and first_prod_date >= '2012-12-01' and first_prod_date < '2015-01-01' --and county = 'MIDLAND (TX)'
                             group by first_prod_date, first_prod_year, first_prod_month
                             order by 1
                             limit 1", BASIN_SET))[,2]




new_prod <- dbGetQuery(dev_base, sprintf("select first_prod_date, sum(new_prod)/%s as new_prod
                       from zsz.crd_new_prod
                       where basin in %s and first_prod_date >= '2012-12-01' and first_prod_date < '2015-01-01' --and county = 'MIDLAND (TX)'
                       group by first_prod_date, first_prod_year, first_prod_month
                       order by 1", first_new_prod, BASIN_SET))



new_prod$first_prod_date <- as.Date(new_prod$first_prod_date)

## prod

#prod <-  sqldf("select prod_date, sum(liq)/1000 as prod 
#               from permian
#                where entity_id in (select entity_id from entity)
#               group by prod_date")

#prod <-  plyr::ddply(PRODUCTION, 'prod_date', summarise, prod = sum(liq)/1000)

#prod <- dbGetQuery(base,"select date as prod_date from ei.ei_flat where seriesid = 'PET.MTTIMXX1.M' and date >= '2015-01-01' order by 1 ")

#updated_prod <- prod[prod$prod_date > max(hist_prod$prod_date),]


new_first_prod <- as.data.frame(matrix(nrow = 15, ncol = 2));
colnames(new_first_prod)<-c('prod_date', 'prod');



#----------------------------------------------------------------------------------------#
### Calculate the state average decline rate.
#monthly_prod = plyr::ddply(permian, .(prod_date, basin), summarise, basin_prod = sum(liq)/1000) %>>% as.data.table()
# Using the last five months production to calulate their weights.
#prod_subset = monthly_prod[(prod_date <= cutoff_date & prod_date >= as.character(format(as.Date(cutoff_date) - 28*5,'%Y-%m-01'))), ]
#prod_subset[, basin_prod := basin_prod/as.numeric(as.Date(format(as.Date(prod_date) + 32,'%Y-%m-01')) - as.Date(prod_date), units = c("days"))]
# Calculate the state total production
#state_total = plyr::ddply(prod_subset, .(prod_date), summarise, state_prod = sum(basin_prod))
#weight = sqldf("select a.*, round(basin_prod/state_prod,6) weight from prod_subset a, state_total b
#               where a.prod_date = b.prod_date")
# Using the last five months' weights to calculate the average weight.
#avg_weight = plyr::ddply(weight, .(basin), summarise, avg_weight = mean(weight)) %>>%
#  as.data.table()

# using the average decline rate of last four years' (2012- 2015)
# to make forward projection for new production .
dcl_year_avg <- dcl[first_prod_year %in% c(2012:2015) & n_mth <= 20, .(avg = mean(avg)), by = .(basin, n_mth)]

dcl_weight_avg = dcl_year_avg
#basin_name_ = avg_weight$basin

#for(i in 1:length(basin_name_)){
#  dcl_weight_avg[basin == basin_name_[i], .(avg = avg*avg_weight[basin == basin_name_[i], avg_weight])]
#}

# calculate the weighted average decline rate for each first_prod_year and n_mth combination.
dcl_state_avg <- plyr::ddply(dcl_weight_avg, .(n_mth), summarise, avg = sum(avg))
dcl_state_avg <- as.data.table(dcl_state_avg)
setkey(dcl_state_avg, n_mth)

## prod from new wells and 15 month forward
test <- as.data.frame(prod[prod$prod_date > max(new_prod$first_prod_date),] )#
colnames(test) <- c("prod_date")


for (i in 1:20) {
  if(as.Date(format(as.Date(max(new_prod$first_prod_date))+32,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
  {
    break
  }
  if(as.Date(format(as.Date(max(new_prod$first_prod_date))+32,'%Y-%m-01')) <= as.Date(max(prod$prod_date)))
  {
    n = nrow(new_prod)
    
    fit <- as.data.frame(cbind(new_prod$new_prod[2:n], new_prod$new_prod[1:(n-1)], mvg_price[1:(n-1),]))
    
    colnames(fit) <- c( "new_prod", "new_prod_lag", "avg")
    
    lm <- lm(new_prod ~ -1 + new_prod_lag + avg , data = fit)
    
    #summary(lm)
    
    data <- as.data.frame(cbind( new_prod$new_prod[n], mvg_price[n,]))
    
    colnames(data) <- c( "new_prod_lag", "avg")
    
    #prod of new wells
    new_prod[n+1,1] <- as.character(format(as.Date(new_prod$first_prod_date[n]+32),'%Y-%m-01')) 
    new_prod[n+1,2] <- round(predict(lm, data),4)
    
    # new first month production
    new_first_prod[i,1] <- as.character(format(as.Date(new_prod$first_prod_date[n]+32),'%Y-%m-01')) 
    new_first_prod[i,2] <- round(predict(lm, data)*first_new_prod,4)
    
    
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
        
        dcl_factor <- 10^(dcl_state_avg[n_mth == (j + 1), avg]/100)
        temp[m+1, 2] <- round((1 + temp$prod[m]) * dcl_factor - 1,0)
        
        
      }
    }
    
    #temp$prod_date <- as.Date(temp$prod_date)
    sql_query <- sprintf("select a.*, coalesce(b.prod, 0)  as prod%s
                         from test a left join temp b on a.prod_date = b.prod_date", i)
    
    test <- sqldf(sql_query)
    

  }
}

new_first_prod



