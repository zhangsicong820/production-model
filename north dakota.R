library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(kimisc)



pgsql <- JDBC("org.postgresql.Driver", "C:/postgresql-9.2-1003.jdbc4.jar", "`")

base<-dbConnect(pgsql, "jdbc:postgresql://ec2-54-204-4-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",user="u9dhckqe2ga9v1",password="pa49dck9aopgfrahuuggva497mh")

#dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-107-20-245-110.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",user="ubp42sjidmq53r",password="p4h3atl4j4ekqc9vmgtcrrku8as")

options("scipen"=100)
#############################################################################################################################################################


## entity with missing date
ndakota <- dbGetQuery(base, "select * from dev.zsz_nd_dec")

ndakota <- mutate(ndakota, comment = "")

ndakota$last_prod_date <- as.Date(ndakota$last_prod_date)

ndakota$prod_date <- as.Date(ndakota$prod_date)


## prod dcl fro williston basin

dcl <- dbGetQuery(base, "select * from dev.zsz_williston_dcl_dec")
max_dcl_year <- max(dcl$first_prod_year)
max_dcl_mth <- max(dcl$n_mth[dcl$first_prod_year == max_dcl_year])

temp_dcl <- dcl[dcl$first_prod_year == (max_dcl_year - 1) & dcl$n_mth > max_dcl_mth, ]
temp_dcl$first_prod_year <- max_dcl_year

dcl <- rbind(dcl, temp_dcl)



## check zeros

zero <- sqldf("select entity_id 
              from ndakota 
              where last_prod_date = prod_date and liq = 0")


for (i in 1:nrow(zero)) {
  #choose prod data for past 6 month
  temp <- ndakota[ndakota$entity_id == zero$entity_id[i],]
  entity_id <- temp$entity_id[1]
  basin <- temp$basin[1]
  
  if (temp$first_prod_year[1] < 1980) {
    first <- 1980 } else {
      first <- temp$first_prod_year[1]
    }
  
  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- max(dcl$n_mth[dcl$basin == basin & dcl$first_prod_year == first])
  
  ## Actual max month of production of the entity
  max_n_mth <- temp$n_mth[temp$prod_date == temp$last_prod_date[1]]
  
  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
    if (first == max(dcl$first_prod_year)) {
      dcl_mth <- basin_max_mth
    }
    
  } else {
    dcl_mth <- max_n_mth
  }
  
  
  
  
  if (temp$liq[temp$n_mth == max_n_mth - 1] == 0) { 
    
    if (temp$liq[temp$n_mth == max_n_mth - 2] == 0) {
      
      if (temp$liq[temp$n_mth == max_n_mth - 3] == 0) {
        ndakota$comment[(ndakota$entity_id == temp$entity_id)] <- "All Zeros"
      } else {
        ndakota$liq[(ndakota$n_mth == max_n_mth - 2) & ndakota$entity_id == entity_id] <- temp$liq[temp$n_mth == max_n_mth - 3] * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == (dcl_mth - 2) & dcl$basin == basin)]/100)
        ndakota$comment[(ndakota$n_mth == max_n_mth - 2) & ndakota$entity_id == entity_id] <- "Updated"
        
        ndakota$liq[(ndakota$n_mth == max_n_mth - 1) & ndakota$entity_id == entity_id] <- temp$liq[temp$n_mth == max_n_mth - 3] * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == (dcl_mth - 1) & dcl$basin == basin)]/100)
        ndakota$comment[(ndakota$n_mth == max_n_mth - 1) & ndakota$entity_id == entity_id] <- "Updated"
        
        ndakota$liq[((ndakota$n_mth == max_n_mth) & ndakota$entity_id == entity_id)] <- ndakota$liq[(ndakota$n_mth == max_n_mth - 1) & ndakota$entity_id == entity_id]  * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth & dcl$basin == basin)]/100)
        ndakota$comment[(ndakota$n_mth == max_n_mth & ndakota$entity_id == entity_id)] <- "Updated"
      }
      
      
    } else {
      
      ndakota$liq[(ndakota$n_mth == max_n_mth - 1) & ndakota$entity_id == entity_id] <- temp$liq[temp$n_mth == max_n_mth - 2] * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == (dcl_mth - 1) & dcl$basin == basin)]/100)
      ndakota$comment[(ndakota$n_mth == max_n_mth - 1) & ndakota$entity_id == entity_id] <- "Updated"
      
      ndakota$liq[((ndakota$n_mth == max_n_mth) & ndakota$entity_id == entity_id)] <- ndakota$liq[(ndakota$n_mth == max_n_mth - 1) & ndakota$entity_id == entity_id]  * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth & dcl$basin == basin)]/100)
      ndakota$comment[(ndakota$n_mth == max_n_mth & ndakota$entity_id == entity_id)] <- "Updated"
    }
    
  } else {
    
    ndakota$liq[((ndakota$n_mth == max_n_mth) & ndakota$entity_id == entity_id)] <- temp$liq[temp$n_mth == max_n_mth - 1] * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth & dcl$basin == basin)]/100)
    ndakota$comment[(ndakota$n_mth == max_n_mth & ndakota$entity_id == entity_id)] <- "Updated"
  }
}

## entity with missing data
missing <- sqldf("with t0 as (
                 select entity_id, avg(liq) as avg
                 from ndakota
                 group by entity_id)
                 
                 select * 
                 from ndakota
                 where entity_id in (select entity_id from t0 where avg >= 20) and prod_date = last_prod_date")

missing <- subset(missing, last_prod_date < '2015-11-01')

## Filling missing data ################################################################################################################################


for (i in 1:nrow(missing)) {
  temp <- missing[i,]
  entity_id <- temp$entity_id
  basin <- temp$basin
  
  
  if (temp$first_prod_year[1] < 1980) {
    first <- 1980 } else {
      first <- temp$first_prod_year
    }
  
  ## max month of production in basin where the entity is  and from the year that entity first start producing
  basin_max_mth <- max(dcl$n_mth[dcl$basin == basin & dcl$first_prod_year == first])
  
  ## Actual max month of production of the entity
  max_n_mth <- temp$n_mth[temp$prod_date == temp$last_prod_date[1]]
  
  ## find the max month of dcl
  if (max_n_mth >= basin_max_mth) {
    dcl_mth <- basin_max_mth
    
   
    for(j in 1:5)
    {
      if(as.Date(format(as.Date(temp$prod_date+32*j),'%Y-%m-01')) > as.Date('2015-11-01'))
      {
        break
      }
      if(as.Date(format(as.Date(temp$prod_date+32*j),'%Y-%m-01')) <= as.Date('2015-11-01'))
      {
        n = nrow(ndakota)
        ndakota[n+1, 1] <- entity_id
        ndakota[n+1, 2] <- basin
        ndakota[n+1, 3] <- temp$first_prod_year
        ndakota[ndakota$entity_id == entity_id, 4] <- as.character(format(as.Date(temp$last_prod_date+32*j),'%Y-%m-01'))
        ndakota[n+1, 4] <- as.character(format(as.Date(temp$last_prod_date+32*j),'%Y-%m-01'))
        ndakota[n+1, 5] <- temp$n_mth + j
        ndakota[n+1, 6] <- as.character(format(as.Date(temp$prod_date+32*j),'%Y-%m-01'))
        if(j == 1) {
          ndakota[n+1, 7] <- round(temp$liq * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth  & dcl$basin == basin)]/100),0)
        } else {
          ndakota[n+1, 7] <- round(ndakota$liq[n] * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth & dcl$basin == basin)]/100),0)
        }
        ndakota[n+1, 8] <- "Inserted"
      }
    }
    
  } else {
    dcl_mth <- max_n_mth 
    
    for(j in 1:5)
    {
      if(as.Date(format(as.Date(temp$prod_date+32*j),'%Y-%m-01')) > as.Date('2015-11-01'))
      {
        break
      }
      if(as.Date(format(as.Date(temp$prod_date+32*j),'%Y-%m-01')) <= as.Date('2015-11-01'))
      {
        n = nrow(ndakota)
        ndakota[n+1, 1] <- entity_id
        ndakota[n+1, 2] <- basin
        ndakota[n+1, 3] <- temp$first_prod_year
        ndakota[ndakota$entity_id == entity_id, 4] <- as.character(format(as.Date(temp$last_prod_date+32*j),'%Y-%m-01'))
        ndakota[n+1, 4] <- as.character(format(as.Date(temp$last_prod_date+32*j),'%Y-%m-01'))
        ndakota[n+1, 5] <- temp$n_mth + j
        ndakota[n+1, 6] <- as.character(format(as.Date(temp$prod_date+32*j),'%Y-%m-01'))
        if(j == 1) {
          ndakota[n+1, 7] <- round(temp$liq * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth + j & dcl$basin == basin)]/100),0)
        } else {
          ndakota[n+1, 7] <- round(ndakota$liq[n] * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth + j & dcl$basin == basin)]/100),0)
        }
        ndakota[n+1, 8] <- "Inserted"
      }
    }
  }
}



##project 15 month forward ######################################################################################################

for (i in 1:15) {
  forward <- sqldf("with t0 as (
                 select entity_id, max(n_mth) as max 
                   from ndakota 
                   where comment != 'All Zeros'
                   group by entity_id),
                   
                   t1 as (
                   select a.entity_id, avg(liq) as avg
                   from ndakota a join t0 b on a.entity_id = b. entity_id
                   where n_mth >= max - 6 and comment != 'All Zeros'
                   group by a.entity_id)
                   
                   select entity_id 
                   from t1
                   where avg >= 20")
  
  
  for (j in 1:nrow(forward)){
    temp <- ndakota[ndakota$entity_id == forward$entity_id[j]& ndakota$last_prod_date == ndakota$prod_date,]
    entity_id <- temp$entity_id
    basin <- temp$basin
    
    
    if (temp$first_prod_year[1] < 1980) {
      first <- 1980 } else {
        first <- temp$first_prod_year
      }
    
    ## max month of production in basin where the entity is  and from the year that entity first start producing
    basin_max_mth <- max(dcl$n_mth[dcl$basin == basin & dcl$first_prod_year == first])
    
    ## Actual max month of production of the entity
    max_n_mth <- temp$n_mth[temp$prod_date == temp$last_prod_date[1]]
    
    ## find the max month of dcl
    if (max_n_mth >= basin_max_mth) {
      dcl_mth <- basin_max_mth
      n = nrow(ndakota)
      ndakota[n+1, 1] <- entity_id
      ndakota[n+1, 2] <- basin
      ndakota[n+1, 3] <- temp$first_prod_year
      ndakota[ndakota$entity_id == entity_id, 4] <- as.character(format(as.Date(temp$last_prod_date+32),'%Y-%m-01'))
      ndakota[n+1, 4] <- as.character(format(as.Date(temp$last_prod_date+32),'%Y-%m-01'))
      ndakota[n+1, 5] <- temp$n_mth + 1
      ndakota[n+1, 6] <- as.character(format(as.Date(temp$prod_date+32),'%Y-%m-01'))
      ndakota[n+1, 7] <- round(temp$liq * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth  & dcl$basin == basin)]/100),0)
      ndakota[n+1, 8] <- "Inserted"
      
      
    } else {
      dcl_mth <- max_n_mth + 1
      n = nrow(ndakota)
      ndakota[n+1, 1] <- entity_id
      ndakota[n+1, 2] <- basin
      ndakota[n+1, 3] <- temp$first_prod_year
      ndakota[ndakota$entity_id == entity_id, 4] <- as.character(format(as.Date(temp$last_prod_date+32),'%Y-%m-01'))
      ndakota[n+1, 4] <- as.character(format(as.Date(temp$last_prod_date+32),'%Y-%m-01'))
      ndakota[n+1, 5] <- temp$n_mth + 1
      ndakota[n+1, 6] <- as.character(format(as.Date(temp$prod_date+32),'%Y-%m-01'))
      ndakota[n+1, 7] <- round(temp$liq * ( 1 + dcl$avg[(dcl$first_prod_year == first & dcl$n_mth == dcl_mth  & dcl$basin == basin)]/100),0)
      ndakota[n+1, 8] <- "Inserted"
    }
  }
}


## forward prediction for new production #########################################################################################################

# sales price
hist_price <- dbGetQuery(dev_base, "with t0 as (
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
                         where prod_type = 'OIL' and sale_date >= '2013-10-01'
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
                      where tradedate = (select max(tradedate) from nymex_nearby where product_symbol = 'CL') and product_symbol = 'CL'")

future_price <- as.data.frame(matrix(nrow = 11, ncol = 3));
colnames(future_price)<-c('year', 'month', 'avg');


##transform future price table
for (i in 1:11){
  
  if(f_price$month + i <= 12){
    future_price$year[i] <- f_price$year
    future_price$month[i] <- f_price$month + i
    future_price$avg[i] <- f_price[, (6+i)]
  } else {
    future_price$year[i] <- f_price$year + 1
    future_price$month [i]<- f_price$month + i - 12
    future_price$avg[i] <- f_price[, (6+i)]
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
new_prod <- dbGetQuery(dev_base, "select first_prod_date, extract('year' from first_prod_date) first_prod_year, 
                       extract('month' from first_prod_date) first_prod_month, 
                       round(sum(liq)/1000/(extract(days from (first_prod_date + interval '1 month' - first_prod_date))),0) as new_prod
                       from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                       where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 and state = 'ND' 
                       and first_prod_date < date_trunc('month', current_date)::DATE - interval '5 month' and first_prod_date = prod_date
                       group by 1,2,3
                       order by 1,2,3")

new_prod$first_prod_date <- as.Date(new_prod$first_prod_date)




## prod

hist_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                        from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                        where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 
                        and state = 'ND' and prod_date < date_trunc('month', current_date)::DATE - interval '5 month'
                        group by prod_date
                        order by 1")

hist_prod$prod_date <- as.Date(hist_prod$prod_date)

## forward prod from hist wells
prod <-  ddply(ndakota, 'prod_date', summarise, sum = sum(liq)/1000)

prod <- mutate(prod, prod = sum/as.numeric(as.Date(format(as.Date(prod_date+32),'%Y-%m-01')) - prod_date, units = c("days")))

updated_prod <- prod[prod$prod_date > max(hist_prod$prod_date),-2]

first_prod <- dbGetQuery(base, "select prod_date, round(sum(liq)/1000/(extract(days from (prod_date + interval '1 month' - prod_date))),0) as prod
                         from di.pden_desc a join di.pden_prod b on a.entity_id = b.entity_id
                         where liq_cum >0 and prod_date >= '2013-12-01' and ALLOC_PLUS IN ('Y','X') and liq >= 0 
                         and state = 'ND' and prod_date >= date_trunc('month', current_date)::DATE - interval '5 month' and prod_date = first_prod_date
                         group by prod_date
                         order by 1")

new_first_prod <- as.data.frame(matrix(nrow = 15, ncol = 2));
colnames(new_first_prod)<-c('prod_date', 'prod');

## prod from new wells and 15 month forward


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
    
    lm <- lm(new_prod ~ -1 + new_prod_lag + prod + avg, data = fit)
    
    #summary(lm)
    
    data <- as.data.frame(cbind(new_prod$new_prod[n], hist_prod$prod[n],Moving_Avg(price$avg, 3)[n,]))
    
    colnames(data) <- c("new_prod_lag", "prod", "avg")
    
    #prod of new wells
    new_prod[n+1,1] <- as.character(format(as.Date(new_prod$first_prod_date[n]+32),'%Y-%m-01')) 
    new_prod[n+1,2] <- new_prod$first_prod_year[n] 
    new_prod[n+1,3] <- new_prod$first_prod_month[n] + 1
    new_prod[n+1,4]<- round(predict(lm, data),0)
    
    # new first month production
    new_first_prod[i,1] <- as.character(format(as.Date(hist_prod$prod_date[n]+32),'%Y-%m-01'))
    new_first_prod[i,2] <- round(predict(lm, data),0)
    
    
    # update the updated production
    temp <- new_first_prod[i,]
    
    for (j in 1:20) {
      if(as.Date(format(as.Date(temp$prod_date)+32*j,'%Y-%m-01')) > as.Date(max(prod$prod_date)))
      {
        break
      }
      if(as.Date(format(as.Date(temp$prod_date)+32*j,'%Y-%m-01'))<= as.Date(max(prod$prod_date)))
      {
        m = nrow(temp)
        temp[m+1, 1] <- as.character(format(as.Date(temp$prod_date[j])+32,'%Y-%m-01'))
        
        if(j < max(dcl$n_mth[dcl$first_prod_year == max(dcl$first_prod_year)])) {
          temp[m+1, 2] <- round(temp$prod[m] * ( 1 + dcl$avg[dcl$first_prod_year == max(dcl$first_prod_year) & dcl$n_mth == (j+1)]/100),0)
        } else {
          temp[m+1, 2] <- round(temp$prod[m] * ( 1 + dcl$avg[dcl$first_prod_year == max(dcl$first_prod_year) & dcl$n_mth == max(dcl$n_mth[dcl$first_prod_year == max(dcl$first_prod_year)])]/100),0)
        }
      }
    }
    
    temp$prod_date <- as.Date(temp$prod_date)
    
    updated_prod <- sqldf("select a.prod_date, a.prod + coalesce(b.prod, 0) as prod
                          from updated_prod a left join temp b on a.prod_date = b.prod_date")
    
    
    ## new total production
    hist_prod[n+1,1] <- as.character(format(as.Date(hist_prod$prod_date[n]+32),'%Y-%m-01')) 
    hist_prod[n+1,2] <- round(updated_prod$prod[updated_prod$prod_date == hist_prod$prod_date[(n+1)]]
                              - if (length(first_prod$prod[first_prod$prod_date == hist_prod[n+1,1]]) == 0) {
                                0
                              } else {
                                first_prod$prod[first_prod$prod_date == hist_prod[n+1,1]]
                              }, 0)
    
  }
  
  
}








