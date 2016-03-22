library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(kimisc)
library(tseries)
library(forecast)

pgsql <- JDBC("org.postgresql.Driver", "C:/postgresql-9.2-1003.jdbc4.jar", "`")

base<-dbConnect(pgsql, "jdbc:postgresql://ec2-54-204-4-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",user="u9dhckqe2ga9v1",password="pa49dck9aopgfrahuuggva497mh")

dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-107-22-244-132.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",user="u2bkiiv8j7scg0",password="p2kn6vk7k2jaqn2haqikl7hpbk5")

options("scipen"=100)
options(stringsAsFactors = F)

#########################################################################################################################

print(Sys.time())

## decline rate for all basins
dcl_all <- dbGetQuery(base, "select * from dev.zsz_crd_dcl")

#dcl_all  <- dbGetQuery(dev_base, "select * from zsz.crd_prod_dcl_log")

## distinct first production year
first_prod_year <- sqldf("select distinct first_prod_year from dcl_all order by 1")

## distinct basins
basin_all <- sqldf("select distinct basin from dcl_all order by 1")

## max decline rate for each basin each year
basin_max_mth_table <- sqldf("select basin, first_prod_year, max(n_mth) as max, max(n_mth) + 15 as max_new
                        from dcl_all
                        group by basin, first_prod_year
                        order by 1, 2")
## latest_prod_year
latest_year <- dbGetQuery(base, "select extract(year from date_trunc('month', current_date - interval '3 month')::DATE)")

## latest_prod_month
latest_mth <- dbGetQuery(base, "select extract(month from date_trunc('month', current_date - interval '3 month')::DATE)")

#create a table of all # of mth of prod for each start year, and basin
mths <- as.data.frame(matrix(nrow = 0, ncol = 3));

#loop through basin
for (i in (1:nrow(basin_all))) {
  
  #loop through first_prod_year
  for (j in (1: nrow(first_prod_year))) {
    t <- as.numeric((latest_year - first_prod_year$first_prod_year[j])*12 + latest_mth)
    mths <- rbind(mths, cbind(rep(basin_all$basin[i], t-1), rep(first_prod_year$first_prod_year[j], t-1), c(2:t)))
    
  }
}

colnames(mths)<-c('basin', 'first_prod_year', 'n_mth');
mths$first_prod_year <- as.numeric(mths$first_prod_year)
mths$n_mth <- as.numeric(mths$n_mth)

## calculate max n_mth for each basin & start prod year
basin_max_mth <- sqldf("select basin, first_prod_year, max(n_mth) as max, max(n_mth) + 15 as new_max
                        from mths
                        group by basin, first_prod_year
                        order by 1, 2")


##missing decline rate
missing_dcl <- sqldf("select a.*
                      from mths a 
                      left join dcl_all b on a.basin = b.basin and a.first_prod_year = b.first_prod_year and a.n_mth = b.n_mth 
                      where b.basin is null
                      order by 1, 2, 3")


for (i in (1: nrow(missing_dcl))) {
  
  n <- nrow(dcl_all)
  dcl_all[n+1, 1] <- missing_dcl[i,1]
  dcl_all[n+1, 2] <- missing_dcl[i,2]
  dcl_all[n+1, 3] <- missing_dcl[i,3] 
  
  sql <- sprintf("select avg(avg) as avg
                  from dcl_all
                  where basin = '%s' and first_prod_year = '%s' and n_mth > '%s' - 7 and n_mth <= '%s' - 1 ", missing_dcl[i,1], missing_dcl[i,2], missing_dcl[i,3], missing_dcl[i,3])
  
  dcl_all[n+1, 4] <- sqldf(sql)
}



## most recent 12 month avg decline rate for each basin, each year
dcl_all_avg12 <- sqldf("select a.basin, a.first_prod_year, case when avg(avg) >= 0 then 0 else avg(avg) end as avg 
                        from dcl_all a left join basin_max_mth b on a.basin = b.basin and a.first_prod_year = b.first_prod_year
                        where a.n_mth > b.max - 25 and a.n_mth != b.max
                        group by a.basin, a.first_prod_year")



## loop through all basins
for (k in (1:nrow(basin_all))) {
  
  basin <- basin_all[k,]
  
  ## first prod year with less than 36 months produced
  replace <- basin_max_mth[basin_max_mth$basin == basin & basin_max_mth$max < 36,]
  
  ## forward 15 month
  for (i in (1:nrow(first_prod_year))) {
    
    temp <- dcl_all[dcl_all$basin == basin & dcl_all$first_prod_year == first_prod_year[i,],]
    #first prod year in temp
    year <- temp$first_prod_year[1]
    #max mth produced in temp
    m <- max(temp$n_mth)
    #latest dcl in temp
    max_mth_avg <- temp$avg[temp$n_mth == m]
    
    if (year %in% replace$first_prod_year) {
      
      sql <- sprintf("select basin, first_prod_year + 1 as first_prod_year, n_mth, avg 
                      from dcl_all
                      where basin = '%s' and first_prod_year = '%s' - 1 and n_mth > '%s' and n_mth <= '%s'", basin, year, replace$max[replace$first_prod_year == year], replace$new_max[replace$first_prod_year == year])
      dcl_all <- rbind(dcl_all, sqldf(sql))
      
    } else {
      for (j in 1:15) {
        n <- nrow(dcl_all)
        dcl_all[n+1, 1] <- temp$basin[1]
        dcl_all[n+1, 2] <- temp$first_prod_year[1]
        dcl_all[n+1, 3] <- max(temp$n_mth) + j
        if(max_mth_avg < 0) {
          dcl_all[n+1, 4] <- temp$avg[temp$n_mth == m]
        } else {
          dcl_all[n+1, 4] <- dcl_all_avg12$avg[dcl_all_avg12$basin == basin & dcl_all_avg12$first_prod_year == year]
        }
      }
    }
  }
}




print(Sys.time())

