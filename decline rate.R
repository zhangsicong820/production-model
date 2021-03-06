library(rJava)
library(RJDBC)
library(sqldf)
library(plyr)
library(kimisc)
library(tseries)
library(forecast)

pgsql <- JDBC("org.postgresql.Driver", "C:/postgresql-9.2-1003.jdbc4.jar", "`")


base<-dbConnect(pgsql, "jdbc:postgresql://ec2-54-204-4-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",
                user="u9dhckqe2ga9v1",password="pa49dck9aopgfrahuuggva497mh")


dev_base <- dbConnect(pgsql, "jdbc:postgresql://ec2-23-21-136-247.compute-1.amazonaws.com:5432/d43mg7o903brjv?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&",
                      user="uc52v8enn6qvod",
                      password="p36h60963tc5mbf1td3ta08ufa0")


options("scipen"=100)
options(stringsAsFactors = F)

#########################################################################################################################

print(Sys.time())

## decline rate for all basins
dcl_all <- dbGetQuery(dev_base, "select * from zxw.crd_prod_log_dcl_1977 order by 1,2,3 ")

#out of sample
#dcl_all <- dbGetQuery(dev_base, "with t0 as (
#                      select * 
#                      from zxw.crd_prod_log_dcl_1977
#                      where first_prod_year != 2015),
#                      
#                      t1 as (
#                      select basin, first_prod_year, max(n_mth) as max
#                      from t0
#                      group by basin, first_prod_year
#                      order by 1, 2)
                      
#                      select distinct a.*
#                      from t0 a join t1 b on a.first_prod_year = b.first_prod_year
#                      where a.n_mth < b.max - 12
#                      order by 2,3")

## distinct first production year
first_prod_year <- sqldf("select distinct basin, first_prod_year from dcl_all where first_prod_year >= 1980 order by 1, 2")

## distinct basins
basin_all <- sqldf("select distinct basin from dcl_all order by 1")

## max decline rate for each basin each year
basin_max_mth_table <- sqldf("select basin, first_prod_year, max(n_mth) as max, max(n_mth) + 20 as max_new
                        from dcl_all
                        --where first_prod_year >= 1980
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
  temp <- first_prod_year[first_prod_year$basin == basin_all[i,],]
  #loop through first_prod_year
  for (j in (1: nrow(temp))) {
    t <- basin_max_mth_table$max[basin_max_mth_table$basin == basin_all[i,] & basin_max_mth_table$first_prod_year == temp$first_prod_year[j]]
    #as.numeric((latest_year - temp$first_prod_year[j])*12 + latest_mth)
    mths <- rbind(mths, cbind(rep(basin_all$basin[i], t-1), rep(temp$first_prod_year[j], t-1), c(2:t)))
    
  }
}

colnames(mths)<-c('basin', 'first_prod_year', 'n_mth');
mths$first_prod_year <- as.numeric(mths$first_prod_year)
mths$n_mth <- as.numeric(mths$n_mth)

## calculate max n_mth for each basin & start prod year
#basin_max_mth <- sqldf("select basin, first_prod_year, max(n_mth) as max, max(n_mth) + 15 as new_max
#                        from mths
#                        group by basin, first_prod_year
#                        order by 1, 2")


##missing decline rate
missing_dcl <- sqldf("select a.*
                      from mths a 
                      left join dcl_all b on a.basin = b.basin and a.first_prod_year = b.first_prod_year and a.n_mth = b.n_mth 
                      where b.basin is null
                      order by 1, 2, 3")

if (nrow(missing_dcl) == 0) {
  print("No missing decline rate")
}else {
  for (l in (1: nrow(missing_dcl))) {
    
    n <- nrow(dcl_all)
    dcl_all[n+1, 1] <- missing_dcl[l,1]
    dcl_all[n+1, 2] <- missing_dcl[l,2]
    dcl_all[n+1, 3] <- missing_dcl[l,3] 
    
    sql <- sprintf("select avg(avg) as avg
                  from dcl_all
                  where basin = '%s' and first_prod_year >= '%s' - 3 and n_mth = '%s'", missing_dcl[l,1], missing_dcl[l,2], missing_dcl[l,3])
    
    dcl_all[n+1, 4] <- sqldf(sql)
  }
  
}




## loop through all basins
for (k in (1:nrow(basin_all))) {
  
  basin <- basin_all[k,]
  
  ## all first prod year for basin
  years <- first_prod_year$first_prod_year[first_prod_year$basin == basin]
  

  ## forward 15 month
  for (h in (1:length(years))) {
    
    temp <- dcl_all[dcl_all$basin == basin & dcl_all$first_prod_year == years[h],]
    #first prod year in temp
    year <- temp$first_prod_year[1]
    #max mth produced in temp
    m <- max(temp$n_mth)

    
    sql <- sprintf("select basin, '%s' as first_prod_year, n_mth, avg(avg) as avg
                    from dcl_all
                    where basin = '%s' and first_prod_year >= ('%s' - 3) and first_prod_year < '%s' and n_mth > '%s' and n_mth <= '%s'
                    group by basin, n_mth
                    order by 1,2,3",year, basin, year, year, m, m + 20)
    
    dcl_ext <- sqldf(sql)
    
    if (nrow(dcl_ext) < 20) {
      sql1 <- sprintf("select basin, first_prod_year, n_mth + 24 as n_mth, avg
                     from dcl_all
                     where basin = '%s' and first_prod_year = '%s' and n_mth > '%s' and n_mth <= '%s'
                     group by basin, n_mth
                     order by 1,2,3", basin, year, m - 24, m )
      
      dcl_ext1 <- sqldf(sql1)
      
      dcl_all <- rbind(dcl_all, dcl_ext1)
    }else {
      dcl_all <- rbind(dcl_all, dcl_ext)
    }
    
    
  
    
  }
}





print(Sys.time())

