rm(list = ls())

library(readxl)
library(tidyverse)
library(ggpubr)
library(gghighlight)
library(snakecase)

setwd("/Users/crcp/wcs_analysis/wcs_kcube")

# function to eliminate outliers
remove_outliers <- function(x){
  lb <- quantile(x, 0.25) - ((quantile(x, 0.75) - quantile(x, 0.25))*1.5)
  ub <- quantile(x, 0.75) + ((quantile(x, 0.75) - quantile(x, 0.25))*1.5)
  return(x > lb & x < ub)
}

keep_outliers <- function(data, col_name){
  vec <- data[,col_name] %>% as.vector() %>% unlist()
  lb <- quantile(vec, 0.25) - ((quantile(vec, 0.75) - quantile(vec, 0.25))*1.5)
  ub <- quantile(vec, 0.75) + ((quantile(vec, 0.75) - quantile(vec, 0.25))*1.5)
  return(vec > lb & vec < ub)
}

# Landing sites (North - South)
landing_sites <- c("Kijangwani", "Kinuni", "Kuruwitu", "Vipingo", "Bureni", "Msumarini",
                   "Kanamai", "Mtwapa", "Marina", "Kenyatta", "Reef", "Nyali", "Waa", "Tiwi",
                   "Tradewinds", "Mwaepe", "Mvuleni", "Mwanyaza", "Mgwani", "Gazi",
                   "Chale", "Mwandamu", "Mkunguni", "Mwaembe", "Munje", "Kibuyuni",
                   "Shimoni", "Wasini", "Vanga", "Mkwiro", "Jimbo")

olddata  <- read_excel("/Users/crcp/wcs_analysis/raw_data/WCS_LegacyData1995_2022.xlsx", sheet = "Data1995_2022_cleaned")
newdata1 <- read_excel("/Users/crcp/wcs_analysis/raw_data/Fishlanding_2021_2022_Collect.xlsx")
newdata2 <- read_excel("/Users/crcp/wcs_analysis/raw_data/Data2021_2024.xlsx")

olddata <- data.frame(lapply(olddata, function(v) {
  if (is.character(v)) return(to_sentence_case(v))
  else return(v)
}), stringsAsFactors = FALSE)

newdata1 <- data.frame(lapply(newdata1, function(v) {
  if (is.character(v)) return(to_sentence_case(v))
  else return(v)
}), stringsAsFactors = FALSE)

newdata2 <- data.frame(lapply(newdata2, function(v) {
  if (is.character(v)) return(to_sentence_case(v))
  else return(v)
}), stringsAsFactors = FALSE)

olddata  <- unique(olddata)
newdata1 <- unique(newdata1)
newdata2 <- unique(newdata2)

# Rename the columns in a way that mirrors the new data columns
olddata <- olddata %>%
  rename( "Gear.type" = "Gear",
          "Gear.new" = "Gear.new",
          "Weight_kg" = "catch.kg")

#Match categories between between datasets (standardize them)
olddata$Gear.new      <- recode(olddata$Gear.new, Net = "Nets", Spear = "Speargun", Trap = "Traps")
olddata$Fish.category <- recode(olddata$Fish.category, Pelagic = "Pelagics")
olddata$New.mngt      <- recode(olddata$New.mngt, `Beach seine and ringnet` = "Beachseine and ringnet")
newdata1$Gear.new     <- recode(newdata1$Gear.new, `Fence trap` = "Fencetrap", Net = "Nets", 
                                Spear = "Speargun", Trap = "Traps")
newdata1$Fish.category<- recode(newdata1$Fish.category, Ray = "Rays", `Mixed catch` = "Rest of catch", 
                                Pelagics = "Pelagic", Shark = "Sharks")
newdata2$Gear.new     <- recode(newdata2$Gear.new, `Fence trap` = "Fencetrap", Net = "Nets", 
                                Spear = "Speargun", Trap = "Traps")
newdata2$Fish.category<- recode(newdata2$Fish.category, `Mixed catch` = "Rest of catch")


#Exclude
olddata <- olddata %>%
  filter(Gear.new != "Fencetrap", Fish.category != "0", Fish.category != "NA", Fish.category != "Lobster", Fish.category != "Pelagics",
         New.mngt != "NA", New.mngt != "Pre community closure", New.mngt != "Pre community closure kikadini", Mngt != "Munje",
         Site != "Msanakani", Site != "Tiwi", Site != "Waa")

newdata1 <- newdata1 %>%
  filter(Gear.type != "Ringnet", Gear.new != "Fencetrap", Gear.new != "Longline", Fish.category != "Pelagic",
         Fish.category != "Lobster", Fish.category != "Rays", Fish.category != "Sharks",
         Site != "Msanakani", Site != "Tiwi", Site != "Waa") 

newdata2 <- newdata2 %>%
  filter(Gear.type != "Ringnet", Gear.new != "Fencetrap", Gear.new != "Longline", Fish.category != "Pelagics",
         Fish.category != "Lobster", Fish.category != "Ray", Fish.category != "Shark",
         Site != "Msanakani", Site != "Tiwi", Site != "Waa")

# Combine the new datasets together
newdata <- rbind(newdata1, newdata2)

# Check Sites duplicated for management types
# Essentially, each site is pegged to it's own form of management
# The errors found were corrected manually in a copy of the original dataset and reloaded.
# The dataset you may have may bring such cases so you can request our version of the dataset and then 
# you'll take note
# Also this function has the ability to check anything unique tagged to a site, e.g Fishing Areas
check_management <- function(data, cols, ...){
  data <- unique(data[,cols]) %>%
    group_by(...) %>%
    mutate(n = n()) %>%
    ungroup()
  ifelse(data$n > 1, return(data %>% subset(n > 1)), return("OK, No Problem!"))
}

check_management(olddata, c("Site", "New.mngt"), Site)
check_management(newdata, c("Site", "New.mngt"), Site)

check_management(olddata, c("Site", "Mngt"), Site)
check_management(newdata, c("Site", "Mngt"), Site)

###  Pooling Gear Data
# Aggregate the old data -----
geardata_old <- olddata %>%
  filter(`Weight_kg` > 0.1) %>%
  # Day, Month, Year, Site, Seascape, New.mngt, Mngt, New.Fishing.Areas, Gear.type, Gear.new
  group_by(Year, Month, Day, Site, New.Fishing.Areas, Seascape, New.mngt, Mngt, Gear.type, Gear.new) %>%
  summarise(total_catch_agg = sum(`Weight_kg`),
            total_catch = mean(Total.catch),
            fishers = mean(`No..of.fishers`),
            boats = mean(No..of.boats, na.rm = T),
            price = mean(Price), .groups = "drop")  %>% ungroup()

# Aggregate the new data
## Pool by total catch as the second-most granular level
# In this case the price aggregation starts by weighting by the fish category weight.
geardata_new_pool_by_total_catch <- newdata %>%
  filter(Weight_kg > 0.1) %>%
  group_by(Date, Day, Month, Year, Site, Seascape, New.mngt, Mngt, Gear.type, Gear.new, New.Fishing.Areas, Total.catch..kg.) %>%
  summarise(total_catch_agg = sum(Weight_kg),
            fishers = mean(No..of.fishers),
            boats = mean(No..of.boats, na.rm = T),
            price = sum(price * Weight_kg) / sum(Weight_kg)) %>% 
  mutate(fishers = round(fishers,0))  %>% ungroup()

## The pooling follows by gear
geardata_new <- geardata_new_pool_by_total_catch %>%
  group_by(Year, Month, Day, Site, `New.Fishing.Areas`, Seascape, `New.mngt`, `Mngt`, `Gear.type`, `Gear.new`) %>%
  summarise(total_catch = mean(Total.catch..kg.),
            total_catch_agg = sum(total_catch_agg),
            fishers = sum(fishers),
            boats = sum(boats),
            price = mean(price), .groups = "drop")  %>% ungroup()

# Perform a row bind (combine both old and new)
gear_day <- rbind(geardata_old, geardata_new) %>%
  mutate(# Add floored date column
    year_month = as.Date(paste(as.character(Year), as.character(Month), "1", sep = "-"), 
                         format = "%Y-%m-%d"),
    Site = factor(Site, levels = landing_sites))  %>% ungroup()

# Check if Areas match with the sites, 
# Reef will have two distinct areas one starts from 2014 when it merged with Msanakani
# Again if you're using the original dataset, you'll get a few others. Ask for the cleaner copy to make comparisons
gear_day[,c("Site", "New.Fishing.Areas")] %>% 
  unique() %>%
  group_by(Site) %>% mutate(n = n()) %>%
  ungroup() %>% filter(n > 1)

# Ommiting gear level, pool the catch data to daily level
catch_day <- gear_day %>%
  group_by(year_month, Day, Month, Year, Site, Seascape, New.mngt, Mngt, New.Fishing.Areas) %>%
  summarise(fishers = sum(fishers),
            boats = mean(boats),
            catch = sum(total_catch_agg),
            price = mean(price), .groups = "drop") %>%
  mutate(effort = fishers / New.Fishing.Areas,
         cpue = (catch / fishers),# / New.Fishing.Areas,
         cpua = catch / New.Fishing.Areas,
         income = cpue * price) %>%
  na.omit() %>%
  # Remove fishers and catch outliers
  group_by(Site) %>%
  filter(fishers != 0, remove_outliers(effort), remove_outliers(cpua), remove_outliers(cpue)) %>%
  ungroup()
#View(catch_day)
# Check if catch day duplicated
catch_day[duplicated(catch_day %>% select(Day, Month, Year, year_month, Seascape, Site)),]

# Splitting the month into two
catch_day$bi_month <- paste(catch_day$Month, as.character(ifelse(catch_day$Day > 15, 2, 1)), sep = "-")
gear_day$bi_month <- paste(gear_day$Month, as.character(ifelse(gear_day$Day > 15, 2, 1)), sep = "-")

# Pool gear data by the month
gear_month <- gear_day %>%
  group_by(year_month, Month, Year, New.Fishing.Areas, Gear.type, Gear.new) %>%
  summarise(fishers = mean(fishers),
            boats = mean(boats),
            catch = mean(total_catch_agg),
            price = mean(price), .groups = "drop") %>%
  mutate(effort = fishers / New.Fishing.Areas,
         cpue = (catch / fishers),# / New.Fishing.Areas,
         cpua = catch / New.Fishing.Areas,
         income = cpue * price,
         fisher_days = effort*(220/12)) %>%
  na.omit() %>%
  group_by(Gear.new) %>%
  filter(fishers != 0, remove_outliers(effort), remove_outliers(cpua), remove_outliers(cpue)) %>%
  ungroup()  %>% ungroup()

# Pool by Site and month
site_month <- catch_day %>%
  group_by(year_month, Month, Year, Site, Seascape, New.mngt, Mngt, New.Fishing.Areas) %>%
  summarise(effort = mean(effort),
            cpue = mean(cpue),
            cpua = mean(cpua),
            income=mean(income),
            price = mean(price),
            fishers = mean(fishers),
            boats = mean(boats)) %>%
  mutate(fisher_days = ifelse(Seascape == "Fringing", effort*(220/12), effort*(210/12)))  %>% ungroup()


# Export the olddata and newdata
write.csv(olddata, "/Users/crcp/wcs_analysis/proc_data/WCS_LegacyData1995_2022.csv", row.names=FALSE)
write.csv(newdata, "/Users/crcp/wcs_analysis/proc_data/Data2021_2024.csv", row.names=FALSE)
write.csv(gear_day, "/Users/crcp/wcs_analysis/proc_data/at_gear_level_daily.csv", row.names=FALSE)
write.csv(catch_day, "/Users/crcp/wcs_analysis/proc_data/at_site_level_daily.csv", row.names=FALSE)
write.csv(site_month, "/Users/crcp/wcs_analysis/proc_data/at_site_level_month_daily_means.csv", row.names=FALSE)
write.csv(gear_month, "/Users/crcp/wcs_analysis/proc_data/at_gear_level_month_daily_means.csv", row.names=FALSE)






