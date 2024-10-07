#rm(list = ls())

library(readxl)
library(tidyverse)
library(ggpubr)
library(gghighlight)
library(snakecase)

setwd("/Users/crcp/wcs_analysis/wcs_kcube")
# Sites from North to South
landing_sites <- c("Kijangwani", "Kinuni", "Kuruwitu", "Vipingo", "Bureni", "Msumarini",
                   "Kanamai", "Mtwapa", "Marina", "Kenyatta", "Reef", "Nyali", "Waa", "Tiwi",
                   "Tradewinds", "Mwaepe", "Mvuleni", "Mwanyaza", "Mgwani", "Gazi",
                   "Chale", "Mwandamu", "Mkunguni", "Mwaembe", "Munje", "Kibuyuni",
                   "Shimoni", "Wasini", "Vanga", "Mkwiro", "Jimbo")

catch_day <- read.csv("/Users/crcp/wcs_analysis/proc_data/at_site_level_daily.csv")
site_month <- read.csv("/Users/crcp/wcs_analysis/proc_data/at_site_level_month_daily_means.csv")
cpi <- read.csv("/Users/crcp/wcs_analysis/raw_data/consumer_price_indexWB.csv")
site_month$year_month <- as.POSIXct(site_month$year_month)
#Adjust revenue for inflation
## CPI Data available is until 2023, forecast CPI value for 2024
cpi.2023 <- cpi[cpi$year <= 2023,]
cpi.lm <- lm(cpi ~ poly(t, 3), data = cpi.2023) # Model
## Assign the forecast to original CPI data
cpi[cpi$year == 2024, "cpi"] <- predict(cpi.lm, newdata = data.frame(t = 65))
cpi$ratio.cpi <- cpi$cpi[cpi$year == 2024] / cpi$cpi

## Plot predictions
plot(x = cpi$year, y = cpi$cpi, type = "l", pch=19, col = "black", xlab = "Time", ylab = "CPI")
lines(x = cpi$year, y = unname(predict(cpi.lm, newdata = data.frame(t = cpi$t))), type = "l", pch=18, col = "blue", lty=2)
points(x = 2024, y = predict(cpi.lm, newdata = data.frame(t = 65))[[1]], pch = 7, col = "red")
legend("topleft", legend = c("Actual CPI", "Forecasted CPI"), col = c("black", "blue"), lty=1:2, cex = 0.7)
legend("bottomright", legend = c("Forecasted CPI 2024"), col = "red", pch = 7, cex = 0.7)

# Compute the adjusted revenue
site_month <- site_month %>%
  merge(cpi[,c("year", "ratio.cpi", "cpi")], by.x = "Year", by.y = "year") %>%
  mutate(price_adj = price * ratio.cpi, # Adjust the price
         revenue = price * cpua, 
         revenue_adj = price_adj * cpua, # Adjusted revenue per km^2
         Site = factor(Site, levels = landing_sites)) 

# EFFORT
ggplot(site_month, aes(x = year_month, y = effort)) +
  geom_point(size = 1, color = "black") + 
  geom_line(linewidth = 0.6, color = "skyblue") +
  facet_wrap(~Site, ncol = 4)  +
  labs(y = expression(bold("Effort (fishers/"~km^2~"/day)")), x = "Time (Year, Month)", title = "Effort vs. time") +
  theme_bw() +
  theme(text = element_text(size = 14, face = "bold"),
        axis.title = element_text(face = "bold"))
ggsave("/Users/crcp/wcs_analysis/outputs/lore_outputs/effortXtime.pdf", width = 14, height = 12)

#-- CPUE
ggplot(site_month, aes(x = year_month, y = cpue)) +
  geom_point(size = 1, color = "black") + 
  geom_line(linewidth = 0.6, color = "skyblue") +
  facet_wrap(~Site, ncol = 4)  +
  labs(y = expression(bold("CPUE (kg/fishers/day)")), x = "Time (Year, Month)", title = "Catch per unit effort (CPUE) vs. time") +
  theme_bw() +
  theme(text = element_text(size = 14, face = "bold"), axis.title = element_text(face = "bold"))
ggsave("/Users/crcp/wcs_analysis/outputs/lore_outputs/cpueXtime.pdf", width = 14, height = 12)


# YIELD
ggplot(site_month, aes(x = year_month, y = cpua)) +
  geom_point(size = 1, color = "black") + 
  geom_line(linewidth = 0.6, color = "skyblue") +
  facet_wrap(~Site, ncol = 4)  +
  labs(y = expression(bold("Yield or CPUA (kg/"~km^2~"/day)")), x = "Time (Year, Month)", title = "Catch per unit area (CPUA) vs. time") +
  theme_bw() +
  theme(text = element_text(size = 14, face = "bold"), axis.title = element_text(face = "bold"))
ggsave("/Users/crcp/wcs_analysis/outputs/lore_outputs/cpua_yieldXtime.pdf", width = 14, height = 12)

# Income
ggplot(site_month, aes(x = year_month, y = income)) +
  geom_point(size = 1, color = "black") + 
  geom_line(linewidth = 0.6, color = "skyblue") +
  facet_wrap(~Site, ncol = 4)  +
  labs(y = expression(bold("Income (ksh/fisher/day)")), x = "Time (Year, Month)", title = "Income vs. time") +
  theme_bw() +
  theme(text = element_text(size = 14, face = "bold"), axis.title = element_text(face = "bold"))
ggsave("/Users/crcp/wcs_analysis/outputs/lore_outputs/incomeXtime.pdf", width = 14, height = 12)

# Revenue
ggplot(site_month, aes(x = year_month, y = revenue)) +
  geom_point(size = 1, color = "black") + 
  geom_line(linewidth = 0.6, color = "skyblue") +
  facet_wrap(~Site, ncol = 4)  +
  labs(y = expression(bold("Revenue (ksh/"~km^2~"/day)")), x = "Time (Year, Month)", title = expression(bold("Revenue/"~km^2~" vs. time")))+
  theme_bw() +
  theme(text = element_text(size = 14, face = "bold"), axis.title = element_text(face = "bold"))
ggsave("/Users/crcp/wcs_analysis/outputs/lore_outputs/revenueXtime.pdf", width = 14, height = 12)

# Revenue adjusted for  2024 CPI
ggplot(site_month, aes(x = year_month, y = revenue_adj)) +
  geom_point(size = 1, color = "black") + 
  geom_line(linewidth = 0.6, color = "skyblue") +
  facet_wrap(~Site, ncol = 4)  +
  labs(y = expression(bold("Revenue (ksh/"~km^2~"/day)")), x = "Time (Year, Month)", 
       title = expression(bold("Revenue adjusted for inflation (2024 cost of living)/"~km^2~" vs. time")))+
  theme_bw() +
  theme(text = element_text(size = 14, face = "bold"), axis.title = element_text(face = "bold"))
ggsave("/Users/crcp/wcs_analysis/outputs/lore_outputs/revenue_adjXtime.pdf", width = 14, height = 12)




