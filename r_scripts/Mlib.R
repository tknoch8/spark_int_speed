library(tidyverse)
library(sparklyr)
library(broom)

config <- spark_config()
config$spark.executor.cores <- 24
config$spark.executor.memory <- "100G"
sc <- spark_connect(master = "local", config = config, version = '3.1.1')

# sc <- spark_connect(master = "local")

dat <- spark_read_parquet(
  sc,
  name = "q3_2020_perf_mobile",
  path = "2020-q1/2020-01-01_performance_mobile_tiles.parquet",
  # options = list(),
  repartition = 0,
  memory = TRUE,
  overwrite = TRUE
  # columns = NULL,
  # schema = NULL,
  # ...
)

dat %>% 
  glimpse()

dat %>% 
  sdf_nrow()

dat %>% 
  sdf_sample(fraction = 0.01) %>% 
  collect() %>% 
  ggplot(aes(avg_d_kbps, devices, size = tests)) +
  scale_x_continuous(trans = "log10") +
  geom_point()

library(tidymodels)

dat <- dat %>% 
  mutate(one_device = if_else(devices == 1, 1L, 0L)) %>% 
  select(-devices)

partitions <- dat %>%
  sdf_random_split(training = 0.7, test = 0.3, seed = 8)

df_training <- partitions$training
df_test <- partitions$test

rf_model <- df_training %>%
  ml_random_forest(one_device ~ ., type = "classification")

pred <- ml_predict(rf_model, iris_test)

ml_multiclass_classification_evaluator(pred)




