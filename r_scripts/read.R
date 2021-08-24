library(tidyverse)
library(sparklyr)
# sparkR.session()
sc <- spark_connect(master = "local")

# q3_2020_perf_mobile <- spark_read_parquet(
#   sc,
#   name = "q3_2020_perf_mobile",
#   path = "2020-q3/2020-07-01_performance_mobile_tiles.parquet",
#   # options = list(),
#   repartition = 0,
#   memory = TRUE,
#   overwrite = TRUE
#   # columns = NULL,
#   # schema = NULL,
#   # ...
# )

# class(q3_2020_perf_mobile)

# sdf_nrow(q3_2020_perf_mobile)

##### combine parquet files #####

parqs <- list.files(pattern = "*mobile_tiles.parquet$", recursive = TRUE) %>% 
  as_tibble() %>% 
  rename(path = 1)

parqs %>% 
  mutate(dat = map(path, ~spark_read_parquet(sc, name = str_match(path, "[0-9]{4}-q[0-9]{1}\\/([0-9]{4}-[0-9]{2}-[0-9]{2}_performance_mobile_tiles).parquet")[,2]), path = .x))

read_parquet_multiple <- function(sc, paths) {
  spark_session(sc) %>% invoke("read") %>% invoke("parquet", as.list(paths))
}

read_parquet_multiple(
  sc,
  list.files(pattern = "*mobile_tiles.parquet$", recursive = TRUE)
)

spark_read_parquet(
  sc,
  name = "test",
  path = as.list(list.files(pattern = "*mobile_tiles.parquet$", recursive = TRUE))
)
