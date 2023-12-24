# Dibimbing, Data Engineering Bootcamp

1. Clone This Repo.
2. Run `make docker-build` for x86 user, or `make docker-build-arm` for arm chip user.

---
```
## docker-build			- Build Docker Images (amd64) including its inter-container network.
## docker-build-arm		- Build Docker Images (arm64) including its inter-container network.
## kafka  		        - Run a kafka container
## spark  		        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## spark-produce  		- Run a faker produce data to kafka
## spark-consume		- Run a code for listen kafka topic

```

---
