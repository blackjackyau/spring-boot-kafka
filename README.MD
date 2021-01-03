# Spring Boot Kafka Sample Project

### Exploring different type of consumers

#### Assign method

- no consumer group, hence, partition offset is managed from the application
- useful in the scenario where the application is only interested to observe the latest events when it is online
- E.g. of usecases
    - in memory application cache invalidation