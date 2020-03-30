# Aquarius Timeseries (AQTS) Time Series Description Processor

[![Build Status](https://travis-ci.com/usgs/aqts-capture-ts-description.svg?branch=master)](https://travis-ci.com/usgs/aqts-capture-ts-description)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-ts-description/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-ts-description)

Insert or update the appropriate time series descriptions to the AQTS capture database and return their unique identifiers for the next step in transforming data for the Observations service.

## Testing
This project contains JUnit tests. Maven can be used to run them (in addition to the capabilities of your IDE).

### Docker Network
A named Docker Network is needed to run the automated tests via maven. The following is a sample command for creating your own local network. In this example the name is aqts and the ip addresses will be 172.25.0.x

```.sh
docker network create --subnet=172.25.0.0/16 aqts
```

### Unit Testing
To run the unit tests of the application use:

```.sh
mvn package
```

### Database Integration Testing with Maven
To additionally start up a Docker database and run the integration tests of the application use:

```.sh
mvn verify
```

### Database Integration Testing with an IDE
To run integration tests against a local Docker database use:

```.sh
docker run -p 127.0.0.1:5437:5432/tcp usgswma/aqts_capture_db:ci
```

Additionally, add an application.yml configuration file at the project root (the following is an example):

```.yaml
TRANSFORM_DATABASE_ADDRESS: localhost
TRANSFORM_DATABASE_PORT: 5437
TRANSFORM_DATABASE_NAME: database_name
TRANSFORM_SCHEMA_NAME: schema_name
TRANSFORM_SCHEMA_OWNER_USERNAME: schema_owner
TRANSFORM_SCHEMA_OWNER_PASSWORD: changeMe
```
