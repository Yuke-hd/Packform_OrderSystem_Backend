# OrderSystem Backend
This application will be using:
 
port `8888` for REST API (App.go)

port `5000` for database 

make sure these ports are not taken by other application

## Project setup

### Prerequisite
Have docker and Go installed

### Initialize database
run 
```
docker-compose up
```
at root folder

run
```
go run DBinit.go
```

at db folder

### Start backend application
run 
```
go run App.go
```
at root folder