# DummyAPIDB
An End-To-End Data Engineering Pipeline

### A Dockerized python application, integrating postgres client and pgadmin managment studio

* run the following commands to set the docker containers which are python app (with all dependencies), postgres, pgadmin
1.  **docker-compose -f docker-compose.yaml up -d**
2.  **docker-compose -f docker-compose.yaml build**
3.  Find the **POSTGRES-CONTAINER ID** of postgres respective image by running **docker ps -aqf "name=postgres"**
4.  Extract the postgress IP Adress by running **docker inspect POSTGRES-CONTAINER ID | grep IPAddress**
5.  Fullfill the corresponding VM host IP Adress in line 209 along with the ssh credentials (password : line 211 and username : line 212) and the postgress IP Adress ,retrieved from the step 4, in 213 line of test.py source code file. Such that the ssh connection to the DB is established
6.  Execute the pipeline by running **docker run -ti --rm --name python-script -v "$PWD":/code python_app_stack python test.py**


*The whole pipeline is demostrated within the **test.ipynd**

*In case you want to perform queries on the pgamdin managment studio visit this portal "IP-Adress::5050" and fullfil the credentials exist in line 19,18 of **docker-compose.yaml** file
*Connect to the database namely **dummyapidb** using the credentials demostrated in lines 47, 49 of **docker-compose.yaml** file
