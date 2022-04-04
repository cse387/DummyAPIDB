### Rest Api hits on https://dummyapi.io/

import requests
from functools import reduce
import psycopg2
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from nose.tools import assert_true,assert_is_none
#get full user data
#api_url = "https://dummyapi.io/data/v1/user/60d0fe4f5311236168a109ca"
api_preffix = "https://dummyapi.io/data/v1/"
api_id = "6243344d20e61633b2e46ba8"
api_url = "https://dummyapi.io/data/v1/user?page=1&limit=50"
# api_url = "https://dummyapi.io/data/v1/user/60d0fe4f5311236168a10a01"
response = requests.get(api_url,  headers={"app-id": api_id, "Content-Type":"text"})
response_json = response.json()
list(response_json.keys())

print("response sample")
print(response_json)

"""
#### Retrieve all records over all pages

**retrieve the non-preview records across all page per data object (users, posts, comments))**

**ranges :**

**limit : [5-50]**

**pages : [0-999]**
"""

def retreive_full_records(object_, api_preffix, api_id, page_upper_limit):
    #args: 
    # object_ : "user", "comment", "post"
    # api_preffix : https://dummyapi.io/data/v1/
    # api_id : 6243344d20e61633b2e46ba8
    # page_upper_limit : [0-999]
    records = []
    if object_ == "user":    
        location_dict = {}
        key_location = 0
        location_records = []    

    ### check of duplicated id using the frontier hash map
    id_frontier = {}

    def check_empty_response(response):    
        if "data" not in response.keys():
            return False    
        else:        
            if len(response["data"]) == 0:
                return False        
        return True

    def check_duplicated_ids (id_):
        nonlocal id_frontier
        try:
            if id_frontier[id_] == 1:
                return False
        except :        
            id_frontier[id_] = 1
        return True

    def get_detail_user_record (id_):
        ### get the full user information
        nonlocal api_preffix, object_, key_location
        if check_duplicated_ids(id_):        
            record = requests.get(api_preffix+object_+"/"+id_,  headers={"app-id":api_id, "Content-Type":"text"}).json()        
            print(record)
            if 'location' in record.keys():
                if len(record['location']) != 0:                
                    temp_location = record["location"]
                    info_concat = reduce(lambda a, b: a.strip() + b.strip(), list(record["location"].values()))
                    
                    if info_concat not in location_dict.keys():
                        location_dict[info_concat] = key_location
                        key_location += 1
                    record["location"] = key_location
                    temp_location["id"] = key_location
                    location_records.append(temp_location)
            return record
        return None

    def sanity_check_posts (record):   
        ### check for duplicated ids
        if not check_duplicated_ids(record['id']):
            return False    
        if "likes" not in record.keys():
            return False
        else:
            if record["likes"] < 0:
                return False
        if "owner" not in record.keys():
            return False
        else:
            if "id" not in record["owner"]:
                return False
            else:
                if record["owner"]["id"] == "":
                    return False
        return True

    def sanity_check_comments (record): 
        ### check for duplicated ids
        if not check_duplicated_ids(record['id']):
            return False    

        if "post" not in record.keys():
            return False
        else:
            if record["post"] == "":
                return False

        if "owner" not in record.keys():
            return False
        else:
            if "id" not in record["owner"]:
                return False
            else:
                if record["owner"]["id"] == "":
                    return False
        return True

    def fix_post_comment_records (record, page, check_post):
        if check_post:
            if sanity_check_posts(record):
                record["owner"] = record["owner"]["id"]
                record["page"]= page
                return record
        elif  sanity_check_comments(record):
            record["owner"] = record["owner"]["id"]
            record["page"]= page
            return record            
        return None

    for page in range(0,page_upper_limit):    
        api_url = api_preffix+object_+"?page="+str(page)+"&limit=50"
        response = requests.get(api_url,  headers={"app-id":api_id, "Content-Type":"text"})    
        assert_true(response.ok)
        response_json = response.json()    
        if check_empty_response(response_json):            
            #in case the object is user unnest the preview records, extract the location data
            if object_ == "user":             
                records += list(map(lambda x:  get_detail_user_record (x['id']), response_json["data"]))                
                #records += [get_detail_user_record (x['id']) for x in response_json["data"]]
                
            elif object_ == "post":
                records += list(map(lambda record : fix_post_comment_records(record, page, True), response_json["data"]))
                
                #records += [fix_post_comment_records(record, page, True) for record in response_json["data"]]
            elif object_ == "comment":
                records += list(map(lambda record : fix_post_comment_records(record, page, False), response_json["data"]))
                #records += [fix_post_comment_records(record, page, False) for record in response_json["data"]]
        else:
            print("Page number ",page)
            print(response_json)
        
    records = list(filter(lambda x:x is not None,records))        
    
    if object_ == "user":
        
        return pd.DataFrame(records), pd.DataFrame(location_records)
    
    elif object_ == "post":        
        pages = []
        look_up_posts_pages = {}
        for p in records:    
            key = p["id"]+str(p["page"])
            if key not in look_up_posts_pages:
                pages.append({'id':p["page"], "post_id":p["id"]})
                look_up_posts_pages [key] = 1
#             del p["page"]
        return pd.DataFrame(records), pd.DataFrame(pages)
        
    return pd.DataFrame(records)


    """
_**Fetch user,location, post, comment records over a bunch of 999 pages.**_

_**In case you want the fetching to finish earlier reduce the page upper limit - no parralized implementation**_
    """

print("Fetching Data....")
user, locations = retreive_full_records(object_ = "user", api_preffix = api_preffix, api_id = api_id, page_upper_limit = 999)
posts, pages = retreive_full_records(object_ = "post", api_preffix = api_preffix, api_id = api_id, page_upper_limit = 999)
comments = retreive_full_records(object_ = "comment", api_preffix = api_preffix, api_id = api_id, page_upper_limit = 999)

print("Data Review ....")
print(user)

print("Locations Frame ....")
print(locations)

print("Posts Frame ....")
print(posts)

print("Comments Frame ....")
print(comments)

### Connect to the database

print("DataBase (Postgres) Connection ....")


ssh_tunnel= SSHTunnelForwarder(
         ('SET-HOST-IP-ADDRESS', 22),
        #  ('localhost', 22),
         ssh_password="SET-PASSWORD",
         ssh_username="SET-USERNAME",
         remote_bind_address=('SET-POSTGRES-IP-ADDRESS', 5432))

ssh_tunnel.start()

ssh_tunnel.local_bind_port

connection = psycopg2.connect(  dbname='dummyapidb'
                              , user="postgres"
                              , password="postgres"
                              , host='127.0.0.1'
                              , port=ssh_tunnel.local_bind_port)#127.0.0.1

# connection = psycopg2.connect(dbname='dummyapidb',
#                              user = "postgres",
#                              password = "postgres",
#                             #  host = "localhost",
#                              host = "127.0.0.1",
#                              port = "5432")

cursor = connection.cursor()

### Tables Initialization

print("Staging Tables Initialization ..")

create_users_tabel = ("""
DROP TABLE IF EXISTS UsersStage cascade;
CREATE TABLE IF NOT EXISTS UsersStage (
  id varchar(255) PRIMARY KEY,
  title varchar(255),
  firstName varchar(255),
  lastName varchar(255),
  picture varchar(255),
  gender varchar(255),
  email varchar(255),
  dateOfBirth date DEFAULT NULL,
  phone varchar(255),
  location_id int,
  registerDate date DEFAULT NULL,
  updatedDate date DEFAULT NULL
);
""")

create_locations_tabel = ("""
DROP TABLE IF EXISTS LocationsStage cascade;
CREATE TABLE IF NOT EXISTS LocationsStage (
  id INT PRIMARY KEY,
  street varchar(255),
  city varchar(255),
  state varchar(255),
  country varchar(255),
  timezone varchar(255)
);
""")

create_posts_tabel = ("""
DROP TABLE IF EXISTS Posts cascade;
CREATE TABLE IF NOT EXISTS Posts (
  id varchar(255),
  image varchar(255),
  likes int,
  tags varchar(255) ARRAY,
  text varchar(255),
  publishDate date DEFAULT NULL,
  user_id varchar(255),
  page_id INT
);
""")

create_comments_tabel = ("""
DROP TABLE IF EXISTS Comments cascade;
CREATE TABLE IF NOT EXISTS Comments (
  id varchar(255),
  message varchar(255),
  user_id varchar(255),
  post_id varchar(255),
  publishDate date DEFAULT NULL,
  page_id INT
);
""")

# """
# Fact Table : pages related to posts
# """
# create_pages_tabel = ("""
# DROP TABLE IF EXISTS Pages cascade;
# CREATE TABLE IF NOT EXISTS Pages (
#   id INT PRIMARY KEY,
#   post_id varchar(255)
# );
# """)

set_data_null_raw_data = (
    """
    UPDATE RawData rd
    SET PublishDate = CASE WHEN rd.PublishDate =  '-INFINITY' THEN NULL ELSE rd.PublishDate END;
    """
    )

### Create the Staging Dimension Data Tables (Users, Locations) & Fact tables (Posts, Comments, Pages)
cursor.execute(create_users_tabel)
cursor.execute(create_locations_tabel)
cursor.execute(create_comments_tabel)
cursor.execute(create_posts_tabel)
# cursor.execute(create_pages_tabel)
#connection.commit()


### Insert the Data Records (Staging Phase)

print("Data ingestion (staging) phase")


def ingest_raw_user_data (raw_data, cursor, connection):
    insert_row_RawData = (
    """
    INSERT INTO UsersStage (    
    id,
    title,
    firstName,
    lastName,
    picture,
    gender,
    email,
    dateOfBirth,
    phone,
    location_id,
    registerDate,
    updatedDate
    ) VALUES (%s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s,%s)
    ON CONFLICT ON CONSTRAINT usersstage_pkey DO NOTHING;
    """)
    for index, row in raw_data.iterrows():                
        row_ = tuple(row.to_list())
        cursor.execute(insert_row_RawData, row_)
        
        
def ingest_raw_location_data (raw_data, cursor, connection):
    insert_row_RawData = (
    """
    INSERT INTO LocationsStage (    
    street,
    city ,
    state,
    country,
    timezone,
    id
    ) VALUES (%s, %s, %s, %s, %s,%s)
    ON CONFLICT ON CONSTRAINT locationsstage_pkey DO NOTHING;
    """)
    for index, row in raw_data.iterrows():                
        row_ = tuple(row.to_list())        
        cursor.execute(insert_row_RawData, row_)
        
def ingest_raw_posts_data (raw_data, cursor, connection):
    insert_row_RawData = (
    """
    ALTER TABLE Posts
    DROP CONSTRAINT IF EXISTS UniqSignPosts;
    ALTER TABLE Posts
    ADD CONSTRAINT UniqSignPosts UNIQUE (id );
    INSERT INTO Posts (    
    id ,
    image,
    likes,
    tags ,
    text ,
    publishDate ,
    user_id,
    page_id
    ) VALUES (%s, %s, %s, %s, %s,%s,%s,%s)
    ON CONFLICT ON CONSTRAINT UniqSignPosts DO NOTHING;
    """)
    for index, row in raw_data.iterrows():                
        row_ = tuple(row.to_list())
        cursor.execute(insert_row_RawData, row_)
        
def ingest_raw_comments_data (raw_data, cursor, connection):
    insert_row_RawData = (
    """
    ALTER TABLE Comments
    DROP CONSTRAINT IF EXISTS UniqSignComments;
    ALTER TABLE Comments
    ADD CONSTRAINT UniqSignComments UNIQUE (id );
    INSERT INTO Comments (    
    id,
    message,
    user_id,
    post_id,
    publishDate,
    page_id
    ) VALUES (%s, %s, %s, %s, %s,%s)
    ON CONFLICT ON CONSTRAINT UniqSignComments DO NOTHING;
    """)
    for index, row in raw_data.iterrows():                
        row_ = tuple(row.to_list())
        cursor.execute(insert_row_RawData, row_)
        
def ingest_raw_pages_data (raw_data, cursor, connection):
    insert_row_RawData = (
    """
    INSERT INTO Pages (    
    id,
    post_id
    ) VALUES (%s, %s)
    """)
    for index, row in raw_data.iterrows():                
        row_ = tuple(row.to_list())
        cursor.execute(insert_row_RawData, row_)
        
ingest_raw_user_data (user, cursor, connection)    
ingest_raw_posts_data (posts, cursor, connection)
ingest_raw_location_data (locations, cursor, connection)
ingest_raw_comments_data (comments, cursor, connection)
# ingest_raw_pages_data (pages, cursor, connection)
connection.commit()

### Define the DW Dimensional Tables (Eliminate Duplication)

print("DW Dimensional Tables Definition ..")

create_users_tabel = ("""
DROP TABLE IF EXISTS Users cascade;
CREATE TABLE IF NOT EXISTS Users (
  id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL primary key, 
  old_id varchar(255),
  title varchar(255),
  firstName varchar(255),
  lastName varchar(255),
  picture varchar(255),
  gender varchar(255),
  email varchar(255),
  dateOfBirth date DEFAULT NULL,
  phone varchar(255),
  location_id int,
  registerDate date DEFAULT NULL,
  updatedDate date DEFAULT NULL
);
""")

create_locations_tabel = ("""
DROP TABLE IF EXISTS Locations cascade;
CREATE TABLE IF NOT EXISTS Locations (
  id bigint GENERATED BY DEFAULT AS IDENTITY NOT NULL primary key,
  old_id INT,
  street varchar(255),
  city varchar(255),
  state varchar(255),
  country varchar(255),
  timezone varchar(255)
);
""")

cursor.execute(create_users_tabel)
cursor.execute(create_locations_tabel)


### Populate DW Data Tables
"""
**Further sanity check for duplication by adding the respective constraints**
"""

print("Populate DW Data Tables..")

config_user =("""

ALTER TABLE Users
    ADD CONSTRAINT UniqSignUsers UNIQUE (title, firstName, lastName, gender, email,dateOfBirth );

INSERT INTO Users(
      old_id,
      title ,
      firstName ,
      lastName,
      picture ,
      gender ,
      email,
      dateOfBirth,
      phone ,
      location_id,
      registerDate ,
      updatedDate
    ) 
SELECT   
  id ,
  title ,
  firstName ,
  lastName ,
  picture ,
  gender ,
  email ,
  dateOfBirth ,
  phone ,
  location_id ,
  registerDate ,
  updatedDate 
FROM UsersStage
ON CONFLICT ON CONSTRAINT UniqSignUsers DO NOTHING;
""")
    
config_location = ("""

ALTER TABLE Locations
    ADD CONSTRAINT UniqSignLocation UNIQUE (street,city, state,country);

INSERT INTO Locations(
    old_id ,
    street,
    city,
    state,
    country,
    timezone
    ) 
SELECT id, 
       street
       ,city
       ,state
       ,country
       ,timezone
FROM LocationsStage
ON CONFLICT ON CONSTRAINT UniqSignLocation DO NOTHING;

""")

cursor.execute(config_user)
cursor.execute(config_location)
connection.commit()


### Update new Dimensional FK Ids on Fact Tables

print("Update Dimensional FK ids on Fact Tables")


update_posts_user_id_FK = ("""
        UPDATE posts
        SET user_id = users.id
        FROM users
        WHERE users.old_id = posts.user_id;
        ALTER TABLE posts
        ALTER COLUMN user_id TYPE bigint USING user_id::bigint;
        """)

update_comments_user_id_FK = ("""
        UPDATE comments
        SET user_id = users.id
        FROM users
        WHERE users.old_id = comments.user_id;
        ALTER TABLE comments
        ALTER COLUMN user_id TYPE bigint USING user_id::bigint;
        """)
 
cursor.execute(update_posts_user_id_FK)
cursor.execute(update_comments_user_id_FK)
connection.commit()


### Set PK on Posts & Comments

print("Set PK on Facts Tables")

set_PK_posts = ("""
        ALTER TABLE posts
        ADD CONSTRAINT posts_pk PRIMARY KEY (id);
        """)
set_PK_comments = ("""
        ALTER TABLE comments
        ADD CONSTRAINT comments_pk PRIMARY KEY (id);
        """)

cursor.execute(set_PK_posts)
cursor.execute(set_PK_comments)
connection.commit()


### Table Intercorrelation Definitions

print("Define correlations amognst tables")

users_locations_many_to_one = ("""
ALTER TABLE Users ADD FOREIGN KEY (location_id) REFERENCES  Locations(id);
""")

posts_users_many_to_one = ("""
ALTER TABLE Posts ADD FOREIGN KEY (user_id) REFERENCES  Users(id);
""")

pages_posts_one_to_many = ("""
ALTER TABLE Pages ADD FOREIGN KEY (post_id) REFERENCES  Posts(id);
""")

comments_pages_many_to_one = ("""
ALTER TABLE Comments ADD FOREIGN KEY (page_id) REFERENCES  Pages(id);
""")

comments_owner_many_to_one = ("""
ALTER TABLE Comments ADD FOREIGN KEY (user_id) REFERENCES  Users(id);
""")

cursor.execute(users_locations_many_to_one)
cursor.execute(posts_users_many_to_one)
# cursor.execute(pages_posts_one_to_many)
# cursor.execute(comments_pages_many_to_one)
cursor.execute(comments_owner_many_to_one)
connection.commit()

### Data Modeling Finished : Perform the Queries

print("Perfoming Respective Queries")

# --  How many new users are added daily?

cursor.execute("""
select round(avg(new_users_added_daily),2) as avg_user_added from (
select count(distinct(id)) as new_users_added_daily from users
group by registerdate) as temp;""")
print("How many new users are added daily?")
print(cursor.fetchall())

# -- Which cities have the most activity, in terms of posts per day?

cursor.execute("""with posts_country as(
select posts.id as post_ids, locations.city as city from posts
left join users on users.id = posts.user_id
left join locations on locations.id = users.location_id)
select city, count(post_ids) as post_volumes from posts_country
group by city
order by post_volumes desc limit 10;""")
print("Which cities have the most activity, in terms of posts per day?")
print(cursor.fetchall())

# -- Which tags are most frequently encountered, across user posts?

cursor.execute("""select tags, count(*) as tags_volume_across_posts from posts
group by tags
having count(*) > 1
order by tags_volume_across_posts desc limit 10;""")
print("Which tags are most frequently encountered, across user posts?")
print(cursor.fetchall())

# -- What is the median comment/post ratio amongst the users with the most posts

cursor.execute("""
DROP FUNCTION IF EXISTS _final_median(INT);

CREATE OR REPLACE FUNCTION _final_median(numeric[])
   RETURNS numeric AS
$$
   SELECT AVG(val)
   FROM (
     SELECT val
     FROM unnest($1) val
     ORDER BY 1
     LIMIT  2 - MOD(array_upper($1, 1), 2)
     OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
   ) sub;
$$
LANGUAGE 'sql' IMMUTABLE;

DROP AGGREGATE IF EXISTS median(numeric);

CREATE AGGREGATE median(numeric) (
  SFUNC=array_append,
  STYPE=numeric[],
  FINALFUNC=_final_median,
  INITCOND='{}'
);

with user_comments_post as (
select users.*,posts.id as posts_id,comments.id as comments_id from users
left join comments on comments.user_id = users.id
left join posts on posts.user_id = users.id)
select round(median(comment_post_ratio),2) from (
select id, count(distinct(comments_id))/count(distinct(posts_id)) as comment_post_ratio   from user_comments_post
group by id
order by count(posts_id) desc limit 10) as comment_post_ratio_;""")
print("What is the median comment/post ratio amongst the users with the most posts?")
print(cursor.fetchall())
# -- What is the average time between registration and first comment?

cursor.execute("""with user_comments as (
select users.id as users_id, users.registerdate as users_registerdate, comments.publishdate as comment_publishdate, comments.id as comments_id from users
left join comments on comments.user_id = users.id)
select round(avg(diff_register_first_comment_publish_date),2) from (
select users_id,min(comment_publishdate) -  min(users_registerdate) as diff_register_first_comment_publish_date from user_comments
group by users_id) as user_comments_results""")
print("What is the average time between registration and first comment?")
print(cursor.fetchall())


cursor.close()
connection.close()