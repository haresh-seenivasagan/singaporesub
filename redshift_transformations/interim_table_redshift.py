# %%
import configparser
from pathlib import Path
import os
from psycopg2 import sql
import psycopg2
import sys


# %%
date = sys.argv[1]
AUTHORFILE = f"{date}_author.csv"
POSTSFILE = f"{date}_posts.csv"
COMMENTSFILE = f"{date}_comments_transformed.csv"
COMMENT_AUTHORFILE = f"{date}_commentAuthor.csv"
print(COMMENT_AUTHORFILE)

# %%
path = Path(__file__)
ROOT_DIR = path.parent.parent.absolute()
config_path = os.path.join(ROOT_DIR, "config.conf")

config = configparser.ConfigParser()
config.read(config_path)

# %%
USERNAME = config.get("redshift_config", "redshift_username")
PASSWORD = config.get("redshift_config", "redshift_password")
HOST = config.get("redshift_config", "redshift_hostname")
PORT = config.get("redshift_config", "redshift_port")
DATABASE = config.get("redshift_config", "redshift_database")

# %%
sql_create_interim_comments_table = sql.SQL("""
DROP TABLE IF EXISTS comments_interim CASCADE;
CREATE TABLE comments_interim (
"comment_id" VARCHAR(7) NOT NULL,
"post_id" VARCHAR(6) NOT NULL,
"author_id" VARCHAR(10),
"body" VARCHAR(10000),
"created_date" TIMESTAMPTZ ,
"score" INT,
"edited" VARCHAR ,
"compound" DECIMAL(5,4),
"comp_score" VARCHAR(4),
PRIMARY KEY(comment_id)
)
""")
print(f's3://comments-transformed-bucket/{COMMENT_AUTHORFILE}')

#s3://comments-transformed-bucket/20220804_comments_transformed.csv
sql_load_comments_interim = sql.SQL(f"""
   COPY comments_interim (comment_id,post_id,author_id,body,created_date,score,edited,compound,comp_score) 
   FROM 's3://comments-transformed-bucket/{COMMENTSFILE}'  credentials 'aws_iam_role=arn:aws:iam::075207432376:role/REDHSIFTACESS'
FORMAT AS CSV DELIMITER ',' QUOTE '"'
region 'ap-southeast-1'

 IGNOREHEADER 1        
 """)                                     


# %%
sql_create_post_interim = sql.SQL("""
DROP  TABLE IF EXISTS post_interim CASCADE;                                
CREATE TABLE post_interim (
    "post_id" VARCHAR(6) NOT NULL ,
    "author_id"  VARCHAR(10) ,
    "created_time" TIMESTAMPTZ NOT NULL,
    "flair" VARCHAR(120) NOT NULL,
    "title" TEXT,
    "body" varchar(40000) default NULL,
    "num_comments" SMALLINT,
    "upvote_ratio" decimal(3,2) NOT NULL,
    "score" INT NOT NULL,
    "url" VARCHAR(2000),
    "distinguised" BOOLEAN default False,
    "is_original_content" BOOLEAN default False,
    "over_18" BOOLEAN default False,
    PRIMARY KEY(post_id)
)
""")

sql_load_post_interim= sql.SQL(f"""
                       
COPY post_interim (title, body, post_id, score, upvote_ratio, flair, created_time, num_comments, author_id, url ,distinguised, is_original_content, over_18) FROM 's3://initial-data-load-bucket/{POSTSFILE}' IAM_ROLE 'arn:aws:iam::075207432376:role/REDHSIFTACESS' FORMAT AS CSV DELIMITER ',' QUOTE '"' REGION AS 'ap-southeast-1'
IGNOREHEADER 1                                                                            
""")


# %%
sql_create_author_post_interim = sql.SQL("""
DROP TABLE IF EXISTS post_author_interim cascade;
CREATE TABLE post_author_interim (
    "author_id" VARCHAR(10) NOT NULL ,
    "acc_creation_date" TIMESTAMPTZ NOT NULL ,
    "author_name" VARCHAR(20) ,
    "link_flair" INT,
    "comment_karma" INT,
    "is_gold" BOOLEAN default False,
    "is_mod" BOOLEAN default False,
    "is_employee" BOOLEAN default False,
    PRIMARY KEY(author_id)
)
""")
sql_load_author_post_interim = sql.SQL(f"""

COPY post_author_interim (author_id, author_name, link_flair, comment_karma, acc_creation_date, is_gold, is_mod, is_employee)FROM 's3://initial-data-load-bucket/{AUTHORFILE}'  credentials 'aws_iam_role=arn:aws:iam::075207432376:role/REDHSIFTACESS'
FORMAT AS CSV DELIMITER ',' QUOTE '"'
region 'ap-southeast-1'

 IGNOREHEADER 1
;

""")

# %%
sql_create_author_comment_interim = sql.SQL("""
DROP TABLE IF EXISTS comment_author_interim;
CREATE TABLE comment_author_interim (
    "author_key" int IDENTITY(1,1) ,
    "author_id" VARCHAR(10) NOT NULL ,
    "comment_id" VARCHAR(7) ,
    "post_id" VARCHAR(6),
    "acc_creation_date" TIMESTAMPTZ NOT NULL ,
    "author_name" VARCHAR(20) ,
    "link_flair" INT,
    "comment_karma" INT,
    "is_gold" BOOLEAN default False,
    "is_mod" BOOLEAN default False,
    "is_employee" BOOLEAN default False,
    PRIMARY KEY(author_key)
)
""")
sql_load_author_comment_interim = sql.SQL(f"""

COPY comment_author_interim (author_id, comment_id ,post_id,author_name, link_flair, comment_karma, acc_creation_date, is_gold, is_mod, is_employee)FROM 's3://initial-data-load-bucket/{COMMENT_AUTHORFILE}'  credentials 'aws_iam_role=arn:aws:iam::075207432376:role/REDHSIFTACESS'
FORMAT AS CSV DELIMITER ',' QUOTE '"'
region 'ap-southeast-1'

 IGNOREHEADER 1

;
"""
)


# %%
def connect_to_redshift():
    try:
        rs_wh = psycopg2.connect(dbname = DATABASE , user = USERNAME , password = PASSWORD , host = HOST , port = PORT)
        rs_wh.autocommit = True

        return rs_wh
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)

# %%
def create_and_fill_author_post_interim(rs_conn):
    rs_conn.execute(sql_create_author_post_interim)
    rs_conn.execute(sql_load_author_post_interim)
def create_and_fill_author_comment_interim(rs_conn):
    rs_conn.execute(sql_create_author_comment_interim)
    rs_conn.execute(sql_load_author_comment_interim)

def create_and_fill_comments_interim(rs_conn):
    rs_conn.execute(sql_create_interim_comments_table)
    rs_conn.execute(sql_load_comments_interim)

def create_and_fill_post_interim(rs_conn):
    rs_conn.execute(sql_create_post_interim)
    rs_conn.execute(sql_load_post_interim)


# %%
def main():
    rs_conn = connect_to_redshift().cursor()
    create_and_fill_comments_interim(rs_conn)
    create_and_fill_post_interim(rs_conn)
    create_and_fill_author_post_interim(rs_conn)
    create_and_fill_author_comment_interim(rs_conn)
    print("done")
    

# %%
main()

# %%



