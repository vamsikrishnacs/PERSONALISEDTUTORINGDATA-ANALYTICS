create database if not exists ${hiveconf:UPLOADTYPE};
use database ${hiveconf:UPLOADTYPE};

drop table if exists auth_user;
create table auth_user(
id int,
username string,
first_name string,
last_name string,
email string,
password string,
is_staff tinyint,
is_active tinyint,
is_superuser tinyint,
last_login string,
date_joined string,
status string,
email_key string,
avatar_typ string,
country string,
show_country tinyint,
date_of_birth string,
interesting_tags string,
ignored_tags string,
email_tag_filter_strategy smallint,
display_tag_filter_strategy smallint,
consecutive_days_visit_count int
)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- -- auth_userprofile
drop table if exists auth_userprofile;
create table auth_userprofile(
id int,
user_id int,
name string,
language string,
location string,
meta string,
courseware string,
gender string,
mailing_address string,
year_of_birth int,
level_of_education string,
goals string,
allowcertificate tinyint)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- -- student_courseenrollment 
drop table if exists student_courseenrollment;
create table student_courseenrollment(
id int,
user_id int,
course_id string,
created string,
is_active tinyint,
mode string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- -- user_api_usercoursetag
drop table if exists user_api_usercoursetag;
create table user_api_usercoursetag(
user_id int,
course_id string,
key string,
value string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- -- user_id_map
drop table if exists user_id_map;
create table user_id_map(
hashid int,
id int,
username string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- Courseware Progress Data
-- -- courseware_studentmodule
drop table if exists courseware_studentmodule;
create table courseware_studentmodule(
id int,
module_type string,
module_id string,
student_id string,
state string,
grade string,
created string,
modified string,
max_grade double,
done string,
course_id string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- Certificate Data
-- -- certificates_generatedcertificate
drop table if exists certificates_generatedcertificate;
create table certificates_generatedcertificate(
id int,
user_id int,
download_url string,
grade string,
course_id string,
key string,
distinction tinyint,
status string,
verify_uuid string,
download_uuid string,
name string, 
created_date string,
modified_date string,
error_reason string,
mode string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t'; 
-- Wiki Data
-- -- wiki_article
drop table if exists wiki_article;
create table wiki_article(
id int,
current_revision_id int,
created string,
modified string,
owner_id int,
group_id int,
group_read tinyint,
group_write tinyint,
other_read tinyint,
other_write tinyint)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
-- -- wiki_articlerevision 
drop table if exists wiki_articlerevision;
create table wiki_articlerevision(
id int,
revision_number int,
user_message string,
automatic_log string,
ip_address string,
user_id int,
modified string,
created string,
previous_revision_id int,
deleted tinyint,
locked tinyint,
article_id int,
content string,
title string)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t';
----------------------------------------------------
----------------------------------------------------
-- EDM Improved Data Models
----------------------------------------------------
----------------------------------------------------
--Logs
-- SQL
drop table if exists edm_auth_user;
create table edm_auth_user(
id int,
username string,
email string,
is_staff tinyint,
is_active tinyint,
last_login string,
date_joined string
);
-- -- auth_userprofile
drop table if exists edm_auth_userprofile;
create table edm_auth_userprofile(
id int,
user_id int,
meta string,
gender string,
mailing_address string,
year_of_birth int,
level_of_education string,
goals string);
 -- -- student_courseenrollment 
drop table if exists edm_student_courseenrollment;
create table edm_student_courseenrollment(
id int,
user_id int,
course_id string,
created string,
is_active tinyint,
mode string);
-- Courseware Progress Data
-- -- courseware_studentmodule
drop table if exists edm_courseware_studentmodule;
create table edm_courseware_studentmodule(
id int,
module_type string,
module_id string,
student_id string,
state string,
grade string,
created string,
modified string,
max_grade double,
done string,
course_id string);
-- Certificate Data
-- -- certificates_generatedcertificate
drop table if exists edm_certificates_generatedcertificate;
create table edm_certificates_generatedcertificate(
id int,
user_id int,
grade string,
course_id string); 
-- Wiki Data Pending!!!!!

--Log Table Entries
-- create required hive table/no formatting required for json(no delimiters)

drop table if exists json_table;
create table json_table (json string);

drop table if exists log_table;
create table log_table(
	username string,
	host string,
	event_source string,
	event_type string,
	course_user_tags string,
	session string,
	course_id string,
	org_id string,
	user_id string,
	path string,
	time string,
	ip string,
	event string,
	agent string,
	page string
	);

drop table if exists rem_log_table;
create table rem_log_table(
	username string,
	host string,
	event_source string,
	event_type string,
	course_user_tags string,
	session string,
	course_id string,
	org_id string,
	user_id string,
	path string,
	time string,
	ip string,
	event string,
	agent string,
	page string
	);


drop table if exists pi_log_table;
create table pi_log_table(
     username string,
    host string,
    event_source string,
    event_type string,
    --Context
    -- module
        session string,
    course_id string,
    org_id string,
    user_id string,
    path string,
    display_name string,
----- context end---- 
    time string,
    ip string,
    -- -- EVENT
 answers string,
 attempts int,
 --correct_map string,--  (dict)
    correctness string,
     hint string,
     hintmode string,
     msg string,
     npoints int,
    queuestate string, --(dict)
             --key int,
             --time string,
 grade int,
 max_grade int,
 problem_id int,
 state string,-- (dict)
 -- cur_prob_state string,
 --submission string,-- (object)
answer string,
     correct boolean,
input_type string,
     question string,
response_type string,
variant int,
 success string,
 failure string,
 orig_score int,
 orig_total int,
 new_score int,
 new_total int,
 problem string,
 old_state string,
 new_state string,
------ event end------
 page string
 );

--
-- Naviagational Events
--
drop table if exists ne_log_table;
create table ne_log_table 
(username string,
event_source string,
event_type string,
ip string,
agent string,
host string,
session string,
--- context -----
--username string,
user_id int,
org_id string,
course_id string,
path string,
---- end context---
--- event---
old int,
new int,
id int,
code string,
currentTime float,
speed string,
old_time string,
new_time string,type string,
current_time string,
old_speed string,
new_speed string,
--code string,
----event end----
time string,
page string 
);

drop table if exists ee_log_table;
create table ee_log_table (
username string,
host string,
event_source string,
event_type string,
name string,
time string,
session string,
ip string,
agent string,
page string,
--Context
    course_id string,
    org_id string,
    user_id string,
    path string,
----- context end---- 
------event -----
mode string);
----event end----

drop table if exists vi_log_table;
create table vi_log_table(
username string,
host string,
event_source string,
event_type string,
time string,
session string,
ip string,
agent string,
page string,
course_id string,
org_id string,
user_id string,
id_play string,
code_play string,
currentTime_play string,
speed_play float,
id_pause string,
code_pause string,
currentTime_pause string,
speed_pause float,
old_time string,
new_time string,
type string,  
current_time string,
old_speed float,
new_speed float,
code_load string,
code_hide string,
currentTime_hide string,
code_show string,
currentTime_show string);
