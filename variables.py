import os
"""
variables.py acts as a storehouse for the large number of queries required in the system internally
There are two types of files
=>DP or edx data packages downloaded from the amazon server
=>LOCAL or locally generated log and sql dump files

"""

#DP PACKAGE DIRS
# # Global Variables
dest = os.path.expanduser("~/separate-data/server-sql-data/")
dest = os.path.expanduser(dest)
dest_json = os.path.expanduser("~/separate-data/server-json-data/")
dest_log = os.path.expanduser("~/separate-data/server-log-data/")
dest_mongo = os.path.expanduser("~/separate-data/server-mongo-data/")
dest_auth_user = dest + "sql-auth-user/"
dest_auth_userprofile = dest + "sql-auth-userprofile/"
dest_courseware_studentmodule = dest + "sql-courseware-studentmodule/"
dest_wiki_article = dest + "sql-wiki-article/"
dest_wiki_articlerevision = dest + "sql-wiki-articlerevision/"
dest_student_courseenrollment = dest + "sql-student-courseenrollment/"
dest_certificates_generatedcertificate = dest + "sql-certificates-generatedcertificate/"
dest_user_api_usercoursetag = dest + "sql-user-api-usercoursetag/"
dest_user_id_map = dest + "sql-user-id-map/"
destarr = [dest, dest_json, dest_log, dest_mongo, dest_auth_user, dest_auth_userprofile, dest_courseware_studentmodule,
           dest_wiki_article, dest_wiki_articlerevision,
           dest_student_courseenrollment, dest_certificates_generatedcertificate, dest_user_api_usercoursetag,
           dest_user_id_map]
##

#COMMON
FS_RESET = "rm -rf ~/separate-data"

HDFS_RESET = '''
		echo "Removing Previously Present Directories"
		hadoop fs -rm -r -f /sqlinput
		hadoop fs -rm -r -f /loginput
		hadoop fs -rm -r -f /jsoninput
		hadoop fs -rm -r -f /mongoinput
		echo "Creating Directories"
		hadoop fs -mkdir /sqlinput
		hadoop fs -mkdir /loginput
		hadoop fs -mkdir /jsoninput
		hadoop fs -mkdir /mongoinput
	'''
DP_HDFS_PORT = '''
		echo "Move Data to HDFS"
		hadoop fs -copyFromLocal ~/separate-data/server-sql-data /sqlinput/
		hadoop fs -copyFromLocal ~/separate-data/server-json-data /jsoninput/
		hadoop fs -copyFromLocal ~/separate-data/server-log-data /loginput/
		hadoop fs -copyFromLocal ~/separate-data/server-mongo-data /mongoinput/
	'''
LOCAL_HDFS_PORT="hadoop fs -copyFromLocal ~/separate-data/local-log-data /loginput/"
DP_CREATEDB = "create database if not exists AMAZONSERVER"
LOCAL_CREATEDB = "create database if not exists LOCALDB"
DP_USEDB = "use AMAZONSERVER"
LOCAL_USEDB = "use LOCALDB"

####SQL TABLES
# EDX DATA PACKAGE (SERVER) VARS
#CREATE
DP_CREATE_SQL_AUTHUSER = '''
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
		 FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_AUTHUSERPROFILE = '''
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
		FIELDS TERMINATED BY '\t'
		'''
DP_CREATE_SQL_STUDENTCOURSEENROLLMENT = '''
		create table student_courseenrollment(
		id int,
		user_id int,
		course_id string,
		created string,
		is_active tinyint,
		mode string)
		 ROW FORMAT DELIMITED
		 FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_USERAPIUSERCOURSETAG = '''
		create table user_api_usercoursetag(
		user_id int,
		course_id string,
		key string,
		value string)
		ROW FORMAT DELIMITED
		FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_USERIDMAP = '''
		create table user_id_map(
		hashid int,
		id int,
		username string)
		ROW FORMAT DELIMITED
		FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_COURSEWARESTUDENTMODULE = '''
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
		FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_CERTIFICATESGENERATEDCERTIFICATE = '''
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
		FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_WIKIARTICLE = '''
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
		FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_WIKIARTICLEREVISION = '''
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
		FIELDS TERMINATED BY '\t'
	'''
DP_CREATE_SQL_EDMAUTHUSER = '''
		create table edm_auth_user(
		id int,
		username string,
		email string,
		is_staff tinyint,
		is_active tinyint,
		last_login string,
		date_joined string
		)
	'''
DP_CREATE_SQL_EDMAUTHUSERPROFILE = '''
		create table edm_auth_userprofile(
		id int,
		user_id int,
		meta string,
		gender string,
		mailing_address string,
		year_of_birth int,
		level_of_education string,
		goals string)
	'''
DP_CREATE_SQL_EDMSTUDENTCOURSEENROLLMENT = '''
		create table edm_student_courseenrollment(
		id int,
		user_id int,
		course_id string,
		created string,
		is_active tinyint,
		mode string)
	'''
DP_CREATE_SQL_EDMCOURSEWARESTUDENTMODULE = '''
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
		course_id string)
	'''
DP_CREATE_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE = '''
		create table edm_certificates_generatedcertificate(
		id int,
		user_id int,
		grade string,
		course_id string)
	'''
#DROP
DP_DROP_SQL_AUTHUSER = "drop table if exists auth_user"
DP_DROP_SQL_AUTHUSERPROFILE = "drop table if exists auth_userprofile"
DP_DROP_SQL_STUDENTCOURSEENROLLMENT = "drop table if exists student_courseenrollment"
DP_DROP_SQL_USERAPIUSERCOURSETAG = "drop table if exists user_api_usercoursetag"
DP_DROP_SQL_USERIDMAP = "drop table if exists user_id_map"
DP_DROP_SQL_COURSEWARESTUDENTMODULE = "drop table if exists courseware_studentmodule"
DP_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE = "drop table if exists certificates_generatedcertificate"
DP_DROP_SQL_WIKIARTICLE = "drop table if exists wiki_article"
DP_DROP_SQL_WIKIARTICLEREVISION = "drop table if exists wiki_articlerevision"
DP_DROP_SQL_EDMAUTHUSER = "drop table if exists edm_auth_user"
DP_DROP_SQL_EDMAUTHUSERPROFILE = "drop table if exists edm_auth_userprofile"
DP_DROP_SQL_EDMSTUDENTCOURSEENROLLMENT = "drop table if exists edm_student_courseenrollment"
DP_DROP_SQL_EDMCOURSEWARESTUDENTMODULE = "drop table if exists edm_courseware_studentmodule"
DP_DROP_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE = "drop table if exists edm_certificates_generatedcertificate"


#LOAD
DP_LOAD_SQL_AUTHUSER = "load data inpath '/sqlinput/server-sql-data/sql-auth-user/*' into table auth_user"
DP_LOAD_SQL_AUTHUSERPROFILE = "load data inpath '/sqlinput/server-sql-data/sql-auth-userprofile/*' into table auth_userprofile"
DP_LOAD_SQL_STUDENTCOURSEENROLLMENT = "load data inpath '/sqlinput/server-sql-data/sql-auth-userprofile/*' into table auth_userprofile"
DP_LOAD_SQL_USERAPIUSERCOURSETAG = "load data inpath '/sqlinput/server-sql-data/sql-user-api-usercoursetag/*' into table user_api_usercoursetag"
DP_LOAD_SQL_USERIDMAP = "load data inpath '/sqlinput/server-sql-data/sql-user-id-map/*' into table user_id_map"
DP_LOAD_SQL_COURSEWARESTUDENTMODULE = "load data inpath '/sqlinput/server-sql-data/sql-courseware-studentmodule/*' into table courseware_studentmodule"
DP_LOAD_SQL_CERTIFICATESGENERATEDCERTIFICATE = "load data inpath '/sqlinput/server-sql-data/sql-certificates-generatedcertificate/*' into table certificates_generatedcertificate"
DP_LOAD_SQL_WIKIARTICLE = "load data inpath '/sqlinput/server-sql-data/sql-wiki-article/*' into table wiki_article"
DP_LOAD_SQL_WIKIARTICLEREVISION = "load data inpath '/sqlinput/server-sql-data/sql-wiki-articlerevision/*' into table  wiki_articlerevision"
DP_LOAD_SQL_EDMAUTHUSER = '''
			insert into table edm_auth_user
			select id,username,email,is_staff,is_active,last_login,date_joined
			from auth_user
			'''
DP_LOAD_SQL_EDMAUTHUSERPROFILE = '''
					insert into table edm_auth_userprofile
					select id,user_id,meta,gender,mailing_address,year_of_birth,level_of_education,goals
					from auth_userprofile
							'''
DP_LOAD_SQL_EDMSTUDENTCOURSEENROLLMENT = '''
		insert into table edm_student_courseenrollment
		select id,user_id,course_id,created,is_active,mode
		from edm_student_courseenrollment
		'''
DP_LOAD_SQL_EDMCOURSEWARESTUDENTMODULE = '''
					insert into table edm_courseware_studentmodule
 					select id,module_type,module_id,student_id,state,grade,
					created,modified,max_grade,done,course_id
					from edm_courseware_studentmodule
					'''
DP_LOAD_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE = '''
		insert into table edm_certificates_generatedcertificate
		select id,user_id,grade,course_id
		from edm_certificates_generatedcertificate
					'''

# LOCAL DB VARS
##CREATE
#Non EDM Schemas are being Created by SQOOP
LOCAL_CREATE_SQL_EDMAUTHUSER=DP_CREATE_SQL_EDMAUTHUSER
LOCAL_CREATE_SQL_EDMAUTHUSERPROFILE=DP_CREATE_SQL_EDMAUTHUSERPROFILE
LOCAL_CREATE_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE=DP_CREATE_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE
LOCAL_CREATE_SQL_EDMCOURSEWARESTUDENTMODULE=DP_CREATE_SQL_EDMCOURSEWARESTUDENTMODULE
LOCAL_CREATE_SQL_EDMSTUDENTCOURSEENROLLMENT=DP_CREATE_SQL_EDMSTUDENTCOURSEENROLLMENT
##LOAD
#Non EDM are being Loaded by SQOOP
LOCAL_LOAD_SQL_EDMAUTHUSER=DP_LOAD_SQL_EDMAUTHUSER
LOCAL_LOAD_SQL_EDMAUTHUSERPROFILE=DP_LOAD_SQL_EDMAUTHUSERPROFILE
LOCAL_LOAD_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE=DP_LOAD_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE
LOCAL_LOAD_SQL_EDMCOURSEWARESTUDENTMODULE=DP_LOAD_SQL_EDMCOURSEWARESTUDENTMODULE
LOCAL_LOAD_SQL_EDMSTUDENTCOURSEENROLLMENT=DP_LOAD_SQL_EDMSTUDENTCOURSEENROLLMENT
##DROP
LOCAL_DROP_SQL_AUTHUSER=DP_DROP_SQL_AUTHUSER
LOCAL_DROP_SQL_AUTHUSERPROFILE=DP_DROP_SQL_AUTHUSERPROFILE
LOCAL_DROP_SQL_AUTHGROUP="drop table if exists auth_group"
LOCAL_DROP_SQL_AUTHUSERGROUP="drop table if exists auth_user_groups"
LOCAL_DROP_SQL_CELERYTASKMETA="drop table if exists celery_taskmeta"
LOCAL_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE=DP_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE
LOCAL_DROP_SQL_COURSEWARESTUDENTMODULE=DP_DROP_SQL_COURSEWARESTUDENTMODULE
LOCAL_DROP_SQL_WIKIARTICLE=DP_DROP_SQL_WIKIARTICLE
LOCAL_DROP_SQL_WIKIARTICLEREVISION=DP_DROP_SQL_WIKIARTICLEREVISION
LOCAL_DROP_SQL_USERIDMAP=DP_DROP_SQL_USERIDMAP
LOCAL_DROP_SQL_USERAPIUSERCOURSETAG=DP_DROP_SQL_USERAPIUSERCOURSETAG
LOCAL_DROP_SQL_STUDENTCOURSEENROLLMENT=DP_DROP_SQL_STUDENTCOURSEENROLLMENT
LOCAL_DROP_SQL_COURSEWARESTUDENTMODULEHISTORY="drop table if exists courseware_studentmodulehistory"
LOCAL_DROP_SQL_EDMAUTHUSER=DP_DROP_SQL_EDMAUTHUSER
LOCAL_DROP_SQL_EDMAUTHUSERPROFILE=DP_DROP_SQL_EDMAUTHUSERPROFILE
LOCAL_DROP_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE=DP_DROP_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE
LOCAL_DROP_SQL_EDMCOURSEWARESTUDENTMODULE=DP_DROP_SQL_EDMCOURSEWARESTUDENTMODULE
LOCAL_DROP_SQL_EDMSTUDENTCOURSEENROLLMENT=DP_DROP_SQL_EDMSTUDENTCOURSEENROLLMENT

###LOG TABLES

#LOCAL
##CREATE
LOCAL_CREATE_LOG_JSONTABLE = "create table json_table (json string)"
LOCAL_CREATE_LOG_LOGTABLE = '''
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
			)
	'''
LOCAL_CREATE_LOG_REMLOGTABLE = '''
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
			)
	'''
LOCAL_CREATE_LOG_PILOGTABLE = '''
		create table pi_log_table(
		     username string,
		    host string,
		    event_source string,
		    event_type string,
		    --Context
		    user_id string,
		    org_id string,
		    session string,
		            -- module
		        display_name string,
		    course_id string,
		    path string,
		    ----- context end----
		    time string,
		    ip string,
		-- -- EVENT---------
		 submission string,-- (object)
		success string,
		failure string,
		grade string,
		correct_map string,
		-- state
		    student_answers string,
		    seed int,
		    done boolean,
		    input_state string,
		answers string,
		attempts int,
		max_grade string,
		problem_id string,
		orig_score int,
		orig_total int,
		new_score int,
		new_total int,
		old_state string,
		new_state string,
		----- event end-----------
		 agent string,
		 page string
		 )
	'''
LOCAL_CREATE_LOG_NELOGTABLE = '''
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
		)
	'''
LOCAL_CREATE_LOG_EELOGTABLE = '''
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
		mode string)
	'''
LOCAL_CREATE_LOG_VILOGTABLE = '''
	create table vi_log_table(
	username string,
	host string,
	event_source string,
	event_type string,
	---- context ----
	user_id string,
	ip string,
	org_id string,
	session string,
	course_id string,
	path string,
	----context end---
	agent string,
	time string,
	page string,
	-------event start------
	id string,
	currentTime string,
	speed float,
	code string,
	old_time string,
	new_time string,
	type string,
	current_time string,
	old_speed float,
	new_speed float)
'''


##LOAD
LOCAL_LOAD_LOG_JSONTABLE = "load data inpath '/loginput/local-log-data/*' into table json_table"
LOCAL_LOAD_LOG_LOGTABLE='''
	insert into table log_table
	 select v1.username,v1.host,v1.event_source,
	 		case when reverse(split(reverse(event_type),'/')[0])!="" then reverse(split(reverse(event_type),'/')[0]) else event_type end as event_type,
			 context.course_user_tags,context.session,context.course_id,context.org_id,context.user_id,context.path,
			 concat_ws(' ',split(split(v1.time,'[.]')[0],'T')[0],split(split(v1.time,'[.]')[0],'T')[1]),
			 --v1.time,
			 v1.ip,v1.event,v1.agent,v1.page
			 from json_table jt lateral view json_tuple
			 (jt.json,'username','host','event_source','event_type','context','time','ip','event','agent','page') v1
			 as username,host,event_source,event_type,context,time,ip,event,agent,page
			 lateral view json_tuple(v1.context,'course_user_tags','session','course_id','org_id','path','user_id') context
			 as course_user_tags,session,course_id,org_id,path,user_id
	'''
LOCAL_LOAD_LOG_REMLOGTABLE = '''
	insert into table rem_log_table
	 select v1.username,v1.host,v1.event_source,
	          case when reverse(split(reverse(event_type),'/')[0])!="" then reverse(split(reverse(event_type),'/')[0]) else event_type end as event_type,
	         context.course_user_tags,context.session,context.course_id,context.org_id,context.user_id,context.path,
	         concat_ws(' ',split(split(v1.time,'[.]')[0],'T')[0],split(split(v1.time,'[.]')[0],'T')[1]),
	         v1.ip,v1.event,v1.agent,v1.page
	         from json_table jt lateral view json_tuple
	         (jt.json,'username','host','event_source','event_type','context','time','ip','event','agent','page') v1
	         as username,host,event_source,event_type,context,time,ip,event,agent,page
	         lateral view json_tuple(v1.context,'course_user_tags','session','course_id','org_id','path','user_id') context
	         as course_user_tags,session,course_id,org_id,path,user_id
	         where not ( event_type  like '%problem_check' )
	         or  not (event_type like '%problem_check_fail' )
	         or not (event_type like '%problem_reset' )
	         or not (event_type like '%problem_rescore' )
	         or not (event_type like '%problem_rescore_fail' )
	         or not (event_type like '%problem_save')
	         or not (event_type like '%problem_show')
	         or not (event_type like '%reset_problem')
	         or not (event_type like '%reset_problem_fail')
	         or not (event_type like '%show_answer')
	         or not (event_type like '%save_problem_fail')
	         or not (event_type like '%save_problem_success')
	         or not (event_type like '%problem_graded')
	         or not (event_type like '%problem_check')
	         or not (event_type like '%problem_get')
	         or not (event_type like '%problem_save')
	         or not (event_type like '%problem_show')
	         or not (event_type like '%seq_goto')
	         or not (event_type like '%seq_next')
	         or not (event_type like '%seq_prev')
	         or not (event_type like '%page_close')
	         or not (event_type like '%edx.course.enrollment.activated')
	         or not (event_type like '%edx.course.enrollment.deactivated')
	         or not (event_type like '%edx.course.enrollment.upgrade.clicked')
	         or not (event_type like '%edx.course.enrollment.upgrade.succeeded')
	         or not (event_type like '%play_video')
	         or not (event_type like '%pause_video')
	         or not (event_type like '%seek_video')
	         or not (event_type like '%speed_change_video')
	         or not (event_type  like '%load_video')
	         or not (event_type  like '%hide_transcript')
	         or not (event_type like '%show_transcript')
	         or not (event_type like '%showanswer' )
	'''
LOCAL_LOAD_LOG_PILOGTABLE = '''
	-- PI Events Log Table
	insert into table pi_log_table
	 -- username,host,event_source,event_type
	 select v1.username,v1.host,v1.event_source,
	 case when reverse(split(reverse(event_type),'/')[0])!="" then reverse(split(reverse(event_type),'/')[0]) else event_type end as event_type,
	 --context
	 context.user_id,context.org_id,context.session,
	 -- context.module.display_name
	 module.display_name,
	 context.course_id,context.path,
	 --time
	 concat_ws(' ',split(split(v1.time,'[.]')[0],'T')[0],split(split(v1.time,'[.]')[0],'T')[1]),
	 --ip,
	 v1.ip,
	 --event
	 event.submission,event.success,event.failure,event.grade,event.correct_map,
	 -- event -- state
	state.student_answers,state.seed,state.done,state.input_state,
	 --event
	 event.answers,event.attempts,event.max_grade,event.problem_id,event.orig_score,event.orig_total,
	 event.new_score,event.new_total,event.old_state,event.new_state,
	 v1.agent,v1.page from json_table jt
	 --headers
	 lateral view json_tuple(jt.json,'username','host','event_source','event_type','context','time','ip','event','agent','page') v1
	 as username,host,event_source,event_type,context,time,ip,event,agent,page
	 --context
	 lateral view json_tuple(v1.context,'course_user_tags','session','course_id','org_id','path','user_id','module') context
	 as course_user_tags,session,course_id,org_id,path,user_id,module
	 --module
	 lateral view json_tuple(context.module,'display_name') module
	 as display_name
	 --event
	 lateral view json_tuple(v1.event,'answers','attempts','correct_map','grade','max_grade','problem_id',
	 	'state','submission','success','failure','orig_score','orig_total','new_score','new_total','old_state','new_state') event
	 as answers,attempts,correct_map,grade,max_grade,problem_id,
	 	state,submission,success,failure,orig_score,orig_total,new_score,new_total,old_state,new_state
	 lateral view json_tuple(event.state,'student_answers','seed','done','input_state') state
	 as student_answers,seed,done,input_state
	 where
	 v1.event_type like '%problem_check'
	 or v1.event_type like '%problem_check_fail'
	 or v1.event_type like '%problem_reset'
	 or v1.event_type like '%problem_rescore'
	 or v1.event_type like '%problem_rescore_fail'
	 or v1.event_type like '%problem_save'
	 or v1.event_type like '%problem_show'
	 or v1.event_type like '%reset_problem'
	 or v1.event_type like '%reset_problem_fail'
	 or v1.event_type like '%show_answer'
	 or v1.event_type like '%showanswer'
	 or v1.event_type like '%save_problem_fail'
	 or v1.event_type like '%save_problem_success'
	 or v1.event_type like '%problem_graded'
	 or v1.event_type like '%problem_check'
	 or v1.event_type like '%problem_get'
	 or v1.event_type like '%problem_save'
	 or v1.event_type like '%problem_show'
	'''
LOCAL_LOAD_LOG_NELOGTABLE = '''
	-- Navigational Events
	insert into table ne_log_table
		select v1.username,v1.event_source,
		case when reverse(split(reverse(event_type),'/')[0])!="" then reverse(split(reverse(event_type),'/')[0]) else event_type end as event_type,
		v1.ip,v1.agent,v1.host,v1.session,
		context.user_id,context.org_id,context.course_id,context.path,
		event.old,event.new,event.id,event.code,event.currentTime,event.speed,event.old_time,event.new_time,event.type,event.current_time,event.old_speed,event.new_speed,
		concat_ws(' ',split(split(v1.time,'[.]')[0],'T')[0],split(split(v1.time,'[.]')[0],'T')[1]),
		v1.page
	 from json_table jt
	 --headers
	 lateral view json_tuple(jt.json,'username','host','event_source','event_type','context','time','ip','event','agent','page','session') v1
	 as username,host,event_source,event_type,context,time,ip,event,agent,page,session
	 --context
	 lateral view json_tuple(v1.context,'course_id','org_id','path','user_id') context
	 as course_id,org_id,path,user_id
	 --event
	 lateral view json_tuple(v1.event,'old','new','id','code','currentTime','speed',
	 	'old_time','new_time','type','current_time','old_speed','new_speed') event
	 as old,new,id,code,currentTime,speed,
	 	old_time,new_time,type,current_time,old_speed,new_speed
	 	where v1.event_type like '%seq_goto'
	 	or v1.event_type like '%seq_next'
	 	or v1.event_type like '%seq_prev'
	 	or v1.event_type like '%page_close'
	'''
LOCAL_LOAD_LOG_EELOGTABLE = '''
	-- EE Log Events
	insert into table ee_log_table
		select v1.username,v1.host,v1.event_source,
		case when reverse(split(reverse(event_type),'/')[0])!="" then reverse(split(reverse(event_type),'/')[0]) else event_type end as event_type,
		v1.name,
		concat_ws(' ',split(split(v1.time,'[.]')[0],'T')[0],split(split(v1.time,'[.]')[0],'T')[1]),
		v1.session,v1.ip,v1.agent,v1.page,
		context.course_id,context.org_id,context.user_id,context.path,
		event.mode
		from json_table jt
		-- headers
		lateral view json_tuple(jt.json,'username','host','event_source','event_type','name','time','session','host','ip','agent','page','context','event') v1
		as username,host,event_source,event_type,name,time,session,host,ip,agent,page,context,event
		-- context
		lateral view json_tuple(v1.context,'course_id','org_id','user_id','path') context as course_id,org_id,user_id,path
		lateral view json_tuple(v1.event,'mode') event as mode
		where v1.event_type like '%edx.course.enrollment.activated'
		or v1.event_type like '%edx.course.enrollment.deactivated'
		or v1.event_type like '%edx.course.enrollment.upgrade.clicked'
		or v1.event_type like '%edx.course.enrollment.upgrade.succeeded'
	'''
LOCAL_LOAD_LOG_VILOGTABLE = '''
	insert into table vi_log_table
	select username,host,event_source,event_type,user_id,
	ip,org_id,session,course_id,path,agent,time,page,
	get_json_object(logs.event,'$.id') as id,
	get_json_object(logs.event,'$.currentTime') as currentTime,
	get_json_object(logs.event,'$.speed') as speed,
	get_json_object(logs.event,'$.code') as code,
	get_json_object(logs.event,'$.old_time') as old_time,
	get_json_object(logs.event,'$.new_time') as new_time,
	get_json_object(logs.event,'$.type') as type,
	get_json_object(logs.event,'$.current_time') as current_time,
	get_json_object(logs.event,'$.old_speed') as old_speed,
	get_json_object(logs.event,'$.new_speed') as new_speed
	from (
	        select v1.username,v1.host,v1.event_source,
	 		case when reverse(split(reverse(event_type),'/')[0])!="" then reverse(split(reverse(event_type),'/')[0]) else event_type end as event_type,
			 context.course_user_tags,context.session,context.course_id,context.org_id,context.user_id,context.path,
			 concat_ws(' ',split(split(v1.time,'[.]')[0],'T')[0],split(split(v1.time,'[.]')[0],'T')[1]) as time,
			 v1.ip,v1.event,v1.agent,v1.page
			 from json_table jt lateral view json_tuple
			 (jt.json,'username','host','event_source','event_type','context','time','ip','event','agent','page') v1
			 as username,host,event_source,event_type,context,time,ip,event,agent,page
			 lateral view json_tuple(v1.context,'course_user_tags','session','course_id','org_id','path','user_id') context
			 as course_user_tags,session,course_id,org_id,path,user_id
		)logs
	where event_type in ('play_video','pause_video','seek_video','speed_change_video','load_video','hide_transcript','show_transcript')
	'''




##DROP
LOCAL_DROP_LOG_LOGTABLE = '''drop table if exists log_table'''
LOCAL_DROP_LOG_JSONTABLE = "drop table if exists json_table"
LOCAL_DROP_LOG_REMLOGTABLE = '''drop table if exists rem_log_table'''
LOCAL_DROP_LOG_PILOGTABLE = '''drop table if exists pi_log_table'''
LOCAL_DROP_LOG_NELOGTABLE = '''drop table if exists ne_log_table'''
LOCAL_DROP_LOG_EELOGTABLE = '''drop table if exists ee_log_table'''
LOCAL_DROP_LOG_VILOGTABLE = '''drop table if exists vi_log_table'''

#Data Packages
##Create
##Load
DP_LOAD_LOG_JSONTABLE = "load data inpath '/loginput/server-log-data/*' into table json_table"
DP_LOAD_LOG_LOGTABLE = LOCAL_LOAD_LOG_LOGTABLE
DP_LOAD_LOG_REMLOGTABLE = LOCAL_LOAD_LOG_REMLOGTABLE
DP_LOAD_LOG_PILOGTABLE = LOCAL_LOAD_LOG_PILOGTABLE
DP_LOAD_LOG_NELOGTABLE = LOCAL_LOAD_LOG_NELOGTABLE
DP_LOAD_LOG_EELOGTABLE = LOCAL_LOAD_LOG_EELOGTABLE
DP_LOAD_LOG_VILOGTABLE=LOCAL_LOAD_LOG_VILOGTABLE

##DROP
DP_DROP_LOG_PILOGTABLE = LOCAL_DROP_LOG_PILOGTABLE
DP_DROP_LOG_VILOGTABLE = LOCAL_DROP_LOG_VILOGTABLE
DP_DROP_LOG_JSONTABLE = LOCAL_DROP_LOG_JSONTABLE
DP_DROP_LOG_LOGTABLE = LOCAL_DROP_LOG_LOGTABLE
DP_DROP_LOG_REMLOGTABLE = LOCAL_DROP_LOG_REMLOGTABLE
DP_DROP_LOG_NELOGTABLE = LOCAL_DROP_LOG_NELOGTABLE
DP_DROP_LOG_EELOGTABLE = LOCAL_DROP_LOG_EELOGTABLE

#Create
DP_CREATE_LOG_LOGTABLE = LOCAL_CREATE_LOG_LOGTABLE
DP_CREATE_LOG_VILOGTABLE = LOCAL_CREATE_LOG_VILOGTABLE
DP_CREATE_LOG_EELOGTABLE = LOCAL_CREATE_LOG_EELOGTABLE
DP_CREATE_LOG_JSONTABLE = LOCAL_CREATE_LOG_JSONTABLE
DP_CREATE_LOG_REMLOGTABLE = LOCAL_CREATE_LOG_REMLOGTABLE
DP_CREATE_LOG_PILOGTABLE = LOCAL_CREATE_LOG_PILOGTABLE
DP_CREATE_LOG_NELOGTABLE = LOCAL_CREATE_LOG_NELOGTABLE


#POSSIBLE EXTRAS
"""
cur.execute('''
        drop table if exists auth_user
    ''')
    cur.execute('''
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
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- -- auth_userprofile
        drop table if exists auth_userprofile
    ''')
    cur.execute('''
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
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- -- student_courseenrollment
        drop table if exists student_courseenrollment
    ''')
    cur.execute('''
        create table student_courseenrollment(
        id int,
        user_id int,
        course_id string,
        created string,
        is_active tinyint,
        mode string)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- -- user_api_usercoursetag
        drop table if exists user_api_usercoursetag
    ''')
    cur.execute('''
        create table user_api_usercoursetag(
        user_id int,
        course_id string,
        key string,
        value string)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- -- user_id_map
        drop table if exists user_id_map
    ''')
    cur.execute('''
        create table user_id_map(
        hashid int,
        id int,
        username string)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- Courseware Progress Data
        -- -- courseware_studentmodule
        drop table if exists courseware_studentmodule
    ''')
    cur.execute('''
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
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- Certificate Data
        -- -- certificates_generatedcertificate
        drop table if exists certificates_generatedcertificate
    ''')
    cur.execute('''
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
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- Wiki Data
        -- -- wiki_article
        drop table if exists wiki_article
    ''')
    cur.execute('''
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
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        -- -- wiki_articlerevision
        drop table if exists wiki_articlerevision
    ''')
    cur.execute('''
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
         FIELDS TERMINATED BY '\t'
    ''')
    cur.execute('''
        ----------------------------------------------------
        ----------------------------------------------------
        -- EDM Improved Data Models
        ----------------------------------------------------
        ----------------------------------------------------
        --Logs
        -- SQL
        drop table if exists edm_auth_user
    ''')
    cur.execute('''
        create table edm_auth_user(
        id int,
        username string,
        email string,
        is_staff tinyint,
        is_active tinyint,
        last_login string,
        date_joined string
        )
    ''')
    cur.execute('''
        -- -- auth_userprofile
        drop table if exists edm_auth_userprofile
    ''')
    cur.execute('''
        create table edm_auth_userprofile(
        id int,
        user_id int,
        meta string,
        gender string,
        mailing_address string,
        year_of_birth int,
        level_of_education string,
        goals string)
    ''')
    cur.execute('''
         -- -- student_courseenrollment
        drop table if exists edm_student_courseenrollment
    ''')
    cur.execute('''
        create table edm_student_courseenrollment(
        id int,
        user_id int,
        course_id string,
        created string,
        is_active tinyint,
        mode string)
    ''')
    cur.execute('''
        -- Courseware Progress Data
        -- -- courseware_studentmodule
        drop table if exists edm_courseware_studentmodule
    ''')
    cur.execute('''
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
        course_id string)
    ''')
    cur.execute('''
        -- Certificate Data
        -- -- certificates_generatedcertificate
        drop table if exists edm_certificates_generatedcertificate
    ''')
    cur.execute('''
        create table edm_certificates_generatedcertificate(
        id int,
        user_id int,
        grade string,
        course_id string)
    ''')
"""