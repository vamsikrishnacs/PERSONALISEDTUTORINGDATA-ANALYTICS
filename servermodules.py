import shutil
from variables import *
from extraModules import *


def uploader(src):
    # Remove directory if previously exists
    run_bash(FS_RESET)
    # shutil.rmtree(os.path.expanduser('~/separate-data'))
    #Create required directories
    dir_create(destarr)
    print destarr
    #Start mix-n-match
    for dirname, dirnames, filenames in os.walk(src):
        for filename in filenames:
            filepath = os.path.join(dirname, filename)
            #JSON STORAGE
            if filename.endswith(".json"):
                shutil.copy(filepath, dest_json)
            #LOG STORAGE
            elif ".log" in filename:
                #print "tracking log found"
		shutil.copy(filepath, dest_log)
            #MONGO STORAGE
            elif filename.endswith(".mongo"):
                shutil.copy(filepath, dest_mongo)
            #SQL STORAGE
            elif filename.endswith(".sql"):
                tablename = filename.split('-')[3]
                if (tablename == "auth_user"):
                    shutil.copy(filepath, dest_auth_user)
                elif (tablename == "auth_userprofile"):
                    shutil.copy(filepath, dest_auth_userprofile)
                elif (tablename == "courseware_studentmodule"):
                    shutil.copy(filepath, dest_courseware_studentmodule)
                elif (tablename == "wiki_article"):
                    shutil.copy(filepath, dest_wiki_article)
                elif (tablename == "wiki_articlerevision"):
                    shutil.copy(filepath, dest_wiki_articlerevision)
                elif (tablename == "student_courseenrollment"):
                    shutil.copy(filepath, dest_student_courseenrollment)
                elif (tablename == "certificates_generatedcertificate"):
                    shutil.copy(filepath, dest_certificates_generatedcertificate)
                elif (tablename == "user_api_usercoursetag"):
                    shutil.copy(filepath, dest_user_api_usercoursetag)
                elif (tablename == "user_id_map"):
                    shutil.copy(filepath, dest_user_id_map)
                elif (tablename == "student_courseenrollment"):
                    shutil.copy(filepath, dest_student_courseenrollment)
    #Cut headers off sql
    #sql_header_cut()
    #Reset HDFS
    run_script(HDFS_RESET)
    #Port Data to HDFS
    run_script(DP_HDFS_PORT)
    #Get a connection to hive
    c = getHiveConn("127.0.0.1", 10000, "PLAIN", "hduser", "")
    hiveCreateSchemas(c)
    hiveLoadSchemas(c)
    print "Upload of Data has been complete"

def updater(src):
    # Remove directory if previously exists
    run_bash(FS_RESET)
    # Create required directories
    dir_create(destarr)
    #Start mix-n-match
    for dirname, dirnames, filenames in os.walk(src):
        for filename in filenames:
            filepath = os.path.join(dirname, filename)
            #JSON STORAGE
            if filename.endswith(".json"):
                shutil.copy(filepath, dest_json)
            #LOG STORAGE
            elif "tracking.log" in filename:
                shutil.copy(filepath, dest_log)
            #MONGO STORAGE
            elif filename.endswith(".mongo"):
                shutil.copy(filepath, dest_mongo)
            #SQL STORAGE
            elif filename.endswith(".sql"):
                tablename = filename.split('-')[3]
                if (tablename == "auth_user"):
                    shutil.copy(filepath, dest_auth_user)
                elif (tablename == "auth_userprofile"):
                    shutil.copy(filepath, dest_auth_userprofile)
                elif (tablename == "courseware_studentmodule"):
                    shutil.copy(filepath, dest_courseware_studentmodule)
                elif (tablename == "wiki_article"):
                    shutil.copy(filepath, dest_wiki_article)
                elif (tablename == "wiki_articlerevision"):
                    shutil.copy(filepath, dest_wiki_articlerevision)
                elif (tablename == "student_courseenrollment"):
                    shutil.copy(filepath, dest_student_courseenrollment)
                elif (tablename == "certificates_generatedcertificate"):
                    shutil.copy(filepath, dest_certificates_generatedcertificate)
                elif (tablename == "user_api_usercoursetag"):
                    shutil.copy(filepath, dest_user_api_usercoursetag)
                elif (tablename == "user_id_map"):
                    shutil.copy(filepath, dest_user_id_map)
                elif (tablename == "student_courseenrollment"):
                    shutil.copy(filepath, dest_student_courseenrollment)
    #Port Data to HDFS
    run_script(DP_HDFS_PORT)
    #Get a connection to hive
    c = getHiveConn("127.0.0.1", 10000, "PLAIN", "hduser", "")
    #Load new Data
    hiveLoadSchemas(c)
    print "Update of Data has been completed"

def hiveLoadSchemas(cur):
    cur.execute(DP_USEDB)
    try:
        cur.execute(DP_LOAD_SQL_AUTHUSER)
        cur.execute(DP_LOAD_SQL_EDMAUTHUSER)
    except:
        print "No sql-auth-user data"
    try:
        cur.execute(DP_LOAD_SQL_AUTHUSERPROFILE)
        cur.execute(DP_LOAD_SQL_EDMAUTHUSERPROFILE)
    except:
        print "No sql-auth-userprofile data"
    try:
        cur.execute(DP_LOAD_SQL_CERTIFICATESGENERATEDCERTIFICATE)
        cur.execute(DP_LOAD_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE)
    except:
        print "No sql-certificates-generatedcertificate data"
    try:
        cur.execute(DP_LOAD_SQL_COURSEWARESTUDENTMODULE)
        cur.execute(DP_LOAD_SQL_EDMCOURSEWARESTUDENTMODULE)
    except:
        print "No sql-courseware-studentmodule data"
    try:
        cur.execute(DP_LOAD_SQL_STUDENTCOURSEENROLLMENT)
        cur.execute(DP_LOAD_SQL_EDMSTUDENTCOURSEENROLLMENT)
    except:
        print "No sql-student-courseenrollment data"
    try:
        cur.execute(DP_LOAD_SQL_USERAPIUSERCOURSETAG)
    except:
        print "No sql-user-api-usercoursetag data"
    try:
        cur.execute(DP_LOAD_SQL_USERIDMAP)
    except:
        print "No sql-user-id-map data"
    try:
        cur.execute(DP_LOAD_SQL_WIKIARTICLE)
    except:
        print "No sql-wiki-article data"
    try:
        cur.execute(DP_LOAD_SQL_WIKIARTICLEREVISION)
    except:
        print "No sql-wiki-articlerevision data"
    try:
        cur.execute(DP_LOAD_LOG_JSONTABLE)
        print "Loading log_table"
        cur.execute(DP_LOAD_LOG_LOGTABLE)
    	cur.execute(LOCAL_LOAD_LOG_VILOGTABLE)
	cur.execute(LOCAL_LOAD_LOG_LOGTABLE)
    	cur.execute(LOCAL_LOAD_LOG_REMLOGTABLE)
    	cur.execute(LOCAL_LOAD_LOG_PILOGTABLE)
    	cur.execute(LOCAL_LOAD_LOG_NELOGTABLE)
    	cur.execute(LOCAL_LOAD_LOG_EELOGTABLE)

    except:
        print "No Log Data"
    cur.close()

def hiveCreateSchemas(cur):
    cur.execute(DP_CREATEDB)
    cur.execute(DP_USEDB)
    cur.execute(DP_DROP_SQL_AUTHUSER)
    cur.execute(DP_CREATE_SQL_AUTHUSER)
    cur.execute(DP_DROP_SQL_AUTHUSERPROFILE)
    cur.execute(DP_CREATE_SQL_AUTHUSERPROFILE)
    cur.execute(DP_DROP_SQL_STUDENTCOURSEENROLLMENT)
    cur.execute(DP_CREATE_SQL_STUDENTCOURSEENROLLMENT)
    cur.execute(DP_DROP_SQL_USERAPIUSERCOURSETAG)
    cur.execute(DP_CREATE_SQL_USERAPIUSERCOURSETAG)
    cur.execute(DP_DROP_SQL_USERIDMAP)
    cur.execute(DP_CREATE_SQL_USERIDMAP)
    cur.execute(DP_DROP_SQL_COURSEWARESTUDENTMODULE)
    cur.execute(DP_CREATE_SQL_COURSEWARESTUDENTMODULE)
    cur.execute(DP_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE)
    cur.execute(DP_CREATE_SQL_CERTIFICATESGENERATEDCERTIFICATE)
    cur.execute(DP_DROP_SQL_WIKIARTICLE)
    cur.execute(DP_CREATE_SQL_WIKIARTICLE)
    cur.execute(DP_DROP_SQL_WIKIARTICLEREVISION)
    cur.execute(DP_CREATE_SQL_WIKIARTICLEREVISION)
    cur.execute(DP_DROP_SQL_EDMAUTHUSER)
    cur.execute(DP_CREATE_SQL_EDMAUTHUSER)
    cur.execute(DP_DROP_SQL_EDMAUTHUSERPROFILE)
    cur.execute(DP_CREATE_SQL_EDMAUTHUSERPROFILE)
    cur.execute(DP_DROP_SQL_STUDENTCOURSEENROLLMENT)
    cur.execute(DP_CREATE_SQL_STUDENTCOURSEENROLLMENT)
    cur.execute(DP_DROP_SQL_EDMCOURSEWARESTUDENTMODULE)
    cur.execute(DP_CREATE_SQL_EDMCOURSEWARESTUDENTMODULE)
    cur.execute(DP_DROP_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE)
    cur.execute(DP_CREATE_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE)
    cur.execute(DP_DROP_LOG_JSONTABLE)
    cur.execute(DP_CREATE_LOG_JSONTABLE)
    cur.execute(DP_DROP_LOG_LOGTABLE)
    cur.execute(DP_CREATE_LOG_LOGTABLE)
    cur.execute(DP_DROP_LOG_REMLOGTABLE)
    cur.execute(DP_CREATE_LOG_REMLOGTABLE)
    cur.execute(DP_DROP_LOG_PILOGTABLE)
    cur.execute(DP_CREATE_LOG_PILOGTABLE)
    cur.execute(DP_DROP_LOG_NELOGTABLE)
    cur.execute(DP_CREATE_LOG_NELOGTABLE)
    cur.execute(DP_DROP_LOG_EELOGTABLE)
    cur.execute(DP_CREATE_LOG_EELOGTABLE)
    cur.execute(DP_DROP_LOG_VILOGTABLE)
    cur.execute(DP_CREATE_LOG_VILOGTABLE)

def dpReset():
    c = getHiveConn("127.0.0.1", 10000, "PLAIN", "hduser", "")
    c.execute(DP_CREATEDB)
    c.execute(DP_USEDB)
    c.execute(DP_DROP_LOG_LOGTABLE)
    c.execute(DP_DROP_LOG_JSONTABLE)
    c.execute(DP_DROP_LOG_REMLOGTABLE)
    c.execute(DP_DROP_LOG_VILOGTABLE)
    c.execute(DP_DROP_LOG_EELOGTABLE)
    c.execute(DP_DROP_LOG_NELOGTABLE)
    c.execute(DP_DROP_LOG_PILOGTABLE)
    c.execute(DP_DROP_SQL_WIKIARTICLEREVISION)
    c.execute(DP_DROP_SQL_WIKIARTICLE)
    c.execute(DP_DROP_SQL_AUTHUSER)
    c.execute(DP_DROP_SQL_AUTHUSERPROFILE)
    c.execute(DP_DROP_SQL_USERIDMAP)
    c.execute(DP_DROP_SQL_USERAPIUSERCOURSETAG)
    c.execute(DP_DROP_SQL_STUDENTCOURSEENROLLMENT)
    c.execute(DP_DROP_SQL_COURSEWARESTUDENTMODULE)
    c.execute(DP_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE)
    c.execute(DP_DROP_SQL_EDMAUTHUSER)
    c.execute(DP_DROP_SQL_EDMAUTHUSERPROFILE)
    c.execute(DP_DROP_SQL_EDMCERTIFICATESGENERATEDCERTIFICATE)
    c.execute(DP_DROP_SQL_EDMCOURSEWARESTUDENTMODULE)
    c.execute(DP_DROP_SQL_EDMSTUDENTCOURSEENROLLMENT)
    c.close()
