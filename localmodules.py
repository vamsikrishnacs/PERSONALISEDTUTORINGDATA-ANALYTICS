import os
import shutil
from extraModules import *
from variables import *

"""
Info
=>This script is only meant for data concerned with locally generated data
It receives a folder as input and recursively looks for specific files in it
It parses two types of files
==>tracking.log(normal files or .gz)
==>MySQL dump files
Flaws
=>src has to be a folder containing everything inside
"""

def local_sql_upload(username, password, src, cur):
    """
    SQL Dump Files => Local SQL Database using source command
    SQL DB => Hive Tables using sqoop
    Careful! It eats a lot of RAM
    :param username: SQL Username
    :param password: SQL Password
    :param src: Source from where to look for sql dump files
    :param cur: Cursor to hive database
    :return: void
    """
    pattern = 'edxapp'
    db = 'edxapp'
    user = username
    passwd = password
    hivedb = 'LOCALDB'
    # Drop Anything Presently Existing
    cur.execute(LOCAL_CREATEDB)
    cur.execute(LOCAL_USEDB)
    cur.execute(LOCAL_DROP_SQL_AUTHUSER)
    cur.execute(LOCAL_DROP_SQL_AUTHUSERPROFILE)
    cur.execute(LOCAL_DROP_SQL_AUTHGROUP)
    cur.execute(LOCAL_DROP_SQL_AUTHUSERGROUP)
    cur.execute(LOCAL_DROP_SQL_CELERYTASKMETA)
    cur.execute(LOCAL_DROP_SQL_STUDENTCOURSEENROLLMENT)
    cur.execute(LOCAL_DROP_SQL_USERAPIUSERCOURSETAG)
    cur.execute(LOCAL_DROP_SQL_USERIDMAP)
    cur.execute(LOCAL_DROP_SQL_COURSEWARESTUDENTMODULE)
    cur.execute(LOCAL_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE)
    cur.execute(LOCAL_DROP_SQL_WIKIARTICLE)
    cur.execute(LOCAL_DROP_SQL_WIKIARTICLEREVISION)
    for dirname, dirnames, filenames in os.walk(src):
        for filename in filenames:
            filepath = os.path.join(dirname, filename)
            if filename.endswith(".sql") and pattern in filename:
                from subprocess import Popen, PIPE

                process = Popen('mysql %s -u%s -p%s' % (db, user, passwd), stdout=PIPE, stdin=PIPE, shell=True)
                output = process.communicate('source ' + filename)[0]
                db = MySQLdb.connect(host='localhost', user=user, passwd=passwd, db=db)
                cursor = db.cursor()
                imp_tables = ['auth_userprofile', 'auth_user_groups', 'auth_group', 'auth_user',
                              'courseware_studentmodule','student_courseenrollment ','certificates_generatedcertificate',
                              'wiki_articlerevision','wiki_article']

                for i in imp_tables:
                    stmt = "SHOW TABLES LIKE '%s'"
                    cursor.execute(stmt)
                    result = cursor.fetchone()
                    if result:
                        print i + "Found!"
                        comm = "sqoop import --connect jdbc:mysql://localhost/edxapp --username %s --password %s --table %s --hive-import --hive-database %s"
                        sqoop_comm = comm % (user, passwd, i, hivedb)
                        run_bash(sqoop_comm)

def local_prepper(src):
    """
    Given a src
    Cluttered Log Files => Arranged Log Files in single place
    =>First creates directory for dumping detected log files
    =>Searches recursively through directory for any log files either in .gz or not and pushes them all into directory
    =>Files are all ported into HDFS after resetting in case previous files are present
    :param src: source for the log files
    :return: void
    """
    print "Prepping Files in Single Destination"
    run_script(FS_RESET)
    dest_log = os.path.expanduser("~/separate-data/local-log-data/")
    create_dir(dest_log)
    for dirname, dirnames, filenames in os.walk(src):
        for filename in filenames:
            filepath = os.path.join(dirname, filename)
            if "tracking" in filename and ".log" in filename:
                shutil.copy(filepath, dest_log)
    print "Reset HDFS"
    run_script(HDFS_RESET)
    print "Port Detected files to HDFS"
    run_script(LOCAL_HDFS_PORT)

def localupload(username, password, src):
    """
    Local Upload is the main fn. for uploading data
    :param username: username of SQL Server
    :param password: password of SQL Server
    :param src: file containing Data
    """
    c = getHiveConn("127.0.0.1", 10000, "PLAIN", "hduser", "")
    hiveLocalCreateSchemas(c)
    local_prepper(src)
    local_sql_upload(username, password, src, c)
    hiveLocalLoadSchemas(c)

def localupdate(username, password, src):
    """
    Same as localupload() except it updates the tables instead of
    uploading them
    :param username:SQL Username
    :param password: SQL Password
    :param src: Source of files to update
    :return:void
    """
    c = getHiveConn("127.0.0.1", 10000, "PLAIN", "hduser", "")
    local_prepper(src)
    local_sql_upload(username, password, src, c)
    hiveLocalLoadSchemas(c,updating=1)

def hiveLocalLoadSchemas(cur,updating = 0):
    """
    Loads tables from HDFS into respective log tables
    :param cur:Cursor to hive database
    :param updating:If files are being updated it must be set to 1
    :return:void
    """
    cur.execute(LOCAL_USEDB)
    if updating==1:
        cur.execute(LOCAL_DROP_LOG_JSONTABLE)
        cur.execute(LOCAL_CREATE_LOG_JSONTABLE)
    cur.execute(LOCAL_LOAD_LOG_JSONTABLE)
    cur.execute(LOCAL_LOAD_LOG_VILOGTABLE)
    cur.execute(LOCAL_LOAD_LOG_LOGTABLE)
    cur.execute(LOCAL_LOAD_LOG_REMLOGTABLE)
    cur.execute(LOCAL_LOAD_LOG_PILOGTABLE)
    cur.execute(LOCAL_LOAD_LOG_NELOGTABLE)
    cur.execute(LOCAL_LOAD_LOG_EELOGTABLE)

def hiveLocalCreateSchemas(cur):
    """
    Drops previously existing tables if any of the same name
    Creates new tables following exactly the required schema
    :param cur:Connection to hive database
    :return:void
    """
    cur.execute(LOCAL_CREATEDB)
    cur.execute(LOCAL_USEDB)
    cur.execute(LOCAL_DROP_LOG_JSONTABLE)
    cur.execute(LOCAL_CREATE_LOG_JSONTABLE)
    cur.execute(LOCAL_DROP_LOG_LOGTABLE)
    cur.execute(LOCAL_CREATE_LOG_LOGTABLE)
    cur.execute(LOCAL_DROP_LOG_REMLOGTABLE)
    cur.execute(LOCAL_CREATE_LOG_REMLOGTABLE)
    cur.execute(LOCAL_DROP_LOG_PILOGTABLE)
    cur.execute(LOCAL_CREATE_LOG_PILOGTABLE)
    cur.execute(LOCAL_DROP_LOG_NELOGTABLE)
    cur.execute(LOCAL_CREATE_LOG_NELOGTABLE)
    cur.execute(LOCAL_DROP_LOG_EELOGTABLE)
    cur.execute(LOCAL_CREATE_LOG_EELOGTABLE)
    cur.execute(LOCAL_DROP_LOG_VILOGTABLE)
    cur.execute(LOCAL_CREATE_LOG_VILOGTABLE)

def localReset():
    """
    Reset Both the Server File System and HDFS(only files or folders altered by the script)
    :return:void
    """
    c = getHiveConn("127.0.0.1", 10000, "PLAIN", "hduser", "")
    c.execute(LOCAL_CREATEDB)
    c.execute(LOCAL_USEDB)
    c.execute(LOCAL_DROP_LOG_LOGTABLE)
    c.execute(LOCAL_DROP_LOG_JSONTABLE)
    c.execute(LOCAL_DROP_LOG_REMLOGTABLE)
    c.execute(LOCAL_DROP_LOG_VILOGTABLE)
    c.execute(LOCAL_DROP_LOG_EELOGTABLE)
    c.execute(LOCAL_DROP_LOG_NELOGTABLE)
    c.execute(LOCAL_DROP_LOG_PILOGTABLE)
    c.execute(LOCAL_DROP_SQL_WIKIARTICLEREVISION)
    c.execute(LOCAL_DROP_SQL_WIKIARTICLE)
    c.execute(LOCAL_DROP_SQL_AUTHUSER)
    c.execute(LOCAL_DROP_SQL_AUTHUSERPROFILE)
    c.execute(LOCAL_DROP_SQL_USERAPIUSERCOURSETAG)
    c.execute(LOCAL_DROP_SQL_AUTHGROUP)
    c.execute(LOCAL_DROP_SQL_AUTHUSERGROUP)
    c.execute(LOCAL_DROP_SQL_STUDENTCOURSEENROLLMENT)
    c.execute(LOCAL_DROP_SQL_COURSEWARESTUDENTMODULE)
    c.execute(LOCAL_DROP_SQL_CELERYTASKMETA)
    c.execute(LOCAL_DROP_SQL_CERTIFICATESGENERATEDCERTIFICATE)
    c.execute(LOCAL_DROP_SQL_COURSEWARESTUDENTMODULEHISTORY)

