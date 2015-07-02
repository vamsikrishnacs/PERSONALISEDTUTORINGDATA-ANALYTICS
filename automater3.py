# Modules
from socket import *
from servermodules import *
from localmodules import *


def port_checker(user,port):
    sock = socket(AF_INET, SOCK_STREAM)
    result = sock.connect_ex((user, int(port)))
    return (1 - result)


def check_connectivity():
    prob_flag = 0
    port_dict = {'hadoop': port_checker("localhost",9000),'derby server': port_checker("localhost",1527), 'NameNode': port_checker("localhost",50070),'Secondary Namenode': port_checker("localhost",50090)}
    for key in port_dict:
        if (port_dict[key] != 1):
            print "Connection Problem in " + key
            prob_flag += 1
    return prob_flag


def main():
    if (check_connectivity() != 0):
        print "Restart Script After Correcting Connection Errors"
        exit()
    if (sys.argv[1] == "usage"):
        print """
            Usage:
            python automater.py amazonserver <mode> <dir-of-edxData>
            mode = upload/update
                OR
            python automater.py local <mode> <mysqluser> <mysqlpassword> <dir-of-localData>
                OR
            python automater.py reset
            """
        exit()
    elif (sys.argv[1] == "amazonserver"):
        if (sys.argv[2] == "upload"):
            src = os.path.expanduser(sys.argv[3])
            uploader(src)
        elif (sys.argv[2] == "update"):
            src = os.path.expanduser(sys.argv[3])
            updater(src)
    elif (sys.argv[1] == "local"):
        if (sys.argv[2] == "upload"):
            localupload(sys.argv[3], sys.argv[4], os.path.expanduser(sys.argv[5]))
        elif (sys.argv[2] == "update"):
            localupdate(sys.argv[3], sys.argv[4], os.path.expanduser(sys.argv[5]))
    elif (sys.argv[1] == "reset"):
        run_script(FS_RESET)
        run_script(HDFS_RESET)
        localReset()
        dpReset()
        # localReset()
    else:
        print "Incorrect arguements provided"
        run_bash("rm -rf ~/separate-data")


if __name__ == '__main__':
    main()
