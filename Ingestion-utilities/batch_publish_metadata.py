import sys
import subprocess

# reload(sys)
# sys.setdefaultencoding('UTF8')
# reload(sys)

ext_name = "ext_table_"
def_name = "ext_table_def_"

for x in range(1, 13):
    extcurname = ext_name + str(x) + ".csv"
    defcurname = def_name + str(x) + ".csv"
#    print (curname)
    extpath = "/anthem_ts/data/metadata/ext_table/" + extcurname
    defpath = "/anthem_ts/data/metadata/ext_table_def/" + defcurname
    ext_command = ["python", "publish_metadata.py", extpath, "ext_table"]
    def_command = ["python", "publish_metadata.py", defpath, "ext_table_def"]
    print(' '.join(ext_command))
    print(' '.join(def_command))
    subprocess.call(ext_command)
    subprocess.call(def_command)
