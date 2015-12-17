###########################################################################
#
#   Copyright 2010-2015, A.J. Rubio-Montero (CIEMAT - Sci-track R&D group)         
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
###########################################################################
#
#   Additional license statement: 
#
#    If you use this code to perform any kind of research, report, 
#    documentation, or development you should properly cite GWutils in your 
#    work with any related paper listed at: 
#         http://rdgroups.ciemat.es/web/sci-track/
#
###########################################################################

#!/usr/bin/python
import os,time,sys,subprocess
environment=os.environ
gw_path=environment['GW_LOCATION']
file_input=str(sys.argv[1])
cmd=gw_path+"/bin/gwhost -f | grep 'HOST_ID=\|HOSTNAME=' | grep -v '.HOST_ID\|.HOSTNAME'"
p = subprocess.Popen(cmd + ' 2>&1', shell = True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
p.wait()
hid_hostname_str = p.communicate()[0]
hid_hostname_list=hid_hostname_str.split('\n')
hid_hostname_list.pop()
hid_hostname_aux=[x.split('=')[1] for x in hid_hostname_list]
hid_hostname_aux.reverse()
host_id_dict=dict(zip(hid_hostname_aux[::2],hid_hostname_aux[1::2]))
f=open(file_input,"r")
data_lines=f.readlines()           
f.close()
data_lines_for_gw=[]
for line in data_lines:
    hostname=line.split('HOSTNAME="')[1].split('"')[0]
    if hostname in  host_id_dict.keys():
        hid=host_id_dict[hostname]
        data_lines_for_gw.append("MONITOR "+hid+" SUCCESS "+line)    
count=0
lines=[]
for line in data_lines_for_gw :
    count+=1
    lines.append(line) 
    if count>9 :
        print ''.join(lines),
        lines=[]
        count=0
        time.sleep(0.02)
if len(lines) > 0 : 
    print ''.join(lines),
