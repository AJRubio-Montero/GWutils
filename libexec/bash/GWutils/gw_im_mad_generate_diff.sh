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

#!/bin/bash
create_diff ()
{
touch $TEST_CHANGES
touch $DYNAMIC_FILE_COPY
sleep 30
HOST_NEW_OLD=""
while true
do
   if [ $PILOT_DYNAMIC_IM_FILE -nt $TEST_CHANGES ]
     then  
         cp $DYNAMIC_FILE_COPY $DYNAMIC_FILE_BAK
         cp $PILOT_DYNAMIC_IM_FILE $DYNAMIC_FILE_COPY
        touch $TEST_CHANGES
         HOSTNAMES=`cat $DYNAMIC_FILE_COPY | awk -F'HOSTNAME="' '{print $2}' | awk -F'"' '{print $1}' | sort -u`
        OLDS=`comm -23 <(echo "$HOST_NEW_OLD") <(echo "$HOSTNAMES") `
        NEWS=`comm -13 <(echo "$HOST_NEW_OLD") <(echo "$HOSTNAMES") `
        INFO=`echo $NEWS | tr '\n' ' ' ` 
        if [ "${INFO}" != " " ]
            then
               echo "DISCOVER - SUCCESS $INFO" > $FIFO_OUTPUT 
            fi
        HID_HOSTNAME=`${GW_LOCATION}/bin/gwhost -f | grep 'HOST_ID=\|HOSTNAME=' | grep -v '.HOST_ID\|.HOSTNAME'`
        for HOSTNAME in $OLDS
          do
            HID=`printf "%s\n" ${HID_HOSTNAME[@]} | grep -v "HOSTNAME=${HOSTNAME}." | grep -n1 "HOSTNAME=${HOSTNAME}" | head -n1 | awk -F'=' '{print $2}' `
            if [ $? -eq 0 ] & [ "${HID}" != "" ]
               then 
                    echo 'MONITOR '${HID}' SUCCESS HOSTNAME="'${HOSTNAME}'" QUEUE_NODECOUNT[0]=0 QUEUE_FREENODECOUNT[0]=0'  > $FIFO_OUTPUT
               fi  
          done
         comm -13 <(sort $DYNAMIC_FILE_BAK) <(sort $DYNAMIC_FILE_COPY)   > ${COMM_FILE}
         python -OO $PYTHON_COMMON/im_diff.py $COMM_FILE >> $FIFO_OUTPUT
         sleep 2
        HOST_NEW_OLD=$HOSTNAMES
     else
        sleep $CALLBACK_TIME
     fi
done
}
if [ -z "${GW_LOCATION}" ]; then
    echo "Please, set GW_LOCATION variable." >&2
    exit -1
fi
if [ "$
    echo "Please, set command line args: <dynamic IM file> <output fifo file> <callback time>" >&2
    exit -1
fi
PILOT_DYNAMIC_IM_FILE=$1
FIFO_OUTPUT=$2
if [ "$3" != "" ] 
then
  CALLBACK_TIME=($3 -5)
else
  CALLBACK_TIME=15
fi
PYTHON_COMMON=${GW_LOCATION}/libexec/python/GWutils/IMutils/
USER_VAR_DIR=${GW_LOCATION}/var/`whoami`
if [ ! -d $USER_VAR_DIR ]; then 
   mkdir $USER_VAR_DIR
fi
TEST_CHANGES="${USER_VAR_DIR}/.gw_im_mad_im_file_test.$$"
COMM_FILE="${USER_VAR_DIR}/.gw_im_mad_final_comm_file.$$"
DYNAMIC_FILE_BAK="${USER_VAR_DIR}/.gw_im_mad_dyn_file_bak.$$"
DYNAMIC_FILE_COPY="${USER_VAR_DIR}/.gw_im_mad_dyn_file_copy.$$"
trap "kill 0" SIGINT SIGTERM EXIT
create_diff 
