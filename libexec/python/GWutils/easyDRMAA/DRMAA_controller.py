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

import os, sys, time
from Queue import Queue
from datetime import datetime
from DRMAA import *
DRMAA_vars={
            "${TOTAL_TASKS}": DRMAA_GW_TOTAL_TASKS,   
            "${JOB_ID}": DRMAA_GW_JOB_ID,
            "${TASK_ID}": DRMAA_GW_TASK_ID,   
            "${PARAM}": DRMAA_GW_PARAM,   
            "${MAX_PARAM}": DRMAA_GW_MAX_PARAM,   
            "${ARCH}": DRMAA_GW_ARCH 
}
DRMAA_vector_vars=[DRMAA_V_ARGV,DRMAA_V_ENV,DRMAA_V_EMAIL,DRMAA_V_GW_INPUT_FILES,DRMAA_V_GW_OUTPUT_FILES,DRMAA_V_GW_RESTART_FILES]
DRMAA_equiv = {}
DRMAA_equiv['NAME']=DRMAA_JOB_NAME
DRMAA_equiv['EXECUTABLE']=DRMAA_REMOTE_COMMAND
DRMAA_equiv['ARGUMENTS']=DRMAA_V_ARGV
DRMAA_equiv['ENVIRONMENT']=DRMAA_V_ENV
DRMAA_equiv['NP']=DRMAA_GW_NP
DRMAA_equiv['INPUT_FILES']=DRMAA_V_GW_INPUT_FILES
DRMAA_equiv['OUTPUT_FILES']=DRMAA_V_GW_OUTPUT_FILES
DRMAA_equiv['STDIN_FILE']=DRMAA_INPUT_PATH
DRMAA_equiv['STDOUT_FILE']=DRMAA_OUTPUT_PATH
DRMAA_equiv['STDERR_FILE']=DRMAA_ERROR_PATH
DRMAA_equiv['RESTART_FILES']=DRMAA_V_GW_RESTART_FILES
DRMAA_equiv['REQUIREMENTS']=DRMAA_GW_REQUIREMENTS
DRMAA_equiv['RANK']=DRMAA_GW_RANK
DRMAA_equiv['DEADLINE']=DRMAA_DEADLINE_TIME
DRMAA_equiv['RESCHEDULE_ON_FAILURE']=DRMAA_GW_RESCHEDULE_ON_FAILURE
DRMAA_equiv['NUMBER_OF_RETRIES']=DRMAA_GW_NUMBER_OF_RETRIES
if 'DRMAA_GW_SUSPENSION_TIMEOUT' in globals(): DRMAA_equiv['SUSPENSION_TIMEOUT']=DRMAA_GW_SUSPENSION_TIMEOUT
def print_log_info(msg,logger=None):
    if logger!=None: logger.info(msg)
    else: print msg
def print_log_err(msg,logger=None):
    if logger!=None: logger.error(msg)
    else: print >> sys.stderr, msg
def handle_if_drmaa_errno_success(result, error, error_msg, logger=None):
    if result != DRMAA_ERRNO_SUCCESS:
        print_log_err( error_msg+" %s" % (error), logger)
        raise Exception(error_msg+" %s" % (error))
def modify_drmma_job_template(jt, tplt_dict, logger=None):
    for k in tplt_dict.keys():
        s=tplt_dict[k]
        for k2 in DRMAA_vars.keys(): s.replace(k2,DRMAA_vars[k2])
        if k in DRMAA_equiv.keys():
            drmaa_attrib=DRMAA_equiv[k]
            if DRMAA_vector_vars.count(drmaa_attrib) > 0 :
                vector=s.split(',')
                (result, error)=drmaa_set_vector_attribute(jt, drmaa_attrib, vector)
            else:
                (result, error)=drmaa_set_attribute(jt, drmaa_attrib, s)  
            handle_if_drmaa_errno_success(result, error, "Error setting job template attribute:",logger)
        else:
            print_log_err("Template attribute not implemented for DRMAA: %s" % (k),logger) 
def setup_drmma_job_template(tplt_dict, logger=None):
    (result, jt, error)=drmaa_allocate_job_template()
    handle_if_drmaa_errno_success(result, error, "drmaa_allocate_job_template() failed:", logger)
    cwd=os.getcwd()
    (result, error)=drmaa_set_attribute(jt, DRMAA_WD, cwd)
    handle_if_drmaa_errno_success(result, error, "Error setting job template attribute:", logger)
    modify_drmma_job_template(jt, tplt_dict, logger)
    return jt
def submit_DRMAA_template(jt, logger=None):
    (result, job_id, error)=drmaa_run_job(jt)
    handle_if_drmaa_errno_success(result,error,"drmaa_run_job() failed: ", logger)
    print_log_err( "Job successfully submited ID: %s" % (job_id), logger )
    (result, value, error)=drmaa_get_attribute(jt, DRMAA_JOB_NAME)
    handle_if_drmaa_errno_success(result,error,"drmaa_run_job() failed: ", logger) 
    os.remove(value)
    print_log_err( "Template file successfully removed: %s" % (job_id), logger)
    return job_id                 
def submit_GW_template(tplt_dict, logger=None):    
    jt=setup_drmma_job_template(tplt_dict, logger) 
    job_id=submit_DRMAA_template(jt, logger)
    return (job_id, jt)                         
from threading import *
class GWjidList(object):
    def __init__(self, maxjobs, jidlist=None):
        self._maxjobs=maxjobs
        self._qsent=Queue(maxjobs)
        self._qdone=Queue(maxjobs)
        self._total_jobs_in_gw=0
        self._condition_total_jobs_in_gw=Condition()
        if jidlist!=None: 
            for jid in jidlist:
                self._qsent.put(jid)
                self._total_jobs_in_gw += 1
    def info_num_jobs(self): return 
    def info_num_done_for_consume(self): return self._qdone.qsize()
class JobControlThread(Thread):
    def __init__(self, gwjidlist, application, logger=None, priv_func=None, args=()):
        if priv_func!=None:            
            private_functions={
                               'produce_jobs': self.produce_jobs,
                               'consume_dones': self.consume_dones,
                               'detect_dones': self.detect_dones,
                               }
            Thread.__init__(self,target=private_functions[priv_func],args=args)
        else:
            Thread.__init__(self)
        self._gwjidlist=gwjidlist
        self._app=application
        self.logger=logger
    def produce_jobs(self, args=None):
        while True:
            self._gwjidlist._condition_total_jobs_in_gw.acquire()
            while self._gwjidlist._total_jobs_in_gw >= self._gwjidlist._maxjobs :
                self._gwjidlist._condition_total_jobs_in_gw.wait()
            self._gwjidlist._total_jobs_in_gw += 1
            self._gwjidlist._condition_total_jobs_in_gw.release()
            try:            
                (tplt_dict, BD_id)= self._app.generate_template()
                if tplt_dict == None :
                    self._gwjidlist._condition_total_jobs_in_gw.acquire()
                    self._gwjidlist._total_jobs_in_gw -= 1
                    self._gwjidlist._condition_total_jobs_in_gw.notify()
                    self._gwjidlist._condition_total_jobs_in_gw.release()
                    time.sleep(10)
                else:
                    (job_id, jt) = submit_GW_template(tplt_dict)
                    (result, error)=drmaa_delete_job_template(jt)
                    handle_if_drmaa_errno_success(result, error, "drmaa_delete_job_template() failed: ", self.logger)
                    print_log_info(' JOB: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' '+ str(job_id), self.logger)
                    self._app.associate_jobid(BD_id,job_id)
                    self._gwjidlist._qsent.put(job_id)
            except:
                time.sleep(1)
    def detect_dones(self, args=None):
        while True:
            time.sleep(0.3)
            job_id= self._gwjidlist._qsent.get(True)
            retry=True
            while retry:
                try: 
                    (result,remote_ps,error)=drmaa_job_ps(job_id)
                    handle_if_drmaa_errno_success(result, error, "drmaa_job_ps() failed: ", self.logger)
                    retry=False 
                except:
                    time.sleep(1)
            if (remote_ps == DRMAA_PS_DONE)  or (remote_ps == DRMAA_PS_FAILED):
                self._gwjidlist._qdone.put((job_id, remote_ps))
            else: 
                self._gwjidlist._qsent.put(job_id)
    def consume_dones(self, args=None):
        while True:
            (job_id, remote_ps) = self._gwjidlist._qdone.get(True)
            if remote_ps==DRMAA_PS_DONE :
                st='DONE'
            else:
                st='FAILED'
            try:
                self._app.get_outputs(job_id,st)
                (result, job_id_out, stat, rusage, error)=drmaa_wait(job_id, DRMAA_TIMEOUT_NO_WAIT)
                handle_if_drmaa_errno_success(result, error, "drmaa_control(terminate) failed: ", self.logger)            
                size=4
                usage=''
                for n in range(0,size) :
                    (result, value)=drmaa_get_next_attr_value(rusage)
                    handle_if_drmaa_errno_success(result, error, "drmaa_control(terminate) failed: ", self.logger)
                    laux=value.split('=')
                    usage+=' '+laux[1]   
                drmaa_release_attr_values(rusage)
                s=datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' '+ str(job_id) + ' ' + str(stat) + ' ' + usage
                print_log_info(' JOB: ' + s, self.logger)
            except:
                time.sleep(1)
            self._gwjidlist._condition_total_jobs_in_gw.acquire()    
            self._gwjidlist._total_jobs_in_gw -= 1
            self._gwjidlist._condition_total_jobs_in_gw.notify()
            self._gwjidlist._condition_total_jobs_in_gw.release()
class JobControl(object):     
    def __init__(self, application, maxjobs,numproducers,numconsumers,logger=None):
        self.jidList=GWjidList(maxjobs)
        self.thread_list=[]
        self.app=application
        self.logger=logger
        for i in range(0, numproducers): 
            self.thread_list.append( JobControlThread(self.jidList, self.app, self.logger,'produce_jobs') )
        self.thread_list.append( JobControlThread(self.jidList, self.app, self.logger,'detect_dones') )
        for i in range(0, numconsumers): 
            self.thread_list.append( JobControlThread(self.jidList, self.app, self.logger, 'consume_dones') )
        for i in self.thread_list:
            i.start()
    def control_threads_status(self):
        print_log_info("Total numer of jobs: " +  str(self.jidList.info_num_jobs()), self.logger)
        print_log_info("Total done waiting for post-processing: " +  str(self.jidList.info_num_done_for_consume()), self.logger)
        print_log_info("Number of threads: "+ str(len(self.thread_list)), self.logger)
        for i in self.thread_list:
            print_log_info("Thread active: "+ str(i.isAlive()), self.logger)
class DRMAA_controller():
    def __init__(self, application, maxjobs, numproducers, numconsumers, logger=None):
        try: 
            (result, error)=drmaa_init(None)
            handle_if_drmaa_errno_success(result,error,"drmaa_init() dailed: ", logger)
            (result, drm_system, error)=drmaa_get_DRM_system()
            print_log_info('DRMAA system: '+drm_system, logger)
            (result, drmaa_impl, error)=drmaa_get_DRMAA_implementation()
            print_log_info('DRMAA implentation: '+drmaa_impl, logger)
            (result, major, minor, error)=drmaa_version()
            print_log_info('DRMAA version: '+str(major)+'.'+str(minor), logger)
            (result, values, error)=drmaa_get_vector_attribute_names()
            print_log_info('DRMAA valid attribute names: '+values, logger)
            jcontrol=JobControl(application, maxjobs, numproducers, numconsumers, logger)
        except:
            sys.exit(-1)
        try:
            while True:
                time.sleep(30)
                jcontrol.control_threads_status()   
        except KeyboardInterrupt:
            pass  
        try:
            (result, error)=drmaa_exit()
            handle_if_drmaa_errno_success(result,error,"drmaa_exit() failed: ", logger)
            print_log_info("The end", logger)
        finally:
            sys.exit(-1)
