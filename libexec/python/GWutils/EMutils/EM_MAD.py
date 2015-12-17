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

import sys,os
import exceptions,traceback
import logging
from threading import *
from GWutils.EMutils.EM_MAD_actions import *
class EM_MAD(object): 
    def __init__(self, timeout, wannalog, ops, ResourceListClass): 
        self.timeout=timeout 
        self.ops=ops       
        if wannalog:
            environment=os.environ
            user=environment['USER']
            gw_path=os.environ['GW_LOCATION']
            self.log_file=gw_path+'/var/'+'em_'+user+'.log.'+str(os.getpid())
            self.logger=logging.getLogger("em")
            self.log_handler=logging.FileHandler(self.log_file)
            self.log_formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
            self.log_handler.setFormatter(self.log_formatter)
            self.logger.addHandler(self.log_handler)
            self.logger.setLevel(logging.INFO)
        else:
            self.logger=None
        self.job_dict={}
        self.ResourceListClass=ResourceListClass
        self.resource_list=None
    def process_line(self):
        line=sys.stdin.readline()
        tmp_line=line.split()         
        action=""
        jid=""
        contact=""
        rsl_file=""
        if len(tmp_line)>0:
                action=tmp_line[0]
        if len(tmp_line)>1:
                jid=tmp_line[1]
        if len(tmp_line)>2:
                contact=tmp_line[2]
        if len(tmp_line)>3:
                rsl_file=" ".join(tmp_line[3:])
        self._process_action(action, jid, contact, rsl_file)
    def _action_init(self,jid,contact,rsl_file):
        if self.logger!=None :  self.logger.info("INITIALIZATION SUCCESS")
        sys.stdout.write("INIT - SUCCESS -\n")
        sys.stdout.flush()
        self.ops.max_num_jobs=int(jid)
        self.resource_list=self.ResourceListClass(self.ops,self.logger) 
    def _action_submit(self,jid,contact,rsl_file):     
        if not self.job_dict.has_key(jid):
            if self.logger!=None :  self.logger.info("jid %s no existe" % jid)
            self.job_dict[jid]=GwJob(jid)
            if self.logger!=None :  self.logger.info("Se crea el job %s" % self.job_dict[jid].jid)
        try:
            if self.logger!=None :  self.logger.info("intento crear thread para jid: %s" % jid)
            t_submit=Submit(self.job_dict[jid],self.logger,contact,rsl_file,self.resource_list)
            t_submit.start()
            t_updater=Status_updater(self.job_dict[jid],self.logger,self.job_dict,self.resource_list)
            t_updater.start()
        except Exception, e:
            if self.logger!=None :  self.logger.info("el thread submit para el jid %s ha fallado" % jid)
            print e.__class__, e
    def _action_recover(self,jid,contact,rsl_file):
        print "RECOVER %s FAILURE Recover not implemented (404)" % jid
        sys.stdout.flush()
    def _action_cancel(self,jid,contact,rsl_file):
        if self.job_dict.has_key(jid):
            t_cancel=Cancel(self.job_dict[jid],self.logger,self.timeout)
            t_cancel.start()
        else:
            print "CANCEL %s FAILURE Job not initialized" % jid
            sys.stdout.flush()
    def _action_poll(self,jid,contact,rsl_file):
        if self.job_dict.has_key(jid):
            try:
                t_poll=Standalone_poll(self.job_dict[jid],self.logger)
                t_poll.start()
            except Exception, e:
                if self.logger!=None :  self.logger.info("el thread POLL para el jid %s ha fallado" % jid)
                print e.__class__, e
        else:
            print "POLL %s FAILURE Job not initialized" % jid
            sys.stdout.flush()
    def _action_finalize(self,jid,contact,rsl_file):
        print "FINALIZE - SUCCESS -"
        sys.stdout.flush()
        sys.exit(0)
    action_functions = {
            'INIT':         _action_init, 
            'SUBMIT':       _action_submit,
            'RECOVER':      _action_recover,
            'CANCEL':       _action_cancel,
            'POLL':         _action_poll,
            'FINALIZE':     _action_finalize,
    }
    def _process_action(self,action, jid, contact, rsl_file):
        if self.logger!=None :  self.logger.info("GOT MESSAGE: \"%s\"" % " ".join([action, jid, contact, rsl_file]))
        if self.logger!=None :  self.logger.info("  Action: %s" % action)
        if self.logger!=None :  self.logger.info("  Jid: %s" % jid)
        if self.logger!=None :  self.logger.info("  Contact: %s" % contact)
        if self.logger!=None :  self.logger.info("  RSL File: %s" % rsl_file)
        tmp_action=action.upper()
        if self.action_functions.has_key(tmp_action):
                self.action_functions[tmp_action](self,jid,contact,rsl_file)
