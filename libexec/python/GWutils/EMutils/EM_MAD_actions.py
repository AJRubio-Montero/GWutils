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
from GWutils.EMutils.GWJob import *
class Submit(GWJobThread):
    def __init__(self,gwjob,logger,contact,rsl_file,resource_list):
        GWJobThread.__init__(self,gwjob,logger)
        self.rsl_file=rsl_file
        self.contact=contact
        self.resource_list=resource_list
    def run(self):
        try:                
            if self.logger!=None :  self.logger.info("Enter in submit thread")  
            file_handler=open(self.rsl_file, 'r')                    
            rsl=file_handler.read()
            file_handler.close()
            if self.logger!=None :  self.logger.info("=====RSL=====")
            if self.logger!=None :  self.logger.info(rsl)
            if self.logger!=None :  self.logger.info("=====RSL=====")
            pilot_id=self.contact.rsplit('/',1)[0].rstrip('/')           
            match=(rsl,pilot_id)
            self._setmatch(match)
            self.resource_list.set_possible_match(self.gwjob)
            (rsl_bis,pilot_id_bis)=self._getmatch()
            if self.logger!=None :  self.logger.info("Matchmaking queued : match=%s" % pilot_id_bis) 
            print "SUBMIT %s SUCCESS %s" % (self.gwjob.jid, self.gwjob.jid)
            sys.stdout.flush()
            if self.logger!=None :  self.logger.info("Job submitted: jid,job_id \"%s\" " % " ".join([self.gwjob.jid,self.gwjob.jid]))
            self._setstatus("PENDING")
            self._setsubmitted(True)
        except Exception, e:
                self._setmonitor(False)
                self._setstatus("FAILED")
                print "SUBMIT %s FAILED Error sending job" % self.gwjob.jid
                print e
                sys.stdout.flush()
                if self.logger!=None :  self.logger.info("Submit failed for: job_id=%s" % self.gwjob.jid)
class Standalone_poll(GWJobThread):
    def __init__(self,gwjob,logger):
        GWJobThread.__init__(self,gwjob,logger)
    def run(self):
        if self.logger!=None :  self.logger.info("Entrando en poll para el job_id=%s" % self.gwjob.jid)
        submitted=self._getsubmitted()       
        if self.logger!=None :  self.logger.info("Standalone_poll: submitted?=%s" % submitted)
        if not submitted:
            print "POLL %s FAILURE Job not yet submitted" % self.gwjob.jid
            sys.stdout.flush()
        else:
            new_status=self._getstatus()
            if self.logger!=None :  self.logger.info("Standalone_poll: job_id=%s, new_status=%s" % (self.gwjob.jid,new_status))
            print "POLL %s SUCCESS %s" % (self.gwjob.jid, new_status)
            sys.stdout.flush()
class Status_updater(GWJobThread):
    def __init__(self,gwjob,logger,job_list,resource_list):
        GWJobThread.__init__(self,gwjob,logger)
        self.job_list=job_list
        self.resource_list=resource_list
    def run(self):
            self._setmonitor(True)
            while self._getmonitor():
                if self.logger!=None :  self.logger.info("Job monitored: job_id=%s" % self.gwjob.jid)
                new_status=self._waitforstatus()
                if self.logger!=None :  self.logger.info("Job status: job_id=%s, new_status=%s" % (self.gwjob.jid,new_status))
                print "CALLBACK %s SUCCESS %s" % (self.gwjob.jid, new_status)
                sys.stdout.flush()
                if new_status=="DONE" or new_status=="FAILED":
                    self._setmonitor(False)
            del self.job_list[self.gwjob.jid]
            if self._getsubmitted(): self.resource_list.remove_possible_match(self.gwjob)
class Cancel(GWJobThread):
    def __init__(self,gwjob,logger,timeout):
        GWJobThread.__init__(self,gwjob,logger)
        self.timeout=timeout
    def run(self):
        submitted=self._getsubmitted()
        if submitted==True:
            self._setsignal('KILL')
            new_status=self._getstatus()
            if new_status!='DONE' and new_status!='FAILED':
                new_status=self._waittimeforstatus(self.timeout + 1)
            if new_status!='DONE' and new_status!='FAILED':
                self._setstatus('DONE')
            if self.logger!=None :  self.logger.info("Job removed jid :status \"%s\" " % " ".join([self.gwjob.jid,new_status]))
            self._setsubmitted(False)
            print "CANCEL %s SUCCESS -" % self.gwjob.jid
            sys.stdout.flush()
        else:
            print "CANCEL %s FAILURE Job not yet submitted or are in a final state" % self.gwjob.jid
            sys.stdout.flush()
