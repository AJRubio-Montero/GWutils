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

from threading import *
from Queue import *
class GwJob(object):
    def __init__(self, jid):
        self.jid=jid
        self._lock_submitted=Lock() 
        self._submitted=False
        self._lock_monitor=Lock()
        self._monitor=False
        self._lock_match=Lock()
        self._rsl=None
        self._pilot_id=None
        self._condition_status=Condition()
        self._status=None
        self._condition_signal=Condition()
        self._signal=None
class GWJobThread(Thread):
    def __init__(self, gwjob, logger=None, priv_func=None, args=()):
        if priv_func!=None:            
            private_functions={
                               '_getsubmitted': self._getsubmitted,
                               '_setsubmitted': self._setsubmitted,
                               '_getmonitor': self._getmonitor,
                               '_setmonitor': self._setmonitor,
                               '_getstatus': self._getstatus,  
                               '_waitforstatus': self._waitforstatus,
                               '_waittimeforstatus': self._waittimeforstatus,
                               '_setstatus': self._setstatus,    
                               '_getsignal': self._getsignal,  
                               '_waitforsignal': self._waitforsignal,
                               '_setsignal': self._setsignal,
                               '_getmatch': self._getmatch,
                               '_setmatch': self._setmatch                              
                               }
            Thread.__init__(self,target=private_functions[priv_func],args=args)
        else:
            Thread.__init__(self)
        self.gwjob=gwjob
        self.logger=logger
    def _getsubmitted(self, q=None):
        self.gwjob._lock_submitted.acquire()
        aux=self.gwjob._submitted
        self.gwjob._lock_submitted.release()
        if q!=None : q.put(aux) 
        return aux
    def _setsubmitted(self, new_submitted):
        self.gwjob._lock_submitted.acquire()
        self.gwjob._submitted=new_submitted
        self.gwjob._lock_submitted.release() 
    def _getmonitor(self, q=None):
        self.gwjob._lock_monitor.acquire()
        aux=self.gwjob._monitor
        self.gwjob._lock_monitor.release()
        if q!=None : q.put(aux) 
        return aux
    def _setmonitor(self, new_monitor):
        self.gwjob._lock_monitor.acquire()
        self.gwjob._monitor=new_monitor
        self.gwjob._lock_monitor.release() 
    def _getmatch(self, q=None):
        self.gwjob._lock_match.acquire()
        rsl=self.gwjob._rsl
        pilot_id=self.gwjob._pilot_id
        self.gwjob._lock_match.release()
        aux=(rsl, pilot_id)
        if q!=None : q.put(aux) 
        return aux
    def _setmatch(self, new_match):
        (rsl, pilot_id)=new_match
        self.gwjob._lock_match.acquire()
        self.gwjob._rsl=rsl
        self.gwjob._pilot_id=pilot_id
        self.gwjob._lock_match.release() 
    def _getstatus(self, q=None):  
        self.gwjob._condition_status.acquire()
        aux=self.gwjob._status
        self.gwjob._condition_status.release()
        if q!=None : q.put(aux) 
        return aux
    def _waitforstatus(self, q=None):    
        self.gwjob._condition_status.acquire()
        self.gwjob._condition_status.wait()
        aux=self.gwjob._status
        self.gwjob._condition_status.release() 
        if q!=None : q.put(aux)
        return aux
    def _waittimeforstatus(self, secs, q=None):    
        self.gwjob._condition_status.acquire()
        self.gwjob._condition_status.wait(secs)
        aux=self.gwjob._status
        self.gwjob._condition_status.release() 
        if q!=None : q.put(aux)
        return aux
    def _setstatus(self, new_status):
        self.gwjob._condition_status.acquire()
        if self.gwjob._status!=new_status : 
            self.gwjob._status=new_status
            self.gwjob._condition_status.notifyAll()
        self.gwjob._condition_status.release() 
    def _getsignal(self, q=None):  
        self.gwjob._condition_signal.acquire()
        aux=self.gwjob._signal
        self.gwjob._condition_signal.release() 
        if q!=None : q.put(aux)
        return aux
    def _waitforsignal(self, q=None):
        self.gwjob._condition_signal.acquire()
        self.gwjob._condition_signal.wait()
        aux=self.gwjob._signal
        self.gwjob._condition_signal.release()
        if q!=None : q.put(aux) 
        return aux
    def _setsignal(self, new_signal):
        self.gwjob._condition_signal.acquire()
        if self.gwjob._signal!=new_signal : 
            self.gwjob._signal=new_signal
            self.gwjob._condition_signal.notifyAll()
        self.gwjob._condition_signal.release() 
class GWJobWrapper(object):
    def _getresult(self, job, func):
        q=Queue(1)
        new_thread = GWJobThread(job, priv_func=func, args=(q,))
        new_thread.start()
        return q.get()
    def _askandgetresult(self, job, func, argument):
        q=Queue(1)
        new_thread = GWJobThread(job, priv_func=func, args=(argument,q))
        new_thread.start()
        return q.get()
    def _setvalue(self, job, func, argument):
        new_thread = GWJobThread(job, priv_func=func, args=(argument,))
        new_thread.start()
    def getsubmitted(self, job):    
        return self._getresult(job,'_getsubmitted')
    def setsubmitted(self, job, argument):    
        return self._setvalue(job,'_setsubmitted', argument)
    def getmonitor(self, job):   
        return self._getresult(job,'_getmonitor')
    def setmonitor(self, job, argument):
        return self._setvalue(job,'_setmonitor', argument)
    def getstatus(self, job):   
        return self._getresult(job,'_getstatus')  
    def waitforstatus(self, job):   
        return self._getresult(job,'_waitforstatus')
    def waittimeforstatus(self, job, argument):
        return self._askandgetresult(job,'_waittimeforstatus', argument)
    def setstatus(self, job, argument):
        return self._setvalue(job,'_setstatus',  argument) 
    def getsignal(self, job):  
        return self._getresult(job,'_getsignal')  
    def waitforsignal(self, job):
        return self._getresult(job,'_waitforsignal')
    def setsignal(self, job, argument):
        return self._setvalue(job,'_setsignal', argument)
    def getmatch(self, job):    
        return self._getresult(job,'_getmatch')
    def setmatch(self, job, argument):    
        return self._setvalue(job,'_setmatch', argument)
