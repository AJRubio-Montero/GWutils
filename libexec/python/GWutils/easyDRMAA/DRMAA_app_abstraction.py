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

class GridApplication(object):
    def generate_template(self):
        tplt={}
        tplt['EXECUTABLE']='/bin/sleep'
        tplt['NAME']='TEST'
        tplt['ARGUMENTS']='60'
        tplt['REQUIREMENTS']='HOSTNAME = "ce01-tic.ciemat.es" '
        return (tplt, None)
    def associate_jobid(self,BD_id,job_id):
        return None
    def get_outputs(self,jid_selected,status):
        return None
