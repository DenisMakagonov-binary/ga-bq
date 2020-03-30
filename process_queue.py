#!/usr/bin/env python
import webapp2
import json
import logging
from google.appengine.api import taskqueue
import hashlib

from bqloader import BQLoader
#https://cloud.google.com/bigquery/quotas#streaming_inserts


# http://localhost:8080/collect?v=1&_v=j79&a=1782300344&t=pageview&_s=1&dl=https%3A%2F%2Fderiv.app%2F&ul=en-gb&de=UTF-8&dt=Trade%20%7C%20Deriv&sd=24-bit&sr=1440x1282&vp=1440x1282&je=0&_u=SCCAAEADQ~&jid=&gjid=&cid=1496860339.1563518559&tid=UA-139927388-1&_gid=1524347747.1573530585&gtm=2wgav3NF7884S&cd2=EN&z=1012091324&cd4=1496860339.1563518559
#ga-tracker-dot-business-intelligence-240201.appspot.com/collect?v=1&_v=j79&a=1782300344&t=pageview&_s=1&dl=https%3A%2F%2Fderiv.app%2F&ul=en-gb&de=UTF-8&dt=Trade%20%7C%20Deriv&sd=24-bit&sr=1440x1282&vp=1440x1282&je=0&_u=SCCAAEADQ~&jid=&gjid=&cid=1496860339.1563518559&tid=UA-139927388-1&_gid=1524347747.1573530585&gtm=2wgav3NF7884S&cd2=EN&z=1012091324&cd4=1496860339.1563518559
#http://localhost:9080/collect?v=1&_v=j79&a=1782300344&t=pageview&_s=1&dl=https%3A%2F%2Fderiv.app%2F&ul=en-gb&de=UTF-8&dt=Trade%20%7C%20Deriv&sd=24-bit&sr=1440x1282&vp=1440x1282&je=0&_u=SCCAAEADQ~&jid=&gjid=&cid=1496860339.1563518559&tid=UA-139927388-1&_gid=1524347747.1573530585&gtm=2wgav3NF7884S&cd2=EN&z=1012091324&cd4=1496860339.1563518559
class MainHandler(webapp2.RequestHandler):

    def get(self):

        q = taskqueue.Queue('pull-queue')
        tasks = q.lease_tasks(30, 1000)
        logging.info(len(tasks))
        if len(tasks)>0:

            bq_loader = BQLoader()
            rows=[]
#TODO change insert id on clientid+timestamp
            for task in tasks:
                #rows.append({'insertId': task.payload[-100:-1]+str(task._eta_usec), 'json':json.loads(task.payload)})
                rows.append({'insertId': hashlib.md5(str(task.payload).encode('utf-8')).hexdigest(), 'json': json.loads(task.payload)})
            logging.info(rows)

            bq_loader.insert_rows(rows)
            q.delete_tasks(tasks)

        self.response.write('ok')




app = webapp2.WSGIApplication([
    ('/tasks/process_queue', MainHandler)
], debug=True)
