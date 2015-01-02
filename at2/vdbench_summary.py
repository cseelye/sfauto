#!/usr/bin/env python2.7

import logging
import json

from sys import argv, exit

import sf_platform.api.ApiCommonUtils as api
import requests
requests.packages.urllib3.disable_warnings()

AT2_USER = "automation@solidfire.com"
AT2_PASSWORD = "solidfire"

def call_method(method, params={}):
    ''' Calls into the AT2 API and returns the result under the 'result' key. '''
    result = api.json_rpc_post("https://autotest2.solidfire.net/json-rpc/1.0/",
                               method_name=method, params=params, time_out=180)

    if 'error' in result:
        raise Exception('Error calling %s: %s. %s' % (method, result['error'], params))
    elif not 'result' in result:
        raise Exception('No "result" field in response to call %s. params=%s response=%s'
                        % (method, params, result))
    else:
        return result['result']

def parse_task(task_instance_id):
    result = call_method('GetTaskInstanceByID', params={'taskInstanceID':task_instance_id,'showCopies':True})
    task_instance = result['taskInstance']
    steps = task_instance['taskInstanceSteps']
    results = []

    for step in steps:
        if  step['taskAsStepID']:
            results = results + parse_task(step['taskAsStepTaskInstanceID'])

        if step['stepDisplayName'] == 'Build vdbench workload':
            workload_name = ''
            for inp in step['inputs']:
                if inp['inputName'] and inp['inputName'] == 'workloadName':
                    workload_name = inp['value']
                    break


        if (step['stepName'] == "vdbench_start" or step['stepName'] == 'config_start_vdbench') and (step['status'] == 'background' or step['result'] in ['pass', 'warning']):
            #for inp in step['inputs']:
            #    if inp['inputName'] and inp['rawInput']:
            #        print inp['inputName'].encode('ascii') + ": " + inp['rawInput'].encode('ascii')

            result = call_method('ListVDBenchOutputForGraphing', params={'taskInstanceStepID':step['taskInstanceStepID']})
            if result.get('vdbenchAverages', None):
                result['vdbenchAverages']['title'] = task_instance['taskName']
                if workload_name:
                    result['vdbenchAverages']['workload'] = workload_name
                results.append(result['vdbenchAverages'])
            else:
                if workload_name:
                    print "Could not grab averages for step: {} workload: {}".format(step['taskName'], workload_name)
                else:
                    print "Could not grab averages for task: {}".format(task_instance['taskName'])

    return results

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <task_instance_id>' % argv[0])
        exit(1)

    logging.disable(logging.INFO)
    api.web_auth("https://autotest2.solidfire.net", AT2_USER, AT2_PASSWORD)

    task_instance_id = int(argv[1])
    results = parse_task(task_instance_id)

    print json.dumps(results, indent=4, separators=(',',': '))

