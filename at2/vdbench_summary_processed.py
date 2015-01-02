#!/usr/bin/env python2.7

import logging
import json
import numpy
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
    collected = {}
    workload_name = ''
    for step in steps:



        if step['stepDisplayName'] == 'Build vdbench workload':
            for inp in step['inputs']:
                if inp['inputName'] and inp['inputName'] == 'workloadName':
                    workload_name = inp['value']
                    break
            if workload_name not in collected:
                collected[workload_name] = []

        if (step['stepName'] == "vdbench_start" or step['stepName'] == 'config_start_vdbench') and (step['status'] == 'background' or step['result'] in ['pass', 'warning']):
            #for inp in step['inputs']:
            #    if inp['inputName'] and inp['rawInput']:
            #        print inp['inputName'].encode('ascii') + ": " + inp['rawInput'].encode('ascii')

            result = call_method('ListVDBenchOutputForGraphing', params={'taskInstanceStepID':step['taskInstanceStepID']})
            if result.get('vdbenchAverages', None):
                result['vdbenchAverages']['title'] = task_instance['taskName']
                if workload_name:
                    result['vdbenchAverages']['workload'] = workload_name
                    collected[workload_name].append(result['vdbenchAverages'])
                else:
                    if result['vdbenchAverages']['title'] not in collected.keys():
                        collected[result['vdbenchAverages']['title']] = []
                    collected[result['vdbenchAverages']['title']].append(result['vdbenchAverages'])

            else:
                if workload_name:
                    print "Missing vdbench averages for step: {} ({}) workload: {}".format(step['stepName'], step['taskInstanceStepID'], workload_name)
                else:
                    print "Missing vdbench averages for task: {}".format(task_instance['taskName'])

            workload_name = ''

    processed_results = []
    for wl in sorted(collected.keys()):
        if not collected[wl]:
            continue
        iops = round(numpy.mean([r['iops'] for r in collected[wl]]), 3)
        iops_std = round(numpy.std([r['iops'] for r in collected[wl]]), 3)
        mbps = round(numpy.mean([r['mbps'] for r in collected[wl]]), 3)
        mbps_std = round(numpy.std([r['mbps'] for r in collected[wl]]), 3)
        latency = round(numpy.mean([r['responseTime'] for r in collected[wl]]), 3)
        latency_std = round(numpy.std([r['responseTime'] for r in collected[wl]]), 3)
        processed_results.append({'workload': wl, 'samples': len(collected[wl]), 'iops': iops, 'iops_std': iops_std, 'mbps': mbps, 'mbps_std': mbps_std, 'latency': latency, 'latency_std': latency_std})

    # Return the averages
    return processed_results

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <task_instance_id>' % argv[0])
        exit(1)

    logging.disable(logging.INFO)
    api.web_auth("https://autotest2.solidfire.net", AT2_USER, AT2_PASSWORD)

    task_instance_id = int(argv[1])
    results = parse_task(task_instance_id)

    # Print the raw results
    #print json.dumps(results, indent=4, separators=(',',': '))
    #print

    # Print out a CSV table
    print
    print ','.join(['Workload Name', 'Mean IOPS', 'Mean MB/s', 'Mean Latency (ms)', 'Std Dev IOPS', 'Std Dev MB/s', 'Std Dev Latency', 'Number of Samples'])
    for r in results:
        print ','.join(map(str, [r['workload'], r['iops'], r['mbps'], r['latency'], r['iops_std'], r['mbps_std'], r['latency_std'], r['samples']]))
