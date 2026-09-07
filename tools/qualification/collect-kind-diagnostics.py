#!/usr/bin/env python3
"""Collect bounded scheduling state from one explicitly selected disposable Kind cluster."""
import argparse
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import time


REASONS = set('Completed Error OOMKilled ContainerCannotRun ImagePullBackOff ErrImagePull CrashLoopBackOff '
              'CreateContainerConfigError CreateContainerError InvalidImageName PodInitializing ContainerCreating '
              'StartError DeadlineExceeded Unschedulable FailedScheduling KubeletNotReady KubeletReady '
              'ContainersNotReady PodCompleted PodFailed PodNotReady PodScheduled NodeStatusUnknown '
              'KubeletHasSufficientMemory KubeletHasNoDiskPressure KubeletHasSufficientPID Evicted BackOff'.split())


def scheduling_signals(message):
    text = str(message).lower()
    return [signal for signal, pattern in {
        'insufficient_cpu': 'insufficient cpu', 'insufficient_memory': 'insufficient memory',
        'pod_limit': 'too many pods', 'untolerated_taint': 'untolerated taint',
        'volume_affinity': 'volume node affinity conflict', 'pod_affinity': 'pod anti-affinity',
        'topology_spread': 'topology spread constraints',
    }.items() if pattern in text]


def fields(value, names):
    output = {name: (str(value[name])[:1024] if isinstance(value[name], str) else value[name])
            for name in names if isinstance(value.get(name), (str, int, bool))}
    if 'reason' in output and output['reason'] not in REASONS:
        output['reason'] = 'Other'
    return output


def conditions(value):
    return [dict(fields(item, ('type', 'status', 'reason')),
                 scheduling_signals=scheduling_signals(item.get('message', '')))
            for item in value.get('conditions', [])[:16]]


def summarize(kind, document):
    items = document.get('items', [])
    selected = items[-128:]
    result = []
    for item in selected:
        metadata, spec, status = (item.get(key, {}) for key in ('metadata', 'spec', 'status'))
        record = fields(metadata, ('name', 'namespace'))
        if kind == 'nodes':
            record.update(allocatable=fields(status.get('allocatable', {}), ('cpu', 'memory', 'pods')),
                          conditions=conditions(status),
                          zone=fields(metadata.get('labels', {}), ('topology.kubernetes.io/zone',)),
                          taints=[fields(taint, ('key', 'value', 'effect')) for taint in spec.get('taints', [])[:16]])
        elif kind == 'pods':
            record.update(fields(spec, ('nodeName',)))
            record.update(fields(status, ('phase', 'reason')))
            record['conditions'] = conditions(status)
            record['containers'] = [dict(fields(container, ('name',)),
                requests=fields(container.get('resources', {}).get('requests', {}), ('cpu', 'memory', 'ephemeral-storage')))
                for container in (spec.get('containers', []) + spec.get('initContainers', []))[:16]]
            record['container_statuses'] = []
            for container in (status.get('containerStatuses', []) + status.get('initContainerStatuses', []))[:16]:
                current = fields(container, ('name', 'ready', 'restartCount'))
                for category in ('state', 'lastState'):
                    current[category] = {state: fields(details, ('reason', 'exitCode', 'signal'))
                        for state, details in container.get(category, {}).items()
                        if state in ('waiting', 'running', 'terminated')}
                record['container_statuses'].append(current)
        else:
            record.update(fields(item, ('type', 'reason', 'count', 'lastTimestamp')))
            record['scheduling_signals'] = scheduling_signals(item.get('message', ''))
            record['object'] = fields(item.get('involvedObject', {}), ('kind', 'namespace', 'name'))
        result.append(record)
    return {'items': result, 'omitted_items': max(0, len(items) - len(selected))}


class CaptureLimitExceeded(ValueError):
    pass


def capture(command, limit=16 * 1024 * 1024, timeout=25):
    deadline = time.monotonic() + timeout
    output = bytearray()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise subprocess.TimeoutExpired(command, timeout)
                chunk = os.read(process.stdout.fileno(), min(65536, limit + 1 - len(output)))
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > limit:
                    raise CaptureLimitExceeded()
        return_code = process.wait(timeout=max(0, deadline - time.monotonic()))
        return subprocess.CompletedProcess(command, return_code, bytes(output))
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.kill()
        process.wait()


def collect(kubeconfig, context):
    if not re.fullmatch(r'kind-[a-z0-9][a-z0-9-]{0,62}', context):
        raise ValueError('an exact Kind context is required')
    output = {'context': context, 'errors': []}
    for kind in ('nodes', 'pods', 'events'):
        command = ['kubectl', '--kubeconfig', str(kubeconfig), '--context', context,
                   '--request-timeout=20s', 'get', kind, '--output=json']
        if kind != 'nodes':
            command += ['--all-namespaces']
        if kind == 'events':
            command += ['--field-selector=type=Warning', '--sort-by=.lastTimestamp']
        try:
            result = capture(command)
            if result.returncode:
                output['errors'].append({'resource': kind, 'exit_code': result.returncode})
            else:
                output[kind] = summarize(kind, json.loads(result.stdout))
        except (OSError, subprocess.TimeoutExpired, ValueError, TypeError, AttributeError) as error:
            output['errors'].append({'resource': kind, 'reason': type(error).__name__})
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kubeconfig', type=Path, required=True)
    parser.add_argument('--context', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = collect(args.kubeconfig, args.context)
    with args.output.open('x', encoding='utf-8') as output:
        json.dump(report, output, ensure_ascii=False, sort_keys=True)
        output.write('\n')


if __name__ == '__main__':
    main()
