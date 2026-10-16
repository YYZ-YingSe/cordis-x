"""Middleware protocol peer: forged capabilities, early replies and unknown outcomes."""
from binary_peer import receive, send, respond
import os
import struct
import sys
import time

mode, trace = sys.argv[1:]
pending = {}
next_operation = 0
grant = None
running = None
old_context = None
next_value = None






def reverse(request, command, payload, action='result'):
    global next_operation
    next_operation += 1
    callback = {k: request[k] for k in ('session', 'activation')}
    callback.update(operation=str(next_operation), command=command, payload=payload)
    pending[callback['operation']] = (request, action)
    send(callback)


def record(event):
    with open(trace, 'a', encoding='utf-8', newline='\n') as output:
        output.write(event + '\n')


def following(request, action, forged=False, context=None):
    p = running['payload']
    reverse(request, 'middleware_next', {
        'call': p['call'], 'grant': '0' * 32 if forged else p['next'],
        'context': p['context'] if context is None else context, 'args': 7,
    }, action)


while True:
    request = receive()
    if 'outcome' in request:
        original, action = pending.pop(request['operation'])
        outcome = request['outcome']
        if action == 'register':
            assert outcome['kind'] == 'success', outcome
            grant = outcome['value']['grant']
            respond(original, {})
        elif action == 'forged_next':
            assert outcome['kind'] == 'refused', outcome
            record('forged_rejected')
            following(original, 'next')
        elif action == 'cross':
            assert outcome['kind'] == 'refused', outcome
            record('cross_rejected')
            respond(original, outcome)
            following(running, 'next')
        elif action == 'next':
            assert outcome['kind'] == 'success', outcome
            next_value = outcome['value']
            following(original, 'duplicate_next')
        elif action == 'duplicate_next':
            assert outcome['kind'] == 'refused', outcome
            assert outcome['error']['code'] == 'duplicate_next', outcome
            record('duplicate_rejected')
            respond(original, next_value)
        elif action == 'detached':
            record('downstream_done')
        elif action == 'effect':
            assert outcome['kind'] == 'success', outcome
            record('registered')
            behavior = mode.removeprefix('terminal_')
            if behavior == 'crash':
                os._exit(71)
            if behavior == 'malformed':
                respond(original, {}, raw=True)
            elif behavior == 'business_error':
                respond(original, error={'code': 'closed', 'message': 'business failure'})
            elif behavior == 'finite':
                respond(original, 9)
            elif behavior == 'next':
                following(original, 'forged_next', forged=True)
            elif behavior == 'early':
                following(original, 'detached')
                # The test terminal writes this only after native next admission.
                deadline = time.monotonic() + 3
                while 'terminal_entered' not in open(trace, encoding='utf-8').read():
                    assert time.monotonic() < deadline
                    time.sleep(0.005)
                respond(original, 9)
            # hang and probe intentionally keep the callback pending.
        else:
            respond(original, outcome)
        continue
    command = request['command']
    payload = request['payload'] or {}
    if command == 'hello':
        respond(request, {
            'middleware': payload['middleware'], 'contract': payload['contract'], 'interface': payload['interface'],
            'dependency_interfaces': payload['dependency_interfaces'],
             'http': None,
            'capabilities': {'middleware': True, 'effects': True},
        })
    elif command == 'activate':
        respond(request, {})
    elif command == 'call':
        method = payload['method']
        if method in ('open', 'duplicate', 'stale', 'unbound'):
            reverse(request, 'middleware_register', {
                'handler': 'm2' if method == 'stale' else 'm1',
                'middleware': 'unbound' if method == 'unbound' else 'request',
                'context': old_context if method == 'stale' else payload['context'],
            }, 'register' if method == 'open' else 'result')
            if mode == 'register_crash':
                os._exit(73)
        elif method == 'probe':
            following(request, 'cross', context=payload['context'])
        elif method == 'late_next':
            following(request, 'result')
        elif method == 'run':
            reverse(request, 'middleware_run', {'context': payload['context'], 'middleware': 'request', 'run': 'q1', 'args': 1})
        elif method == 'forged_cleanup':
            reverse(request, 'cleanup_call', {'context': payload['context'], 'dependency': 'undeclared', 'method': 'write', 'args': None})
        else:
            reverse(request, 'middleware_dispose', {'handler': 'm1', 'grant': '0' * 32 if method == 'forged' else grant})
    elif command in ('middleware_call', 'middleware_terminal'):
        record('started')
        running = request
        old_context = payload['context']
        if mode == 'reject':
            respond(request, {'code': 'overloaded', 'message': 'not admitted'}, True)
        else:
            reverse(request, 'effect_register', {'effect': 'e1', 'context': payload['context'], 'label': 'middleware effect'}, 'effect')
    elif command == 'middleware_release':
        record('released')
        respond(request, {})
    elif command == 'effect_release':
        record('cleanup')
        respond(request, {})
    elif command == 'deactivate':
        record('deactivated')
        respond(request, {})
        break
