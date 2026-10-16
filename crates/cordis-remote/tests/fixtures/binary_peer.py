"""Protobuf-only fault peer I/O using the standard generated transport classes.

The Python dictionaries are a test authoring interface. Business values are
encoded with the advertised protobuf descriptor, never embedded in control.
"""
import base64
import struct
import sys
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, json_format, struct_pb2

import transport_pb2
from contracts import COMPOSER

incoming, outgoing = {}, {}
interface = None
dependencies = {}
pools = {}

def exact(size):
    data = bytearray()
    while len(data) < size:
        part = sys.stdin.buffer.read(size - len(data))
        if not part:
            raise EOFError()
        data.extend(part)
    return bytes(data)

def message(shape=None):
    if shape is None:
        return struct_pb2.Value()
    encoded = shape['descriptor']
    if encoded not in pools:
        pool = descriptor_pool.DescriptorPool()
        # Contract descriptor sets omit the standard definitions.
        from google.protobuf import empty_pb2, wrappers_pb2
        for file in (descriptor_pb2.DESCRIPTOR, struct_pb2.DESCRIPTOR, empty_pb2.DESCRIPTOR, wrappers_pb2.DESCRIPTOR):
            pool.AddSerializedFile(file.serialized_pb)
        files = descriptor_pb2.FileDescriptorSet.FromString(base64.b64decode(encoded))
        for file in files.file:
            pool.AddSerializedFile(file.SerializeToString())
        pools[encoded] = pool
    return message_factory.GetMessageClass(pools[encoded].FindMessageTypeByName(shape['message']))()

def body(value, shape=None):
    msg = message(shape)
    json_format.ParseDict(value, msg)
    return msg.SerializeToString()

def unbody(value, shape=None):
    msg = message(shape)
    msg.ParseFromString(value)
    return json_format.MessageToDict(msg)

def method(table, name):
    return table['value'] if table['mode'] == 'dynamic' else table['value'][name]

def signature(request):
    command, p = request['command'], request['payload'] or {}
    if command == 'call':
        return method(interface['methods'], p['method'])
    if command in ('dependency_call', 'cleanup_call'):
        target = dependencies.get(p['dependency'])
        if target:
            return method(target['methods'], p['method'])
    return {'args': None, 'result': None}

def business_field(command):
    if command in ('call','resource_acquire','resource_call','dependency_call','cleanup_call','dependency_resource_acquire','dependency_resource_call','cleanup_resource_call','event_call','event_emit','middleware_call','middleware_terminal','middleware_run','middleware_next'):
        return 'args'
    return {'child_install':'config','value_provide':'value'}.get(command)

def frame_base(value):
    frame = transport_pb2.Frame(session=value['session'], operation=value['operation'])
    frame.activation.CopyFrom(transport_pb2.Identity(**value['activation']))
    return frame

def read_frame():
    size = struct.unpack('>I', exact(4))[0]
    assert 0 < size <= 1048576
    return transport_pb2.Frame.FromString(exact(size))

def receive():
    global interface, dependencies
    frame = read_frame()
    value = {'session':frame.session, 'activation':json_format.MessageToDict(frame.activation), 'operation':frame.operation}
    if frame.WhichOneof('direction') == 'request':
        p = frame.request.payload
        control = json_format.MessageToDict(p.control)
        value.update(command=frame.request.command, payload=control)
        if value['command'] == 'hello':
            interface, dependencies = control['interface'], control['dependency_interfaces']
        field = business_field(value['command'])
        if field:
            assert len(p.messages) == 1
            value['payload'] = dict(control, **{field:unbody(p.messages[0], signature(value)['args'])})
        incoming[value['operation']] = value
    else:
        kind = frame.response.WhichOneof('outcome')
        if kind == 'success':
            p = frame.response.success
            control = json_format.MessageToDict(p.control)
            if p.messages:
                assert len(p.messages) == 1
                control = unbody(p.messages[0], signature(outgoing[value['operation']])['result'])
            value['outcome'] = {'kind':'success','value':control}
        else:
            value['outcome'] = {'kind':'failure' if kind=='rejected' else kind,'error':json_format.MessageToDict(getattr(frame.response,kind))}
    return value

def encode(value):
    frame = frame_base(value)
    if 'command' in value:
        outgoing[value['operation']] = value
        frame.request.command = value['command']
        control = value['payload']
        field = business_field(value['command'])
        if field:
            control = dict(control)
            data = control.pop(field)
            # Composition probes deliberately use the same generated lifecycle schema as their host bindings.
            if value['command'] in ('child_install','value_provide'):
                import json
                contract = json.loads(COMPOSER)
                controls = contract['controls']
                shape = controls.get(control['factory'], {}).get('config') if field=='config' else controls.get(control['service'], {}).get('value')
            else:
                shape = signature(value)['args']
            # Null typed config is an intentional malformed-input probe.
            frame.request.payload.messages.append(b'\x80' if value['command'] in ('child_install','value_provide') and not isinstance(data,dict) and shape else body(data, shape))
        json_format.ParseDict(control,frame.request.payload.control)
    else:
        outcome = value['outcome']
        kind = outcome['kind']
        if kind != 'success':
            json_format.ParseDict(outcome['error'],getattr(frame.response,'rejected' if kind=='failure' else kind))
        else:
            request = incoming[value['operation']]
            command = request['command']
            control = outcome['value']
            p = frame.response.success
            if not value.get('_raw'):
                if command in ('call','resource_call','middleware_call','middleware_terminal'):
                    p.messages.append(body(control,signature(request)['result']))
                    control = None
                elif command == 'event_call' and control.get('kind') == 'break':
                    control = dict(control)
                    p.messages.append(body(control.pop('value')))
            json_format.ParseDict(control,p.control)
    return frame.SerializeToString()

def send(value):
    data = encode(value)
    sys.stdout.buffer.write(struct.pack('>I',len(data))+data)
    sys.stdout.buffer.flush()

def respond(request, value=None, failure=False, error=None, raw=False):
    result = {k:request[k] for k in ('session','activation','operation')}
    result['_raw'] = raw
    result['outcome'] = {'kind':'refused','error':value} if failure else ({'kind':'failure','error':error} if error else {'kind':'success','value':value})
    send(result)
