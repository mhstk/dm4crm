import json
import time
import traceback
from typing import Dict

from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
# Create your views here.
from django.views.decorators.csrf import csrf_exempt

from .models.workspace import Workspace, GeneralNode, InitialNode, NonInitialNode


def get_workspace():
    ws = Workspace.get_workspace()
    ws.engine_type = 'pandas'
    ws.new_engine()
    return ws


def my_processor(sleep_interval):
    lines = [
        'Little brown lady',
        'Jumped into the blue water',
        'And smiled'
    ]
    start_time = time.time()
    while True:
        for line in lines:
            elapsed_time = int(time.time() - start_time)
            print(f"[{elapsed_time:>10} s] {line}\n")
            yield f"[{elapsed_time:>10} s] {line}\n"
            time.sleep(sleep_interval)
        yield "=========== Here we go again ===========\n"


def streamed(request):
    print("Here")
    sleep_interval = int(request.GET.get('sleep', 1))
    response = StreamingHttpResponse(my_processor(sleep_interval), content_type='text')
    return response


@csrf_exempt
def reset_workspace(request):
    if request.method == 'POST':
        ws = get_workspace()
        ws.reset_workspace()
        return HttpResponse("Success")
    return HttpResponse("BAD")


@csrf_exempt
def create_node(request, node_name):
    if request.method != 'POST':
        return HttpResponse("BAD")

    ws = get_workspace()
    if request.body:
        data = json.loads(request.body)
    else:
        data = {}
    if node_name not in ws.available_nodes:
        return HttpResponse("BAD: node_name not available")

    data = {key: data[key] for key in data.keys() if key in ws.available_nodes[node_name].__slots__}
    node_id = ws.create_node(node_name, **data)
    res = {"node_id": node_id}
    return HttpResponse(json.dumps(res))


@csrf_exempt
def node_name_info(request, node_name):
    ws = get_workspace()
    if node_name not in ws.get_available_nodes():
        return JsonResponse({"status": "fail", "message": "Node name is not valid"})
    temp: GeneralNode = ws.get_available_nodes()[node_name]()
    ports: Dict = {}
    if isinstance(temp, InitialNode):
        ports = {
            "in_ports": 0,
            "out_ports": len(list(temp.get_out_ports().keys()))}
    elif isinstance(temp, NonInitialNode):
        ports = {
            "in_ports": len(list(temp.get_in_ports().keys())),
            "out_ports": len(list(temp.get_out_ports().keys()))
        }
    info = {
        **ports,
        "params": str(list(ws.get_available_nodes()[node_name].__slots__))
    }
    return JsonResponse({"status": "succ", "data": info})


@csrf_exempt
def default_connect_node(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        origin_node_id = data["origin_node_id"]
        dest_node_id = data["dest_node_id"]
        origin_port = 0
        dest_port = 0
        if 'origin_port' in data:
            origin_port = data["origin_port"]
        if 'dest_port' in data:
            dest_port = data["dest_port"]
        ws = get_workspace()
        ws.connect_nodes(origin_node_id, dest_node_id, origin_port, dest_port)
        return HttpResponse("Success")


@csrf_exempt
def edit_node(request, node_id):
    if request.method == 'POST':
        data = json.loads(request.body)
        ws = get_workspace()
        node_id = int(node_id)
        data = {key: data[key] for key in data.keys() if key in ws.get_nodes()[node_id].__slots__}
        ws.get_nodes()[node_id].set_attribute(**data)
        return HttpResponse("Success")


@csrf_exempt
def remove(request, node_id):
    if request.method == 'DELETE':
        ws = get_workspace()
        node_id = int(node_id)
        res = ws.remove_node(node_id)
        if res:
            return HttpResponse("Success")
        else:
            return HttpResponse("Failure")
    return HttpResponse("BAD")


@csrf_exempt
def get_schema(request, node_id):
    if request.method == 'GET':
        ws = get_workspace()
        node_id = int(node_id)
        ws.compile(node_id)
        schema = ws.get_schema()
        return HttpResponse(str(schema))


@csrf_exempt
def show(request, node_id):
    if request.method == 'GET':
        try:
            ws = get_workspace()
            node_id = int(node_id)
            ws.compile(node_id)
            rows = ws.show()
            return JsonResponse(rows)
        except Exception as e:
            # return JsonResponse({"error": traceback.format_exc()})
            return JsonResponse({"state": -1, "output": "", "error": str(e)})


@csrf_exempt
def get_curr_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(ws.get_nodes()))


@csrf_exempt
def get_available_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return JsonResponse(ws.get_available_nodes_categorized())


@csrf_exempt
def get_connected_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(ws.get_connected_nodes()))
