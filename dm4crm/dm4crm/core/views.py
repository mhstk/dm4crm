import json
from django.http import HttpResponse, JsonResponse
# Create your views here.
from django.views.decorators.csrf import csrf_exempt
from .models.workspace import Workspace


def get_workspace():
    ws = Workspace.get_workspace()
    ws.engine_type = 'pandas'
    ws.new_engine()
    return ws


@csrf_exempt
def reset_workspace(request):
    if request.method == 'GET':
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
        return HttpResponse("Node name is not valid")
    return HttpResponse(str(list(ws.get_available_nodes()[node_name].__slots__)))


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
        ws = get_workspace()
        node_id = int(node_id)
        ws.compile(node_id)
        rows = ws.show()
        return JsonResponse(rows)


@csrf_exempt
def get_curr_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(ws.get_nodes()))


@csrf_exempt
def get_available_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(list(ws.get_available_nodes().keys())))


@csrf_exempt
def get_connected_nodes(request):
    ws = get_workspace()
    if request.method == 'GET':
        return HttpResponse(str(ws.get_connected_nodes()))
