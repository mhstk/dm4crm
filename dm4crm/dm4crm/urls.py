"""dm4crm URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from .core import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('createNode/<node_name>/', views.create_node),
    path('<node_name>/info', views.node_name_info),
    path('currNodes/', views.get_curr_nodes),
    path('availableNodes/', views.get_available_nodes),
    path('connectedNodes/', views.get_connected_nodes),
    path('defualtConnectNode/', views.default_connect_node),
    path('<node_id>/schema/', views.get_schema),
    path('<node_id>/show/', views.show),
    path('<node_id>/edit/', views.edit_node),
    path('<node_id>/remove/', views.remove),
]
