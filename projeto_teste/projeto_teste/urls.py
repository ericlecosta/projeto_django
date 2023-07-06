
from django.contrib import admin
from django.urls import path
from app_teste import views

urlpatterns = [
    # rota, view responsavel, nome de referencia
    path('', views.home,name='home'),
    path('testes/', views.conexoes,name='lista_conexao')
]
