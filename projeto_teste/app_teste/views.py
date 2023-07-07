from django.shortcuts import render
import csv

# Create your views

def home(request):
    return render(request,'testes/home.html')

def conexoes(request):
    csv_fp = open('conexoes.csv', 'r')
    reader = csv.DictReader(csv_fp,delimiter=';')
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    return render(request,'testes/conexao.html',{'data' : out})

def conf_conexao(request):
    csv_fp = open('conexoes.csv', 'r')
    reader = csv.DictReader(csv_fp,delimiter=';')
    out = [row for row in reader]
    reg_v = {'no_dados': '', 'host': '', 'database': '', 'port': '', 'user': '', 'password': '', 'st_conexao': ''}
    alt_v = "Teste"
    for reg in out:
        if  request.POST.get(reg.get('no_dados')) != None:
            reg_v = reg
    return render(request,'testes/config_con.html',{'data' : reg_v, 'st_altera' : alt_v})
